// Copyright (c) 2019 Jason White
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use async_trait::async_trait;
use backoff::future::retry;
use backoff::ExponentialBackoff;
use bytes::{Bytes, BytesMut};
use derive_more::{Display, From};
use futures::{stream, stream::TryStreamExt};
use http::{HeaderMap, StatusCode};
use rusoto_core::request::BufferedHttpResponse;
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_credential::{
    AutoRefreshingProvider, DefaultCredentialsProvider, ProvideAwsCredentials,
};
use rusoto_s3::{
    CompleteMultipartUploadError, CompleteMultipartUploadRequest,
    CompletedMultipartUpload, CompletedPart, CreateMultipartUploadError,
    CreateMultipartUploadRequest, GetObjectError, GetObjectRequest,
    HeadBucketError, HeadBucketRequest, HeadObjectError, HeadObjectRequest,
    PutObjectError, PutObjectRequest, S3Client, StreamingBody, UploadPartError,
    UploadPartRequest, S3,
};
use rusoto_sts::WebIdentityProvider;
use tokio::io::AsyncReadExt;

use super::{LFSObject, Storage, StorageKey, StorageStream};
use rusoto_s3::util::{PreSignedRequest, PreSignedRequestOption};
use std::time::Duration;

type BoxedCredentialProvider =
    Box<dyn ProvideAwsCredentials + Send + Sync + 'static>;

#[derive(Debug, From, Display)]
pub enum Error {
    Get(RusotoError<GetObjectError>),
    Put(RusotoError<PutObjectError>),
    CreateMultipart(RusotoError<CreateMultipartUploadError>),
    Upload(RusotoError<UploadPartError>),
    CompleteMultipart(RusotoError<CompleteMultipartUploadError>),
    Head(RusotoError<HeadObjectError>),

    Stream(std::io::Error),

    /// Initialization error.
    Init(InitError),

    /// The uploaded object is too large.
    TooLarge(u64),

    Tls(rusoto_core::request::TlsError),

    Credentials(rusoto_credential::CredentialsError),
}

impl ::std::error::Error for Error {}

#[derive(Debug, Display)]
pub enum InitError {
    #[display(fmt = "Invalid S3 bucket name")]
    Bucket,

    #[display(fmt = "Invalid S3 credentials")]
    Credentials,

    #[display(fmt = "{}", _0)]
    Other(String),
}

impl InitError {
    /// Converts the initialization error into an backoff error. Useful for not
    /// retrying certain errors.
    pub fn into_backoff(self) -> backoff::Error<InitError> {
        // Certain types of errors should never be retried.
        match self {
            InitError::Bucket | InitError::Credentials => {
                backoff::Error::Permanent(self)
            }
            _ => backoff::Error::Transient(self),
        }
    }
}

impl From<RusotoError<HeadBucketError>> for InitError {
    fn from(err: RusotoError<HeadBucketError>) -> Self {
        match err {
            RusotoError::Credentials(_) => InitError::Credentials,
            RusotoError::Unknown(r) => {
                // Rusoto really sucks at correctly reporting errors.
                // Lets work around that here.
                match r.status {
                    StatusCode::NOT_FOUND => InitError::Bucket,
                    StatusCode::FORBIDDEN => InitError::Credentials,
                    _ => InitError::Other(format!(
                        "S3 returned HTTP status {}",
                        r.status
                    )),
                }
            }
            RusotoError::Service(HeadBucketError::NoSuchBucket(_)) => {
                InitError::Bucket
            }
            x => InitError::Other(x.to_string()),
        }
    }
}

/// Amazon S3 storage backend.
pub struct Backend<C = S3Client> {
    /// S3 client.
    client: C,

    // AWS Credentials. Used for signing URLs.
    credential_provider: BoxedCredentialProvider,

    /// Name of the bucket to use.
    bucket: String,

    /// Prefix for objects.
    prefix: String,

    /// URL for the CDN. Example: https://lfscdn.myawesomegit.com
    cdn: Option<String>,

    region: Region,
}

impl Backend {
    pub async fn new(
        bucket: String,
        mut prefix: String,
        cdn: Option<String>,
    ) -> Result<Self, Error> {
        // Ensure the prefix doesn't end with a '/'.
        while prefix.ends_with('/') {
            prefix.pop();
        }

        let region = if let Ok(endpoint) = std::env::var("AWS_S3_ENDPOINT") {
            // If a custom endpoint is set, do not use the AWS default
            // (us-east-1). Instead, check environment variables for a region
            // name.
            let name = std::env::var("AWS_DEFAULT_REGION")
                .or_else(|_| std::env::var("AWS_REGION"))
                .map_err(|_| {
                    InitError::Other(
                        "$AWS_S3_ENDPOINT was set without $AWS_DEFAULT_REGION \
                         or $AWS_REGION being set. Custom endpoints don't \
                         make sense without also setting a region."
                            .into(),
                    )
                })?;

            Region::Custom { name, endpoint }
        } else {
            Region::default()
        };

        log::info!(
            "Connecting to S3 bucket '{}' at region '{}'",
            bucket,
            region.name()
        );

        // Check if there is any k8s credential provider. If there is, use it.
        let k8s_provider = WebIdentityProvider::from_k8s_env();

        let (client, credential_provider): (_, BoxedCredentialProvider) =
            if k8s_provider.credentials().await.is_ok() {
                log::info!("Using credentials from Kubernetes");
                let provider = AutoRefreshingProvider::new(k8s_provider)?;
                let client = S3Client::new_with(
                    HttpClient::new()?,
                    provider.clone(),
                    region.clone(),
                );
                (client, Box::new(provider))
            } else {
                let client = S3Client::new(region.clone());
                let provider = DefaultCredentialsProvider::new()?;
                (client, Box::new(provider))
            };

        Backend::with_client(
            client,
            bucket,
            prefix,
            cdn,
            region,
            credential_provider,
        )
        .await
    }
}

impl<C> Backend<C> {
    pub async fn with_client(
        client: C,
        bucket: String,
        prefix: String,
        cdn: Option<String>,
        region: Region,
        credential_provider: BoxedCredentialProvider,
    ) -> Result<Self, Error>
    where
        C: S3 + Clone,
    {
        // Perform a HEAD operation to check that the bucket exists and that
        // our credentials work. This helps catch very common errors early on
        // in application startup.
        let req = HeadBucketRequest {
            bucket: bucket.clone(),
            ..Default::default()
        };

        let c = client.clone();

        // We need to retry here so that any fake S3 services have a chance to
        // start up alongside Rudolfs.
        retry(ExponentialBackoff::default(), || async {
            // Note that we don't retry certain failures, like credential or
            // missing bucket errors. These are unlikely to be transient
            // errors.
            c.head_bucket(req.clone())
                .await
                .map_err(InitError::from)
                .map_err(InitError::into_backoff)
        })
        .await?;

        log::info!("Successfully authorized with AWS");

        Ok(Backend {
            client,
            bucket,
            prefix,
            cdn,
            region,
            credential_provider,
        })
    }

    fn key_to_path(&self, key: &StorageKey) -> String {
        format!("{}/{}/{}", self.prefix, key.namespace(), key.oid().path())
    }
}

#[async_trait]
impl<C> Storage for Backend<C>
where
    C: S3 + Send + Sync,
{
    type Error = Error;

    async fn get(
        &self,
        key: &StorageKey,
    ) -> Result<Option<LFSObject>, Self::Error> {
        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            key: self.key_to_path(key),
            response_content_type: Some("application/octet-stream".into()),
            ..Default::default()
        };

        Ok(match self.client.get_object(request).await {
            Ok(object) => Ok(Some(LFSObject::new(
                object.content_length.unwrap() as u64,
                Box::pin(object.body.unwrap().map_ok(Bytes::from)),
            ))),
            Err(RusotoError::Service(GetObjectError::NoSuchKey(_))) => Ok(None),
            Err(err) => Err(err),
        }?)
    }

    async fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> Result<(), Self::Error> {
        let (_len, stream) = value.into_parts();

        let mu_response = retry(ExponentialBackoff::default(), || async {
            Ok(self
                .client
                .create_multipart_upload(CreateMultipartUploadRequest {
                    bucket: self.bucket.clone(),
                    key: self.key_to_path(&key),
                    ..Default::default()
                })
                .await?)
        })
        .await?;

        // Okay to unwrap. This would only be None  there is a bug in either
        // Rusoto or S3 itself.
        let upload_id = mu_response.upload_id.unwrap();

        // 100 MB
        const CHUNK_SIZE: usize = 100 * 1024 * 1024;

        let mut buffer = BytesMut::with_capacity(CHUNK_SIZE);
        let mut part_number = 1;
        let mut completed_parts = Vec::new();
        let mut streaming_body = StreamingBody::new(stream).into_async_read();

        loop {
            let size = streaming_body.read_buf(&mut buffer).await?;

            if buffer.len() < CHUNK_SIZE && size != 0 {
                continue;
            }

            let chunk = buffer.split().freeze();

            let up_response = retry(ExponentialBackoff::default(), || async {
                let chunk = chunk.clone();
                let chunk_len = chunk.len();
                let body =
                    StreamingBody::new(Box::pin(stream::once(async move {
                        Ok(chunk)
                    })));

                let req = UploadPartRequest {
                    content_length: Some(chunk_len as i64),
                    body: Some(body),
                    bucket: self.bucket.clone(),
                    key: self.key_to_path(&key),
                    part_number,
                    upload_id: upload_id.clone(),
                    ..Default::default()
                };
                Ok(self.client.upload_part(req).await?)
            })
            .await?;

            completed_parts.push(CompletedPart {
                e_tag: up_response.e_tag.clone(),
                part_number: Some(part_number),
            });

            if size == 0 {
                // The stream has ended.
                break;
            } else {
                part_number += 1;
            }
        }

        // Complete the upload.
        retry(ExponentialBackoff::default(), || async {
            let req = CompleteMultipartUploadRequest {
                bucket: self.bucket.clone(),
                key: self.key_to_path(&key),
                multipart_upload: Some(CompletedMultipartUpload {
                    parts: Some(completed_parts.clone()),
                }),
                upload_id: upload_id.clone(),
                ..Default::default()
            };

            let output = self.client.complete_multipart_upload(req).await?;

            // Workaround: https://github.com/rusoto/rusoto/issues/1936
            // Rusoto may return `Ok` when there is a failure.
            if output.location == None
                && output.e_tag == None
                && output.bucket == None
                && output.key == None
            {
                return Err(RusotoError::Unknown(BufferedHttpResponse {
                    status: StatusCode::from_u16(500).unwrap(),
                    headers: HeaderMap::with_capacity(0),
                    body: Bytes::from_static(b"HTTP 500 internal error"),
                })
                .into());
            }

            Ok(())
        })
        .await?;

        Ok(())
    }

    async fn size(&self, key: &StorageKey) -> Result<Option<u64>, Self::Error> {
        let request = HeadObjectRequest {
            bucket: self.bucket.clone(),
            key: self.key_to_path(key),
            ..Default::default()
        };

        Ok(match self.client.head_object(request).await {
            Ok(object) => Ok(Some(object.content_length.unwrap() as u64)),
            Err(RusotoError::Unknown(e)) if e.status == 404 => {
                // There is a bug in Rusoto that causes it to always return an
                // "unknown" error when the key does not exist. Thus we must
                // check the error code manually.
                //
                // See: https://github.com/rusoto/rusoto/issues/716
                Ok(None)
            }
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => {
                Ok(None)
            }
            Err(err) => Err(err),
        }?)
    }

    /// This never deletes objects from S3 and always returns success. This may
    /// be changed in the future.
    async fn delete(&self, _key: &StorageKey) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Always returns an empty stream. This may be changed in the future.
    fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error> {
        Box::pin(stream::empty())
    }

    fn public_url(&self, key: &StorageKey) -> Option<String> {
        self.cdn
            .as_ref()
            .map(|cdn| format!("{}/{}", cdn, self.key_to_path(key)))
    }

    async fn upload_url(
        &self,
        key: &StorageKey,
        expires_in: Duration,
    ) -> Option<String> {
        // Don't use a presigned URL if we're not using a CDN. Otherwise,
        // uploads will bypass the encryption process and fail to download.
        self.cdn.as_ref()?;

        let request = PutObjectRequest {
            bucket: self.bucket.clone(),
            key: self.key_to_path(key),
            ..Default::default()
        };
        let credentials = self.credential_provider.credentials().await.ok()?;
        let presigned_url = request.get_presigned_url(
            &self.region,
            &credentials,
            &PreSignedRequestOption { expires_in },
        );
        Some(presigned_url)
    }
}
