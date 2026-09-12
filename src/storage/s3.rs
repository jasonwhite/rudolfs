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
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_bucket::HeadBucketError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use backoff::ExponentialBackoff;
use backoff::future::retry;
use bytes::{Bytes, BytesMut};
use futures::stream;
use futures::stream::{Stream, StreamExt};

use super::{LFSObject, Storage, StorageKey, StorageStream};

use anyhow::{Context as _, Error};

#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error("Invalid S3 bucket name")]
    Bucket,

    #[error("Invalid S3 credentials")]
    Credentials,

    #[error("{0}")]
    Other(String),
}

impl InitError {
    /// Converts the initialization error into a backoff error. Useful for not
    /// retrying certain errors.
    pub fn into_backoff(self) -> backoff::Error<InitError> {
        // Certain types of errors should never be retried.
        match self {
            InitError::Bucket | InitError::Credentials => {
                backoff::Error::Permanent(self)
            }
            _ => backoff::Error::Transient {
                err: self,
                retry_after: None, /* NOTE: None causes us to follow retry
                                    * policy here */
            },
        }
    }
}

impl From<SdkError<HeadBucketError>> for InitError {
    fn from(err: SdkError<HeadBucketError>) -> Self {
        match err {
            SdkError::ServiceError(service_err) => {
                let err = service_err.into_err();
                if err.is_not_found() {
                    InitError::Bucket
                } else {
                    InitError::Other(err.to_string())
                }
            }
            SdkError::ConstructionFailure(_) => InitError::Credentials,
            err => InitError::Other(err.to_string()),
        }
    }
}

/// Wrapper that converts an S3 `ByteStream` into the byte stream type expected
/// by the storage interface.
struct S3ByteStream(ByteStream);

impl Stream for S3ByteStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0)
            .poll_next(cx)
            .map_err(std::io::Error::other)
    }
}

/// Amazon S3 storage backend.
pub struct Backend {
    /// S3 client.
    client: Client,

    /// Name of the bucket to use.
    bucket: String,

    /// Prefix for objects.
    prefix: String,

    /// URL for the CDN. Example: https://lfscdn.myawesomegit.com
    cdn: Option<String>,
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

        let mut config_loader = aws_config::defaults(BehaviorVersion::latest());

        // Custom endpoints (e.g. MinIO) usually don't support virtual-host
        // style addressing, so path-style must be forced.
        let mut force_path_style = false;

        if let Ok(endpoint) = std::env::var("AWS_S3_ENDPOINT") {
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

            config_loader = config_loader
                .endpoint_url(endpoint)
                .region(Region::new(name));

            force_path_style = true;
        }

        // The default credential chain includes:
        // 1. Environment variables
        // 2. The AWS credentials file (~/.aws/credentials)
        // 3. IAM instance profiles
        // 4. Web identity tokens (e.g. Kubernetes IRSA)
        let sdk_config = config_loader.load().await;

        let region =
            sdk_config
                .region()
                .map(|r| r.to_string())
                .unwrap_or_else(|| {
                    tracing::warn!(
                        "No AWS region configured. Falling back to us-east-1."
                    );
                    "us-east-1".to_string()
                });

        tracing::info!(
            "Connecting to S3 bucket '{}' at region '{}'",
            bucket,
            region
        );

        let client = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(force_path_style)
            .build();

        let client = Client::from_conf(client);

        Backend::with_client(client, bucket, prefix, cdn).await
    }
}

impl Backend {
    pub async fn with_client(
        client: Client,
        bucket: String,
        prefix: String,
        cdn: Option<String>,
    ) -> Result<Self, Error> {
        // Perform a HEAD operation to check that the bucket exists and that
        // our credentials work. This helps catch very common errors early on
        // in application startup.
        //
        // We need to retry here so that any fake S3 services have a chance to
        // start up alongside Rudolfs.
        retry(ExponentialBackoff::default(), || async {
            // Note that we don't retry certain failures, like credential or
            // missing bucket errors. These are unlikely to be transient
            // errors.
            client
                .head_bucket()
                .bucket(&bucket)
                .send()
                .await
                .map_err(InitError::from)
                .map_err(InitError::into_backoff)
        })
        .await
        .with_context(|| {
            format!("HEAD operation failed on bucket '{bucket}'")
        })?;

        tracing::info!("Successfully authorized with AWS");

        Ok(Backend {
            client,
            bucket,
            prefix,
            cdn,
        })
    }

    fn key_to_path(&self, key: &StorageKey) -> String {
        if self.prefix.is_empty() {
            format!("{}/{}", key.namespace(), key.oid().path())
        } else {
            format!("{}/{}/{}", self.prefix, key.namespace(), key.oid().path())
        }
    }
}

#[async_trait]
impl Storage for Backend {
    type Error = Error;

    async fn get(
        &self,
        key: &StorageKey,
    ) -> Result<Option<LFSObject>, Self::Error> {
        let object = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key_to_path(key))
            .response_content_type("application/octet-stream")
            .send()
            .await;

        match object {
            Ok(object) => {
                let len = object.content_length().unwrap_or(0) as u64;
                let stream = S3ByteStream(object.body);
                Ok(Some(LFSObject::new(len, Box::pin(stream))))
            }
            Err(err) => {
                if let Some(GetObjectError::NoSuchKey(_)) =
                    err.as_service_error()
                {
                    return Ok(None);
                }

                Err(err.into())
            }
        }
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
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(self.key_to_path(&key))
                .send()
                .await?)
        })
        .await?;

        // Okay to unwrap. This would only be None if there is a bug in
        // either the SDK or S3 itself.
        let upload_id = mu_response.upload_id().unwrap().to_string();

        // 100 MB
        const CHUNK_SIZE: usize = 100 * 1024 * 1024;

        let mut buffer = BytesMut::with_capacity(CHUNK_SIZE);
        let mut part_number = 1;
        let mut completed_parts = Vec::new();
        let mut stream = stream;

        loop {
            // Accumulate bytes into the buffer until we have a full chunk or
            // the stream ends.
            let mut stream_done = false;

            while buffer.len() < CHUNK_SIZE {
                match stream.next().await {
                    Some(Ok(bytes)) => buffer.extend_from_slice(&bytes),
                    Some(Err(e)) => return Err(e.into()),
                    None => {
                        stream_done = true;
                        break;
                    }
                }
            }

            let chunk = buffer.split().freeze();

            let up_response = retry(ExponentialBackoff::default(), || async {
                let chunk = chunk.clone();
                let body = ByteStream::from(chunk);

                Ok(self
                    .client
                    .upload_part()
                    .bucket(&self.bucket)
                    .key(self.key_to_path(&key))
                    .part_number(part_number)
                    .upload_id(&upload_id)
                    .body(body)
                    .send()
                    .await?)
            })
            .await?;

            completed_parts.push(
                CompletedPart::builder()
                    .e_tag(up_response.e_tag().unwrap_or(""))
                    .part_number(part_number)
                    .build(),
            );

            if stream_done {
                // The stream has ended.
                break;
            } else {
                part_number += 1;
            }
        }

        // Complete the upload.
        retry(ExponentialBackoff::default(), || async {
            let multipart_upload = CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts.clone()))
                .build();

            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(self.key_to_path(&key))
                .multipart_upload(multipart_upload)
                .upload_id(&upload_id)
                .send()
                .await?;

            Ok(())
        })
        .await?;

        Ok(())
    }

    async fn size(&self, key: &StorageKey) -> Result<Option<u64>, Self::Error> {
        let object = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(self.key_to_path(key))
            .send()
            .await;

        match object {
            Ok(object) => Ok(Some(object.content_length().unwrap_or(0) as u64)),
            Err(err) => {
                if let Some(HeadObjectError::NotFound(_)) =
                    err.as_service_error()
                {
                    return Ok(None);
                }

                Err(err.into())
            }
        }
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

        let presigning_config =
            PresigningConfig::expires_in(expires_in).ok()?;

        let presigned_request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.key_to_path(key))
            .presigned(presigning_config)
            .await
            .ok()?;

        Some(presigned_request.uri().to_string())
    }
}
