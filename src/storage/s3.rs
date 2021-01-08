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
use backoff::{future::FutureOperation, ExponentialBackoff};
use bytes::Bytes;
use derive_more::{Display, From};
use futures::{stream, stream::TryStreamExt};
use http::StatusCode;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    GetObjectError, GetObjectRequest, HeadBucketError, HeadBucketRequest,
    HeadObjectError, HeadObjectRequest, PutObjectError, PutObjectRequest,
    S3Client, StreamingBody, S3,
};

use super::{LFSObject, Storage, StorageKey, StorageStream};

#[derive(Debug, From, Display)]
pub enum Error {
    Get(RusotoError<GetObjectError>),
    Put(RusotoError<PutObjectError>),
    Head(RusotoError<HeadObjectError>),

    /// Initialization error.
    Init(InitError),

    /// The uploaded object is too large.
    TooLarge(u64),
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

    /// Name of the bucket to use.
    bucket: String,

    /// Prefix for objects.
    prefix: String,
}

impl Backend {
    pub async fn new(
        bucket: String,
        mut prefix: String,
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

        Backend::with_client(S3Client::new(region), bucket, prefix).await
    }
}

impl<C> Backend<C> {
    pub async fn with_client(
        client: C,
        bucket: String,
        prefix: String,
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
        (|| {
            async {
                // Note that we don't retry certain failures, like credential or
                // missing bucket errors. These are unlikely to be transient
                // errors.
                c.head_bucket(req.clone())
                    .await
                    .map_err(InitError::from)
                    .map_err(InitError::into_backoff)
            }
        })
        .retry(ExponentialBackoff::default())
        .await?;

        log::info!("Successfully authorized with AWS");

        Ok(Backend {
            client,
            bucket,
            prefix,
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
        let (len, stream) = value.into_parts();

        let request = PutObjectRequest {
            bucket: self.bucket.clone(),
            key: self.key_to_path(&key),
            content_length: Some(len as i64),
            body: Some(StreamingBody::new(stream)),
            ..Default::default()
        };

        self.client.put_object(request).await?;

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
}
