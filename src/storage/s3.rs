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
use bytes::Bytes;
use derive_more::{Display, From};
use futures::{future, stream, Future, Stream};
use futures_backoff::retry;
use http::StatusCode;
use log;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    GetObjectError, GetObjectRequest, HeadBucketError, HeadBucketRequest,
    HeadObjectError, HeadObjectRequest, PutObjectError, PutObjectRequest,
    S3Client, StreamingBody, S3,
};

use super::{LFSObject, Storage, StorageFuture, StorageKey, StorageStream};

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
                    _ => {
                        InitError::Other("S3 returned an unknown error".into())
                    }
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
    pub fn new(
        bucket: String,
        mut prefix: String,
    ) -> impl Future<Item = Self, Error = Error> {
        // Ensure the prefix doesn't end with a '/'.
        while prefix.ends_with('/') {
            prefix.pop();
        }

        // `Region::default` will get try to get the region from the environment
        // and fallback to a default if it isn't found.
        let mut region = Region::default();
        if let Ok(endpoint) = std::env::var("AWS_S3_ENDPOINT") {
            region = Region::Custom {
                name: region.name().to_owned(),
                endpoint,
            }
        }

        Backend::with_client(S3Client::new(region), bucket, prefix)
    }
}

impl<C> Backend<C> {
    pub fn with_client(
        client: C,
        bucket: String,
        prefix: String,
    ) -> impl Future<Item = Self, Error = Error>
    where
        C: S3 + Clone,
    {
        // Perform a HEAD operation to check that the bucket exists and that
        // our credentials work. This helps catch very common
        // errors early on application startup.
        let req = HeadBucketRequest {
            bucket: bucket.clone(),
        };

        let c = client.clone();

        retry(move || {
            c.head_bucket(req.clone()).map_err(|e| {
                log::error!("Failed to query S3 bucket ('{}'). Retrying...", e);
                e
            })
        })
        .map_err(InitError::from)
        .from_err()
        .map(move |()| Backend {
            client,
            bucket,
            prefix,
        })
    }

    fn key_to_path(&self, key: &StorageKey) -> String {
        format!("{}/{}/{}", self.prefix, key.namespace(), key.oid().path())
    }
}

impl<C> Storage for Backend<C>
where
    C: S3,
{
    type Error = Error;

    fn get(
        &self,
        key: &StorageKey,
    ) -> StorageFuture<Option<LFSObject>, Self::Error> {
        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            key: self.key_to_path(key),
            response_content_type: Some("application/octet-stream".into()),
            ..Default::default()
        };

        Box::new(
            self.client
                .get_object(request)
                .then(move |result| match result {
                    Ok(object) => Ok(Some(LFSObject::new(
                        object.content_length.unwrap() as u64,
                        Box::new(object.body.unwrap().map(Bytes::from)),
                    ))),
                    Err(err) => {
                        if let RusotoError::Service(
                            GetObjectError::NoSuchKey(_),
                        ) = &err
                        {
                            Ok(None)
                        } else {
                            Err(err)
                        }
                    }
                })
                .from_err(),
        )
    }

    fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> StorageFuture<(), Self::Error> {
        let (len, stream) = value.into_parts();

        let request = PutObjectRequest {
            bucket: self.bucket.clone(),
            key: self.key_to_path(&key),
            content_length: Some(len as i64),
            body: Some(StreamingBody::new(stream)),
            ..Default::default()
        };

        Box::new(self.client.put_object(request).map(|_| ()).from_err())
    }

    fn size(
        &self,
        key: &StorageKey,
    ) -> StorageFuture<Option<u64>, Self::Error> {
        let request = HeadObjectRequest {
            bucket: self.bucket.clone(),
            key: self.key_to_path(key),
            ..Default::default()
        };

        Box::new(
            self.client
                .head_object(request)
                .then(|result| match result {
                    Ok(object) => {
                        Ok(Some(object.content_length.unwrap() as u64))
                    }
                    Err(err) => {
                        match &err {
                            RusotoError::Unknown(e) => {
                                // There is a bug in Rusoto that causes it to
                                // always return an "unknown" error when the key
                                // does not exist. Thus we must check the error
                                // code manually. See:
                                // https://github.com/rusoto/rusoto/issues/716
                                if e.status == 404 {
                                    Ok(None)
                                } else {
                                    Err(err)
                                }
                            }
                            RusotoError::Service(
                                HeadObjectError::NoSuchKey(_),
                            ) => Ok(None),
                            _ => Err(err),
                        }
                    }
                })
                .from_err(),
        )
    }

    /// This never deletes objects from S3 and always returns success. This may
    /// be changed in the future.
    fn delete(&self, _key: &StorageKey) -> StorageFuture<(), Self::Error> {
        Box::new(future::ok(()))
    }

    /// Always returns an empty stream. This may be changed in the future.
    fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error> {
        Box::new(stream::empty())
    }
}
