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
use std::io;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use derive_more::{Display, From};
use futures::stream::TryStreamExt;

use crate::sha256::{Sha256VerifyError, VerifyStream};

use super::{LFSObject, Storage, StorageKey, StorageStream};

#[derive(Debug, Display, From)]
enum Error {
    Verify(Sha256VerifyError),
    Io(io::Error),
}

impl std::error::Error for Error {}

/// Verifies LFS objects as they are uploaded or downloaded.
///
/// If corruption is detected upon upload, the object is rejected.
///
/// If corruption is detected upon download, the object is deleted from the
/// underlying storage backend.
///
/// Note that this must be composed with decrypted data. Otherwise, the
/// verification will always fail. Thus, this should be the last thing between
/// the client and the other storage backends.
pub struct Backend<S> {
    storage: Arc<S>,
}

impl<S> Backend<S> {
    pub fn new(storage: S) -> Self {
        Backend {
            storage: Arc::new(storage),
        }
    }
}

#[async_trait]
impl<S> Storage for Backend<S>
where
    S: Storage + Send + Sync + 'static,
    S::Error: 'static,
{
    type Error = S::Error;

    /// Verifies that the downloaded object has a valid SHA256. If it doesn't,
    /// then it means our storage is corrupted. Thus, we automatically delete it
    /// from storage if it is corrupted. If storage backends are composed
    /// correctly, then it should only delete cache storage (not permanent
    /// storage).
    async fn get(
        &self,
        key: &StorageKey,
    ) -> Result<Option<LFSObject>, Self::Error> {
        match self.storage.get(key).await? {
            Some(obj) => {
                let (len, stream) = obj.into_parts();

                let stream = VerifyStream::new(
                    stream.map_err(Error::from),
                    len,
                    *key.oid(),
                );

                let key = key.clone();
                let storage = self.storage.clone();

                let stream = stream.map_err(move |err| {
                    match err {
                        Error::Verify(err) => {
                            tracing::error!(
                                "Found corrupted object {} ({})",
                                key.oid(),
                                err
                            );

                            let storage = storage.clone();
                            let key = key.clone();

                            // Delete the corrupted object from storage.
                            tokio::spawn(
                                async move { storage.delete(&key).await },
                            );

                            io::Error::new(
                                io::ErrorKind::Other,
                                "found corrupted object",
                            )
                        }
                        Error::Io(err) => err,
                    }
                });

                Ok(Some(LFSObject::new(len, Box::pin(stream))))
            }
            None => Ok(None),
        }
    }

    async fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> Result<(), Self::Error> {
        let (len, stream) = value.into_parts();

        let stream =
            VerifyStream::new(stream.map_err(Error::from), len, *key.oid())
                .map_err(move |err| match err {
                    Error::Verify(err) => {
                        io::Error::new(io::ErrorKind::Other, err)
                    }
                    Error::Io(err) => io::Error::new(io::ErrorKind::Other, err),
                });

        self.storage
            .put(key, LFSObject::new(len, Box::pin(stream)))
            .await
    }

    async fn size(&self, key: &StorageKey) -> Result<Option<u64>, Self::Error> {
        self.storage.size(key).await
    }

    async fn delete(&self, key: &StorageKey) -> Result<(), Self::Error> {
        self.storage.delete(key).await
    }

    fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error> {
        self.storage.list()
    }

    async fn total_size(&self) -> Option<u64> {
        self.storage.total_size().await
    }

    async fn max_size(&self) -> Option<u64> {
        self.storage.max_size().await
    }

    fn public_url(&self, key: &StorageKey) -> Option<String> {
        self.storage.public_url(key)
    }

    async fn upload_url(
        &self,
        key: &StorageKey,
        expires_in: Duration,
    ) -> Option<String> {
        self.storage.upload_url(key, expires_in).await
    }
}
