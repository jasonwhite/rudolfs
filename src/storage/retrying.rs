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
use std::sync::Arc;

use async_trait::async_trait;
use backoff::{future::FutureOperation, ExponentialBackoff};

use super::{LFSObject, Storage, StorageKey, StorageStream};

/// Implements retries for certain operations.
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

    async fn get(
        &self,
        key: &StorageKey,
    ) -> Result<Option<LFSObject>, Self::Error> {
        // Due to their streaming nature, this can't be retried by the server.
        // The client should retry this if it fails.
        self.storage.get(key).await
    }

    async fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> Result<(), Self::Error> {
        // Due to their streaming nature, this can't be retried by the server.
        // The client should retry this if it fails.
        self.storage.put(key, value).await
    }

    async fn size(&self, key: &StorageKey) -> Result<Option<u64>, Self::Error> {
        (|| async { Ok(self.storage.size(&key).await?) })
            .retry(ExponentialBackoff::default())
            .await
    }

    async fn delete(&self, key: &StorageKey) -> Result<(), Self::Error> {
        (|| async { Ok(self.storage.delete(&key).await?) })
            .retry(ExponentialBackoff::default())
            .await
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

    async fn upload_url(&self, key: &StorageKey) -> Option<String> {
        self.storage.upload_url(key).await
    }
}
