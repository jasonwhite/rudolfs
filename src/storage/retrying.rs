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

use futures_backoff::retry;

use super::{LFSObject, Storage, StorageFuture, StorageKey, StorageStream};

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

impl<S> Storage for Backend<S>
where
    S: Storage + Send + Sync + 'static,
    S::Error: 'static,
{
    type Error = S::Error;

    fn get(
        &self,
        key: &StorageKey,
    ) -> StorageFuture<Option<LFSObject>, Self::Error> {
        self.storage.get(key)
    }

    fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> StorageFuture<(), Self::Error> {
        self.storage.put(key, value)
    }

    fn size(
        &self,
        key: &StorageKey,
    ) -> StorageFuture<Option<u64>, Self::Error> {
        let key = key.clone();
        let storage = self.storage.clone();
        Box::new(retry(move || storage.size(&key)))
    }

    fn delete(&self, key: &StorageKey) -> StorageFuture<(), Self::Error> {
        let key = key.clone();
        let storage = self.storage.clone();
        Box::new(retry(move || storage.delete(&key)))
    }

    fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error> {
        self.storage.list()
    }

    fn total_size(&self) -> Option<u64> {
        self.storage.total_size()
    }

    fn max_size(&self) -> Option<u64> {
        self.storage.max_size()
    }
}
