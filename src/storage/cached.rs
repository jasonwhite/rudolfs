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
use std::fmt;
use std::sync::{Arc, Mutex};

use futures::{
    future::{self, Either},
    stream, Future, Stream,
};
use tokio;

use crate::lfs::Oid;
use crate::lru::Cache;

use super::{LFSObject, Storage, StorageFuture, StorageStream};

#[derive(Debug)]
pub enum Error<C, S> {
    /// An error that occurred in the cache.
    Cache(C),

    /// An error that occurred in the storage backend.
    Storage(S),
}

impl<C, S> fmt::Display for Error<C, S>
where
    C: fmt::Display,
    S: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Cache(x) => fmt::Display::fmt(&x, f),
            Error::Storage(x) => fmt::Display::fmt(&x, f),
        }
    }
}

impl<C, S> Error<C, S> {
    pub fn from_cache(error: C) -> Self {
        Error::Cache(error)
    }

    pub fn from_storage(error: S) -> Self {
        Error::Storage(error)
    }
}

impl<C, S> std::error::Error for Error<C, S>
where
    C: fmt::Debug + fmt::Display,
    S: fmt::Debug + fmt::Display,
{
}

/// Combines a cache with a permanent storage backend such that if a query to
/// the cache fails, it falls back to a permanent storage backend.
pub struct Backend<C, S> {
    lru: Arc<Mutex<Cache>>,
    max_size: u64,
    cache: Arc<C>,
    storage: Arc<S>,
}

impl<C, S> Backend<C, S>
where
    C: Storage,
    S: Storage,
{
    pub fn new(
        max_size: u64,
        cache: C,
        storage: S,
    ) -> impl Future<Item = Self, Error = C::Error> {
        Cache::from_stream(cache.list()).and_then(move |mut lru| {
            let cache = Arc::new(cache);

            // Prune the cache. The maximum size setting may have changed
            // between server invocations. Thus, prune it down right away
            // instead of waiting for a client to do an upload.
            prune_cache(&mut lru, max_size, cache.clone()).map(move |count| {
                if count > 0 {
                    log::info!("Pruned {} entries from the cache", count);
                }

                Backend {
                    lru: Arc::new(Mutex::new(lru)),
                    max_size,
                    cache,
                    storage: Arc::new(storage),
                }
            })
        })
    }
}

/// Returns a future that prunes the least recently used entries that cause the
/// storage to exceed the given maximum size.
///
/// If the stream is thrown away, then the items will be removed from the
/// in-memory LRU cache, but not physically deleted.
fn prune_cache<S>(
    lru: &mut Cache,
    max_size: u64,
    storage: Arc<S>,
) -> impl Future<Item = usize, Error = S::Error>
where
    S: Storage,
{
    if max_size == 0 {
        // The cache can have unlimited size.
        return Either::A(future::ok(0));
    }

    let mut to_delete = Vec::new();

    while lru.size() > max_size {
        if let Some((oid, _)) = lru.pop() {
            to_delete.push(oid);
        }
    }

    Either::B(stream::iter_ok(to_delete).fold(0, move |acc, oid| {
        storage.delete(&oid).map(move |()| acc + 1)
    }))
}

fn cache_and_prune<C>(
    cache: Arc<C>,
    key: Oid,
    obj: LFSObject,
    lru: Arc<Mutex<Cache>>,
    max_size: u64,
) -> impl Future<Item = (), Error = ()>
where
    C: Storage,
{
    let len = obj.len();

    cache
        .put(&key, obj)
        .and_then(move |()| {
            // Add the object info to our LRU cache once the download from
            // permanent storage is complete.
            let mut lru = lru.lock().unwrap();
            lru.push(key, len);

            // Prune the cache.
            prune_cache(&mut lru, max_size, cache).map(move |count| {
                if count > 0 {
                    log::info!("Pruned {} entries from the cache", count);
                }
            })
        })
        .map_err(move |err| {
            log::error!("Error caching {} ({})", key, err);
        })
}

impl<C, S> Storage for Backend<C, S>
where
    S: Storage + Send + Sync + 'static,
    S::Error: 'static,
    C: Storage + Send + Sync + 'static,
    C::Error: 'static,
{
    type Error = Error<C::Error, S::Error>;

    /// Tries to query the cache first. If that fails, falls back to the
    /// permanent storage backend.
    fn get(&self, key: &Oid) -> StorageFuture<Option<LFSObject>, Self::Error> {
        // TODO: Keep stats on cache hits and misses. We can then display those
        // stats on a web page or send them to another service such as
        // Prometheus.
        let key = *key;

        if let Ok(mut lru) = self.lru.lock() {
            if lru.get_refresh(&key).is_some() {
                // Cache hit!
                //
                // TODO: Verify the stream as we send it back. If the SHA256 is
                // incorrect, delete it and let the client try again.
                let storage = self.storage.clone();
                let lru2 = self.lru.clone();

                return Box::new(
                    self.cache.get(&key).map_err(Error::from_cache).and_then(
                        move |obj| match obj {
                            Some(obj) => Either::A(future::ok(Some(obj))),
                            None => {
                                // If the cache doesn't actually have it, delete
                                // the entry from our LRU. This can happen if
                                // the cache is cleared out manually.
                                let mut lru = lru2.lock().unwrap();
                                lru.remove(&key);

                                // Fall back to permanent storage. Note that
                                // this won't actually cache the object. This
                                // will be done next time the same object is
                                // requested.
                                Either::B(
                                    storage
                                        .get(&key)
                                        .map_err(Error::from_storage),
                                )
                            }
                        },
                    ),
                );
            }
        }

        // Cache miss. Get the object from permanent storage. If successful, we
        // to cache the resulting byte stream.
        let lru = self.lru.clone();
        let max_size = self.max_size;
        let cache = self.cache.clone();

        Box::new(
            self.storage
                .get(&key)
                .map_err(Error::from_storage)
                .and_then(move |obj| match obj {
                    Some(obj) => {
                        // Cache the returned LFS object.
                        let (a, b) = obj.split();

                        // Cache the object in the background.  Whether or not
                        // this succeeds shouldn't prevent the client from
                        // getting the LFS object. For example, even if we run
                        // out of disk space, the server should still continue
                        // operating.
                        tokio::spawn(cache_and_prune(
                            cache, key, b, lru, max_size,
                        ));

                        // Send the object from permanent-storage.
                        Either::A(future::ok(Some(a)))
                    }
                    None => {
                        // The permanent storage also doesn't have it.
                        //
                        // Note that we cannot cache the non-existence of an
                        // object because the storage backend might be shared by
                        // multiple caches.
                        Either::B(future::ok(None))
                    }
                }),
        )
    }

    fn put(
        &self,
        key: &Oid,
        value: LFSObject,
    ) -> StorageFuture<(), Self::Error> {
        let key = *key;
        let lru = self.lru.clone();
        let max_size = self.max_size;
        let cache = self.cache.clone();

        let (a, b) = value.split();

        // Cache the object in the background. Whether or not this succeeds
        // shouldn't prevent the client from uploading the LFS object to
        // permanent storage. For example, even if we run out of disk space, the
        // server should still continue operating.
        tokio::spawn(cache_and_prune(cache, key, b, lru, max_size));

        Box::new(self.storage.put(&key, a).map_err(Error::from_storage))
    }

    fn size(&self, key: &Oid) -> StorageFuture<Option<u64>, Self::Error> {
        // Get just the size of an object without perturbing the LRU ordering.
        // Only downloads or uploads need to perturb the LRU ordering.
        let lru = self.lru.lock().unwrap();
        if let Some(size) = lru.get(key) {
            // Cache hit!
            Box::new(future::ok(Some(size)))
        } else {
            // Cache miss. Check permanent storage.
            Box::new(self.storage.size(key).map_err(Error::from_storage))
        }
    }

    /// Deletes an item from the cache (not from permanent storage).
    fn delete(&self, key: &Oid) -> StorageFuture<(), Self::Error> {
        // Only ever delete items from the cache. This may be called when
        // a corrupted object is detected.
        Box::new(self.cache.delete(key).map_err(Error::from_cache))
    }

    /// Returns a stream of cached items.
    fn list(&self) -> StorageStream<(Oid, u64), Self::Error> {
        // TODO: Use the internal linked hash map instead to get this list.
        Box::new(self.cache.list().map_err(Error::from_cache))
    }

    /// Returns the total size of the LRU cache (not the total size of the
    /// permanent storage).
    fn total_size(&self) -> Option<u64> {
        let lru = self.lru.lock().unwrap();
        Some(lru.size())
    }

    /// Returns the maximum size of the LRU cache (not the maximum size of the
    /// permanent storage).
    fn max_size(&self) -> Option<u64> {
        if self.max_size == 0 {
            None
        } else {
            Some(self.max_size)
        }
    }
}
