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
use std::io;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::{
    future::{self, Either},
    stream,
    sync::oneshot,
    Future, Stream,
};
use tokio;

use crate::lru;

use super::{LFSObject, Storage, StorageFuture, StorageKey, StorageStream};

type Cache = lru::Cache<StorageKey>;

#[derive(Debug)]
pub enum Error<C, S> {
    /// An error that occurred in the cache.
    Cache(C),

    /// An error that occurred in the storage backend.
    Storage(S),

    /// An error that occurred in the stream.
    Stream(io::Error),
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
            Error::Stream(x) => fmt::Display::fmt(&x, f),
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

    pub fn from_stream(error: io::Error) -> Self {
        Error::Stream(error)
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
        if let Some((key, _)) = lru.pop() {
            to_delete.push(key);
        }
    }

    Either::B(stream::iter_ok(to_delete).fold(0, move |acc, key| {
        storage.delete(&key).map(move |()| acc + 1)
    }))
}

fn cache_and_prune<C>(
    cache: Arc<C>,
    key: StorageKey,
    obj: LFSObject,
    lru: Arc<Mutex<Cache>>,
    max_size: u64,
) -> impl Future<Item = (), Error = C::Error>
where
    C: Storage,
{
    let len = obj.len();

    let oid = *key.oid();

    cache
        .put(key.clone(), obj)
        .and_then(move |()| {
            // Add the object info to our LRU cache once the download from
            // permanent storage is complete.
            let mut lru = lru.lock().unwrap();
            lru.push(key, len);

            prune_cache(&mut lru, max_size, cache).map(move |count| {
                if count > 0 {
                    log::info!("Pruned {} entries from the cache", count);
                }
            })
        })
        .map_err(move |err| {
            log::error!("Error caching {} ({})", oid, err);
            err
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
    fn get(
        &self,
        key: &StorageKey,
    ) -> StorageFuture<Option<LFSObject>, Self::Error> {
        // TODO: Keep stats on cache hits and misses. We can then display those
        // stats on a web page or send them to another service such as
        // Prometheus.
        if let Ok(mut lru) = self.lru.lock() {
            if lru.get_refresh(key).is_some() {
                // Cache hit!
                let storage = self.storage.clone();
                let lru2 = self.lru.clone();

                let key = key.clone();

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
        let key = key.clone();

        Box::new(
            self.storage
                .get(&key)
                .map_err(Error::from_storage)
                .and_then(move |obj| match obj {
                    Some(obj) => {
                        // Cache the returned LFS object.
                        let (f, a, b) = obj.fanout();

                        // Cache the object in the background.  Whether or not
                        // this succeeds shouldn't prevent the client from
                        // getting the LFS object. For example, even if we run
                        // out of disk space, the server should still continue
                        // operating.
                        let cache = cache_and_prune(
                            cache,
                            key.clone(),
                            b,
                            lru,
                            max_size,
                        )
                        .map_err(Error::from_cache);

                        tokio::spawn(
                            f.map_err(Error::from_stream)
                                .join(cache)
                                .map(|((), ())| ())
                                .map_err(move |err: Self::Error| {
                                    log::error!(
                                        "Error caching {} ({})",
                                        key,
                                        err
                                    );
                                }),
                        );

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
        key: StorageKey,
        value: LFSObject,
    ) -> StorageFuture<(), Self::Error> {
        let lru = self.lru.clone();
        let max_size = self.max_size;
        let cache = self.cache.clone();

        let (f, a, b) = value.fanout();

        // Note: We can only cache an object if it is successfully uploaded to
        // the store. Thus, we do something clever with this one shot channel.
        //
        // When the permanent storage finishes receiving its LFS object, we send
        // a signal to be received by an empty chunk at the end of the stream
        // going to the cache. Then, the cache only receives its last (empty)
        // chunk when the LFS object has been successfully stored.
        let (signal_sender, signal_receiver) = oneshot::channel();

        let store = self
            .storage
            .put(key.clone(), a)
            .map(move |()| {
                // Send a signal to the cache so that it can complete its write.
                signal_sender.send(()).unwrap_or(())
            })
            .map_err(Error::from_storage);

        let (len, stream) = b.into_parts();

        // Add an empty chunk to the end of the stream whose only job is to
        // complete when it receives a signal that the upload completed to
        // permanent storage.
        let stream = stream.chain(
            signal_receiver
                .map(|()| Bytes::new())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                .into_stream(),
        );

        let cache = cache_and_prune(
            cache,
            key,
            LFSObject::new(len, Box::new(stream)),
            lru,
            max_size,
        )
        .map_err(Error::from_cache);

        Box::new(
            f.map_err(Error::from_stream)
                .join3(cache, store)
                .map(|(_, (), ())| ()),
        )
    }

    fn size(
        &self,
        key: &StorageKey,
    ) -> StorageFuture<Option<u64>, Self::Error> {
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
    fn delete(&self, key: &StorageKey) -> StorageFuture<(), Self::Error> {
        // Only ever delete items from the cache. This may be called when
        // a corrupted object is detected.
        Box::new(self.cache.delete(key).map_err(Error::from_cache))
    }

    /// Returns a stream of cached items.
    fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error> {
        // TODO: Use the LRU instead to get this list.
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
