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
use core::marker::Unpin;

use std::hash::Hash;

use futures::stream::{TryStream, TryStreamExt};
use linked_hash_map::LinkedHashMap;

/// A least recently used (LRU) cache.
pub struct Cache<K> {
    // A linked hash map is used to implement an efficient LRU cache.
    map: LinkedHashMap<K, u64>,

    // Total size of the cache. This is equal to the sum of the values in the
    // map. We use this to determine if the cache has grown too large and must
    // be pruned.
    size: u64,
}

impl<K> Cache<K>
where
    K: Eq + Hash,
{
    /// Creates a new, empty cache.
    pub fn new() -> Self {
        Cache {
            map: LinkedHashMap::new(),
            size: 0,
        }
    }

    /// Returns the size (in bytes) of the cache.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Loads the cache from a stream of entries. Note that this throws away any
    /// LRU information. Since the server shouldn't be restarted very often,
    /// this shouldn't be a problem in practice. Frequently used entries will
    /// naturally bubble back up to the top.
    pub async fn from_stream<S>(mut stream: S) -> Result<Self, S::Error>
    where
        S: TryStream<Ok = (K, u64)> + Unpin,
    {
        let mut cache = Cache::new();

        while let Some((oid, len)) = stream.try_next().await? {
            cache.push(oid, len);
        }

        Ok(cache)
    }

    /// Removes the least recently used item. Returns `None` if the cache is
    /// empty. When the cache gets too large, this should be called in a loop in
    /// order to bring it below the threshold.
    pub fn pop(&mut self) -> Option<(K, u64)> {
        if let Some((k, v)) = self.map.pop_front() {
            self.size -= v;
            Some((k, v))
        } else {
            None
        }
    }

    /// Removes an entry from the cache. Returns the size of the object if it
    /// exists, or `None` if it didn't exist in the cache.
    pub fn remove(&mut self, key: &K) -> Option<u64> {
        self.map.remove(key).map(|size| {
            self.size -= size;
            size
        })
    }

    /// Gets an entry by key without perturbing the LRU ordering.
    pub fn get(&self, key: &K) -> Option<u64> {
        self.map.get(key).cloned()
    }

    /// Gets an entry by key. If the entry exists, it will be touched so that it
    /// becomes the most recently used item.
    pub fn get_refresh(&mut self, key: &K) -> Option<u64> {
        self.map.get_refresh(key).cloned()
    }

    /// Adds an entry to the cache. Returns the previous value value of the
    /// cache item if it already existed. Returns `None` if the entry did not
    /// previously exist. In either case, the entry is always touched so that it
    /// becomes the most recently used item.
    pub fn push(&mut self, key: K, value: u64) -> Option<u64> {
        self.size += value;

        if let Some(old_value) = self.map.insert(key, value) {
            self.size -= old_value;
            Some(old_value)
        } else {
            None
        }
    }
}
