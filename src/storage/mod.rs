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
mod cached;
mod disk;
mod encrypt;
#[cfg(feature = "faulty")]
mod faulty;
mod retrying;
mod s3;
mod verify;

pub use cached::{Backend as Cached, Error as CacheError};
pub use disk::Backend as Disk;
pub use encrypt::Backend as Encrypted;
#[cfg(feature = "faulty")]
pub use faulty::Backend as Faulty;
pub use retrying::Backend as Retrying;
pub use s3::{Backend as S3, Error as S3Error};
pub use verify::Backend as Verify;

use std::fmt;

use bytes::Bytes;
use std::io;

use crate::lfs::Oid;
use futures::{future::Either, sync::mpsc, Future, Sink, Stream};

pub type S3DiskCache = Cached<Disk, S3>;

/// Future returned by storage operations.
pub type StorageFuture<T, E> = Box<dyn Future<Item = T, Error = E> + Send>;

/// Stream returned by storage operations.
pub type StorageStream<T, E> = Box<dyn Stream<Item = T, Error = E> + Send>;

/// The byte stream of an LFS object.
pub type ByteStream = Box<dyn Stream<Item = Bytes, Error = io::Error> + Send>;

/// A namespace is used to categorize stored LFS objects. The storage
/// implementation is free to ignore this. However, it can be useful when
/// pruning old LFS objects from permanent storage.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Namespace {
    org: String,
    project: String,
}

impl Namespace {
    pub fn new(org: String, project: String) -> Self {
        // TODO: Ensure that the strings do not contain any illegal paths.
        Namespace { org, project }
    }

    #[allow(unused)]
    pub fn split(self) -> (String, String) {
        (self.org, self.project)
    }

    pub fn org(&self) -> &str {
        &self.org
    }

    pub fn project(&self) -> &str {
        &self.project
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.org(), self.project())
    }
}

/// A key into storage.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct StorageKey {
    namespace: Namespace,
    oid: Oid,
}

impl StorageKey {
    pub fn new(namespace: Namespace, oid: Oid) -> Self {
        StorageKey { oid, namespace }
    }

    pub fn into_parts(self) -> (Namespace, Oid) {
        (self.namespace, self.oid)
    }

    pub fn oid(&self) -> &Oid {
        &self.oid
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }
}

impl fmt::Display for StorageKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.oid)
    }
}

/// An LFS object to be uploaded or downloaded.
pub struct LFSObject {
    /// Size of the object.
    len: u64,

    /// The stream of bytes.
    stream: ByteStream,
}

impl LFSObject {
    pub fn new(len: u64, stream: ByteStream) -> Self {
        LFSObject { len, stream }
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn stream(self) -> ByteStream {
        self.stream
    }

    pub fn into_parts(self) -> (u64, ByteStream) {
        (self.len, self.stream)
    }

    /// Duplicates the underlying byte stream such that we have two identical
    /// LFS object streams that must be consumed in lock-step.
    ///
    /// This is useful for caching LFS objects while simultaneously sending them
    /// to a client.
    pub fn fanout(
        self,
    ) -> (impl Future<Item = (), Error = io::Error>, Self, Self) {
        let (len, stream) = self.into_parts();

        let (sender_a, receiver_a) = mpsc::channel(0);
        let (sender_b, receiver_b) = mpsc::channel(0);

        let sink = sender_a
            .fanout(sender_b)
            .sink_map_err(|e| io::Error::new(io::ErrorKind::Other, e));

        let receiver_a = receiver_a.map_err(|()| {
            io::Error::new(io::ErrorKind::Other, "failed receiving byte stream")
        });
        let receiver_b = receiver_b.map_err(|()| {
            io::Error::new(io::ErrorKind::Other, "failed receiving byte stream")
        });

        let f = stream.forward(sink).map(|_| ());
        let a = LFSObject::new(len, Box::new(receiver_a));
        let b = LFSObject::new(len, Box::new(receiver_b));

        (f, a, b)
    }
}

/// Trait for abstracting away the storage medium.
pub trait Storage {
    type Error: fmt::Display + Send;

    /// Gets an entry from the storage medium.
    fn get(
        &self,
        key: &StorageKey,
    ) -> StorageFuture<Option<LFSObject>, Self::Error>;

    /// Sets an entry in the storage medium.
    fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> StorageFuture<(), Self::Error>;

    /// Gets the size of the object. Returns `None` if the object does not
    /// exist.
    fn size(&self, key: &StorageKey)
        -> StorageFuture<Option<u64>, Self::Error>;

    /// Deletes an object.
    ///
    /// Permanent storage backends may choose to never delete objects, always
    /// returning success.
    fn delete(&self, key: &StorageKey) -> StorageFuture<(), Self::Error>;

    /// Returns a stream of all the object IDs in the storage medium.
    fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error>;

    /// Gets the total size of the storage, if known.
    fn total_size(&self) -> Option<u64> {
        None
    }

    /// Gets the maximum size of the storage.
    ///
    /// This should return `None` if the storage size is unbounded. This is only
    /// applicable to caches.
    fn max_size(&self) -> Option<u64> {
        None
    }
}

impl<S> Storage for Box<S>
where
    S: Storage + ?Sized,
{
    type Error = S::Error;

    fn get(
        &self,
        key: &StorageKey,
    ) -> StorageFuture<Option<LFSObject>, Self::Error> {
        (**self).get(key)
    }

    fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> StorageFuture<(), Self::Error> {
        (**self).put(key, value)
    }

    fn size(
        &self,
        key: &StorageKey,
    ) -> StorageFuture<Option<u64>, Self::Error> {
        (**self).size(key)
    }

    fn delete(&self, key: &StorageKey) -> StorageFuture<(), Self::Error> {
        (**self).delete(key)
    }

    fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error> {
        (**self).list()
    }

    fn total_size(&self) -> Option<u64> {
        (**self).total_size()
    }

    fn max_size(&self) -> Option<u64> {
        (**self).max_size()
    }
}

impl<A, B> Storage for Either<A, B>
where
    A: Storage + Send + Sync + 'static,
    B: Storage<Error = A::Error> + Send + Sync + 'static,
{
    type Error = A::Error;

    fn get(
        &self,
        key: &StorageKey,
    ) -> StorageFuture<Option<LFSObject>, Self::Error> {
        match self {
            Either::A(x) => x.get(key),
            Either::B(x) => x.get(key),
        }
    }

    fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> StorageFuture<(), Self::Error> {
        match self {
            Either::A(x) => x.put(key, value),
            Either::B(x) => x.put(key, value),
        }
    }

    fn size(
        &self,
        key: &StorageKey,
    ) -> StorageFuture<Option<u64>, Self::Error> {
        match self {
            Either::A(x) => x.size(key),
            Either::B(x) => x.size(key),
        }
    }

    fn delete(&self, key: &StorageKey) -> StorageFuture<(), Self::Error> {
        match self {
            Either::A(x) => x.delete(key),
            Either::B(x) => x.delete(key),
        }
    }

    fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error> {
        match self {
            Either::A(x) => x.list(),
            Either::B(x) => x.list(),
        }
    }

    fn total_size(&self) -> Option<u64> {
        match self {
            Either::A(x) => x.total_size(),
            Either::B(x) => x.total_size(),
        }
    }

    fn max_size(&self) -> Option<u64> {
        match self {
            Either::A(x) => x.max_size(),
            Either::B(x) => x.max_size(),
        }
    }
}
