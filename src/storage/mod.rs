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

use crate::lfs::Oid;

use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    channel::mpsc,
    sink::SinkExt,
    stream::{BoxStream, Stream, StreamExt},
    Future,
};

pub type S3DiskCache = Cached<Disk, S3>;

/// Stream returned by storage operations.
pub type StorageStream<T, E> = BoxStream<'static, Result<T, E>>;

/// The byte stream of an LFS object.
pub type ByteStream = Pin<
    Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + 'static>,
>;

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
    pub fn into_parts(self) -> (String, String) {
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
    ) -> (impl Future<Output = Result<(), io::Error>>, Self, Self) {
        let (len, stream) = self.into_parts();

        let (sender_a, receiver_a) = mpsc::channel::<Bytes>(0);
        let (sender_b, receiver_b) = mpsc::channel::<Bytes>(0);

        let sink = sender_a
            .fanout(sender_b)
            .sink_map_err(|e| io::Error::new(io::ErrorKind::Other, e));

        let receiver_a = receiver_a.map(|x| -> io::Result<_> { Ok(x) });
        let receiver_b = receiver_b.map(|x| -> io::Result<_> { Ok(x) });

        let f = stream.forward(sink);
        let a = LFSObject::new(len, Box::pin(receiver_a));
        let b = LFSObject::new(len, Box::pin(receiver_b));

        (f, a, b)
    }
}

/// Trait for abstracting away the storage medium.
#[async_trait]
pub trait Storage {
    type Error: fmt::Display + Send;

    /// Gets an entry from the storage medium.
    async fn get(
        &self,
        key: &StorageKey,
    ) -> Result<Option<LFSObject>, Self::Error>;

    /// Sets an entry in the storage medium.
    async fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> Result<(), Self::Error>;

    /// Gets the size of the object. Returns `None` if the object does not
    /// exist.
    async fn size(&self, key: &StorageKey) -> Result<Option<u64>, Self::Error>;

    /// Deletes an object.
    ///
    /// Permanent storage backends may choose to never delete objects, always
    /// returning success.
    async fn delete(&self, key: &StorageKey) -> Result<(), Self::Error>;

    /// Returns a stream of all the object IDs in the storage medium.
    fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error>;

    /// Gets the total size of the storage, if known.
    async fn total_size(&self) -> Option<u64> {
        None
    }

    /// Gets the maximum size of the storage.
    ///
    /// This should return `None` if the storage size is unbounded. This is only
    /// applicable to caches.
    async fn max_size(&self) -> Option<u64> {
        None
    }

    /// Returns a publicly accessible URL
    fn public_url(&self, key: &StorageKey) -> Option<String>;

    /// Returns a signed URL
    async fn upload_url(
        &self,
        key: &StorageKey,
        expires_in: Duration,
    ) -> Option<String>;
}

#[async_trait]
impl<S> Storage for Arc<S>
where
    S: Storage + Send + Sync,
{
    type Error = S::Error;

    #[inline]
    async fn get(
        &self,
        key: &StorageKey,
    ) -> Result<Option<LFSObject>, Self::Error> {
        self.as_ref().get(key).await
    }

    #[inline]
    async fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> Result<(), Self::Error> {
        self.as_ref().put(key, value).await
    }

    #[inline]
    async fn size(&self, key: &StorageKey) -> Result<Option<u64>, Self::Error> {
        self.as_ref().size(key).await
    }

    #[inline]
    async fn delete(&self, key: &StorageKey) -> Result<(), Self::Error> {
        self.as_ref().delete(key).await
    }

    #[inline]
    fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error> {
        self.as_ref().list()
    }

    #[inline]
    async fn total_size(&self) -> Option<u64> {
        self.as_ref().total_size().await
    }

    #[inline]
    async fn max_size(&self) -> Option<u64> {
        self.as_ref().max_size().await
    }

    fn public_url(&self, key: &StorageKey) -> Option<String> {
        self.as_ref().public_url(key)
    }

    async fn upload_url(
        &self,
        key: &StorageKey,
        expires_in: Duration,
    ) -> Option<String> {
        self.as_ref().upload_url(key, expires_in).await
    }
}
