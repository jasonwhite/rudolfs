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

use derive_more::{Display, From};
use futures::{
    future::{self, Either},
    Future, Stream,
};

use crate::lfs::Oid;
use crate::sha256::{Sha256VerifyError, VerifyStream};

use super::{LFSObject, Storage, StorageFuture, StorageStream};

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
    fn get(&self, key: &Oid) -> StorageFuture<Option<LFSObject>, Self::Error> {
        let key = *key;

        let storage = self.storage.clone();

        Box::new(self.storage.get(&key).map(move |obj| match obj {
            Some(obj) => {
                let (len, stream) = obj.into_parts();

                let stream =
                    VerifyStream::new(stream.map_err(Error::from), len, key)
                        .or_else(move |err| match err {
                            Error::Verify(err) => {
                                log::error!(
                                    "Deleting corrupted object {} ({})",
                                    key,
                                    err
                                );

                                Either::A(storage.delete(&key).then(|_| {
                                    Err(io::Error::new(
                                        io::ErrorKind::Other,
                                        "found corrupted object",
                                    ))
                                }))
                            }
                            Error::Io(err) => Either::B(future::err(err)),
                        });

                Some(LFSObject::new(len, Box::new(stream)))
            }
            None => None,
        }))
    }

    fn put(
        &self,
        key: &Oid,
        value: LFSObject,
    ) -> StorageFuture<(), Self::Error> {
        let (len, stream) = value.into_parts();

        let stream = VerifyStream::new(stream.map_err(Error::from), len, *key)
            .map_err(move |err| match err {
                Error::Verify(err) => io::Error::new(io::ErrorKind::Other, err),
                Error::Io(err) => io::Error::new(io::ErrorKind::Other, err),
            });

        self.storage.put(key, LFSObject::new(len, Box::new(stream)))
    }

    fn size(&self, key: &Oid) -> StorageFuture<Option<u64>, Self::Error> {
        self.storage.size(key)
    }

    fn delete(&self, key: &Oid) -> StorageFuture<(), Self::Error> {
        self.storage.delete(key)
    }

    fn list(&self) -> StorageStream<(Oid, u64), Self::Error> {
        self.storage.list()
    }

    fn total_size(&self) -> Option<u64> {
        self.storage.total_size()
    }

    fn max_size(&self) -> Option<u64> {
        self.storage.max_size()
    }
}
