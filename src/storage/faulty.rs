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

use derive_more::{Display, From};
use futures::{try_ready, Async, Future, Poll, Stream};
use rand::{self, Rng};

use super::{LFSObject, Storage, StorageFuture, StorageKey, StorageStream};

#[derive(Debug, Display, From)]
enum Error {
    Fault(FaultError),
    Io(io::Error),
}

impl std::error::Error for Error {}

/// The "Faulty McFaultFace" storage backend. This is used for failure injection
/// testing.
///
/// This is a storage backend adaptor that will have its uploads or downloads
/// randomly fail. The system should be robust enough to handle these failures
/// gracefully.
pub struct Backend<S> {
    storage: S,
}

impl<S> Backend<S> {
    pub fn new(storage: S) -> Self {
        Backend { storage }
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
        Box::new(self.storage.get(key).map(move |obj| -> Option<_> {
            let (len, stream) = obj?.into_parts();

            Some(LFSObject::new(len, Box::new(FaultyStream::new(stream))))
        }))
    }

    fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> StorageFuture<(), Self::Error> {
        let (len, stream) = value.into_parts();

        let stream = FaultyStream::new(stream);

        self.storage.put(key, LFSObject::new(len, Box::new(stream)))
    }

    fn size(
        &self,
        key: &StorageKey,
    ) -> StorageFuture<Option<u64>, Self::Error> {
        self.storage.size(key)
    }

    fn delete(&self, key: &StorageKey) -> StorageFuture<(), Self::Error> {
        self.storage.delete(key)
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

#[derive(Debug, Display)]
#[display(fmt = "injected fault")]
pub struct FaultError;

impl std::error::Error for FaultError {}

impl From<FaultError> for io::Error {
    fn from(error: FaultError) -> io::Error {
        io::Error::new(io::ErrorKind::Other, error.to_string())
    }
}

/// A stream that has random failures.
///
/// One out of 256 items of the stream will fail.
pub struct FaultyStream<S> {
    /// The underlying stream.
    stream: S,
}

impl<S> FaultyStream<S> {
    pub fn new(stream: S) -> Self {
        FaultyStream { stream }
    }
}

impl<S> Stream for FaultyStream<S>
where
    S: Stream,
    S::Error: From<FaultError>,
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let item = try_ready!(self.stream.poll());

        match item {
            Some(item) => {
                if rand::thread_rng().gen::<u8>() == 0 {
                    Err(FaultError.into())
                } else {
                    Ok(Async::Ready(Some(item)))
                }
            }
            None => {
                // End of stream.
                Ok(Async::Ready(None))
            }
        }
    }
}
