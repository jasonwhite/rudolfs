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
use std::time::Duration;

use crate::storage::ByteStream;
use async_trait::async_trait;
use derive_more::{Display, From};
use futures::StreamExt;
use rand::{self, Rng};

use super::{LFSObject, Storage, StorageKey, StorageStream};

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

fn faulty_stream(stream: ByteStream) -> ByteStream {
    Box::pin(stream.map(|item| {
        if rand::thread_rng().gen::<u8>() == 0 {
            Err(io::Error::new(io::ErrorKind::Other, "injected fault"))
        } else {
            item
        }
    }))
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
        let obj = self.storage.get(key).await;
        match obj {
            Ok(Some(lfs_obj)) => {
                let (len, s) = lfs_obj.into_parts();
                let fs = faulty_stream(s);
                Ok(Some(LFSObject::new(len, Box::pin(fs))))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> Result<(), Self::Error> {
        let (len, stream) = value.into_parts();

        self.storage
            .put(key, LFSObject::new(len, faulty_stream(stream)))
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

#[derive(Debug, Display)]
#[display("injected fault")]
pub struct FaultError;

impl std::error::Error for FaultError {}

impl From<FaultError> for io::Error {
    fn from(error: FaultError) -> io::Error {
        io::Error::new(io::ErrorKind::Other, error.to_string())
    }
}
