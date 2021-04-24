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
use chacha::{ChaCha, KeyStream};
use std::io;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{stream::StreamExt, Stream};

use super::{LFSObject, Storage, StorageKey, StorageStream};

/// A storage adaptor that encrypts/decrypts all data that passes through.
pub struct Backend<S> {
    storage: S,
    key: [u8; 32],
}

impl<S> Backend<S> {
    pub fn new(key: [u8; 32], storage: S) -> Self {
        Backend { key, storage }
    }
}

fn xor_stream<S>(
    mut chacha: ChaCha,
    stream: S,
) -> impl Stream<Item = Result<Bytes, io::Error>>
where
    S: Stream<Item = Result<Bytes, io::Error>>,
{
    stream.map(move |bytes| {
        let mut bytes = BytesMut::from(bytes?.as_ref());

        chacha.xor_read(bytes.as_mut()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "reached end of xchacha20 keystream",
            )
        })?;

        Ok(bytes.freeze())
    })
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
        // Use the first part of the SHA256 as the nonce.
        let mut nonce: [u8; 24] = [0; 24];
        nonce.copy_from_slice(&key.oid().bytes()[0..24]);

        let chacha = ChaCha::new_xchacha20(&self.key, &nonce);

        Ok(self.storage.get(key).await?.map(move |obj| {
            let (len, stream) = obj.into_parts();
            LFSObject::new(len, Box::pin(xor_stream(chacha, stream)))
        }))
    }

    async fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> Result<(), Self::Error> {
        // Use the first part of the SHA256 as the nonce.
        let mut nonce: [u8; 24] = [0; 24];
        nonce.copy_from_slice(&key.oid().bytes()[0..24]);

        let chacha = ChaCha::new_xchacha20(&self.key, &nonce);

        let (len, stream) = value.into_parts();
        let stream = xor_stream(chacha, stream);

        self.storage
            .put(key, LFSObject::new(len, Box::pin(stream)))
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

    async fn upload_url(&self, key: &StorageKey) -> Option<String> {
        self.storage.upload_url(key).await
    }
}
