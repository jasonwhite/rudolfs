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
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;

use bytes::BytesMut;
use futures::{future, Future, Stream};
use tokio::{
    self,
    codec::{BytesCodec, Framed},
    fs,
};
use uuid::Uuid;

use super::{LFSObject, Storage, StorageFuture, StorageStream};
use crate::lfs::Oid;

pub struct Backend {
    root: PathBuf,
}

impl Backend {
    pub fn new(root: PathBuf) -> impl Future<Item = Self, Error = io::Error> {
        // TODO: Clean out files in the "incomplete" folder.
        future::ok(Backend { root })
    }

    // Use sub directories in order to better utilize the file system's internal
    // tree data structure.
    fn key_to_path(&self, oid: &Oid) -> PathBuf {
        self.root.join(format!("objects/{}", oid.path()))
    }
}

impl Storage for Backend {
    type Error = io::Error;

    fn get(&self, key: &Oid) -> StorageFuture<Option<LFSObject>, Self::Error> {
        Box::new(
            fs::File::open(self.key_to_path(key))
                .and_then(fs::File::metadata)
                .then(move |result| {
                    Ok(match result {
                        Ok((file, metadata)) => {
                            let stream = Framed::new(file, BytesCodec::new())
                                .map(BytesMut::freeze);

                            Some(LFSObject::new(
                                metadata.len(),
                                Box::new(stream),
                            ))
                        }
                        Err(err) => match err.kind() {
                            io::ErrorKind::NotFound => None,
                            _ => return Err(err),
                        },
                    })
                }),
        )
    }

    fn put(
        &self,
        key: &Oid,
        value: LFSObject,
    ) -> StorageFuture<(), Self::Error> {
        let path = self.key_to_path(key);
        let dir = path.parent().unwrap().to_path_buf();

        let incomplete = self.root.join("incomplete");
        let temp_path = incomplete.join(Uuid::new_v4().to_string());
        let temp_path2 = temp_path.clone();

        Box::new(
            fs::create_dir_all(incomplete)
                .and_then(move |()| fs::File::create(temp_path))
                .and_then(move |file| {
                    value.stream().forward(Framed::new(file, BytesCodec::new()))
                })
                .and_then(move |_| fs::create_dir_all(dir))
                .and_then(move |()| fs::rename(temp_path2, path)),
        )
    }

    fn size(&self, key: &Oid) -> StorageFuture<Option<u64>, Self::Error> {
        let path = self.key_to_path(key);

        Box::new(
            fs::metadata(path)
                .map(move |metadata| Some(metadata.len()))
                .or_else(move |err| match err.kind() {
                    io::ErrorKind::NotFound => Ok(None),
                    _ => Err(err),
                }),
        )
    }

    fn delete(&self, key: &Oid) -> StorageFuture<(), Self::Error> {
        Box::new(fs::remove_file(self.key_to_path(key)).or_else(move |err| {
            match err.kind() {
                io::ErrorKind::NotFound => Ok(()),
                _ => Err(err),
            }
        }))
    }

    /// Lists the objects that are on disk.
    ///
    /// The directory structure is assumed to be like this:
    ///
    ///     objects
    ///     ├── 00
    ///     │   ├── 07
    ///     │   │   └── 0007941906960...
    ///     │   └── ff
    ///     │       └── 00ff9e9c69224...
    ///     ├── 01
    ///     │   ├── 89
    ///     │   │   └── 0189e5fd19477...
    ///     │   └── f5
    ///     │       └── 01f5c45c65e62...
    ///                 ^^^^
    ///
    /// Note that the first four characters are repeated in the file name so
    /// that transforming the file name into an object ID is simpler.
    fn list(&self) -> StorageStream<(Oid, u64), Self::Error> {
        let path = self.root.join("objects");

        Box::new(
            fs::read_dir(path)
                .flatten_stream()
                .map(move |entry| fs::read_dir(entry.path()).flatten_stream())
                .flatten()
                .map(move |entry| fs::read_dir(entry.path()).flatten_stream())
                .flatten()
                .and_then(move |entry| {
                    let path = entry.path();
                    future::poll_fn(move || entry.poll_metadata())
                        .map(move |metadata| (path, metadata))
                })
                .filter_map(move |(path, metadata)| {
                    if let Some(oid) = path
                        .file_name()
                        .and_then(OsStr::to_str)
                        .and_then(|s| Oid::from_str(s).ok())
                    {
                        if metadata.is_file() {
                            Some((oid, metadata.len()))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }),
        )
    }
}
