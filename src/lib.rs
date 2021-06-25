// Copyright (c) 2021 Jason White
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
mod app;
mod error;
mod hyperext;
mod lfs;
mod logger;
mod lru;
mod sha256;
mod storage;
mod util;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use futures::future::{self, Future, TryFutureExt};
use hyper::{
    self,
    server::conn::{AddrIncoming, AddrStream},
    service::make_service_fn,
};

use crate::app::App;
use crate::error::Error;
use crate::logger::Logger;
use crate::storage::{Cached, Disk, Encrypted, Retrying, Storage, Verify, S3};

#[cfg(feature = "faulty")]
use crate::storage::Faulty;

/// Represents a running LFS server.
pub trait Server: Future<Output = hyper::Result<()>> {
    /// Returns the local address this server is bound to.
    fn addr(&self) -> SocketAddr;
}

impl<S, E> Server for hyper::Server<AddrIncoming, S, E>
where
    hyper::Server<AddrIncoming, S, E>: Future<Output = hyper::Result<()>>,
{
    fn addr(&self) -> SocketAddr {
        self.local_addr()
    }
}

#[derive(Debug)]
pub struct Cache {
    /// Path to the cache.
    dir: PathBuf,

    /// Maximum size of the cache, in bytes.
    max_size: u64,
}

impl Cache {
    pub fn new(dir: PathBuf, max_size: u64) -> Self {
        Self { dir, max_size }
    }
}

#[derive(Debug)]
pub struct S3ServerBuilder {
    bucket: String,
    key: [u8; 32],
    prefix: Option<String>,
    cdn: Option<String>,
    cache: Option<Cache>,
}

impl S3ServerBuilder {
    pub fn new(bucket: String, key: [u8; 32]) -> Self {
        Self {
            bucket,
            prefix: None,
            cdn: None,
            key,
            cache: None,
        }
    }

    /// Sets the bucket to use.
    pub fn bucket(&mut self, bucket: String) -> &mut Self {
        self.bucket = bucket;
        self
    }

    /// Sets the encryption key to use.
    pub fn key(&mut self, key: [u8; 32]) -> &mut Self {
        self.key = key;
        self
    }

    /// Sets the prefix to use.
    pub fn prefix(&mut self, prefix: String) -> &mut Self {
        self.prefix = Some(prefix);
        self
    }

    /// Sets the base URL of the CDN to use. This is incompatible with
    /// encryption since the LFS object is not sent to Rudolfs.
    pub fn cdn(&mut self, url: String) -> &mut Self {
        self.cdn = Some(url);
        self
    }

    /// Sets the cache to use. If not specified, then no local disk cache is
    /// used. All objects will get sent directly to S3.
    pub fn cache(&mut self, cache: Cache) -> &mut Self {
        self.cache = Some(cache);
        self
    }

    /// Spawns the server. The server must be awaited on in order to accept
    /// incoming client connections and run.
    pub async fn spawn(
        mut self,
        addr: SocketAddr,
    ) -> Result<Box<dyn Server + Unpin + Send>, Box<dyn std::error::Error>>
    {
        let prefix = self.prefix.unwrap_or_else(|| String::from("lfs"));

        if self.cdn.is_some() {
            log::warn!(
                "A CDN was specified. Since uploads and downloads do not flow \
                 through Rudolfs in this case, they will *not* be encrypted."
            );

            if let Some(_) = self.cache.take() {
                log::warn!(
                    "A local disk cache does not work with a CDN and will be \
                     disabled."
                );
            }
        }

        let s3 = S3::new(self.bucket, prefix, self.cdn)
            .map_err(Error::from)
            .await?;

        // Retry certain operations to S3 to make it more reliable.
        let s3 = Retrying::new(s3);

        // Add a little instability for testing purposes.
        #[cfg(feature = "faulty")]
        let s3 = Faulty::new(s3);

        match self.cache {
            Some(cache) => {
                // Use disk storage as a cache.
                let disk = Disk::new(cache.dir).map_err(Error::from).await?;

                #[cfg(feature = "faulty")]
                let disk = Faulty::new(disk);

                let cache = Cached::new(cache.max_size, disk, s3).await?;
                let storage = Verify::new(Encrypted::new(self.key, cache));
                Ok(Box::new(spawn_server(storage, &addr)))
            }
            None => {
                let storage = Verify::new(Encrypted::new(self.key, s3));
                Ok(Box::new(spawn_server(storage, &addr)))
            }
        }
    }

    /// Spawns the server and runs it to completion. This will run forever
    /// unless there is an error or the server shuts down gracefully.
    pub async fn run(
        self,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let server = self.spawn(addr).await?;

        log::info!("Listening on {}", server.addr());

        server.await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct LocalServerBuilder {
    path: PathBuf,
    key: [u8; 32],
    cache: Option<Cache>,
}

impl LocalServerBuilder {
    /// Creates a local server builder. `path` is the path to the folder where
    /// all of the LFS data will be stored.
    pub fn new(path: PathBuf, key: [u8; 32]) -> Self {
        Self {
            path,
            key,
            cache: None,
        }
    }

    /// Sets the encryption key to use.
    pub fn key(&mut self, key: [u8; 32]) -> &mut Self {
        self.key = key;
        self
    }

    /// Sets the cache to use. If not specified, then no local disk cache is
    /// used. It is uncommon to want to use this when the object storage is
    /// already local. However, a cache may be useful when the data storage path
    /// is on a mounted network file system. In such a case, the network file
    /// system could be slow and the local disk storage could be fast.
    pub fn cache(&mut self, cache: Cache) -> &mut Self {
        self.cache = Some(cache);
        self
    }

    /// Spawns the server. The server must be awaited on in order to accept
    /// incoming client connections and run.
    pub async fn spawn(
        self,
        addr: SocketAddr,
    ) -> Result<impl Server, Box<dyn std::error::Error>> {
        let storage = Disk::new(self.path).map_err(Error::from).await?;
        let storage = Verify::new(Encrypted::new(self.key, storage));

        log::info!("Local disk storage initialized.");

        Ok(spawn_server(storage, &addr))
    }

    /// Spawns the server and runs it to completion. This will run forever
    /// unless there is an error or the server shuts down gracefully.
    pub async fn run(
        self,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let server = self.spawn(addr).await?;

        log::info!("Listening on {}", server.addr());

        server.await?;
        Ok(())
    }
}

fn spawn_server<S>(storage: S, addr: &SocketAddr) -> impl Server
where
    S: Storage + Send + Sync + 'static,
    S::Error: Into<Error>,
    Error: From<S::Error>,
{
    let storage = Arc::new(storage);

    let new_service = make_service_fn(move |socket: &AddrStream| {
        // Create our app.
        let service = App::new(storage.clone());

        // Add logging middleware
        future::ok::<_, Infallible>(Logger::new(socket.remote_addr(), service))
    });

    hyper::Server::bind(&addr).serve(new_service)
}
