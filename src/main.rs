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
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;

use futures::future::{self, TryFutureExt};
use hex::FromHex;
use hyper::{self, server::conn::AddrStream, service::make_service_fn, Server};
use structopt::StructOpt;

use crate::app::App;
use crate::error::Error;
use crate::logger::Logger;
use crate::storage::{Cached, Disk, Encrypted, Retrying, Verify, S3};

#[cfg(feature = "faulty")]
use crate::storage::Faulty;

#[derive(StructOpt)]
struct Args {
    /// Host or address to listen on.
    #[structopt(long = "host", default_value = "0.0.0.0:8080")]
    host: String,

    /// Root directory of the object cache.
    #[structopt(long = "cache-dir")]
    cache_dir: PathBuf,

    /// Logging level to use.
    #[structopt(long = "log-level", default_value = "info")]
    log_level: log::LevelFilter,

    /// Amazon S3 bucket to use.
    #[structopt(long = "s3-bucket")]
    s3_bucket: String,

    /// Amazon S3 path prefix to use.
    #[structopt(long = "s3-prefix", default_value = "lfs")]
    s3_prefix: String,

    /// Encryption key to use.
    #[structopt(long = "key", parse(try_from_str = FromHex::from_hex))]
    key: [u8; 32],

    /// Maximum size of the cache, in bytes. Set to 0 for an unlimited cache
    /// size.
    #[structopt(long = "max-cache-size", default_value = "50 GiB")]
    max_cache_size: human_size::Size,
}

impl Args {
    async fn main(self) -> Result<(), Box<dyn std::error::Error>> {
        // Initialize logging.
        let mut logger_builder = pretty_env_logger::formatted_timed_builder();
        logger_builder.filter_module("rudolfs", self.log_level);

        if let Ok(env) = std::env::var("RUST_LOG") {
            // Support the addition of RUST_LOG to help with debugging
            // dependencies, such as Hyper.
            logger_builder.parse_filters(&env);
        }

        logger_builder.init();

        // Find a socket address to bind to. This will resolve domain names.
        let addr = self
            .host
            .to_socket_addrs()?
            .next()
            .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 8080)));

        // Convert cache size to bytes.
        let max_cache_size =
            self.max_cache_size.into::<human_size::Byte>().value() as u64;
        let key = self.key;

        // Initialize our storage backends.
        let disk = Disk::new(self.cache_dir).map_err(Error::from);
        let s3 = S3::new(self.s3_bucket, self.s3_prefix).map_err(Error::from);

        log::info!("Initializing storage...");
        let (disk, s3) = future::try_join(disk, s3).await?;

        // Retry certain operations to S3 to make it more reliable.
        let s3 = Retrying::new(s3);

        // Add a little instability for testing purposes.
        #[cfg(feature = "faulty")]
        let s3 = Faulty::new(s3);
        #[cfg(feature = "faulty")]
        let disk = Faulty::new(disk);

        // Use the disk as a cache.
        let storage = Cached::new(max_cache_size, disk, s3).await?;

        // Encrypt/decrypt and verify all incoming and outgoing data.
        let storage = Arc::new(Verify::new(Encrypted::new(key, storage)));

        // Create our service factory.
        let new_service = make_service_fn(move |socket: &AddrStream| {
            // Create our app.
            let service = App::new(storage.clone());

            // Add logging middleware
            future::ok::<_, Infallible>(Logger::new(
                socket.remote_addr(),
                service,
            ))
        });

        // Create the server.
        let server = Server::bind(&addr).serve(new_service);

        log::info!("Listening on {}", server.local_addr());

        // Run the server.
        server.await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let exit_code = if let Err(err) = Args::from_args().main().await {
        log::error!("{}", err);
        1
    } else {
        0
    };

    std::process::exit(exit_code);
}
