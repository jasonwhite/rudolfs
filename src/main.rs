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
use crate::storage::{Cached, Disk, Encrypted, Retrying, Storage, Verify, S3};

#[cfg(feature = "faulty")]
use crate::storage::Faulty;

// Additional help to append to the end when `--help` is specified.
static AFTER_HELP: &str = include_str!("help.md");

#[derive(StructOpt)]
#[structopt(after_help = AFTER_HELP)]
struct Args {
    #[structopt(flatten)]
    global: GlobalArgs,

    #[structopt(subcommand)]
    backend: Backend,
}

#[derive(StructOpt)]
enum Backend {
    /// Starts the server with S3 as the storage backend.
    #[structopt(name = "s3")]
    S3(S3Args),

    /// Starts the server with the local disk as the storage backend.
    #[structopt(name = "local")]
    Local(LocalArgs),
}

#[derive(StructOpt)]
struct GlobalArgs {
    /// Host or address to listen on.
    #[structopt(long = "host", env = "RUDOLFS_HOST")]
    host: Option<String>,

    #[structopt(long = "port", default_value = "8080", env = "PORT")]
    port: u16,

    /// Encryption key to use.
    #[structopt(
        long = "key",
        parse(try_from_str = FromHex::from_hex),
        env = "RUDOLFS_KEY"
    )]
    key: [u8; 32],

    /// Root directory of the object cache. If not specified or if the local
    /// disk is the storage backend, then no local disk cache will be used.
    #[structopt(long = "cache-dir", env = "RUDOLFS_CACHE_DIR")]
    cache_dir: Option<PathBuf>,

    /// Maximum size of the cache, in bytes. Set to 0 for an unlimited cache
    /// size.
    #[structopt(
        long = "max-cache-size",
        default_value = "50 GiB",
        env = "RUDOLFS_MAX_CACHE_SIZE"
    )]
    max_cache_size: human_size::Size,

    /// Logging level to use.
    #[structopt(
        long = "log-level",
        default_value = "info",
        env = "RUDOLFS_LOG"
    )]
    log_level: log::LevelFilter,
}

#[derive(StructOpt)]
struct S3Args {
    /// Amazon S3 bucket to use.
    #[structopt(long, env = "RUDOLFS_S3_BUCKET")]
    bucket: String,

    /// Amazon S3 path prefix to use.
    #[structopt(long, default_value = "lfs", env = "RUDOLFS_S3_PREFIX")]
    prefix: String,

    /// Logging level to use.
    #[structopt(long = "cdn", env = "RUDOLFS_S3_CDN")]
    cdn: Option<String>,
}

#[derive(StructOpt)]
struct LocalArgs {
    /// Directory where the LFS files should be stored. This directory will be
    /// created if it does not exist.
    #[structopt(long, env = "RUDOLFS_LOCAL_PATH")]
    path: PathBuf,
}

impl Args {
    async fn main(self) -> Result<(), Box<dyn std::error::Error>> {
        // Initialize logging.
        let mut logger_builder = pretty_env_logger::formatted_timed_builder();
        logger_builder.filter_module("rudolfs", self.global.log_level);

        if let Ok(env) = std::env::var("RUST_LOG") {
            // Support the addition of RUST_LOG to help with debugging
            // dependencies, such as Hyper.
            logger_builder.parse_filters(&env);
        }

        logger_builder.init();

        // Find a socket address to bind to. This will resolve domain names.
        let addr = match self.global.host {
            Some(ref host) => host
                .to_socket_addrs()?
                .next()
                .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 8080))),
            None => SocketAddr::from(([0, 0, 0, 0], self.global.port)),
        };

        log::info!("Initializing storage...");

        match self.backend {
            Backend::S3(s3) => s3.run(addr, self.global).await?,
            Backend::Local(local) => local.run(addr, self.global).await?,
        }

        Ok(())
    }
}

impl S3Args {
    async fn run(
        self,
        addr: SocketAddr,
        global_args: GlobalArgs,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let s3 = S3::new(self.bucket, self.prefix, self.cdn)
            .map_err(Error::from)
            .await?;

        // Retry certain operations to S3 to make it more reliable.
        let s3 = Retrying::new(s3);

        // Add a little instability for testing purposes.
        #[cfg(feature = "faulty")]
        let s3 = Faulty::new(s3);

        match global_args.cache_dir {
            Some(cache_dir) => {
                // Convert cache size to bytes.
                let max_cache_size = global_args
                    .max_cache_size
                    .into::<human_size::Byte>()
                    .value() as u64;

                // Use disk storage as a cache.
                let disk = Disk::new(cache_dir).map_err(Error::from).await?;

                #[cfg(feature = "faulty")]
                let disk = Faulty::new(disk);

                let cache = Cached::new(max_cache_size, disk, s3).await?;
                let storage =
                    Verify::new(Encrypted::new(global_args.key, cache));
                run_server(storage, &addr).await?;
            }
            None => {
                let storage = Verify::new(Encrypted::new(global_args.key, s3));
                run_server(storage, &addr).await?;
            }
        }

        Ok(())
    }
}

impl LocalArgs {
    async fn run(
        self,
        addr: SocketAddr,
        global_args: GlobalArgs,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let storage = Disk::new(self.path).map_err(Error::from).await?;
        let storage = Verify::new(Encrypted::new(global_args.key, storage));

        log::info!("Local disk storage initialized.");

        run_server(storage, &addr).await?;
        Ok(())
    }
}

async fn run_server<S>(storage: S, addr: &SocketAddr) -> hyper::Result<()>
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

    let server = Server::bind(&addr).serve(new_service);

    log::info!("Listening on {}", server.local_addr());

    server.await?;
    Ok(())
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
