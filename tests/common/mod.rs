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
#![allow(unused)]
use std::fs::{self, File};
use std::io;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::Path;

use duct::cmd;
use rand::Rng;

/// Bind test server to localhost port 0. We don't want this server to be
/// externally visible.
pub const SERVER_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);

/// A temporary git repository.
pub struct GitRepo {
    repo: tempfile::TempDir,
}

impl GitRepo {
    /// Initialize a temporary synthetic git repository. It is set up to be
    /// connected to our LFS server.
    pub fn init(lfs_server: SocketAddr) -> io::Result<Self> {
        let repo = tempfile::TempDir::new()?;
        let path = repo.path();

        cmd!("git", "init", ".").dir(path).run()?;
        cmd!("git", "lfs", "install").dir(path).run()?;
        cmd!("git", "remote", "add", "origin", "fake_remote")
            .dir(path)
            .run()?;
        cmd!(
            "git",
            "config",
            "lfs.url",
            format!("http://{}/api/test/test", lfs_server)
        )
        .dir(path)
        .run()?;
        cmd!("git", "config", "user.name", "Foo Bar")
            .dir(path)
            .run()?;
        cmd!("git", "config", "user.email", "foobar@example.com")
            .dir(path)
            .run()?;
        cmd!("git", "lfs", "track", "*.bin").dir(path).run()?;
        cmd!("git", "add", ".gitattributes").dir(path).run()?;
        cmd!("git", "commit", "-m", "Initial commit")
            .dir(path)
            .run()?;

        Ok(Self { repo })
    }

    pub fn clone_repo(&self) -> io::Result<Self> {
        let repo = tempfile::TempDir::new()?;
        let src_dir_str = self
            .repo
            .path()
            .to_str()
            .expect("could not convert src repo path to str");
        let dst_dir_str = repo
            .path()
            .to_str()
            .expect("could not convert src repo path to str");
        cmd!("git", "clone", src_dir_str, dst_dir_str).run()?;
        Ok(Self { repo })
    }

    /// Adds a random file with the given size and random number generator. The
    /// file is also staged with `git add`.
    pub fn add_random<R: Rng>(
        &self,
        path: &Path,
        size: usize,
        rng: &mut R,
    ) -> io::Result<()> {
        let mut file = File::create(self.repo.path().join(path))?;
        gen_file(&mut file, size, rng)?;
        cmd!("git", "add", path).dir(self.repo.path()).run()?;
        Ok(())
    }

    /// Commits the currently staged files.
    pub fn commit(&self, message: &str) -> io::Result<()> {
        cmd!("git", "commit", "-m", message)
            .dir(self.repo.path())
            .run()?;
        Ok(())
    }

    pub fn lfs_push(&self) -> io::Result<()> {
        cmd!("git", "lfs", "push", "origin", "master")
            .dir(self.repo.path())
            .run()?;
        Ok(())
    }

    pub fn lfs_pull(&self) -> io::Result<()> {
        cmd!("git", "lfs", "pull").dir(self.repo.path()).run()?;
        Ok(())
    }

    pub fn pull(&self) -> io::Result<()> {
        cmd!("git", "pull").dir(self.repo.path()).run()?;
        Ok(())
    }

    /// Deletes all cached LFS files in `.git/lfs/`. This will force a
    /// re-download from the server.
    pub fn clean_lfs(&self) -> io::Result<()> {
        fs::remove_dir_all(self.repo.path().join(".git/lfs"))
    }
}

fn gen_file<W, R>(
    writer: &mut W,
    mut size: usize,
    rng: &mut R,
) -> io::Result<()>
where
    W: io::Write,
    R: Rng,
{
    let mut buf = [0u8; 4096];

    while size > 0 {
        let to_write = buf.len().min(size);

        let buf = &mut buf[..to_write];
        rng.fill(buf);
        writer.write_all(buf)?;

        size -= to_write;
    }

    Ok(())
}

pub fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_module("rudolfs", log::LevelFilter::max())
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}
