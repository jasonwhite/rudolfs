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

use core::mem;
use core::ops::Deref;
use core::pin::Pin;
use core::task::{Context, Poll};

use std::path::{Path, PathBuf};

use tokio::{
    fs,
    io::{self, AsyncRead, AsyncWrite, ReadBuf},
};

/// A temporary file path. When dropped, the file is deleted.
#[derive(Debug)]
pub struct TempPath(PathBuf);

impl TempPath {
    /// Renames the file without deleting it.
    pub async fn persist<P: AsRef<Path>>(
        mut self,
        new_path: P,
    ) -> Result<(), io::Error> {
        // Don't drop self. We want to avoid deleting the file here and also
        // avoid leaking memory.
        let path = mem::replace(&mut self.0, PathBuf::new());
        mem::forget(self);

        fs::rename(path, new_path).await
    }
}

impl Deref for TempPath {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for TempPath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        // Note that this is a synchronous call. We can't really return a future
        // to do this.
        let _ = std::fs::remove_file(&self.0);
    }
}

/// A temporary async file.
pub struct NamedTempFile {
    path: TempPath,
    file: fs::File,
}

impl NamedTempFile {
    pub async fn new<P>(temp_path: P) -> Result<Self, io::Error>
    where
        P: AsRef<Path> + Send + 'static,
    {
        let path = TempPath(temp_path.as_ref().to_owned());
        let file = fs::File::create(temp_path).await?;
        Ok(NamedTempFile { path, file })
    }

    pub fn into_parts(self) -> (fs::File, TempPath) {
        (self.file, self.path)
    }

    pub async fn persist<P: AsRef<Path>>(
        self,
        new_path: P,
    ) -> Result<fs::File, io::Error> {
        let (file, path) = self.into_parts();
        path.persist(new_path).await?;
        Ok(file)
    }
}

impl AsRef<Path> for NamedTempFile {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<fs::File> for NamedTempFile {
    fn as_ref(&self) -> &fs::File {
        &self.file
    }
}

impl AsMut<fs::File> for NamedTempFile {
    fn as_mut(&mut self) -> &mut fs::File {
        &mut self.file
    }
}

impl AsyncRead for NamedTempFile {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_read(cx, buf)
    }
}

impl AsyncWrite for NamedTempFile {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.file).poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.file).poll_shutdown(cx)
    }
}
