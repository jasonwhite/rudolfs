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
use std::mem;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use futures::{Future, Poll};
use tokio::{
    fs,
    io::{AsyncRead, AsyncWrite},
};

/// A temporary file path. When dropped, the file is deleted.
#[derive(Debug)]
pub struct TempPath(PathBuf);

impl TempPath {
    /// Renames the file without deleting it.
    pub fn persist<P: AsRef<Path>>(
        mut self,
        new_path: P,
    ) -> impl Future<Item = (), Error = io::Error> {
        // Don't drop self. We want to avoid deleting the file here and also
        // avoid leaking memory.
        let path = mem::replace(&mut self.0, PathBuf::new());
        mem::forget(self);

        fs::rename(path, new_path)
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
    pub fn new<P>(temp_path: P) -> impl Future<Item = Self, Error = io::Error>
    where
        P: AsRef<Path> + Send + 'static,
    {
        let path = TempPath(temp_path.as_ref().to_owned());
        fs::File::create(temp_path)
            .map(move |file| NamedTempFile { path, file })
    }

    pub fn into_parts(self) -> (fs::File, TempPath) {
        (self.file, self.path)
    }

    pub fn persist<P: AsRef<Path>>(
        self,
        new_path: P,
    ) -> impl Future<Item = fs::File, Error = io::Error> {
        let (file, path) = self.into_parts();
        path.persist(new_path).map(move |()| file)
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

impl io::Read for NamedTempFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl io::Write for NamedTempFile {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl AsyncRead for NamedTempFile {
    #[inline]
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        self.file.prepare_uninitialized_buffer(buf)
    }
}

impl AsyncWrite for NamedTempFile {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.file.shutdown()
    }
}
