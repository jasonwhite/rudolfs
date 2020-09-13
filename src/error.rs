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

use crate::sha256::{Sha256Error, Sha256VerifyError};
use crate::storage::{self, Storage};

// Define a type so we can return multiple types of errors
#[derive(Debug, From, Display)]
pub enum Error {
    Io(io::Error),
    Http(http::Error),
    Hyper(hyper::Error),
    Json(serde_json::Error),
    Sha256Error(Sha256Error),
    Sha256VerifyError(Sha256VerifyError),
    S3(storage::S3Error),
    S3DiskCache(<storage::S3DiskCache as Storage>::Error),
    Askama(askama::Error),
}

impl std::error::Error for Error {}
