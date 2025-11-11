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
use hyper::{
    Request,
    http::uri::{self, Authority, Scheme, Uri},
};
use std::convert::TryFrom;

pub trait RequestExt {
    /// Gets the scheme based on the headers in the request.
    ///
    /// First checks `X-Forwarded-Proto` and then falls back to the HTTP/2
    /// `:scheme` header.
    fn scheme(&self) -> Option<Scheme>;

    /// Gets the authority based on the headers in the request.
    ///
    /// First checks the HTTP/2 header `:authority` and then falls back to the
    /// `Host` header.
    fn authority(&self) -> Option<Authority>;

    fn base_uri(&self) -> uri::Builder {
        Uri::builder()
            .scheme(self.scheme().unwrap_or(Scheme::HTTP))
            .authority(
                self.authority()
                    .unwrap_or_else(|| Authority::from_static("localhost")),
            )
    }
}

impl<B> RequestExt for Request<B> {
    fn scheme(&self) -> Option<Scheme> {
        self.headers()
            .get("X-Forwarded-Proto")
            .or_else(|| self.headers().get(":scheme"))
            .and_then(|scheme| Scheme::try_from(scheme.as_bytes()).ok())
    }

    fn authority(&self) -> Option<Authority> {
        self.headers()
            .get("X-Forwarded-Host")
            .or_else(|| self.headers().get(":authority"))
            .or_else(|| self.headers().get("Host"))
            .and_then(|authority| {
                Authority::try_from(authority.as_bytes()).ok()
            })
    }
}
