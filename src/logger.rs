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
use core::task::{Context, Poll};

use std::fmt;
use std::net::SocketAddr;
use std::time::Instant;

use futures::future::{BoxFuture, FutureExt};
use humantime::format_duration;
use hyper::{service::Service, Body, Request, Response};

/// Wraps a service to provide logging on both the request and the response.
pub struct Logger<S> {
    remote_addr: SocketAddr,
    service: S,
}

impl<S> Logger<S> {
    pub fn new(remote_addr: SocketAddr, service: S) -> Self {
        Logger {
            remote_addr,
            service,
        }
    }
}

impl<S> Service<Request<Body>> for Logger<S>
where
    S: Service<Request<Body>, Response = Response<Body>>,
    S::Future: Send + 'static,
    S::Error: fmt::Display + Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut Context,
    ) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let remote_addr = self.remote_addr;

        let start = Instant::now();

        Box::pin(self.service.call(req).inspect(
            move |response| match response {
                Ok(response) => tracing::info!(
                    "[{}] {} {} - {} ({})",
                    remote_addr.ip(),
                    method,
                    uri,
                    response.status(),
                    format_duration(start.elapsed()),
                ),
                Err(err) => tracing::error!(
                    "[{}] {} {} - {} ({})",
                    remote_addr.ip(),
                    method,
                    uri,
                    err,
                    format_duration(start.elapsed()),
                ),
            },
        ))
    }
}
