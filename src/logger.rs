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
use std::fmt;
use std::net::SocketAddr;
use std::time::Instant;

use futures::Future;
use humantime::format_duration;
use hyper::{service::Service, Request, Response};
use log;

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

impl<S> Service for Logger<S>
where
    S: Service,
    S::Future: Send + 'static,
    S::Error: fmt::Display + Send + 'static,
{
    type ReqBody = S::ReqBody;
    type ResBody = S::ResBody;
    type Error = S::Error;
    type Future = Box<
        dyn Future<Item = Response<Self::ResBody>, Error = Self::Error> + Send,
    >;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let remote_addr = self.remote_addr;

        let start = Instant::now();

        Box::new(self.service.call(req).then(move |response| {
            // TODO: Add a duration of how long it took to respond to the
            // request.
            match &response {
                Ok(response) => log::info!(
                    "[{}] {} {} - {} ({})",
                    remote_addr.ip(),
                    method,
                    uri,
                    response.status(),
                    format_duration(start.elapsed()),
                ),
                Err(err) => log::error!(
                    "[{}] {} {} - {} ({})",
                    remote_addr.ip(),
                    method,
                    uri,
                    err,
                    format_duration(start.elapsed()),
                ),
            };

            response
        }))
    }
}
