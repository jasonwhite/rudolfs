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
use std::ops::Deref;

use derive_more::From;
use futures::{Future, IntoFuture, Poll, Stream};
use http::uri::{Authority, Scheme};
use hyper::{
    body::{Payload, Sender},
    Body as HBody, Chunk, HeaderMap, Request, Response,
};
use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Debug, Default, From)]
pub struct Body(HBody);

impl Body {
    pub fn empty() -> Self {
        HBody::empty().into()
    }

    #[allow(unused)]
    pub fn channel() -> (Sender, Self) {
        let (sender, body) = HBody::channel();
        (sender, body.into())
    }

    /// Deserializes the body as json. Returns a future of the deserialized
    /// json.
    pub fn into_json<T>(self) -> impl Future<Item = T, Error = Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.concat2()
            .from_err::<Error>()
            .and_then(|body| Ok(serde_json::from_slice(&body)?))
            .from_err()
    }

    /// Serialize a value to JSON.
    pub fn json<T>(value: &T) -> Result<Self, Error>
    where
        T: Serialize,
    {
        let body: HBody = serde_json::to_vec_pretty(&value)?.into();
        Ok(body.into())
    }

    pub fn wrap_stream<S>(stream: S) -> Self
    where
        S: Stream + Send + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Chunk: From<S::Item>,
    {
        Body(HBody::wrap_stream(stream))
    }
}

impl Into<HBody> for Body {
    fn into(self) -> HBody {
        self.0
    }
}

impl From<String> for Body {
    fn from(s: String) -> Self {
        Body(s.into())
    }
}

impl From<&'static str> for Body {
    fn from(slice: &'static str) -> Self {
        Body(slice.into())
    }
}

type ChunkedStream = Box<
    dyn Stream<
            Item = Chunk,
            Error = Box<dyn std::error::Error + 'static + Sync + Send>,
        >
        + 'static
        + Send,
>;

impl From<ChunkedStream> for Body {
    fn from(stream: ChunkedStream) -> Self {
        Body(stream.into())
    }
}

impl Deref for Body {
    type Target = HBody;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Stream for Body {
    type Item = <HBody as Stream>::Item;
    type Error = <HBody as Stream>::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.poll()
    }
}

impl Payload for Body {
    type Data = <HBody as Payload>::Data;
    type Error = <HBody as Payload>::Error;

    fn poll_data(&mut self) -> Poll<Option<Self::Data>, Self::Error> {
        <HBody as Payload>::poll_data(&mut self.0)
    }

    fn poll_trailers(&mut self) -> Poll<Option<HeaderMap>, Self::Error> {
        <HBody as Payload>::poll_trailers(&mut self.0)
    }

    fn is_end_stream(&self) -> bool {
        <HBody as Payload>::is_end_stream(&self.0)
    }

    fn content_length(&self) -> Option<u64> {
        <HBody as Payload>::content_length(&self.0)
    }
}

type BoxResponse<T> = Box<dyn Future<Item = Response<T>, Error = Error> + Send>;

pub trait IntoResponse<T> {
    /// Convert something into a response.
    fn response(self) -> BoxResponse<T>;
}

impl<F, T, U> IntoResponse<U> for F
where
    F: IntoFuture<Item = Response<T>, Error = Error>,
    F::Future: Send + 'static,
    T: Into<U>,
{
    fn response(self) -> BoxResponse<U> {
        Box::new(
            self.into_future()
                .map(move |r| -> Response<U> { into_response(r) }),
        )
    }
}

pub fn into_request<T, U>(req: Request<T>) -> Request<U>
where
    T: Into<U>,
{
    let (parts, body) = req.into_parts();
    Request::from_parts(parts, body.into())
}

fn into_response<T, U>(res: Response<T>) -> Response<U>
where
    T: Into<U>,
{
    let (parts, body) = res.into_parts();
    Response::from_parts(parts, body.into())
}

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
}

impl<B> RequestExt for Request<B> {
    fn scheme(&self) -> Option<Scheme> {
        self.headers()
            .get("X-Forwarded-Proto")
            .or_else(|| self.headers().get(":scheme"))
            .and_then(|scheme| {
                Scheme::from_shared(scheme.as_bytes().into()).ok()
            })
    }

    fn authority(&self) -> Option<Authority> {
        self.headers()
            .get(":authority")
            .or_else(|| self.headers().get("Host"))
            .and_then(|authority| {
                Authority::from_shared(authority.as_bytes().into()).ok()
            })
    }
}
