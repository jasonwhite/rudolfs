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
use std::io;

use core::task::{Context, Poll};

use askama::Template;
use futures::{
    future::{self, BoxFuture},
    stream::TryStreamExt,
};
use http::{self, header, StatusCode, Uri};
use hyper::{self, body::Body, service::Service, Method, Request, Response};
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::hyperext::RequestExt;
use crate::lfs;
use crate::storage::{LFSObject, Namespace, Storage, StorageKey};

async fn from_json<T>(mut body: Body) -> Result<T, Error>
where
    T: for<'de> Deserialize<'de>,
{
    let mut buf = Vec::new();

    while let Some(chunk) = body.try_next().await? {
        buf.extend(chunk);
    }

    Ok(serde_json::from_slice(&buf)?)
}

fn into_json<T>(value: &T) -> Result<Body, Error>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec_pretty(value)?;
    Ok(bytes.into())
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate<'a> {
    title: &'a str,
    api: Uri,
}

#[derive(Clone)]
pub struct App<S> {
    storage: S,
}

impl<S> App<S> {
    pub fn new(storage: S) -> Self {
        App { storage }
    }
}

impl<S> App<S>
where
    S: Storage + Send + Sync,
    S::Error: Into<Error>,
    Error: From<S::Error>,
{
    /// Handles the index route.
    fn index(req: Request<Body>) -> Result<Response<Body>, Error> {
        let template = IndexTemplate {
            title: "Rudolfs",
            api: req.base_uri().path_and_query("/api").build().unwrap(),
        };

        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(template.render()?.into())?)
    }

    /// Generates a "404 not found" response.
    fn not_found(_req: Request<Body>) -> Result<Response<Body>, Error> {
        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("Not found".into())?)
    }

    /// Handles `/api` routes.
    async fn api(
        storage: S,
        req: Request<Body>,
    ) -> Result<Response<Body>, Error> {
        let mut parts = req.uri().path().split('/').filter(|s| !s.is_empty());

        // Skip over the '/api' part.
        assert_eq!(parts.next(), Some("api"));

        // Extract the namespace.
        let namespace = match (parts.next(), parts.next()) {
            (Some(org), Some(project)) => {
                Namespace::new(org.into(), project.into())
            }
            _ => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("Missing org/project in URL"))?)
            }
        };

        match parts.next() {
            Some("object") => {
                // Upload or download a single object.
                let oid = parts.next().and_then(|x| x.parse::<lfs::Oid>().ok());
                let oid = match oid {
                    Some(oid) => oid,
                    None => {
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from("Missing OID parameter."))?)
                    }
                };

                let key = StorageKey::new(namespace, oid);

                match *req.method() {
                    Method::GET => Self::download(storage, req, key).await,
                    Method::PUT => Self::upload(storage, req, key).await,
                    _ => Self::not_found(req),
                }
            }
            Some("objects") => match (req.method(), parts.next()) {
                (&Method::POST, Some("batch")) => {
                    Self::batch(storage, req, namespace).await
                }
                (&Method::POST, Some("verify")) => {
                    Self::verify(storage, req, namespace).await
                }
                _ => Self::not_found(req),
            },
            _ => Self::not_found(req),
        }
    }

    /// Downloads a single LFS object.
    async fn download(
        storage: S,
        _req: Request<Body>,
        key: StorageKey,
    ) -> Result<Response<Body>, Error> {
        if let Some(object) = storage.get(&key).await? {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, object.len())
                .body(Body::wrap_stream(object.stream()))?)
        } else {
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())?)
        }
    }

    /// Uploads a single LFS object.
    async fn upload(
        storage: S,
        req: Request<Body>,
        key: StorageKey,
    ) -> Result<Response<Body>, Error> {
        let len = req
            .headers()
            .get("Content-Length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        let len = match len {
            Some(len) => len,
            None => {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("Invalid Content-Length header."))
                    .map_err(Into::into);
            }
        };

        // Verify the SHA256 of the uploaded object as it is being uploaded.
        let stream = req
            .into_body()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e));

        let object = LFSObject::new(len, Box::pin(stream));

        storage.put(key, object).await?;

        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())?)
    }

    /// Verifies that an LFS object exists on the server.
    async fn verify(
        storage: S,
        req: Request<Body>,
        namespace: Namespace,
    ) -> Result<Response<Body>, Error> {
        let val: lfs::VerifyRequest = from_json(req.into_body()).await?;
        let key = StorageKey::new(namespace, val.oid);

        if let Some(size) = storage.size(&key).await? {
            if size == val.size {
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::empty())?);
            }
        }

        // Object doesn't exist or the size is incorrect.
        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())?)
    }

    /// Batch API endpoint for the Git LFS server spec.
    ///
    /// See also:
    /// https://github.com/git-lfs/git-lfs/blob/master/docs/api/batch.md
    async fn batch(
        storage: S,
        req: Request<Body>,
        namespace: Namespace,
    ) -> Result<Response<Body>, Error> {
        // Get the host name and scheme.
        let uri = req.base_uri().path_and_query("/").build().unwrap();

        match from_json::<lfs::BatchRequest>(req.into_body()).await {
            Ok(val) => {
                let operation = val.operation;

                // For each object, check if it exists in the storage
                // backend.
                let objects = val.objects.into_iter().map(|object| {
                    let uri = uri.clone();
                    let key = StorageKey::new(namespace.clone(), object.oid);

                    async {
                        let size = storage.size(&key).await;

                        let (namespace, _) = key.into_parts();
                        Ok(basic_response(
                            uri, &storage, object, operation, size, namespace,
                        ))
                    }
                });

                let objects = future::try_join_all(objects).await?;
                let response = lfs::BatchResponse {
                    transfer: Some(lfs::Transfer::Basic),
                    objects,
                };

                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(into_json(&response)?)?)
            }
            Err(err) => {
                let response = lfs::BatchResponseError {
                    message: err.to_string(),
                    documentation_url: None,
                    request_id: None,
                };

                Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(into_json(&response).unwrap())?)
            }
        }
    }
}

fn basic_response<E, S>(
    uri: Uri,
    storage: &S,
    object: lfs::RequestObject,
    op: lfs::Operation,
    size: Result<Option<u64>, E>,
    namespace: Namespace,
) -> lfs::ResponseObject
where
    E: fmt::Display,
    S: Storage,
{
    if let Ok(Some(size)) = size {
        // Ensure that the client and server agree on the size of the object.
        if object.size != size {
            return lfs::ResponseObject {
                oid: object.oid,
                size,
                error: Some(lfs::ObjectError {
                    code: 400,
                    message: format!(
                        "bad object size: requested={}, actual={}",
                        object.size, size
                    ),
                }),
                authenticated: Some(true),
                actions: None,
            };
        }
    }

    let size = match size {
        Ok(size) => size,
        Err(err) => {
            log::error!("batch response error: {}", err);

            // Return a generic "500 - Internal Server Error" for objects that
            // we failed to get the size of. This is usually caused by some
            // intermittent problem on the storage backend. A retry strategy
            // should be implemented on the storage backend to help mitigate
            // this possibility because the git-lfs client does not currenty
            // implement retries in this case.
            return lfs::ResponseObject {
                oid: object.oid,
                size: object.size,
                error: Some(lfs::ObjectError {
                    code: 500,
                    message: err.to_string(),
                }),
                authenticated: Some(true),
                actions: None,
            };
        }
    };

    let href = storage
        .public_url(&StorageKey::new(namespace.clone(), object.oid))
        .unwrap_or_else(|| {
            format!("{}api/{}/object/{}", uri, namespace, object.oid)
        });

    let action = lfs::Action {
        href,
        header: None,
        expires_in: None,
        expires_at: None,
    };

    match op {
        lfs::Operation::Upload => {
            // If the object does exist, then we should not return any action.
            //
            // If the object does not exist, then we should return an upload
            // action.
            match size {
                Some(size) => lfs::ResponseObject {
                    oid: object.oid,
                    size,
                    error: None,
                    authenticated: Some(true),
                    actions: None,
                },
                None => lfs::ResponseObject {
                    oid: object.oid,
                    size: object.size,
                    error: None,
                    authenticated: Some(true),
                    actions: Some(lfs::Actions {
                        download: None,
                        upload: Some(action),
                        verify: Some(lfs::Action {
                            href: format!(
                                "{}api/{}/objects/verify",
                                uri, namespace
                            ),
                            header: None,
                            expires_in: None,
                            expires_at: None,
                        }),
                    }),
                },
            }
        }
        lfs::Operation::Download => {
            // If the object does not exist, then we should return a 404 error
            // for this object.
            match size {
                Some(size) => lfs::ResponseObject {
                    oid: object.oid,
                    size,
                    error: None,
                    authenticated: Some(true),
                    actions: Some(lfs::Actions {
                        download: Some(action),
                        upload: None,
                        verify: None,
                    }),
                },
                None => lfs::ResponseObject {
                    oid: object.oid,
                    size: object.size,
                    error: Some(lfs::ObjectError {
                        code: 404,
                        message: "object not found".into(),
                    }),
                    authenticated: Some(true),
                    actions: None,
                },
            }
        }
    }
}

impl<S> Service<Request<Body>> for App<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    S::Error: Into<Error> + 'static,
    Error: From<S::Error>,
{
    type Response = Response<Body>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut Context,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        if req.uri().path() == "/" {
            Box::pin(future::ready(Self::index(req)))
        } else if req.uri().path().starts_with("/api/") {
            Box::pin(Self::api(self.storage.clone(), req))
        } else {
            Box::pin(future::ready(Self::not_found(req)))
        }
    }
}
