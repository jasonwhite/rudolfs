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
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::sha256::Sha256;

pub type Oid = Sha256;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Upload,
    Download,
}

/// A transfer adaptor.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Transfer {
    /// Basic transfer adapter.
    Basic,

    LfsStandaloneFile,

    /// Catch-all for custom transfer adapters.
    #[serde(other)]
    Custom,
}

/// An LFS object in a request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestObject {
    /// String OID of the LFS object.
    pub oid: Oid,

    /// Integer byte size of the LFS object.
    pub size: u64,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Action {
    /// URL to hit for the object.
    pub href: String,

    /// Optional hash of string HTTP header key/value pairs to apply to the
    /// request.
    pub header: Option<BTreeMap<String, String>>,

    /// Whole number of seconds after local client time when transfer will
    /// expire. Preferred over `expires_at` if both are provided.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_in: Option<i32>,

    /// String ISO 8601 formatted timestamp for when the given action expires
    /// (usually due to a temporary token).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectError {
    /// HTTP response code.
    pub code: u32,

    /// Error message.
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Actions {
    /// A download action. The action href is to receive a GET request. The
    /// response contains the LFS object data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download: Option<Action>,

    /// An upload action. The action href is to receive a PUT request with the
    /// LFS object data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload: Option<Action>,

    /// A verify action. The action href is to receive a POST request after
    /// a successful upload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verify: Option<Action>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseObject {
    /// String OID of the LFS object.
    pub oid: Oid,

    /// Integer byte size of the LFS object.
    pub size: u64,

    /// An error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ObjectError>,

    /// Optional boolean specifying whether the request for this specific
    /// object is authenticated. If ommitted or `false`, Git LFS will
    /// attempt to find credentials for this URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authenticated: Option<bool>,

    /// Object containing the next actions for this object. Applicable actions
    /// depend on which `operation` is specified in the request. How these
    /// properties are interpreted depends on which transfer adapter the client
    /// will be using.
    ///
    /// This will be `None` if there are no actions to perform. This will be
    /// the case for upload operations if the server already has the
    /// objects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actions: Option<Actions>,
}

/// A batch request.
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchRequest {
    /// The operation being performed.
    pub operation: Operation,

    /// An optional array of string identifiers for transfer adapters that the
    /// client has configured. If ommitted, the `basic` transfer adaptor *must*
    /// be assumed by the server.
    ///
    /// Note: Git LFS currently only supports the `basic` transfer adapter.
    /// This property was added for future compatibility with some experimental
    /// tranfer adapters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfers: Option<Vec<Transfer>>,

    /// Optional object describing the server ref that the objects belong to.
    ///
    /// Note: Added in v2.4.
    #[serde(rename = "ref")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refs: Option<BTreeMap<String, String>>,

    /// An array of objects to upload/download.
    pub objects: Vec<RequestObject>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchResponse {
    /// String identifier of the transfer adapter that the server prefers. This
    /// *must* be one of the given `transfer` identifiers from the request.
    /// Servers can assume the `basic` transfer adaptor if `None` was given.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer: Option<Transfer>,

    /// An array of objects to download or upload.
    pub objects: Vec<ResponseObject>,
}

/// An error response.
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchResponseError {
    /// The error message.
    pub message: String,

    /// Optional string to give the user a place to report errors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documentation_url: Option<String>,

    /// Optional string unique identifier for the request. Useful for
    /// debugging.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// A request to verify an LFS object.
#[derive(Debug, Serialize, Deserialize)]
pub struct VerifyRequest {
    /// Object ID.
    pub oid: Oid,

    /// Size of the object. If this doesn't match the size of the real object,
    /// then an error code should be returned.
    pub size: u64,
}
