// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::proto::{AuthenticateRequest as ProtoAuthenticateRequest, AuthenticateResponse};
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug, Clone)]
pub struct AuthenticateRequest {
    pub(crate) inner_request: ProtoAuthenticateRequest,
}

impl AuthenticateRequest {
    /// Build a SASL/PLAIN authenticate request acting as the authenticated user itself.
    /// Token format: `\0<username>\0<password>` (NUL-separated UTF-8).
    pub fn new_plain(username: &str, password: &str) -> Self {
        Self::new_plain_with_authorization_id("", username, password)
    }

    /// Build a SASL/PLAIN authenticate request carrying an authorization id (RFC 4616:
    /// `<authzid>\0<authcid>\0<passwd>`), i.e. the identity to impersonate.
    ///
    /// The server permits impersonation only when the authenticated user is granted it via the
    /// server-side JAAS `impersonate_<username>` option; an empty `authorization_id` means the
    /// client acts as the authenticated user itself.
    pub fn new_plain_with_authorization_id(
        authorization_id: &str,
        username: &str,
        password: &str,
    ) -> Self {
        let mut token =
            Vec::with_capacity(authorization_id.len() + 1 + username.len() + 1 + password.len());
        token.extend_from_slice(authorization_id.as_bytes());
        token.push(0u8);
        token.extend_from_slice(username.as_bytes());
        token.push(0u8);
        token.extend_from_slice(password.as_bytes());

        Self {
            inner_request: ProtoAuthenticateRequest {
                protocol: "PLAIN".to_string(),
                token,
            },
        }
    }

    /// Build an authenticate request from a server challenge (for multi-round auth).
    pub fn from_challenge(protocol: &str, challenge: Vec<u8>) -> Self {
        Self {
            inner_request: ProtoAuthenticateRequest {
                protocol: protocol.to_string(),
                token: challenge,
            },
        }
    }
}

impl RequestBody for AuthenticateRequest {
    type ResponseBody = AuthenticateResponse;
    const API_KEY: ApiKey = ApiKey::Authenticate;
}

impl_write_type!(AuthenticateRequest);
impl_read_type!(AuthenticateResponse);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_plain_token_format() {
        let req = AuthenticateRequest::new_plain("admin", "secret");
        assert_eq!(req.inner_request.protocol, "PLAIN");
        assert_eq!(req.inner_request.token, b"\0admin\0secret");
    }

    #[test]
    fn test_new_plain_empty_credentials() {
        let req = AuthenticateRequest::new_plain("", "");
        assert_eq!(req.inner_request.token, b"\0\0");
    }

    #[test]
    fn test_authorization_id_prefixes_the_token() {
        // RFC 4616: `authzid \0 authcid \0 passwd`. The server acts as `alice` after
        // authenticating the connection as `gateway_svc`.
        let req =
            AuthenticateRequest::new_plain_with_authorization_id("alice", "gateway_svc", "secret");
        assert_eq!(req.inner_request.protocol, "PLAIN");
        assert_eq!(req.inner_request.token, b"alice\0gateway_svc\0secret");
    }

    #[test]
    fn test_empty_authorization_id_is_the_self_identity_form() {
        // An empty authzid must produce exactly the token `new_plain` produces, so the
        // server-side self-identity path is byte-identical with and without the new API.
        let explicit = AuthenticateRequest::new_plain_with_authorization_id("", "admin", "secret");
        let implicit = AuthenticateRequest::new_plain("admin", "secret");
        assert_eq!(explicit.inner_request.token, implicit.inner_request.token);
        assert_eq!(explicit.inner_request.token, b"\0admin\0secret");
    }

    #[test]
    fn test_authorization_id_supports_utf8_identities() {
        let req = AuthenticateRequest::new_plain_with_authorization_id("用户1", "svc", "pw");
        assert_eq!(req.inner_request.token, "用户1\0svc\0pw".as_bytes());
    }
}
