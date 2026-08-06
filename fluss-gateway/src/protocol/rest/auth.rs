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

//! HTTP Basic authentication adapter for the REST protocol layer.
//!
//! This module owns exactly one job: translating the `Authorization` header into a neutral
//! [`ClientCredential`] and running it through the configured [`crate::auth::Authenticator`]. It
//! deliberately knows nothing about authentication *modes*; the mode semantics live entirely in
//! the authenticator implementations:
//!
//! - no header → [`ClientCredential::Trust`] with the [`ANONYMOUS_USERNAME`]. The trust
//!   authenticator accepts it (frictionless local use), a password store rejects it with 401.
//! - `Basic user:pass` → [`ClientCredential::Password`]. The trust authenticator takes the
//!   username at face value and ignores the password — matching the FIP's `curl -u alice:ignored`
//!   examples — while a password store verifies it.
//! - a malformed header (unknown scheme, bad base64, no colon, non-UTF-8) → 401 immediately; a
//!   broken credential is an error, never an identity claim.
//!
//! Every 401 carries the shared error envelope plus a `WWW-Authenticate` challenge.

use crate::auth::{ClientCredential, Secret};
use crate::error::GatewayError;
use crate::protocol::rest::{RequestId, RestState, error_response};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine;

/// The username recorded when a request carries no credential at all.
///
/// The gateway fabricates this identity (no client claimed it), which is acceptable under trust
/// mode only; identity-propagating deployments must not combine `identity-mode: user` with trust
/// authentication, and the configuration layer enforces that when user mode lands.
pub const ANONYMOUS_USERNAME: &str = "anonymous";

/// The challenge issued with every 401, per RFC 7617.
pub const WWW_AUTHENTICATE_CHALLENGE: &str = "Basic realm=\"fluss-gateway\"";

/// Translates the `Authorization` header into a neutral credential.
///
/// Absence is an anonymous trust claim; presence must be a well-formed `Basic` credential. The
/// error message never echoes header contents.
pub(crate) fn credential_from_headers(
    headers: &HeaderMap,
) -> Result<ClientCredential, GatewayError> {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Ok(ClientCredential::Trust {
            username: ANONYMOUS_USERNAME.to_string(),
        });
    };
    let malformed = || GatewayError::unauthenticated("invalid Authorization header");
    let value = value.to_str().map_err(|_| malformed())?;
    let (scheme, encoded) = value.split_once(' ').ok_or_else(malformed)?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return Err(malformed());
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .map_err(|_| malformed())?;
    let decoded = String::from_utf8(decoded).map_err(|_| malformed())?;
    let (username, password) = decoded.split_once(':').ok_or_else(malformed)?;
    Ok(ClientCredential::Password {
        username: username.to_string(),
        secret: Secret::new(password),
    })
}

/// Middleware guarding the data and control planes: resolves the caller's
/// [`crate::auth::Principal`] and makes it available to handlers as a request extension, or
/// answers 401 with a challenge.
pub(crate) async fn require_authentication(
    State(state): State<RestState>,
    mut request: Request,
    next: Next,
) -> Response {
    let credential = match credential_from_headers(request.headers()) {
        Ok(credential) => credential,
        Err(error) => return challenge(&error, &request),
    };
    match state.authenticator.authenticate(credential).await {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(auth_error) => challenge(&GatewayError::from(auth_error), &request),
    }
}

/// Shapes one authentication failure: the shared envelope plus the `WWW-Authenticate` challenge.
fn challenge(error: &GatewayError, request: &Request) -> Response {
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .cloned()
        .unwrap_or_default();
    let mut response = error_response(error, &request_id);
    if response.status() == axum::http::StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static(WWW_AUTHENTICATE_CHALLENGE),
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with_authorization(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn no_header_is_an_anonymous_trust_claim() {
        let credential = credential_from_headers(&HeaderMap::new()).unwrap();
        assert_eq!(
            credential,
            ClientCredential::Trust {
                username: ANONYMOUS_USERNAME.to_string()
            }
        );
    }

    #[test]
    fn a_basic_header_becomes_a_password_credential() {
        let encoded = base64::engine::general_purpose::STANDARD.encode("alice:s3cret");
        for scheme in ["Basic", "basic", "BASIC"] {
            let headers = headers_with_authorization(&format!("{scheme} {encoded}"));
            let credential = credential_from_headers(&headers).unwrap();
            let ClientCredential::Password { username, secret } = credential else {
                panic!("expected a password credential");
            };
            assert_eq!(username, "alice");
            assert_eq!(secret.expose(), "s3cret");
        }
    }

    #[test]
    fn a_password_may_itself_contain_colons() {
        let encoded = base64::engine::general_purpose::STANDARD.encode("alice:pa:ss");
        let headers = headers_with_authorization(&format!("Basic {encoded}"));
        let ClientCredential::Password { secret, .. } = credential_from_headers(&headers).unwrap()
        else {
            panic!("expected a password credential");
        };
        assert_eq!(secret.expose(), "pa:ss");
    }

    #[test]
    fn malformed_headers_are_rejected_without_echoing_contents() {
        let no_colon = base64::engine::general_purpose::STANDARD.encode("no-colon-here");
        for value in [
            "Bearer whatever",            // unknown scheme
            "Basic !!!not-base64!!!",     // invalid base64
            "Basic",                      // no payload
            &format!("Basic {no_colon}"), // no username:password separator
            "Basic /w==",                 // valid base64, invalid UTF-8 (0xFF)
        ] {
            let headers = headers_with_authorization(value);
            let error = credential_from_headers(&headers).unwrap_err();
            assert_eq!(
                error.kind(),
                crate::error::ErrorKind::Unauthenticated,
                "{value}"
            );
            assert!(!error.message().contains("not-base64"), "{value}");
        }
    }
}
