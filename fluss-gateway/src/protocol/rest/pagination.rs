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

//! Self-contained keyset pagination tokens for catalog collections.
//!
//! Fluss has no server-side pagination for its listing RPCs, so the gateway paginates client-side: it fetches the
//! full sorted name list on every page and returns the first `page_size` names strictly greater than the token's
//! last-seen name. Keyset semantics mean no duplicates and no skipped pre-existing entries under concurrent DDL.
//!
//! The token is **entirely self-describing**: base64url (unpadded) of a compact JSON object. It carries no
//! server-side handle, so any gateway instance serves any page of any collection with no shared state. The
//! payload is pinned per resource:
//!
//! | Collection | Payload |
//! |---|---|
//! | databases  | `{"v":1,"k":"databases","after":"<last database>"}` |
//! | tables     | `{"v":1,"k":"tables","db":"<database>","after":"<last table>"}` |
//! | partitions | `{"v":1,"k":"partitions","db":"<database>","t":"<table>","after":"<last partition>"}` |
//!
//! A token whose version, kind, or scope does not match the endpoint it was presented to is rejected as an
//! invalid argument (HTTP 400), never silently reinterpreted.

use crate::backend::model::TableRef;
use crate::error::GatewayError;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

/// Payload version of every token this build emits and accepts.
pub const PAGE_TOKEN_VERSION: u8 = 1;

/// Which catalog collection a page token belongs to, including its containing resources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageScope {
    /// `GET /v1/clusters/{cluster}/databases`.
    Databases,
    /// `GET /v1/clusters/{cluster}/databases/{database}/tables`.
    Tables { database: String },
    /// `GET /v1/clusters/{cluster}/databases/{database}/tables/{table}/partitions`.
    Partitions { database: String, table: String },
}

impl PageScope {
    /// Scope for the table listing of one database.
    pub fn tables(database: impl Into<String>) -> Self {
        Self::Tables {
            database: database.into(),
        }
    }

    /// Scope for the partition listing of one table.
    pub fn partitions(table: &TableRef) -> Self {
        Self::Partitions {
            database: table.database.clone(),
            table: table.table.clone(),
        }
    }

    /// Stable collection discriminator stored in the token as `k`.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Databases => "databases",
            Self::Tables { .. } => "tables",
            Self::Partitions { .. } => "partitions",
        }
    }

    fn database(&self) -> Option<&str> {
        match self {
            Self::Databases => None,
            Self::Tables { database } | Self::Partitions { database, .. } => Some(database),
        }
    }

    fn table(&self) -> Option<&str> {
        match self {
            Self::Databases | Self::Tables { .. } => None,
            Self::Partitions { table, .. } => Some(table),
        }
    }
}

/// Wire payload of a page token. Field names are single letters to keep tokens short.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageTokenPayload {
    v: u8,
    k: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    db: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    t: Option<String>,
    after: String,
}

/// Encodes the continuation token for a page whose last returned entry was `after`.
pub fn encode_page_token(scope: &PageScope, after: &str) -> Result<String, GatewayError> {
    if after.is_empty() {
        return Err(GatewayError::internal(
            "cannot build a page token from an empty last entry",
        ));
    }
    let payload = PageTokenPayload {
        v: PAGE_TOKEN_VERSION,
        k: scope.kind().to_string(),
        db: scope.database().map(str::to_string),
        t: scope.table().map(str::to_string),
        after: after.to_string(),
    };
    let json = serde_json::to_vec(&payload)
        .map_err(|error| GatewayError::internal(format!("failed to encode page token: {error}")))?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

/// Decodes a continuation token and returns the last-seen entry it carries.
///
/// The token must belong to exactly the collection identified by `scope`; a mismatch in version, kind, database,
/// or table is rejected rather than reinterpreted.
pub fn decode_page_token(token: &str, scope: &PageScope) -> Result<String, GatewayError> {
    let malformed =
        || GatewayError::invalid_argument("`page_token` is not a valid opaque metadata token");
    let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| malformed())?;
    let payload: PageTokenPayload = serde_json::from_slice(&bytes).map_err(|_| malformed())?;

    let mismatch = || {
        GatewayError::invalid_argument("`page_token` does not belong to this metadata collection")
    };
    if payload.v != PAGE_TOKEN_VERSION {
        return Err(mismatch());
    }
    if payload.k != scope.kind() {
        return Err(mismatch());
    }
    if payload.db.as_deref() != scope.database() {
        return Err(mismatch());
    }
    if payload.t.as_deref() != scope.table() {
        return Err(mismatch());
    }
    if payload.after.is_empty() {
        return Err(malformed());
    }
    Ok(payload.after)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    fn decode_payload(token: &str) -> serde_json::Value {
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(token).unwrap()).unwrap()
    }

    #[test]
    fn payloads_are_pinned_per_resource() {
        let databases = encode_page_token(&PageScope::Databases, "fluss").unwrap();
        assert_eq!(
            decode_payload(&databases),
            serde_json::json!({"v": 1, "k": "databases", "after": "fluss"})
        );

        let tables = encode_page_token(&PageScope::tables("fluss"), "users").unwrap();
        assert_eq!(
            decode_payload(&tables),
            serde_json::json!({"v": 1, "k": "tables", "db": "fluss", "after": "users"})
        );

        let partitions = encode_page_token(
            &PageScope::partitions(&TableRef::new("fluss", "orders")),
            "eu",
        )
        .unwrap();
        assert_eq!(
            decode_payload(&partitions),
            serde_json::json!({
                "v": 1, "k": "partitions", "db": "fluss", "t": "orders", "after": "eu"
            })
        );
    }

    #[test]
    fn tokens_round_trip_within_their_own_scope() {
        for (scope, after) in [
            (PageScope::Databases, "fluss"),
            (PageScope::tables("fluss"), "users"),
            (
                PageScope::partitions(&TableRef::new("fluss", "orders")),
                "eu$2024",
            ),
        ] {
            let token = encode_page_token(&scope, after).unwrap();
            assert_eq!(decode_page_token(&token, &scope).unwrap(), after);
        }
    }

    #[test]
    fn tokens_are_url_safe_and_unpadded() {
        let token = encode_page_token(&PageScope::tables("f?f/f"), "a+b/c=d").unwrap();
        assert!(
            token
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "{token}"
        );
        assert_eq!(
            decode_page_token(&token, &PageScope::tables("f?f/f")).unwrap(),
            "a+b/c=d"
        );
    }

    #[test]
    fn a_token_from_another_collection_is_rejected() {
        let token = encode_page_token(&PageScope::tables("fluss"), "users").unwrap();
        for wrong in [
            PageScope::Databases,
            PageScope::tables("other"),
            PageScope::partitions(&TableRef::new("fluss", "users")),
        ] {
            let error = decode_page_token(&token, &wrong).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidArgument);
            assert!(
                error.message().contains("does not belong"),
                "{}",
                error.message()
            );
        }
    }

    #[test]
    fn a_token_from_another_version_is_rejected() {
        let payload = serde_json::json!({"v": 2, "k": "databases", "after": "fluss"});
        let token = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap());
        assert_eq!(
            decode_page_token(&token, &PageScope::Databases)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidArgument
        );
    }

    #[test]
    fn malformed_tokens_are_rejected_as_invalid_arguments() {
        for token in [
            "",
            "!!!not base64!!!",
            &URL_SAFE_NO_PAD.encode(b"not json"),
            &URL_SAFE_NO_PAD.encode(serde_json::to_vec(&serde_json::json!({"v": 1})).unwrap()),
            &URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(
                    &serde_json::json!({"v": 1, "k": "databases", "after": "x", "extra": 1}),
                )
                .unwrap(),
            ),
            &URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&serde_json::json!({"v": 1, "k": "databases", "after": ""}))
                    .unwrap(),
            ),
        ] {
            let error = decode_page_token(token, &PageScope::Databases).unwrap_err();
            assert_eq!(
                error.kind(),
                ErrorKind::InvalidArgument,
                "accepted {token:?}"
            );
        }
    }

    #[test]
    fn an_empty_last_entry_is_an_internal_error_not_a_client_error() {
        assert_eq!(
            encode_page_token(&PageScope::Databases, "")
                .unwrap_err()
                .kind(),
            ErrorKind::Internal
        );
    }
}
