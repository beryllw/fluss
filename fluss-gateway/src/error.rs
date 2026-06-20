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

//! Gateway domain error classification.
//!
//! Single internal error taxonomy that backend / fluss-datafusion / auth /
//! validation errors map INTO, and that protocol boundaries map OUT of. The
//! domain error carries ONLY business semantics: it never contains a PG error
//! code, an HTTP status, or a gRPC status — those are the responsibility of the
//! protocol adapter at the boundary layer.
//! Design: `design/core-session.md` and `DESIGN.md` §2.

use thiserror::Error;

/// Unified gateway domain error.
///
/// Variants express domain semantics only; the protocol layer is responsible
/// for mapping these into protocol-specific error formats (e.g. PG error code,
/// HTTP status). See `design/direct-path.md` for the REST mapping table.
#[derive(Debug, Error)]
pub enum GatewayError {
    /// Caller-supplied input was malformed or semantically invalid.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The caller could not be authenticated.
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),

    /// The caller is authenticated but not permitted to perform the action.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// No session exists for the given id (closed, expired, or never opened).
    #[error("session not found: {0}")]
    SessionNotFound(String),

    /// No operation exists for the given id.
    #[error("operation not found: {0}")]
    OperationNotFound(String),

    /// The referenced database does not exist.
    #[error("database not found: {database}")]
    DatabaseNotFound { database: String },

    /// The referenced table does not exist.
    #[error("table not found: {database}.{table}")]
    TableNotFound { database: String, table: String },

    /// A table to be created already exists.
    #[error("table already exists: {database}.{table}")]
    TableAlreadyExists { database: String, table: String },

    /// The requested capability is not supported by this build.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// A gateway-level deadline elapsed before the work completed.
    #[error("timed out: {0}")]
    Timeout(String),

    /// The work was cancelled cooperatively (cancel request / client disconnect).
    #[error("cancelled: {0}")]
    Cancelled(String),

    /// A backend / Fluss-access failure surfaced into the gateway.
    #[error("backend error: {0}")]
    Backend(String),

    /// An unexpected internal failure that does not fit another variant.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Convenience alias for fallible gateway operations.
pub type GatewayResult<T> = Result<T, GatewayError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variants_construct_and_render() {
        let e = GatewayError::InvalidArgument("bad limit".into());
        assert!(e.to_string().contains("bad limit"));

        let e = GatewayError::TableNotFound {
            database: "db".into(),
            table: "t".into(),
        };
        assert_eq!(e.to_string(), "table not found: db.t");

        let e = GatewayError::DatabaseNotFound {
            database: "db".into(),
        };
        assert!(e.to_string().contains("db"));
    }

    #[test]
    fn result_alias_is_usable() {
        fn ok() -> GatewayResult<u8> {
            Ok(7)
        }
        fn err() -> GatewayResult<u8> {
            Err(GatewayError::Cancelled("client gone".into()))
        }
        assert_eq!(ok().unwrap(), 7);
        assert!(matches!(err(), Err(GatewayError::Cancelled(_))));
    }
}
