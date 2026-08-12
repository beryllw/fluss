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

//! Gateway error taxonomy and the REST error envelope.
//!
//! [`ErrorKind`] represents client-visible failure conditions independently of the HTTP framework. The REST adapter
//! obtains each status code from [`ErrorKind::http_status`].
//!
//! The taxonomy is deliberately closed at fifteen kinds. There is no `GONE` or `CURSOR_NOT_LOCAL` because the
//! gateway holds no cursors. The gateway applies no request rate limiting — the only per-request bounds are
//! input-validation caps, surfacing as [`ErrorKind::LimitExceeded`] (413) or
//! [`ErrorKind::InvalidArgument`] (400) — but per-user act-as connections are a bounded resource, so
//! [`ErrorKind::ResourceExhausted`] (429, with a `Retry-After` header per FIP-49) reports connection-capacity
//! exhaustion under the user identity mode.
//!
//! FIP-49 error-model notes: the FIP's `database_not_empty` (409) condition is carried by
//! [`ErrorKind::FailedPrecondition`], and its `*_not_found` / `*_already_exists` families collapse onto
//! [`ErrorKind::NotFound`] / [`ErrorKind::AlreadyExists`] with the resource named in
//! [`ErrorDetails`], keeping one stable code per condition kind.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Client-visible condition kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// The request contains malformed input, an invalid identifier, or a type mismatch. Maps to HTTP 400.
    InvalidArgument,
    /// The request carries no usable credential, or the credential failed verification. Maps to HTTP 401.
    Unauthenticated,
    /// The authenticated principal is not allowed to perform the operation. Maps to HTTP 403.
    Unauthorized,
    /// The requested database, table, or partition does not exist. Maps to HTTP 404.
    NotFound,
    /// A create operation conflicts with an existing resource. Maps to HTTP 409.
    AlreadyExists,
    /// Current resource state prevents the requested operation. Maps to HTTP 409.
    FailedPrecondition,
    /// The operation or table format is not supported. Maps to HTTP 501.
    Unsupported,
    /// The request media type is not supported. Maps to HTTP 415.
    UnsupportedMediaType,
    /// The `Accept` header does not allow a supported response type. Maps to HTTP 406.
    NotAcceptable,
    /// The request exceeds a configured input-validation size limit. Maps to HTTP 413.
    LimitExceeded,
    /// A bounded resource (per-user act-as connections) is at capacity. Maps to HTTP 429.
    ResourceExhausted,
    /// The request exceeded its deadline. Maps to HTTP 504.
    DeadlineExceeded,
    /// Work was cancelled by the caller or by shutdown. Maps to HTTP 499.
    Cancelled,
    /// The backend is unavailable or the gateway is not ready. Maps to HTTP 503.
    Unavailable,
    /// The Fluss backend failed in a way the gateway cannot classify further. Maps to HTTP 500
    /// with the FIP-49 `backend` code, distinguishable from a gateway-internal failure.
    Backend,
    /// An unexpected internal failure occurred. Maps to HTTP 500.
    Internal,
}

impl ErrorKind {
    /// Every kind in declaration order.
    ///
    /// Kept in sync with the enum by [`ErrorKind::ordinal`], whose exhaustive match stops compiling when a
    /// variant is added without extending this table.
    pub const ALL: [ErrorKind; 16] = [
        ErrorKind::InvalidArgument,
        ErrorKind::Unauthenticated,
        ErrorKind::Unauthorized,
        ErrorKind::NotFound,
        ErrorKind::AlreadyExists,
        ErrorKind::FailedPrecondition,
        ErrorKind::Unsupported,
        ErrorKind::UnsupportedMediaType,
        ErrorKind::NotAcceptable,
        ErrorKind::LimitExceeded,
        ErrorKind::ResourceExhausted,
        ErrorKind::DeadlineExceeded,
        ErrorKind::Cancelled,
        ErrorKind::Unavailable,
        ErrorKind::Backend,
        ErrorKind::Internal,
    ];

    /// Position of this kind within [`ErrorKind::ALL`].
    pub fn ordinal(self) -> usize {
        match self {
            ErrorKind::InvalidArgument => 0,
            ErrorKind::Unauthenticated => 1,
            ErrorKind::Unauthorized => 2,
            ErrorKind::NotFound => 3,
            ErrorKind::AlreadyExists => 4,
            ErrorKind::FailedPrecondition => 5,
            ErrorKind::Unsupported => 6,
            ErrorKind::UnsupportedMediaType => 7,
            ErrorKind::NotAcceptable => 8,
            ErrorKind::LimitExceeded => 9,
            ErrorKind::ResourceExhausted => 10,
            ErrorKind::DeadlineExceeded => 11,
            ErrorKind::Cancelled => 12,
            ErrorKind::Unavailable => 13,
            ErrorKind::Backend => 14,
            ErrorKind::Internal => 15,
        }
    }

    /// Stable machine-readable code carried in the error envelope, for example `not_found`.
    pub fn code(self) -> &'static str {
        match self {
            ErrorKind::InvalidArgument => "invalid_argument",
            ErrorKind::Unauthenticated => "unauthenticated",
            ErrorKind::Unauthorized => "unauthorized",
            ErrorKind::NotFound => "not_found",
            ErrorKind::AlreadyExists => "already_exists",
            ErrorKind::FailedPrecondition => "failed_precondition",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::UnsupportedMediaType => "unsupported_media_type",
            ErrorKind::NotAcceptable => "not_acceptable",
            ErrorKind::LimitExceeded => "limit_exceeded",
            ErrorKind::ResourceExhausted => "resource_exhausted",
            ErrorKind::DeadlineExceeded => "timeout",
            ErrorKind::Cancelled => "cancelled",
            ErrorKind::Unavailable => "unavailable",
            ErrorKind::Backend => "backend",
            ErrorKind::Internal => "internal",
        }
    }

    /// The REST HTTP mapping table.
    ///
    /// Kept as a plain `u16` so this module stays free of HTTP framework types. The REST adapter converts to its own
    /// status type.
    pub fn http_status(self) -> u16 {
        match self {
            ErrorKind::InvalidArgument => 400,
            ErrorKind::Unauthenticated => 401,
            ErrorKind::Unauthorized => 403,
            ErrorKind::NotFound => 404,
            ErrorKind::AlreadyExists | ErrorKind::FailedPrecondition => 409,
            ErrorKind::Unsupported => 501,
            ErrorKind::UnsupportedMediaType => 415,
            ErrorKind::NotAcceptable => 406,
            ErrorKind::LimitExceeded => 413,
            ErrorKind::ResourceExhausted => 429,
            ErrorKind::DeadlineExceeded => 504,
            ErrorKind::Cancelled => 499,
            ErrorKind::Unavailable => 503,
            ErrorKind::Backend | ErrorKind::Internal => 500,
        }
    }

    /// Whether repeating an otherwise unchanged request may succeed.
    ///
    /// This is the default for a kind. A native failure whose `FlussError::is_retriable()` disagrees overrides it
    /// per error through [`GatewayError::with_retryable`].
    pub fn default_retryable(self) -> bool {
        match self {
            ErrorKind::DeadlineExceeded | ErrorKind::Unavailable | ErrorKind::ResourceExhausted => {
                true
            }
            ErrorKind::InvalidArgument
            | ErrorKind::Unauthenticated
            | ErrorKind::Unauthorized
            | ErrorKind::NotFound
            | ErrorKind::AlreadyExists
            | ErrorKind::FailedPrecondition
            | ErrorKind::Unsupported
            | ErrorKind::UnsupportedMediaType
            | ErrorKind::NotAcceptable
            | ErrorKind::LimitExceeded
            | ErrorKind::Cancelled
            | ErrorKind::Backend
            | ErrorKind::Internal => false,
        }
    }
}

/// Gateway-internal error: a condition kind plus a client-safe message.
///
/// Messages must never contain stack traces, internal addresses, or wire payloads. Operational detail belongs in
/// the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayError {
    kind: ErrorKind,
    message: String,
    details: Option<ErrorDetails>,
    /// Overrides [`ErrorKind::default_retryable`] when the native layer knows better.
    retryable: Option<bool>,
}

/// Optional protocol-neutral structured context for a public error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_name: Option<String>,
}

impl GatewayError {
    /// The message reaches the client verbatim, so keep it free of internal detail.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            details: None,
            retryable: None,
        }
    }

    /// A malformed or rejected request argument. Answered with HTTP 400.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, message)
    }

    /// A request without a usable credential, or whose credential failed verification. Answered with HTTP 401.
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unauthenticated, message)
    }

    /// An operation the authenticated principal is not allowed to perform. Answered with HTTP 403.
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unauthorized, message)
    }

    /// A named database, table, or partition that does not exist. Answered with HTTP 404.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }

    /// A create operation targeting a resource that already exists.
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::AlreadyExists, message)
    }

    /// An operation rejected because the current resource state does not permit it.
    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::FailedPrecondition, message)
    }

    /// An operation or table format the gateway does not implement. Answered with HTTP 501.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported, message)
    }

    /// A request-size or configured input-validation limit was exceeded. Answered with HTTP 413.
    pub fn limit_exceeded(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::LimitExceeded, message)
    }

    /// A bounded resource, such as the per-user act-as connection pool, is at capacity.
    /// Answered with HTTP 429 and a `Retry-After` header.
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::ResourceExhausted, message)
    }

    /// The request ran past its deadline. Answered with HTTP 504.
    pub fn deadline_exceeded(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::DeadlineExceeded, message)
    }

    /// Work cancelled by its caller or by gateway shutdown.
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Cancelled, message)
    }

    /// Creates a transient backend-unavailable error.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unavailable, message)
    }

    /// A Fluss backend failure the gateway cannot classify further. Answered with HTTP 500 and the
    /// FIP-49 `backend` code, so callers can tell it from a gateway-internal failure.
    pub fn backend(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Backend, message)
    }

    /// An unexpected failure with no better classification. Answered with HTTP 500 and logged.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message)
    }

    /// The condition this error represents, which decides the HTTP status and the envelope code.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the safe client-facing message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Stable code carried in the error envelope.
    ///
    /// Per FIP-49 the vocabulary is resource-specific where a resource is known: an error whose
    /// kind names a resource and that carries machine-readable resource context answers
    /// `database_not_found`, `table_already_exists`, `database_not_empty`, and so on. `cluster`
    /// follows the same `*_not_found` pattern as a natural extension — the FIP table predates the
    /// multi-cluster path segment. An error without resource context keeps its kind's generic
    /// code, so the gateway never guesses which resource a bare failure was about.
    pub fn code(&self) -> &'static str {
        let resource = self
            .details
            .as_ref()
            .and_then(|details| details.resource_kind.as_deref());
        match (self.kind, resource) {
            (ErrorKind::NotFound, Some("cluster")) => "cluster_not_found",
            (ErrorKind::NotFound, Some("database")) => "database_not_found",
            (ErrorKind::NotFound, Some("table")) => "table_not_found",
            (ErrorKind::NotFound, Some("partition")) => "partition_not_found",
            (ErrorKind::AlreadyExists, Some("cluster")) => "cluster_already_exists",
            (ErrorKind::AlreadyExists, Some("database")) => "database_already_exists",
            (ErrorKind::AlreadyExists, Some("table")) => "table_already_exists",
            (ErrorKind::AlreadyExists, Some("partition")) => "partition_already_exists",
            // The one precondition the FIP names: dropping a non-empty database. Every other
            // precondition failure (e.g. a table changing during write preflight) keeps the
            // generic code.
            (ErrorKind::FailedPrecondition, Some("database")) => "database_not_empty",
            _ => self.kind.code(),
        }
    }

    /// Whether repeating an otherwise unchanged request may succeed.
    ///
    /// Defaults to [`ErrorKind::default_retryable`] unless an explicit verdict was recorded.
    pub fn retryable(&self) -> bool {
        self.retryable
            .unwrap_or_else(|| self.kind.default_retryable())
    }

    /// Records an explicit retry verdict, typically `FlussError::is_retriable()` from the native layer.
    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = Some(retryable);
        self
    }

    /// Adds machine-readable resource context without changing the stable error code.
    pub fn with_resource(
        mut self,
        resource_kind: impl Into<String>,
        resource_name: Option<impl Into<String>>,
    ) -> Self {
        self.details = Some(ErrorDetails {
            resource_kind: Some(resource_kind.into()),
            resource_name: resource_name.map(Into::into),
        });
        self
    }

    /// Returns optional machine-readable context for protocol adapters.
    pub fn details(&self) -> Option<&ErrorDetails> {
        self.details.as_ref()
    }
}

impl fmt::Display for GatewayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code(), self.message)
    }
}

impl std::error::Error for GatewayError {}

/// REST error envelope: `{"error": {"code", "message", "request_id", "retryable", "details"?}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
}

/// Body of the REST error envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
    pub request_id: String,
    /// Machine-readable retry guidance, derived from the error kind or from `FlussError::is_retriable()`.
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<ErrorDetails>,
}

impl ErrorEnvelope {
    /// Builds a public error envelope with the correlated request ID.
    pub fn new(error: &GatewayError, request_id: impl Into<String>) -> Self {
        Self {
            error: ErrorBody {
                code: error.code().to_string(),
                message: error.message().to_string(),
                request_id: request_id.into(),
                retryable: error.retryable(),
                details: error.details().cloned(),
            },
        }
    }

    /// Builds an envelope for a failure that never had a [`GatewayError`], such as a framework-produced status.
    ///
    /// Routing every construction through a constructor keeps callers from leaving a stale struct literal behind
    /// when the envelope gains a field.
    pub fn from_parts(
        code: impl Into<String>,
        message: impl Into<String>,
        request_id: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            error: ErrorBody {
                code: code.into(),
                message: message.into(),
                request_id: request_id.into(),
                retryable,
                details: None,
            },
        }
    }
}

/// Attaches machine-readable resource context to the error kinds that name a resource.
#[allow(dead_code)] // The resource-naming emitters arrive with the capability PRs; the wire contract ships now.
pub(crate) fn resource_error(
    error: GatewayError,
    resource_kind: &'static str,
    resource_name: impl Into<String>,
) -> GatewayError {
    if error.details().is_some()
        || !matches!(
            error.kind(),
            ErrorKind::NotFound | ErrorKind::AlreadyExists | ErrorKind::FailedPrecondition
        )
    {
        return error;
    }
    error.with_resource(resource_kind, Some(resource_name.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The frozen taxonomy. Adding a variant breaks [`ErrorKind::ordinal`] first, then this table.
    const CONTRACT: [(ErrorKind, u16, &str, bool); 16] = [
        (ErrorKind::InvalidArgument, 400, "invalid_argument", false),
        (ErrorKind::Unauthenticated, 401, "unauthenticated", false),
        (ErrorKind::Unauthorized, 403, "unauthorized", false),
        (ErrorKind::NotFound, 404, "not_found", false),
        (ErrorKind::AlreadyExists, 409, "already_exists", false),
        (
            ErrorKind::FailedPrecondition,
            409,
            "failed_precondition",
            false,
        ),
        (ErrorKind::Unsupported, 501, "unsupported", false),
        (
            ErrorKind::UnsupportedMediaType,
            415,
            "unsupported_media_type",
            false,
        ),
        (ErrorKind::NotAcceptable, 406, "not_acceptable", false),
        (ErrorKind::LimitExceeded, 413, "limit_exceeded", false),
        (
            ErrorKind::ResourceExhausted,
            429,
            "resource_exhausted",
            true,
        ),
        (ErrorKind::DeadlineExceeded, 504, "timeout", true),
        (ErrorKind::Cancelled, 499, "cancelled", false),
        (ErrorKind::Unavailable, 503, "unavailable", true),
        (ErrorKind::Backend, 500, "backend", false),
        (ErrorKind::Internal, 500, "internal", false),
    ];

    #[test]
    fn taxonomy_is_frozen_and_exhaustively_mapped() {
        assert_eq!(ErrorKind::ALL.len(), CONTRACT.len());
        for (index, (kind, status, code, retryable)) in CONTRACT.into_iter().enumerate() {
            assert_eq!(kind.ordinal(), index, "{code} is out of declaration order");
            assert_eq!(ErrorKind::ALL[index], kind, "ALL disagrees for {code}");
            assert_eq!(kind.http_status(), status, "status for {code}");
            assert_eq!(kind.code(), code);
            assert_eq!(kind.default_retryable(), retryable, "retryable for {code}");
        }
    }

    #[test]
    fn only_connection_capacity_maps_to_429_and_nothing_maps_to_a_cursor_status() {
        for kind in ErrorKind::ALL {
            let status = kind.http_status();
            assert_eq!(
                status == 429,
                kind == ErrorKind::ResourceExhausted,
                "{} unexpectedly maps to 429",
                kind.code()
            );
            assert_ne!(status, 410, "{} maps to a cursor status", kind.code());
        }
    }

    #[test]
    fn envelope_shape() {
        let err = GatewayError::not_found("table `db.missing` does not exist");
        let envelope = ErrorEnvelope::new(&err, "req-123");
        let json = serde_json::to_value(&envelope).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "error": {
                    "code": "not_found",
                    "message": "table `db.missing` does not exist",
                    "request_id": "req-123",
                    "retryable": false,
                }
            })
        );
    }

    #[test]
    fn explicit_retry_verdict_overrides_the_kind_default() {
        let derived = GatewayError::unavailable("Fluss is unavailable");
        assert!(derived.retryable());

        let overridden = GatewayError::internal("decode failed").with_retryable(true);
        assert!(overridden.retryable());
        assert!(
            !GatewayError::unavailable("permanently gone")
                .with_retryable(false)
                .retryable()
        );
        assert_eq!(
            serde_json::to_value(ErrorEnvelope::new(&overridden, "req-1")).unwrap()["error"]["retryable"],
            serde_json::json!(true)
        );
    }

    #[test]
    fn retains_protocol_neutral_resource_details() {
        let error = GatewayError::not_found("table does not exist")
            .with_resource("table", Some("fluss.missing"));

        assert_eq!(
            error.details(),
            Some(&ErrorDetails {
                resource_kind: Some("table".to_string()),
                resource_name: Some("fluss.missing".to_string()),
            })
        );
        assert_eq!(
            serde_json::to_value(ErrorEnvelope::new(&error, "request-7")).unwrap(),
            serde_json::json!({
                "error": {
                    "code": "table_not_found",
                    "message": "table does not exist",
                    "request_id": "request-7",
                    "retryable": false,
                    "details": {
                        "resource_kind": "table",
                        "resource_name": "fluss.missing"
                    }
                }
            })
        );
    }

    #[test]
    fn framework_failures_get_an_envelope_without_a_gateway_error() {
        let envelope = ErrorEnvelope::from_parts(
            "method_not_allowed",
            "method not allowed",
            "request-9",
            false,
        );
        assert_eq!(
            serde_json::to_value(&envelope).unwrap(),
            serde_json::json!({
                "error": {
                    "code": "method_not_allowed",
                    "message": "method not allowed",
                    "request_id": "request-9",
                    "retryable": false,
                }
            })
        );
    }

    #[test]
    fn resource_context_is_added_only_to_resource_naming_kinds() {
        let named = resource_error(GatewayError::not_found("gone"), "table", "db.t");
        assert_eq!(
            named.details().and_then(|d| d.resource_name.clone()),
            Some("db.t".to_string())
        );
        let untouched = resource_error(GatewayError::internal("boom"), "table", "db.t");
        assert!(untouched.details().is_none());
    }

    /// The FIP-49 vocabulary: an error carrying resource context answers the resource-specific
    /// code; one without context keeps its kind's generic code.
    #[test]
    fn resource_context_specialises_the_wire_code() {
        let cases: [(GatewayError, &str, &str); 5] = [
            (GatewayError::not_found("x"), "cluster", "cluster_not_found"),
            (
                GatewayError::not_found("x"),
                "database",
                "database_not_found",
            ),
            (GatewayError::not_found("x"), "table", "table_not_found"),
            (
                GatewayError::already_exists("x"),
                "partition",
                "partition_already_exists",
            ),
            (
                GatewayError::failed_precondition("x"),
                "database",
                "database_not_empty",
            ),
        ];
        for (error, resource, expected) in cases {
            let named = error.with_resource(resource, Some("name"));
            assert_eq!(named.code(), expected);
            let envelope = serde_json::to_value(ErrorEnvelope::new(&named, "r")).unwrap();
            assert_eq!(envelope["error"]["code"], expected);
        }

        // Without resource context the generic codes hold — the gateway never guesses.
        assert_eq!(GatewayError::not_found("x").code(), "not_found");
        assert_eq!(GatewayError::already_exists("x").code(), "already_exists");
        assert_eq!(
            GatewayError::failed_precondition("x").code(),
            "failed_precondition"
        );
        // A precondition on a table (e.g. it changed during preflight) is not "not empty".
        assert_eq!(
            GatewayError::failed_precondition("x")
                .with_resource("table", Some("db.t"))
                .code(),
            "failed_precondition"
        );
        // Backend and internal stay distinguishable (FIP-49 `backend` / `internal`).
        assert_eq!(GatewayError::backend("x").code(), "backend");
        assert_eq!(GatewayError::internal("x").code(), "internal");
    }
}
