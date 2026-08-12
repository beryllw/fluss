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

//! Generated OpenAPI 3.1 document served at `GET /v1/openapi.json`.
//!
//! The document is derived from the routers themselves by
//! [`utoipa_axum::router::OpenApiRouter::split_for_parts`] — there is no hand-maintained list of paths or
//! schemas anywhere in the crate, so the served contract cannot drift from the mounted routes. This module owns
//! only the shared error schemas, the serve handler, and the post-pass hooks applied to the generated value.

use crate::protocol::rest::{RestState, json_response};
use axum::extract::State;
use axum::response::Response;
use serde::Serialize;
use serde_json::{Value, json};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Stable error codes of the gateway: the FIP-49 vocabulary, resource-specific where the error
/// names a resource, exactly as serialized on the wire.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = ErrorCode)]
#[allow(dead_code)] // Schema-only enum; real errors use the HTTP-independent ErrorKind.
pub(crate) enum ErrorCodeSchema {
    InvalidArgument,
    Unauthenticated,
    Unauthorized,
    NotFound,
    ClusterNotFound,
    DatabaseNotFound,
    TableNotFound,
    PartitionNotFound,
    AlreadyExists,
    ClusterAlreadyExists,
    DatabaseAlreadyExists,
    TableAlreadyExists,
    PartitionAlreadyExists,
    FailedPrecondition,
    DatabaseNotEmpty,
    Unsupported,
    UnsupportedMediaType,
    NotAcceptable,
    LimitExceeded,
    ResourceExhausted,
    Timeout,
    Cancelled,
    Unavailable,
    Backend,
    Internal,
    /// Entry-level only: a KV write rejected by storage backpressure (never a request status).
    StorageBackpressure,
}

/// Machine-readable resource context carried by resource-naming errors.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ErrorDetailsSchema {
    pub resource_kind: Option<String>,
    pub resource_name: Option<String>,
}

/// Body of the shared error envelope.
#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ErrorBody)]
pub(crate) struct ErrorBodySchema {
    pub code: ErrorCodeSchema,
    pub message: String,
    #[schema(value_type = String, format = "uuid")]
    pub request_id: String,
    /// Whether repeating an otherwise unchanged request may succeed.
    pub retryable: bool,
    pub details: Option<ErrorDetailsSchema>,
}

/// The envelope every failing response uses.
#[derive(Debug, Serialize, ToSchema)]
#[schema(
    as = ErrorEnvelope,
    examples(json!({
        "error": {
            "code": "table_not_found",
            "message": "table does not exist",
            "request_id": "8f6c7f4a-f9b8-4c71-91ec-6e5578d7a913",
            "retryable": false,
            "details": {"resource_kind": "table"}
        }
    }))
)]
pub(crate) struct ErrorEnvelopeSchema {
    pub error: ErrorBodySchema,
}

/// Seeds the generated document with the schemas that no single handler owns.
#[derive(OpenApi)]
#[openapi(components(schemas(
    ErrorCodeSchema,
    ErrorDetailsSchema,
    ErrorBodySchema,
    ErrorEnvelopeSchema
)))]
struct SharedSchemas;

/// OpenAPI routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::with_openapi(SharedSchemas::openapi()).routes(routes!(serve))
}

/// Applies the gateway's post-passes to the router-generated document.
///
/// Called once by [`crate::protocol::rest::build_router`]. The passes are deliberately separate so that
/// documentation work can extend them without touching router assembly.
pub(crate) fn finalize(api: utoipa::openapi::OpenApi) -> Value {
    let mut document = serde_json::to_value(api).expect("generated OpenAPI is serializable");
    apply_info(&mut document);
    apply_servers(&mut document);
    apply_security(&mut document);
    apply_tags(&mut document);
    apply_response_headers(&mut document);
    document
}

/// Replaces the utoipa-axum library defaults in `info` with this crate's own metadata.
fn apply_info(document: &mut Value) {
    document["info"] = json!({
        "title": "fluss-gateway",
        "description": "Stateless REST gateway for Apache Fluss",
        "version": env!("CARGO_PKG_VERSION"),
        "license": {
            "name": "Apache-2.0",
            "url": "https://www.apache.org/licenses/LICENSE-2.0"
        }
    });
}

/// The gateway serves the API at the listener root; a relative server keeps the document
/// host-agnostic.
fn apply_servers(document: &mut Value) {
    document["servers"] = json!([{"url": "/"}]);
}

/// An explicit empty root security array: honest for this PR — no authentication exists yet.
/// The authentication capability PR will introduce securitySchemes and per-operation requirements.
fn apply_security(document: &mut Value) {
    document["security"] = json!([]);
}

/// Post-pass hook for tag descriptions. Intentionally empty until the documentation pass lands.
fn apply_tags(_document: &mut Value) {}

/// Post-pass hook for shared response headers. Intentionally empty until the documentation pass lands.
fn apply_response_headers(_document: &mut Value) {}

/// Serves the generated OpenAPI 3.1 document as JSON.
#[utoipa::path(
    get,
    path = "/v1/openapi.json",
    operation_id = "getOpenApi",
    tag = "metadata",
    responses((status = 200, description = "OpenAPI 3.1 document"))
)]
pub(crate) async fn serve(State(state): State<RestState>) -> Response {
    let document = state.openapi.get().cloned().unwrap_or_else(|| json!({}));
    json_response(&document).expect("OpenAPI JSON is serializable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::rest::test_support;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    /// Fetches the document exactly as the gateway serves it.
    async fn served_document() -> Value {
        let state = test_support::test_state();
        state.readiness.set_serving();
        let app = crate::protocol::rest::build_router(state, &test_support::test_options());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// The checked-in `openapi.yaml` next to this crate's `Cargo.toml` (FIP-49).
    fn checked_in_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("openapi.yaml")
    }

    /// Regenerates the checked-in `openapi.yaml` from the typed contract: `just openapi`.
    #[tokio::test]
    #[ignore = "rewrites openapi.yaml in the working tree; run via `just openapi`"]
    async fn export_checked_in_document() {
        let yaml =
            serde_yaml::to_string(&served_document().await).expect("the document serializes");
        std::fs::write(checked_in_path(), yaml).expect("openapi.yaml is writable");
    }

    /// The checked-in document always matches the served one, so the published specification
    /// cannot drift from the implementation (FIP-49 schema-validation contract).
    #[tokio::test]
    async fn the_checked_in_document_matches_the_served_one() {
        let checked_in = std::fs::read_to_string(checked_in_path())
            .expect("openapi.yaml is checked in; regenerate it with `just openapi`");
        let checked_in: Value =
            serde_yaml::from_str(&checked_in).expect("openapi.yaml parses as YAML");
        assert_eq!(
            checked_in,
            served_document().await,
            "openapi.yaml is stale; regenerate it with `just openapi`"
        );
    }

    #[tokio::test]
    async fn served_document_is_generated_from_the_mounted_routes() {
        let document = served_document().await;

        assert_eq!(document["openapi"], "3.1.0");
        assert_eq!(document["info"]["title"], "fluss-gateway");
        assert_eq!(document["info"]["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(document["info"]["license"]["name"], "Apache-2.0");
        assert!(
            document["info"].get("contact").is_none(),
            "the library-default contact must not leak"
        );
        assert!(
            !document["servers"]
                .as_array()
                .expect("servers array")
                .is_empty(),
            "a relative root server is declared"
        );
        assert!(
            document["security"]
                .as_array()
                .expect("security array")
                .is_empty(),
            "root security is explicitly empty until authentication lands"
        );
        assert_eq!(
            document["paths"]["/v1/openapi.json"]["get"]["operationId"],
            "getOpenApi"
        );
        assert!(
            document["components"]["schemas"]["ErrorEnvelope"].is_object(),
            "the shared error envelope is registered"
        );
        assert_eq!(
            document["components"]["schemas"]["ErrorBody"]["properties"]["retryable"]["type"],
            "boolean"
        );
    }

    #[tokio::test]
    async fn the_document_declares_no_scan_or_cursor_path() {
        let document = served_document().await;
        let paths = document["paths"].as_object().expect("paths object");
        for path in paths.keys() {
            assert!(!path.contains("/scan"), "stateless gateway exposes {path}");
            assert!(!path.contains("cursor"), "stateless gateway exposes {path}");
            assert!(
                !path.contains("offsets"),
                "stateless gateway exposes {path}"
            );
        }
    }
}
