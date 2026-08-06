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

/// Stable error codes of the gateway, mirroring [`crate::error::ErrorKind`].
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[schema(as = ErrorCode)]
#[allow(dead_code)] // Schema-only enum; real errors use the HTTP-independent ErrorKind.
pub(crate) enum ErrorCodeSchema {
    InvalidArgument,
    NotFound,
    AlreadyExists,
    FailedPrecondition,
    Unsupported,
    UnsupportedMediaType,
    NotAcceptable,
    LimitExceeded,
    DeadlineExceeded,
    Cancelled,
    Unavailable,
    Internal,
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
            "code": "not_found",
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
    apply_license(&mut document);
    apply_tags(&mut document);
    apply_response_headers(&mut document);
    document
}

/// Declares the project license on the generated document.
fn apply_license(document: &mut Value) {
    document["info"]["license"] = json!({
        "name": "Apache-2.0",
        "url": "https://www.apache.org/licenses/LICENSE-2.0"
    });
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
    use crate::backend::testing::TestBackend;
    use crate::protocol::rest::test_support;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    #[tokio::test]
    async fn served_document_is_generated_from_the_mounted_routes() {
        let app = test_support::app(Arc::new(TestBackend::new()));
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
        let document: Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(document["openapi"], "3.1.0");
        assert_eq!(document["info"]["license"]["name"], "Apache-2.0");
        assert_eq!(
            document["paths"]["/v1/clusters"]["get"]["operationId"],
            "listClusters"
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
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let document: Value = serde_json::from_slice(&bytes).unwrap();
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
