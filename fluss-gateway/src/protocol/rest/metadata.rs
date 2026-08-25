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

//! Metadata discovery: listing the databases of a cluster and the tables of a database.
//!
//! Both handlers do the same four things and nothing else: check that the cluster is one this gateway
//! serves (404), validate the page parameters (400), call the backend, and cut the page out of the
//! answer. Every other status — 429, 500, 503, 504 — is emitted by shared admission or middleware,
//! or propagated from [`crate::backend::FlussBackend`], so this module knows nothing about
//! connections, identity modes, or capacity.

use crate::backend::context::RequestContext;
use crate::backend::types::ClusterId;
use crate::backend::{FlussBackend, unknown_cluster};
use crate::error::{ErrorEnvelope, GatewayResult};
use crate::protocol::rest::pagination::{Collection, Page};
use crate::protocol::rest::{
    RestState, error_response, json_response, request_context, request_id,
};
use axum::extract::{Path, Request, State};
use axum::response::Response;
use serde::Serialize;
use std::sync::Arc;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Response of `GET /v1/clusters/{cluster}/databases`.
#[derive(Debug, Serialize, ToSchema)]
pub struct DatabasesResponse {
    pub databases: Vec<String>,
    /// Present only while more entries follow.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// Response of `GET /v1/clusters/{cluster}/databases/{database}/tables`.
#[derive(Debug, Serialize, ToSchema)]
pub struct TablesResponse {
    pub tables: Vec<String>,
    /// Present only while more entries follow.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// Metadata routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new()
        .routes(routes!(list_databases))
        .routes(routes!(list_tables))
}

/// Lists the databases of one configured cluster.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster}/databases",
    operation_id = "listDatabases",
    tag = "metadata",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("max_results" = Option<usize>, Query,
            description = "Maximum entries to return. Defaults to 100, capped at 1000.",
            minimum = 1, maximum = 1000),
        ("page_token" = Option<String>, Query,
            description = "Opaque token from the `next_page_token` of the previous response."),
    ),
    responses(
        (status = 200, description = "Databases in lexical order", body = DatabasesResponse),
        (status = 400, description = "Invalid page parameter or page token", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn list_databases(
    State(state): State<RestState>,
    Path(cluster): Path<String>,
    request: Request,
) -> Response {
    let request_id = request_id(&request);
    let prepared = prepare(&state, &request, &cluster, Collection::Databases, None);
    let result = async {
        let (backend, page, ctx) = prepared?;
        let (databases, next_page_token) = page.apply(backend.list_databases(&ctx).await?);
        json_response(&DatabasesResponse {
            databases,
            next_page_token,
        })
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Lists the tables of one database.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster}/databases/{database}/tables",
    operation_id = "listTables",
    tag = "metadata",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Database name"),
        ("max_results" = Option<usize>, Query,
            description = "Maximum entries to return. Defaults to 100, capped at 1000.",
            minimum = 1, maximum = 1000),
        ("page_token" = Option<String>, Query,
            description = "Opaque token from the `next_page_token` of the previous response."),
    ),
    responses(
        (status = 200, description = "Tables in lexical order", body = TablesResponse),
        (status = 400, description = "Invalid page parameter or page token", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster or database", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn list_tables(
    State(state): State<RestState>,
    Path((cluster, database)): Path<(String, String)>,
    request: Request,
) -> Response {
    let request_id = request_id(&request);
    let prepared = prepare(
        &state,
        &request,
        &cluster,
        Collection::Tables,
        Some(&database),
    );
    let result = async {
        let (backend, page, ctx) = prepared?;
        let (tables, next_page_token) = page.apply(backend.list_tables(&ctx, &database).await?);
        json_response(&TablesResponse {
            tables,
            next_page_token,
        })
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Validates the cluster (404) and the page request (400), then builds the backend context.
///
/// Everything that reads the HTTP request happens here, before the backend is called: a malformed page
/// token is therefore always a 400, never the 404 of a parent resource that happens not to exist. The
/// cluster is checked first, because a path resource that does not exist outranks a bad query
/// parameter.
fn prepare(
    state: &RestState,
    request: &Request,
    cluster: &str,
    collection: Collection,
    scope: Option<&str>,
) -> GatewayResult<(Arc<dyn FlussBackend>, Page, RequestContext)> {
    if !state.backend.has_cluster(cluster) {
        return Err(unknown_cluster(cluster));
    }
    let page = Page::parse(request.uri(), cluster, collection, scope)?;
    let cluster = ClusterId::try_from(cluster).expect("the backend serves this cluster ID");
    let ctx = request_context(cluster, request);
    Ok((state.backend.clone(), page, ctx))
}

#[cfg(test)]
mod tests {
    use crate::backend::FlussBackend;
    use crate::backend::fake::FakeFlussBackend;
    use crate::error::GatewayError;
    use crate::protocol::rest::test_support;
    use axum::body::Body;
    use axum::http::{Method, Request as HttpRequest, StatusCode};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    /// A serving router over one configured cluster backed by the fixture.
    fn app(backend: Arc<FakeFlussBackend>) -> axum::Router {
        app_with(backend, test_support::test_options())
    }

    fn app_with(
        backend: Arc<FakeFlussBackend>,
        options: crate::protocol::rest::RestOptions,
    ) -> axum::Router {
        let state = test_support::state_with_backend(backend as Arc<dyn FlussBackend>);
        state.readiness.set_serving();
        crate::protocol::rest::build_router(state, &options)
    }

    fn catalog() -> Arc<FakeFlussBackend> {
        Arc::new(FakeFlussBackend::with_catalog(&[
            ("sales", &["orders", "customers"]),
            ("ops", &[]),
        ]))
    }

    async fn get(app: &axum::Router, path: &str) -> (StatusCode, serde_json::Value) {
        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    /// Both collections answer in lexical order and omit the token on a complete page.
    #[tokio::test]
    async fn the_collections_answer_in_order_without_a_token() {
        let app = app(catalog());

        let (status, body) = get(&app, "/v1/clusters/default/databases").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({"databases": ["ops", "sales"]}));

        let (status, body) = get(&app, "/v1/clusters/default/databases/sales/tables").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({"tables": ["customers", "orders"]}));

        // An empty database is an empty list, not a 404.
        let (status, body) = get(&app, "/v1/clusters/default/databases/ops/tables").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({"tables": []}));
    }

    /// A page hands out the token that continues it, and the token is scoped to its endpoint.
    #[tokio::test]
    async fn a_partial_page_carries_a_token_scoped_to_its_endpoint() {
        let app = app(catalog());

        let (_, body) = get(&app, "/v1/clusters/default/databases?max_results=1").await;
        assert_eq!(body["databases"], serde_json::json!(["ops"]));
        let token = body["next_page_token"]
            .as_str()
            .expect("a partial page carries a token")
            .to_string();

        let (status, body) = get(
            &app,
            &format!("/v1/clusters/default/databases?page_token={token}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({"databases": ["sales"]}));

        // The same token on the tables endpoint is a bad request, never a reinterpretation.
        let (status, body) = get(
            &app,
            &format!("/v1/clusters/default/databases/sales/tables?page_token={token}"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");
    }

    /// An unconfigured or malformed cluster is `cluster_not_found`, and a page parameter is validated
    /// before the parent resource is looked up so a bad token cannot answer 404.
    #[tokio::test]
    async fn unknown_clusters_and_bad_parameters_keep_their_own_status() {
        let app = app(catalog());

        for path in [
            "/v1/clusters/other/databases",
            "/v1/clusters/Not%20A%20Cluster/databases",
            "/v1/clusters/other/databases/sales/tables",
        ] {
            let (status, body) = get(&app, path).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
            assert_eq!(body["error"]["code"], "cluster_not_found", "{path}");
        }

        for path in [
            "/v1/clusters/default/databases?max_results=0",
            "/v1/clusters/default/databases?page_token=nope!",
            // The database does not exist, but the token is what is wrong.
            "/v1/clusters/default/databases/missing/tables?max_results=99999",
        ] {
            let (status, body) = get(&app, path).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
            assert_eq!(body["error"]["code"], "invalid_argument", "{path}");
        }

        let (status, body) = get(&app, "/v1/clusters/default/databases/missing/tables").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "database_not_found");
    }

    /// Every status beyond the adapter's own 400 and 404 is the backend error, mapped mechanically.
    #[tokio::test]
    async fn backend_failures_are_mapped_without_the_adapter_deciding() {
        for (failure, status, code) in [
            (
                GatewayError::unavailable("Fluss is unavailable"),
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
            ),
            (
                GatewayError::backend("Fluss denied the gateway"),
                StatusCode::INTERNAL_SERVER_ERROR,
                "backend",
            ),
        ] {
            let backend = catalog();
            backend.fail_with(failure);
            let (answered, body) = get(&app(backend), "/v1/clusters/default/databases").await;
            assert_eq!(answered, status);
            assert_eq!(body["error"]["code"], code);
        }
    }

    /// A backend that outruns the request budget answers 504 from the handler's own deadline.
    #[tokio::test]
    async fn a_slow_backend_answers_the_request_timeout() {
        let backend = catalog();
        backend.set_latency(Duration::from_secs(30));
        let options = crate::protocol::rest::RestOptions {
            request_timeout: Duration::from_millis(200),
            ..test_support::test_options()
        };

        let (status, body) = get(
            &app_with(backend, options),
            "/v1/clusters/default/databases",
        )
        .await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(body["error"]["code"], "timeout");
    }

    #[tokio::test]
    async fn the_collections_are_read_only() {
        let app = app(catalog());
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method(Method::DELETE)
                    .uri("/v1/clusters/default/databases")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}
