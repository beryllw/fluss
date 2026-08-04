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

//! Health endpoints.
//!
//! - `GET /health/live` answers from the event loop without a backend RPC.
//! - `GET /health/ready` uses process state and cached per-cluster reachability, never a request-time probe.
//! - `GET /health` returns process, listener, and per-cluster cached health.

use crate::backend::model::{ClusterHealthReport, ClusterStatus};
use crate::protocol::rest::{RestState, json_response, shaped};
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use serde::Serialize;
use serde_json::json;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Health routes merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new()
        .routes(routes!(live))
        .routes(routes!(ready))
        .routes(routes!(health))
}

/// Response of `GET /health/live`.
#[derive(Debug, Serialize, ToSchema)]
pub struct LiveResponse {
    pub status: String,
}

/// Ready response of `GET /health/ready`.
#[derive(Debug, Serialize, ToSchema)]
pub struct ReadinessResponse {
    pub status: String,
}

/// Not-ready response of `GET /health/ready`.
#[derive(Debug, Serialize, ToSchema)]
pub struct UnreadyResponse {
    pub status: String,
    /// Stable reason, either `starting` or `shutting_down`.
    pub reason: String,
}

/// Liveness and uptime of the gateway process.
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthProcessResponse {
    pub status: String,
    pub uptime_seconds: u64,
}

/// State of the REST listener.
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthRestListenerResponse {
    pub serving: bool,
    pub accepting: bool,
    pub bind_address: String,
}

/// Listener diagnostics of the gateway.
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthListenersResponse {
    pub rest: HealthRestListenerResponse,
}

/// Cached diagnostics for one configured Fluss cluster.
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthClusterEntryResponse {
    pub id: String,
    /// One of `unknown`, `available`, or `unavailable`.
    pub state: String,
    pub reachable: bool,
    pub error: Option<String>,
    pub fluss_cluster: ClusterHealthResponse,
}

/// Replica health of the Fluss cluster. Counts are absent when the cluster cannot be reached.
#[derive(Debug, Serialize, ToSchema)]
pub struct ClusterHealthResponse {
    /// One of `GREEN`, `YELLOW`, `RED`, or `UNKNOWN`.
    pub status: String,
    pub num_replicas: Option<i32>,
    pub in_sync_replicas: Option<i32>,
    pub num_leader_replicas: Option<i32>,
    pub active_leader_replicas: Option<i32>,
}

/// Response of `GET /health`.
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthResponse {
    pub process: HealthProcessResponse,
    pub listeners: HealthListenersResponse,
    pub clusters: Vec<HealthClusterEntryResponse>,
}

/// Liveness: answering at all is the signal. Never performs a backend RPC.
#[utoipa::path(
    get,
    path = "/health/live",
    operation_id = "getLiveness",
    tag = "health",
    responses((status = 200, description = "Process is alive", body = LiveResponse))
)]
pub(crate) async fn live() -> Response {
    json_response(&json!({ "status": "live" })).expect("static JSON value is serializable")
}

/// Readiness: the listener is accepting new work and the process is not draining.
#[utoipa::path(
    get,
    path = "/health/ready",
    operation_id = "getReadiness",
    tag = "health",
    responses(
        (status = 200, description = "Gateway accepts data requests", body = ReadinessResponse),
        (status = 503, description = "Gateway is not ready", body = UnreadyResponse)
    )
)]
pub(crate) async fn ready(State(state): State<RestState>) -> Response {
    if !state.readiness.is_accepting() {
        let reason = if state.readiness.is_shutting_down() {
            "shutting_down"
        } else {
            "starting"
        };
        return unready(reason);
    }
    json_response(&json!({ "status": "ready" })).expect("static JSON value is serializable")
}

/// Builds the dedicated readiness failure shape rather than the general API error envelope.
///
/// Carries the same `Retry-After` value [`crate::protocol::rest::error_response`] sets for
/// unavailable-kind errors, matching the documented 503 contract.
fn unready(reason: &str) -> Response {
    let mut response = json_response(&json!({ "status": "unready", "reason": reason }))
        .expect("static JSON value is serializable");
    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    shaped(response)
}

/// Diagnostics always return 200. The payload carries the detail.
#[utoipa::path(
    get,
    path = "/health",
    operation_id = "getHealth",
    tag = "health",
    responses((status = 200, description = "Gateway and Fluss diagnostics", body = HealthResponse))
)]
pub(crate) async fn health(State(state): State<RestState>) -> Response {
    let clusters = state
        .clusters
        .snapshots()
        .into_iter()
        .map(|snapshot| {
            let fluss_cluster = snapshot
                .health
                .report
                .as_ref()
                .map(cluster_json)
                .unwrap_or_else(|| json!({ "status": ClusterStatus::Unknown.as_str() }));
            json!({
                "id": snapshot.id,
                "state": snapshot.state.as_str(),
                "reachable": snapshot.health.reachable,
                "error": (!snapshot.health.reachable).then_some(snapshot.health.reason),
                "fluss_cluster": fluss_cluster,
            })
        })
        .collect::<Vec<_>>();

    json_response(&json!({
        "process": {
            "status": "live",
            "uptime_seconds": state.started_at.elapsed().as_secs(),
        },
        "listeners": {
            "rest": {
                "serving": state.readiness.is_serving(),
                "accepting": state.readiness.is_accepting(),
                "bind_address": state.bind_address.to_string(),
            },
        },
        "clusters": clusters,
    }))
    .expect("static JSON value is serializable")
}

/// Converts a reachable cluster report into the diagnostics wire shape.
fn cluster_json(report: &ClusterHealthReport) -> serde_json::Value {
    json!({
        "status": report.status.as_str(),
        "num_replicas": report.num_replicas,
        "in_sync_replicas": report.in_sync_replicas,
        "num_leader_replicas": report.num_leader_replicas,
        "active_leader_replicas": report.active_leader_replicas,
    })
}

#[cfg(test)]
mod tests {
    use crate::backend::testing::TestBackend;
    use crate::protocol::rest::test_support;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::response::Response;
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn get(app: axum::Router, path: &str) -> Response {
        app.oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("json body")
    }

    #[tokio::test]
    async fn live_needs_no_backend() {
        let backend = Arc::new(TestBackend::new());
        backend.set_available(false);
        let response = get(test_support::app(backend), "/health/live").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "live");
    }

    #[tokio::test]
    async fn ready_when_serving_and_backend_reachable() {
        let response = get(
            test_support::app(Arc::new(TestBackend::new())),
            "/health/ready",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "ready");
    }

    #[tokio::test]
    async fn ready_when_all_clusters_are_unreachable() {
        let backend = Arc::new(TestBackend::new());
        let state = test_support::state_with_backend(backend);
        state
            .clusters
            .set_unavailable_for_test("default", "backend_unreachable");
        let app = crate::protocol::rest::build_router(state, &test_support::test_options());
        let response = get(app, "/health/ready").await;
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["status"], "ready");
    }

    #[tokio::test]
    async fn unready_during_shutdown() {
        let backend = Arc::new(TestBackend::new());
        let state = test_support::state_with_backend(backend);
        state.readiness.begin_shutdown();
        let app = crate::protocol::rest::build_router(state, &test_support::test_options());
        let response = get(app, "/health/ready").await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
        assert_eq!(body_json(response).await["reason"], "shutting_down");
    }

    #[tokio::test]
    async fn diagnostics_reports_green_cluster() {
        let response = get(test_support::app(Arc::new(TestBackend::new())), "/health").await;
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["process"]["status"], "live");
        assert_eq!(json["listeners"]["rest"]["serving"], true);
        assert_eq!(json["listeners"]["rest"]["bind_address"], "127.0.0.1:8080");
        assert_eq!(json["clusters"][0]["id"], "default");
        assert_eq!(json["clusters"][0]["state"], "available");
        assert_eq!(json["clusters"][0]["reachable"], true);
        assert_eq!(json["clusters"][0]["fluss_cluster"]["status"], "GREEN");
        assert_eq!(json["clusters"][0]["fluss_cluster"]["num_replicas"], 6);
    }

    #[tokio::test]
    async fn diagnostics_reports_unknown_when_unreachable() {
        let backend = Arc::new(TestBackend::new());
        let state = test_support::state_with_backend(backend);
        state
            .clusters
            .set_unavailable_for_test("default", "backend_unreachable");
        let app = crate::protocol::rest::build_router(state, &test_support::test_options());
        let response = get(app, "/health").await;
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["clusters"][0]["state"], "unavailable");
        assert_eq!(json["clusters"][0]["reachable"], false);
    }
}
