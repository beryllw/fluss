// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cached multi-cluster discovery.

use crate::protocol::rest::{RestState, json_response};
use axum::extract::State;
use axum::response::Response;
use serde::Serialize;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// One configured cluster and its cached reachability.
#[derive(Debug, Serialize, ToSchema)]
pub struct ClusterEntryResponse {
    pub id: String,
    /// One of `unknown`, `available`, or `unavailable`.
    pub state: String,
}

/// Response of `GET /v1/clusters`.
#[derive(Debug, Serialize, ToSchema)]
pub struct ClustersResponse {
    pub clusters: Vec<ClusterEntryResponse>,
}

/// Routes merged into the global control surface.
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new().routes(routes!(list_clusters))
}

/// Lists configured cluster IDs and cached state without probing.
#[utoipa::path(
    get,
    path = "/v1/clusters",
    operation_id = "listClusters",
    tag = "clusters",
    responses((status = 200, description = "Configured clusters in lexical order", body = ClustersResponse))
)]
pub(crate) async fn list_clusters(State(state): State<RestState>) -> Response {
    let clusters = state
        .clusters
        .snapshots()
        .into_iter()
        .map(|snapshot| ClusterEntryResponse {
            id: snapshot.id.to_string(),
            state: snapshot.state.as_str().to_string(),
        })
        .collect();
    json_response(&ClustersResponse { clusters }).expect("cluster discovery is serializable")
}

#[cfg(test)]
mod tests {
    use crate::backend::registry::ClusterRegistry;
    use crate::backend::testing::TestBackend;
    use crate::protocol::rest::test_support;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn multi_cluster_app() -> axum::Router {
        let clusters = Arc::new(ClusterRegistry::from_test_entries(vec![
            ("zeta".to_string(), None, None),
            (
                "alpha".to_string(),
                Some(Arc::new(TestBackend::new())),
                Some(test_support::green_report()),
            ),
        ]));
        clusters.set_unavailable_for_test("zeta", "backend_unreachable");
        crate::protocol::rest::build_router(
            test_support::state_with_clusters(clusters),
            &test_support::test_options(),
        )
    }

    async fn get(app: axum::Router, path: &str) -> (StatusCode, serde_json::Value) {
        let response = app
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn discovery_is_sorted_and_reports_cached_state_without_probing() {
        let app = multi_cluster_app();
        let (status, body) = get(app.clone(), "/v1/clusters").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["clusters"][0]["id"], "alpha");
        assert_eq!(body["clusters"][0]["state"], "available");
        assert_eq!(body["clusters"][1]["id"], "zeta");
        assert_eq!(body["clusters"][1]["state"], "unavailable");
    }

    #[tokio::test]
    async fn repeated_requests_return_an_identical_body() {
        // Cluster discovery is derived entirely from configuration plus cached health, so it is a pure
        // function of process state: no request can change what a later one sees.
        let app = multi_cluster_app();
        let (_, first) = get(app.clone(), "/v1/clusters").await;
        let (_, second) = get(app, "/v1/clusters").await;
        assert_eq!(first, second);
    }
}
