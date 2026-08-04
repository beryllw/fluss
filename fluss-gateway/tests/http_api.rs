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

//! End-to-end HTTP tests over the full production wiring with the fixture backend.
//!
//! These drive a real listener through a real HTTP client, so they exercise the lifecycle, the middleware stack,
//! and the router exactly as deployed — only the Fluss cluster is replaced.

mod support;

use fluss_gateway::backend::testing::TestBackend;
use serde_json::json;
use std::sync::Arc;
use support::{Instance, single_cluster, start_instance};

async fn gateway() -> Instance {
    start_instance(single_cluster(Arc::new(TestBackend::new()))).await
}

#[tokio::test]
async fn health_probes_answer_without_touching_the_backend() {
    let instance = gateway().await;

    let live = instance.api.get_ok("/health/live").await;
    assert_eq!(live["status"], "live");

    let ready = instance.api.get_ok("/health/ready").await;
    assert_eq!(ready["status"], "ready");

    let health = instance.api.get_ok("/health").await;
    assert_eq!(health["listeners"]["rest"]["serving"], true);
    assert_eq!(health["clusters"][0]["id"], "default");
    assert_eq!(health["clusters"][0]["state"], "available");
    assert_eq!(health["clusters"][0]["fluss_cluster"]["status"], "GREEN");

    instance.shutdown().await;
}

#[tokio::test]
async fn cluster_discovery_lists_the_configured_cluster() {
    let instance = gateway().await;

    let clusters = instance.api.get_ok("/v1/clusters").await;
    assert_eq!(
        clusters,
        json!({"clusters": [{"id": "default", "state": "available"}]})
    );

    instance.shutdown().await;
}

#[tokio::test]
async fn the_openapi_document_is_served_and_generated_from_the_router() {
    let instance = gateway().await;

    let document = instance.api.get_ok("/v1/openapi.json").await;
    assert_eq!(document["openapi"], "3.1.0");
    assert_eq!(document["info"]["license"]["name"], "Apache-2.0");
    assert!(document["paths"]["/v1/clusters"]["get"].is_object());
    assert!(document["components"]["schemas"]["ErrorEnvelope"].is_object());

    instance.shutdown().await;
}

#[tokio::test]
async fn an_unknown_route_returns_the_shared_error_envelope() {
    let instance = gateway().await;

    let response = instance.api.get("/v1/nope").await;
    assert_eq!(response.status(), 404);
    assert!(response.headers().contains_key("x-request-id"));
    let body: serde_json::Value = response.json().await.expect("JSON body");
    assert_eq!(body["error"]["code"], "NOT_FOUND");
    assert_eq!(body["error"]["retryable"], false);
    assert!(body["error"]["request_id"].as_str().is_some());

    instance.shutdown().await;
}
