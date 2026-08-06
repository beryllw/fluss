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
use support::{
    Instance, password_config, single_cluster, start_instance, start_instance_with_config,
};

async fn gateway() -> Instance {
    start_instance(single_cluster(Arc::new(TestBackend::new()))).await
}

/// A gateway enforcing password authentication with one known user, `alice:secret`.
async fn password_gateway() -> Instance {
    start_instance_with_config(
        password_config("alice:secret"),
        single_cluster(Arc::new(TestBackend::new())),
    )
    .await
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
    assert_eq!(body["error"]["code"], "not_found");
    assert_eq!(body["error"]["retryable"], false);
    assert!(body["error"]["request_id"].as_str().is_some());

    instance.shutdown().await;
}

/// The write and lookup routes are mounted and reject an empty body with a 400 envelope.
///
/// The point of the assertion is the *shape*: a registered route must never panic, never return a bare 500, and
/// never fall through to the 404 fallback. All data-plane routes are now implemented, so an empty JSON body
/// fails request validation; their real behaviour is covered by their handler tests.
#[tokio::test]
async fn every_data_plane_route_is_mounted_and_answers_with_an_error_envelope() {
    let instance = gateway().await;
    let table = "/v1/clusters/default/databases/fluss/tables/users";

    let mut responses = Vec::new();
    for path in [
        format!("{table}/records"),
        format!("{table}/records/lookup"),
        format!("{table}/records/prefix-lookup"),
    ] {
        responses.push((
            path.clone(),
            instance.api.post_json(&path, &json!({})).await,
        ));
    }

    for (path, response) in responses {
        assert_eq!(response.status(), 400, "{path}");
        let body: serde_json::Value = response.json().await.expect("JSON body");
        assert_eq!(body["error"]["code"], "invalid_argument", "{path}");
        assert_eq!(body["error"]["retryable"], false, "{path}");
    }

    instance.shutdown().await;
}

/// The default trust mode accepts requests without credentials (principal `anonymous`) and takes a
/// Basic username at face value, ignoring the password — matching the FIP's `curl -u alice:ignored`.
#[tokio::test]
async fn trust_mode_accepts_anonymous_and_basic_identities() {
    let instance = gateway().await;

    let anonymous = instance.api.get("/v1/clusters/default/databases").await;
    assert_eq!(anonymous.status(), 200);

    let named = instance
        .api
        .get_with_basic("/v1/clusters/default/databases", "alice", "ignored")
        .await;
    assert_eq!(named.status(), 200);

    instance.shutdown().await;
}

/// Password mode guards every data- and control-plane route: only the health probes stay open.
#[tokio::test]
async fn password_mode_enforces_credentials_on_data_and_control_planes() {
    let instance = password_gateway().await;
    let databases = "/v1/clusters/default/databases";

    // The right credential reaches the backend.
    let authorized = instance
        .api
        .get_with_basic(databases, "alice", "secret")
        .await;
    assert_eq!(authorized.status(), 200);

    // A wrong password is a 401 envelope with the challenge header and a request id.
    let rejected = instance
        .api
        .get_with_basic(databases, "alice", "wrong")
        .await;
    assert_eq!(rejected.status(), 401);
    assert_eq!(
        rejected
            .headers()
            .get("www-authenticate")
            .and_then(|v| v.to_str().ok()),
        Some("Basic realm=\"fluss-gateway\"")
    );
    assert!(rejected.headers().contains_key("x-request-id"));
    let body: serde_json::Value = rejected.json().await.expect("JSON body");
    assert_eq!(body["error"]["code"], "unauthenticated");
    assert_eq!(body["error"]["retryable"], false);

    // Anonymous and malformed credentials are rejected the same way.
    let anonymous = instance.api.get(databases).await;
    assert_eq!(anonymous.status(), 401);
    assert!(anonymous.headers().contains_key("www-authenticate"));
    let malformed = instance
        .api
        .get_with_header(databases, "authorization", "Basic !!!not-base64!!!")
        .await;
    assert_eq!(malformed.status(), 401);

    // The control plane is guarded too; only the health probes stay open.
    assert_eq!(instance.api.get("/v1/clusters").await.status(), 401);
    assert_eq!(instance.api.get("/v1/openapi.json").await.status(), 401);
    assert_eq!(instance.api.get("/health/live").await.status(), 200);
    assert_eq!(instance.api.get("/health").await.status(), 200);

    instance.shutdown().await;
}

/// An unknown user and a wrong password produce identical envelopes, so the API cannot be used to
/// enumerate configured users.
#[tokio::test]
async fn password_mode_does_not_reveal_whether_the_user_exists() {
    let instance = password_gateway().await;
    let databases = "/v1/clusters/default/databases";

    let unknown_user = instance
        .api
        .get_with_basic(databases, "mallory", "secret")
        .await;
    let wrong_password = instance
        .api
        .get_with_basic(databases, "alice", "wrong")
        .await;
    assert_eq!(unknown_user.status(), 401);
    assert_eq!(wrong_password.status(), 401);

    let unknown_body: serde_json::Value = unknown_user.json().await.expect("JSON body");
    let wrong_body: serde_json::Value = wrong_password.json().await.expect("JSON body");
    assert_eq!(unknown_body["error"]["code"], wrong_body["error"]["code"]);
    assert_eq!(
        unknown_body["error"]["message"],
        wrong_body["error"]["message"]
    );

    instance.shutdown().await;
}

/// The catalog is readable and mutable end to end over the real listener, and paging is stateless.
#[tokio::test]
async fn the_catalog_can_be_read_and_mutated_over_the_real_listener() {
    let instance = gateway().await;
    let base = "/v1/clusters/default/databases";

    let created = instance
        .api
        .post_json(base, &json!({"name": "analytics"}))
        .await;
    assert_eq!(created.status(), 201);
    assert_eq!(
        created.headers()["location"],
        "/v1/clusters/default/databases/analytics"
    );

    // A one-entry page carries a token; the page after it is derived from the token alone, with nothing retained
    // between the two requests.
    let first = instance.api.get_ok(&format!("{base}?max_results=1")).await;
    assert_eq!(first["databases"], json!(["analytics"]));
    let token = first["next_page_token"].as_str().expect("token");
    let second = instance
        .api
        .get_ok(&format!("{base}?page_token={token}"))
        .await;
    assert_eq!(second["databases"], json!(["fluss"]));
    assert!(
        second.get("next_page_token").is_none(),
        "last page: {second}"
    );

    let table = instance
        .api
        .get_ok(&format!("{base}/fluss/tables/users"))
        .await;
    assert_eq!(table["kind"], "PRIMARY_KEY");
    assert!(table["table_id"].is_string(), "64-bit-safe table id");

    let dropped = instance.api.delete(&format!("{base}/analytics")).await;
    assert_eq!(dropped.status(), 204);

    instance.shutdown().await;
}

/// A request whose declared length exceeds the body cap is rejected from its headers alone.
///
/// Sent as a raw HTTP request that never writes the body: the gateway answers 413 (never 429)
/// before any payload exists, and reading instead of writing avoids racing the early close.
#[tokio::test]
async fn an_oversized_body_is_rejected_with_413_and_never_429() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let instance = gateway().await;
    let address = instance.gateway.local_addr();

    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    let request = format!(
        "POST /v1/clusters/default/databases/fluss/tables/users/records HTTP/1.1\r\n\
         Host: {address}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         \r\n",
        64 * 1024 * 1024
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send headers");

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read response");
    let response = String::from_utf8_lossy(&response);
    assert!(
        response.starts_with("HTTP/1.1 413"),
        "expected 413, got: {response}"
    );
    assert!(response.contains("limit_exceeded"), "got: {response}");

    instance.shutdown().await;
}

#[tokio::test]
async fn an_unknown_cluster_is_not_found_rather_than_unavailable() {
    let instance = gateway().await;

    let response = instance.api.get("/v1/clusters/missing/databases").await;

    // The route exists for every cluster id, so the only thing that can fail here is the cluster lookup itself:
    // an unconfigured cluster is a missing resource, never a transient backend outage.
    assert_eq!(response.status(), 404);
    let body: serde_json::Value = response.json().await.expect("JSON body");
    assert_eq!(body["error"]["code"], "not_found");
    assert_eq!(body["error"]["details"]["resource_kind"], "cluster");

    instance.shutdown().await;
}
