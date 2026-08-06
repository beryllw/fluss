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

#![cfg(feature = "integration_tests")]

//! End-to-end suite against a dockerized Fluss cluster.
//!
//! Gated behind the `integration_tests` feature because it needs Docker. Without the feature the whole file
//! compiles away, so the test target still builds in the default gate. Run it with
//! `cargo test --features integration_tests --test e2e_cluster`.
//!
//! One shared cluster serves every journey: it exposes a plaintext listener for the trust and
//! password gateways and a SASL listener for the service-account gateway. Each test starts its own
//! in-process gateway on an ephemeral port over the production `lifecycle::start` path, so the
//! native backend, the reconnect supervisor, and the real HTTP listener are all exercised.

mod support;

use fluss_gateway::auth::Secret;
use fluss_gateway::config::{AuthenticationMode, GatewayConfig};
use fluss_gateway::lifecycle::{RunningGateway, start};
use fluss_test_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
use serde_json::{Value, json};
use std::sync::LazyLock;
use std::time::Duration;
use support::Api;

/// One SASL user for the service-account journey; the plaintext listener needs no credentials.
const SERVICE_ACCOUNT: &str = "gateway_svc";
const SERVICE_SECRET: &str = "svc-secret";

/// The shared cluster, started once for the whole suite. Ports 19123 (SASL) and 19223 (plaintext)
/// keep it clear of the fluss-rs integration suite's cluster on 9123/9223.
static CLUSTER: LazyLock<FlussTestingCluster> = LazyLock::new(|| {
    std::thread::spawn(|| {
        let runtime = tokio::runtime::Runtime::new().expect("cluster runtime");
        runtime.block_on(async {
            FlussTestingClusterBuilder::new("gateway-e2e")
                .with_sasl(vec![(
                    SERVICE_ACCOUNT.to_string(),
                    SERVICE_SECRET.to_string(),
                )])
                .with_port(19123)
                .build()
                .await
        })
    })
    .join()
    .expect("cluster thread")
});

/// Ephemeral-port gateway configuration whose single `default` cluster dials `bootstrap`.
fn gateway_config(bootstrap: &str) -> GatewayConfig {
    let mut config = support::ephemeral_config();
    let cluster = config
        .clusters
        .values_mut()
        .next()
        .expect("the default cluster");
    cluster.bootstrap_servers = vec![bootstrap.to_string()];
    config.validate().expect("valid e2e configuration");
    config
}

/// Starts a gateway over the production entry point and binds a client to it.
async fn start_gateway(config: GatewayConfig) -> (RunningGateway, Api) {
    let gateway = start(config).await.expect("the gateway starts");
    let api = Api::new(format!("http://{}", gateway.local_addr()));
    (gateway, api)
}

/// Polls cluster discovery until the backend connects, with optional Basic credentials.
async fn wait_until_available(api: &Api, credentials: Option<(&str, &str)>) {
    for _ in 0..240 {
        let response = match credentials {
            Some((user, pass)) => api.get_with_basic("/v1/clusters", user, pass).await,
            None => api.get("/v1/clusters").await,
        };
        if response.status() == 200 {
            let body: Value = response.json().await.expect("JSON body");
            if body["clusters"][0]["state"] == "available" {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("the cluster never became available through the gateway");
}

/// Asserts a status while keeping the response body in the failure message.
async fn expect_status(response: reqwest::Response, expected: u16, label: &str) -> Value {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert_eq!(status, expected, "{label}: {body}");
    serde_json::from_str(&body).unwrap_or(Value::Null)
}

/// The whole data plane over a real cluster: DDL round trip, a mixed write batch with per-entry
/// outcomes, key and prefix lookups against real data, upsert convergence, and the drop.
#[tokio::test]
async fn plaintext_journey_covers_ddl_write_and_lookups() {
    let bootstrap = CLUSTER.plaintext_bootstrap_servers().to_string();
    let (gateway, api) = start_gateway(gateway_config(&bootstrap)).await;
    wait_until_available(&api, None).await;

    let tables = "/v1/clusters/default/databases/fluss/tables";
    let table = format!("{tables}/e2e_users");

    // Create: primary key (region, id) with the bucket key on its prefix, so prefix lookups route.
    let created = api
        .post_json(
            tables,
            &json!({
                "table_name": "e2e_users",
                "columns": [
                    {"name": "region", "data_type": {"type": "STRING", "nullable": false}},
                    {"name": "id", "data_type": {"type": "BIGINT", "nullable": false}},
                    {"name": "name", "data_type": {"type": "STRING", "nullable": true}}
                ],
                "primary_key": {"columns": ["region", "id"]},
                "distribution": {"bucket_count": 3, "bucket_keys": ["region"]}
            }),
        )
        .await;
    expect_status(created, 201, "create table").await;

    // Describe reads back what was created.
    let described = expect_status(api.get(&table).await, 200, "describe table").await;
    assert_eq!(described["primary_key"]["columns"], json!(["region", "id"]));

    // A mixed batch: two upserts plus a delete of an absent key, which converges on a PK table.
    let written = expect_status(
        api.post_json(
            &format!("{table}/records"),
            &json!({"entries": [
                {"id": "w1", "upsert": {"region": "eu", "id": 1, "name": "alice"}},
                {"id": "w2", "upsert": {"region": "us", "id": 2, "name": "bob"}},
                {"id": "w3", "delete": {"region": "eu", "id": 9}}
            ]}),
        )
        .await,
        200,
        "write batch",
    )
    .await;
    assert_eq!(written["success_count"], 3, "{written}");
    assert_eq!(written["error_count"], 0, "{written}");

    // Key lookups: two hits, one miss.
    let looked_up = expect_status(
        api.post_json(
            &format!("{table}/records/lookup"),
            &json!({"keys": [
                {"region": "eu", "id": 1},
                {"region": "us", "id": 2},
                {"region": "eu", "id": 9}
            ]}),
        )
        .await,
        200,
        "lookup",
    )
    .await;
    assert_eq!(looked_up["results"][0]["found"], true, "{looked_up}");
    assert_eq!(looked_up["results"][0]["row"]["name"], "alice");
    assert_eq!(looked_up["results"][1]["found"], true);
    assert_eq!(looked_up["results"][2]["found"], false);

    // Prefix lookup over the bucket key returns the region's rows.
    let prefixed = expect_status(
        api.post_json(
            &format!("{table}/records/prefix-lookup"),
            &json!({"prefix_columns": ["region"], "prefixes": [{"region": "eu"}]}),
        )
        .await,
        200,
        "prefix lookup",
    )
    .await;
    // BIGINT values render as JSON strings, per the FIP type mapping.
    assert_eq!(prefixed["results"][0]["rows"][0]["id"], "1", "{prefixed}");

    // Upserting the same key again converges instead of duplicating.
    expect_status(
        api.post_json(
            &format!("{table}/records"),
            &json!({"entries": [{"id": "w4", "upsert": {"region": "eu", "id": 1, "name": "alice2"}}]}),
        )
        .await,
        200,
        "second upsert",
    )
    .await;
    let converged = expect_status(
        api.post_json(
            &format!("{table}/records/lookup"),
            &json!({"keys": [{"region": "eu", "id": 1}]}),
        )
        .await,
        200,
        "lookup after upsert",
    )
    .await;
    assert_eq!(converged["results"][0]["row"]["name"], "alice2");

    // Drop, and the table is gone.
    let dropped = api.delete(&table).await;
    assert_eq!(dropped.status(), 204);
    let gone = api.get(&table).await;
    assert_eq!(gone.status(), 404);

    gateway.shutdown().await.expect("clean shutdown");
}

/// Password authentication guards the whole surface over a real cluster: anonymous requests are
/// rejected with a challenge, and the configured user completes a real metadata call.
#[tokio::test]
async fn password_gateway_enforces_credentials_over_a_real_cluster() {
    let bootstrap = CLUSTER.plaintext_bootstrap_servers().to_string();
    let mut config = gateway_config(&bootstrap);
    config.security.authentication = AuthenticationMode::Password;
    config.security.users = Some("alice:secret123".to_string());
    config.validate().expect("valid password configuration");
    let (gateway, api) = start_gateway(config).await;

    let denied = api.get("/v1/clusters/default/databases").await;
    assert_eq!(denied.status(), 401);
    assert!(denied.headers().contains_key("www-authenticate"));

    wait_until_available(&api, Some(("alice", "secret123"))).await;
    let listed = expect_status(
        api.get_with_basic("/v1/clusters/default/databases", "alice", "secret123")
            .await,
        200,
        "list databases",
    )
    .await;
    assert!(
        listed["databases"]
            .as_array()
            .expect("databases array")
            .iter()
            .any(|db| db == "fluss"),
        "{listed}"
    );

    gateway.shutdown().await.expect("clean shutdown");
}

/// The service-account transition mode end to end: the gateway authenticates to the SASL listener
/// as the configured account and the whole data plane works over that connection, while a wrong
/// secret keeps the cluster unavailable instead of falling back to plaintext.
#[tokio::test]
async fn sasl_service_account_gateway_reaches_the_cluster() {
    let bootstrap = CLUSTER
        .sasl_bootstrap_servers()
        .expect("the cluster exposes a SASL listener")
        .to_string();

    let mut config = gateway_config(&bootstrap);
    {
        let cluster = config
            .clusters
            .values_mut()
            .next()
            .expect("the default cluster");
        cluster.service_account = Some(SERVICE_ACCOUNT.to_string());
        cluster.service_password = Some(Secret::from(SERVICE_SECRET));
    }
    config.validate().expect("valid SASL configuration");
    let (gateway, api) = start_gateway(config).await;
    wait_until_available(&api, None).await;

    // A real create + write + lookup proves the SASL connection carries the data plane.
    let tables = "/v1/clusters/default/databases/fluss/tables";
    let table = format!("{tables}/e2e_sasl");
    expect_status(
        api.post_json(
            tables,
            &json!({
                "table_name": "e2e_sasl",
                "columns": [
                    {"name": "id", "data_type": {"type": "BIGINT", "nullable": false}},
                    {"name": "note", "data_type": {"type": "STRING", "nullable": true}}
                ],
                "primary_key": {"columns": ["id"]}
            }),
        )
        .await,
        201,
        "create table over SASL",
    )
    .await;
    let written = expect_status(
        api.post_json(
            &format!("{table}/records"),
            &json!({"entries": [{"id": "s1", "upsert": {"id": 7, "note": "via sasl"}}]}),
        )
        .await,
        200,
        "write over SASL",
    )
    .await;
    assert_eq!(written["success_count"], 1, "{written}");
    let looked_up = expect_status(
        api.post_json(
            &format!("{table}/records/lookup"),
            &json!({"keys": [{"id": 7}]}),
        )
        .await,
        200,
        "lookup over SASL",
    )
    .await;
    assert_eq!(looked_up["results"][0]["row"]["note"], "via sasl");
    gateway.shutdown().await.expect("clean shutdown");

    // The wrong secret is rejected by the server: the cluster stays unavailable and a data-plane
    // request answers the UNAVAILABLE envelope instead of silently degrading.
    let mut config = gateway_config(&bootstrap);
    {
        let cluster = config
            .clusters
            .values_mut()
            .next()
            .expect("the default cluster");
        cluster.service_account = Some(SERVICE_ACCOUNT.to_string());
        cluster.service_password = Some(Secret::from("not-the-secret"));
    }
    config
        .validate()
        .expect("valid configuration, wrong secret");
    let (gateway, api) = start_gateway(config).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let clusters = expect_status(api.get("/v1/clusters").await, 200, "discovery").await;
    assert_eq!(
        clusters["clusters"][0]["state"], "unavailable",
        "{clusters}"
    );
    let refused = api.get("/v1/clusters/default/databases").await;
    let status = refused.status();
    let body: Value = refused.json().await.expect("JSON body");
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["error"]["code"], "UNAVAILABLE", "{body}");
    gateway.shutdown().await.expect("clean shutdown");
}
