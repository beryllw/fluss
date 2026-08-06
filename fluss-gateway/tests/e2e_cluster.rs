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
//!
//! Container names and host ports are fixed, so invoking the suite again while the previous run's
//! containers are still being torn down can race; leave a few seconds between back-to-back runs.

mod support;

use fluss_gateway::auth::Secret;
use fluss_gateway::config::{AuthenticationMode, GatewayConfig, IdentityMode};
use fluss_gateway::lifecycle::{RunningGateway, start};
use fluss_test_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
use serde_json::{Value, json};
use std::collections::HashMap;
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

/// The append path of a log table with a NOT NULL column, which the primary-key journeys never
/// touch. This is the journey that exposed the client's IPC overhead probe rejecting synthetic
/// nulls against a non-nullable schema, failing every append to such a table.
#[tokio::test]
async fn log_table_append_journey_accepts_non_nullable_columns() {
    let bootstrap = CLUSTER.plaintext_bootstrap_servers().to_string();
    let (gateway, api) = start_gateway(gateway_config(&bootstrap)).await;
    wait_until_available(&api, None).await;

    let tables = "/v1/clusters/default/databases/fluss/tables";
    let created = api
        .post_json(
            tables,
            &json!({
                "table_name": "e2e_events",
                "columns": [
                    {"name": "ts", "data_type": {"type": "BIGINT", "nullable": false}},
                    {"name": "message", "data_type": {"type": "STRING", "nullable": true}}
                ]
            }),
        )
        .await;
    assert_eq!(created.status(), 201, "create log table");

    let written = expect_status(
        api.post_json(
            &format!("{tables}/e2e_events/records"),
            &json!({"entries": [
                {"id": "e1", "append": {"ts": "1700000000000", "message": "hello"}},
                {"id": "e2", "append": {"ts": "1700000000001", "message": null}}
            ]}),
        )
        .await,
        200,
        "append batch",
    )
    .await;
    assert_eq!(written["success_count"], 2, "{written}");
    assert_eq!(written["failures"], json!([]), "{written}");

    expect_status(
        api.delete(&format!("{tables}/e2e_events")).await,
        204,
        "drop log table",
    )
    .await;
    gateway.shutdown().await.expect("clean shutdown");
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
    assert_eq!(body["error"]["code"], "unavailable", "{body}");
    gateway.shutdown().await.expect("clean shutdown");
}

/// The act-as journeys need a Fluss image with SASL/PLAIN impersonation support (built from this
/// branch); the released images do not carry it yet, so they are gated behind an env variable:
///
/// ```bash
/// FLUSS_IMPERSONATION_IMAGE=apache/fluss FLUSS_IMPERSONATION_VERSION=fip49-poc \
///     cargo test --features integration_tests --test e2e_cluster
/// ```
fn impersonation_image() -> Option<(String, String)> {
    let image = std::env::var("FLUSS_IMPERSONATION_IMAGE").ok()?;
    let tag = std::env::var("FLUSS_IMPERSONATION_VERSION").unwrap_or_else(|_| "latest".to_string());
    Some((image, tag))
}

/// An impersonation-enabled cluster: the service account may act as `alice` and `bob` — and
/// deliberately not as anyone else. Ports 19323/19423 keep it clear of the shared cluster.
static ACT_AS_CLUSTER: LazyLock<FlussTestingCluster> = LazyLock::new(|| {
    let (image, tag) = impersonation_image().expect("checked by the test before first use");
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().expect("cluster runtime");
        runtime.block_on(async {
            FlussTestingClusterBuilder::new("gateway-e2e-act-as")
                .with_sasl(vec![(
                    SERVICE_ACCOUNT.to_string(),
                    SERVICE_SECRET.to_string(),
                )])
                .with_sasl_impersonation(vec![(
                    SERVICE_ACCOUNT.to_string(),
                    "alice,bob".to_string(),
                )])
                .with_image(image, tag)
                .with_port(19323)
                .build()
                .await
        })
    })
    .join()
    .expect("cluster thread")
});

/// The FIP-49 user identity mode end to end: every REST principal reaches Fluss over its own
/// SASL connection carrying its name as the authorization id. Users inside the server-side
/// allowlist complete real writes and lookups; a user outside it authenticates to the gateway
/// but the server refuses the act-as connection, so the request answers UNAVAILABLE.
#[tokio::test]
async fn user_identity_mode_acts_as_end_users_end_to_end() {
    if impersonation_image().is_none() {
        eprintln!(
            "skipping act-as e2e: set FLUSS_IMPERSONATION_IMAGE to an impersonation-enabled image"
        );
        return;
    }
    let bootstrap = ACT_AS_CLUSTER
        .sasl_bootstrap_servers()
        .expect("the cluster exposes a SASL listener")
        .to_string();

    // User identity mode demands verified client identities, so the gateway runs password mode;
    // carol is a legitimate gateway user who is missing from the server-side allowlist.
    let mut config = gateway_config(&bootstrap);
    {
        let cluster = config
            .clusters
            .values_mut()
            .next()
            .expect("the default cluster");
        cluster.service_account = Some(SERVICE_ACCOUNT.to_string());
        cluster.service_password = Some(Secret::from(SERVICE_SECRET));
        cluster.identity_mode = IdentityMode::User;
    }
    config.security.authentication = AuthenticationMode::Password;
    config.security.users = Some("alice:pw-a,bob:pw-b,carol:pw-c".to_string());
    config.validate().expect("valid user-mode configuration");
    let (gateway, api) = start_gateway(config).await;
    wait_until_available(&api, Some(("alice", "pw-a"))).await;

    // Alice creates the table over her own act-as connection.
    let tables = "/v1/clusters/default/databases/fluss/tables";
    let table = format!("{tables}/e2e_act_as");
    expect_status(
        api.post_json_with_basic(
            tables,
            &json!({
                "table_name": "e2e_act_as",
                "columns": [
                    {"name": "id", "data_type": {"type": "BIGINT", "nullable": false}},
                    {"name": "author", "data_type": {"type": "STRING", "nullable": true}}
                ],
                "primary_key": {"columns": ["id"]}
            }),
            "alice",
            "pw-a",
        )
        .await,
        201,
        "create table as alice",
    )
    .await;

    // Alice and bob write over their own connections; bob's lookup sees both rows.
    for (user, pass, entry_id, row_id) in [("alice", "pw-a", "a1", 1), ("bob", "pw-b", "b1", 2)] {
        let written = expect_status(
            api.post_json_with_basic(
                &format!("{table}/records"),
                &json!({"entries": [
                    {"id": entry_id, "upsert": {"id": row_id, "author": user}}
                ]}),
                user,
                pass,
            )
            .await,
            200,
            &format!("write as {user}"),
        )
        .await;
        assert_eq!(written["success_count"], 1, "{written}");
    }
    let looked_up = expect_status(
        api.post_json_with_basic(
            &format!("{table}/records/lookup"),
            &json!({"keys": [{"id": 1}, {"id": 2}]}),
            "bob",
            "pw-b",
        )
        .await,
        200,
        "lookup as bob",
    )
    .await;
    assert_eq!(looked_up["results"][0]["row"]["author"], "alice");
    assert_eq!(looked_up["results"][1]["row"]["author"], "bob");

    // Carol clears gateway authentication but the server refuses to let the service account act
    // as her: the act-as dial is definitively rejected, which is her 403.
    let refused = api
        .post_json_with_basic(
            &format!("{table}/records/lookup"),
            &json!({"keys": [{"id": 1}]}),
            "carol",
            "pw-c",
        )
        .await;
    let status = refused.status();
    let body: Value = refused.json().await.expect("JSON body");
    assert_eq!(status, 403, "{body}");
    assert_eq!(body["error"]["code"], "unauthorized", "{body}");
    assert_eq!(body["error"]["retryable"], false, "{body}");

    gateway.shutdown().await.expect("clean shutdown");
}

/// A backpressure-tuned cluster, following the server's own KvTabletTest recipe: a 1 KiB RocksDB
/// write buffer shrinks the flush budget to roughly twenty write-buffer-sized files (~20 KiB), so
/// any ordinary storm batch is rejected by the admission gate *before* touching RocksDB — a
/// deterministic hard StorageBackpressureException with no memtable stalls — while single small
/// rows still fit. The proactive Fluss L0 trigger is parked high so the soft-throttle path stays
/// quiet and the journey observes the hard-rejection contract alone. Needs the branch image for
/// the backpressure protocol chain; port 19523 keeps it clear of the other clusters.
static BACKPRESSURE_CLUSTER: LazyLock<FlussTestingCluster> = LazyLock::new(|| {
    let (image, tag) = impersonation_image().expect("checked by the test before first use");
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().expect("cluster runtime");
        runtime.block_on(async {
            let mut conf = HashMap::new();
            conf.insert("kv.rocksdb.writebuffer.size".to_string(), "1kb".to_string());
            conf.insert(
                "kv.backpressure.l0-slowdown-trigger".to_string(),
                "1000000".to_string(),
            );
            FlussTestingClusterBuilder::new_with_cluster_conf("gateway-e2e-backpressure", &conf)
                .with_image(image, tag)
                .with_port(19523)
                .build()
                .await
        })
    })
    .join()
    .expect("cluster thread")
});

/// Pseudo-random alphanumeric noise: incompressible enough that a row's wire size stays close
/// to its logical size, so the server-side flush-budget arithmetic is driven by real bytes.
fn noise(seed: u64, len: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = String::with_capacity(len + 8);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        for byte in state.to_le_bytes() {
            out.push(char::from(b'a' + (byte % 26)));
        }
    }
    out.truncate(len);
    out
}

/// The FIP-49 `storage_backpressure` contract end to end: driving the KV store past its flush
/// budget makes the server hard-reject writes, the client retries inside the delivery budget and
/// then surfaces the typed code, and the gateway reports it only inside `failures[]` of a 200
/// partial-success response — retriable, rejected, and never a whole-request HTTP status.
#[tokio::test]
async fn storage_backpressure_surfaces_as_entry_level_retriable_failures() {
    if impersonation_image().is_none() {
        eprintln!(
            "skipping backpressure e2e: set FLUSS_IMPERSONATION_IMAGE to an image built from this branch"
        );
        return;
    }
    let bootstrap = BACKPRESSURE_CLUSTER
        .plaintext_bootstrap_servers()
        .to_string();

    // A short delivery budget makes the client exhaust its backpressure retries quickly, so the
    // typed failure surfaces within the test window instead of being retried away. It still has
    // to fit at least one full round trip plus a retry, or every batch just expires in flight as
    // the FIP's indeterminate timeout before a rejection ever reaches it.
    let mut config = gateway_config(&bootstrap);
    config.write.max_delivery_time = fluss_gateway::config::ConfigDuration::from_secs(4);
    config.validate().expect("valid backpressure configuration");
    let (gateway, api) = start_gateway(config).await;
    wait_until_available(&api, None).await;

    // One bucket concentrates every write on the same RocksDB instance.
    let tables = "/v1/clusters/default/databases/fluss/tables";
    let table = format!("{tables}/e2e_backpressure");
    expect_status(
        api.post_json(
            tables,
            &json!({
                "table_name": "e2e_backpressure",
                "columns": [
                    {"name": "id", "data_type": {"type": "BIGINT", "nullable": false}},
                    {"name": "payload", "data_type": {"type": "STRING", "nullable": true}}
                ],
                "primary_key": {"columns": ["id"]},
                "distribution": {"bucket_count": 1, "bucket_keys": ["id"]}
            }),
        )
        .await,
        201,
        "create table",
    )
    .await;

    // A gentle write first: the cluster genuinely accepts data before the storm begins.
    let warmed_up = expect_status(
        api.post_json(
            &format!("{table}/records"),
            &json!({"entries": [{"id": "warm", "upsert": {"id": 0, "payload": "warm-up"}}]}),
        )
        .await,
        200,
        "warm-up write",
    )
    .await;
    assert_eq!(warmed_up["success_count"], 1, "{warmed_up}");

    // A batch of one hundred 4 KiB rows into the single bucket: every PutKv the client
    // assembles from them exceeds the ~20 KiB flush budget on its own, so the admission gate
    // rejects each attempt outright and the client's retry budget drains into the typed failure.
    let mut backpressure_failures = 0usize;
    let mut successes = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut batch = 0u64;
    'drive: while std::time::Instant::now() < deadline {
        let entries: Vec<Value> = (0..100u64)
            .map(|row| {
                let id = batch * 1_000_000 + row;
                json!({"id": format!("e{id}"), "upsert": {"id": id, "payload": noise(id, 4096)}})
            })
            .collect();
        batch += 1;
        let response = api
            .post_json(&format!("{table}/records"), &json!({"entries": entries}))
            .await;
        // The FIP contract: backpressure is never a whole-request HTTP status.
        let status = response.status();
        let body: Value = response.json().await.expect("JSON body");
        assert_eq!(status, 200, "{body}");
        successes += body["success_count"].as_u64().unwrap_or(0) as usize;
        for failure in body["failures"].as_array().into_iter().flatten() {
            if failure["error_code"] == "storage_backpressure" {
                assert_eq!(failure["retryable"], true, "{failure}");
                assert_eq!(failure["completion"], "rejected", "{failure}");
                backpressure_failures += 1;
            } else {
                // A batch that expires in flight before a rejection reaches it is the FIP's
                // indeterminate timeout — legitimate under overload, but not what this
                // journey is hunting for.
                assert_eq!(
                    failure["error_code"], "timeout",
                    "unexpected entry failure under backpressure: {failure}"
                );
            }
        }
        if backpressure_failures > 0 {
            break 'drive;
        }
    }

    assert!(
        backpressure_failures > 0,
        "the cluster never rejected a write with storage_backpressure \
         ({successes} rows written in {batch} rounds)"
    );

    gateway.shutdown().await.expect("clean shutdown");
}
