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

//! T2/T4 — REST direct-path integration tests (P5).
//!
//! Drives the spawned REST frontend with a real HTTP client (`reqwest`) over
//! loopback TCP, against the `FakeInstance` in `harness`. Covers the P5
//! completion criteria: direct write (KvUpsert / KvDelete / LogAppend) over both
//! body encodings (JSON rows + Arrow IPC stream), the three read-only metadata
//! endpoints, the domain→HTTP error map (404 / 501 / 401), the deferred read
//! endpoints returning 501, and the semantic guarantee that a direct request
//! creates no session. No Fluss cluster is required.

mod harness;

use std::collections::HashMap;
use std::sync::Arc;

use fluss_gateway::auth::{ConfigUserStoreAuthenticator, StoredSecret};
use harness::{FakeInstance, RestTestServer, WriteKind};
use sha2::Digest;

const JSON: &str = "application/json";
const ARROW: &str = "application/vnd.apache.arrow.stream";

/// Basic auth header for `user` with an ignored password (Phase 1 trust).
fn basic(user: &str) -> String {
    basic_with_password(user, "ignored")
}

fn basic_with_password(user: &str, password: &str) -> String {
    let raw = format!("{user}:{password}");
    encode_base64(raw.as_bytes())
}

fn encode_base64(input: &[u8]) -> String {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        out.push(A[(b[0] >> 2) as usize] as char);
        out.push(A[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            A[(((b[1] & 0x0f) << 2) | (b[2] >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            A[(b[2] & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// JSON rows matching the canned `(id int, name text)` schema.
fn json_rows() -> &'static str {
    r#"[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]"#
}

/// An Arrow IPC stream of 3 `(id, name)` rows.
fn arrow_rows() -> Vec<u8> {
    use std::sync::Arc as StdArc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;

    let schema = StdArc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            StdArc::new(Int32Array::from(vec![10, 20, 30])),
            StdArc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

#[tokio::test]
async fn kv_upsert_json_succeeds_and_reaches_instance() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables/t/records", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(json_rows())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows_written"], 2);

    let writes = server.instance.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].kind, WriteKind::KvUpsert);
    assert_eq!(writes[0].table.database, "db");
    assert_eq!(writes[0].table.table, "t");
    assert_eq!(writes[0].principal, "alice", "principal must come from Basic auth");
    assert_eq!(writes[0].cluster, "default", "cluster must come from path prefix");
    assert_eq!(writes[0].rows, 2);
}

#[tokio::test]
async fn log_append_arrow_stream_succeeds() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables/t/records", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("bob")))
        .header("Content-Type", ARROW)
        .body(arrow_rows())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows_written"], 3);

    let writes = server.instance.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].rows, 3);
    assert_eq!(writes[0].principal, "bob");
}

#[tokio::test]
async fn kv_delete_routes_to_delete_kind() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!(
            "{}/databases/db/tables/t/records:delete",
            server.base_url()
        ))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(r#"[{"id":1,"name":"x"}]"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let writes = server.instance.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].kind, WriteKind::KvDelete);
}

#[tokio::test]
async fn metadata_endpoints_return_deterministic_values() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();
    let auth = format!("Basic {}", basic("alice"));

    // list databases
    let resp = client
        .get(format!("{}/databases", server.base_url()))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["databases"], serde_json::json!(["fluss"]));

    // list tables
    let resp = client
        .get(format!("{}/databases/db/tables", server.base_url()))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["tables"], serde_json::json!(["t"]));

    // get table info
    let resp = client
        .get(format!("{}/databases/db/tables/t", server.base_url()))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["database"], "db");
    assert_eq!(body["table"], "t");
    assert_eq!(body["columns"].as_array().unwrap().len(), 2);
    assert_eq!(body["columns"][0]["name"], "id");
}

#[tokio::test]
async fn unknown_table_maps_to_404() {
    let instance = Arc::new(FakeInstance::new());
    instance.missing_tables.lock().unwrap().push("ghost".into());
    let server = RestTestServer::start_with(instance).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!(
            "{}/databases/db/tables/ghost/records",
            server.base_url()
        ))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(json_rows())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "table_not_found");
}

#[tokio::test]
async fn missing_auth_maps_to_401() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables/t/records", server.base_url()))
        .header("Content-Type", JSON)
        .body(json_rows())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "unauthenticated");
    // The write must never reach the instance when unauthenticated.
    assert!(server.instance.writes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn password_authenticator_accepts_correct_password_and_rejects_others() {
    let mut users = HashMap::new();
    users.insert("alice".to_string(), StoredSecret::Plain("secret123".into()));
    users.insert(
        "bob".to_string(),
        StoredSecret::Sha256(sha2::Sha256::digest(b"secret456").into()),
    );
    let auth = Arc::new(ConfigUserStoreAuthenticator::new(users));
    let server = RestTestServer::start_with_authenticator(Arc::new(FakeInstance::new()), auth).await;
    let client = reqwest::Client::new();

    let ok = client
        .get(format!("{}/databases", server.base_url()))
        .header("Authorization", format!("Basic {}", basic_with_password("alice", "secret123")))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200, "correct plaintext password accepted");

    let ok = client
        .get(format!("{}/databases", server.base_url()))
        .header("Authorization", format!("Basic {}", basic_with_password("bob", "secret456")))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200, "sha256-configured user sends cleartext password");

    for authz in [
        basic_with_password("alice", "wrong"),
        basic_with_password("nobody", "secret123"),
    ] {
        let resp = client
            .get(format!("{}/databases", server.base_url()))
            .header("Authorization", format!("Basic {}", authz))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "unauthenticated");
    }
}

#[tokio::test]
async fn bad_content_type_maps_to_400() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables/t/records", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", "text/csv")
        .body("id,name\n1,a")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "invalid_argument");
}

#[tokio::test]
async fn deferred_read_endpoints_return_501() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();
    let auth = format!("Basic {}", basic("alice"));

    for path in ["lookup", "prefix-scan", "log-scan"] {
        let resp = client
            .post(format!(
                "{}/databases/db/tables/t/{path}",
                server.base_url()
            ))
            .header("Authorization", &auth)
            .header("Content-Type", JSON)
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 501, "{path} should be Not Implemented");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "unsupported");
    }
}

/// Semantic guarantee (direct-path.md §7): a direct REST request creates no
/// `GatewaySession`. We drive several writes + metadata reads and assert the
/// fake never had `open_session` called.
#[tokio::test]
async fn direct_path_opens_no_session() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();
    let auth = format!("Basic {}", basic("alice"));

    client
        .post(format!("{}/databases/db/tables/t/records", server.base_url()))
        .header("Authorization", &auth)
        .header("Content-Type", JSON)
        .body(json_rows())
        .send()
        .await
        .unwrap();
    client
        .get(format!("{}/databases", server.base_url()))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();

    assert_eq!(
        *server.instance.sessions_opened.lock().unwrap(),
        0,
        "direct path must not open any GatewaySession"
    );
}

/// at-least-once (direct-path.md §6): a successful 2xx means backend ack. This
/// test pins the success contract; the unknown-on-timeout case is documented
/// behavior (no rollback, no cancel) and is asserted at the unit level via the
/// error map (Timeout -> 504) rather than racing a real timeout here.
#[tokio::test]
async fn successful_write_is_backend_ack() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables/t/records", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(json_rows())
        .send()
        .await
        .unwrap();

    assert!(resp.status().is_success());
    // Exactly one ack recorded, with the rows we submitted (no phantom retries).
    assert_eq!(server.instance.writes.lock().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// table management (DDL) — design/direct-path.md "表管理（DDL）API"
// ---------------------------------------------------------------------------

const CREATE_BODY: &str = r#"{
  "table_name": "gw_kv",
  "columns": [
    {"name": "id",   "type": "INT",    "nullable": false},
    {"name": "name", "type": "STRING"}
  ],
  "primary_key": ["id"],
  "distribution": {"bucket_keys": ["id"], "bucket_count": 1},
  "configs": [{"name": "k", "value": "v"}]
}"#;

#[tokio::test]
async fn create_table_succeeds_and_reaches_instance() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(CREATE_BODY)
        .send()
        .await
        .unwrap();

    // 201 Created, returning the new table's metadata (from the fake's get_table_info).
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["database"], "db");
    assert_eq!(body["table"], "gw_kv");

    let created = server.instance.created_tables.lock().unwrap();
    assert_eq!(created.len(), 1, "create reached the instance exactly once");
    assert_eq!(created[0].table.database, "db");
    assert_eq!(created[0].table.table, "gw_kv");
    assert_eq!(created[0].primary_key, vec!["id".to_string()]);
    assert_eq!(created[0].columns.len(), 2);
    assert!(!created[0].columns[0].nullable, "id parsed as NOT NULL");
    assert!(created[0].columns[1].nullable, "name defaults to nullable");
}

#[tokio::test]
async fn create_table_validate_only_does_not_create() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let vbody = r#"{"table_name":"gw_kv","columns":[{"name":"id","type":"INT"}],"validate_only":true}"#;
    let resp = client
        .post(format!("{}/databases/db/tables", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(vbody)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "validate_only returns 200, not 201");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["validate_only"], true);
    assert!(
        server.instance.created_tables.lock().unwrap().is_empty(),
        "validate_only must not create the table"
    );
}

#[tokio::test]
async fn create_table_duplicate_maps_to_409() {
    let instance = Arc::new(FakeInstance::new());
    instance.existing_tables.lock().unwrap().push("gw_kv".into());
    let server = RestTestServer::start_with(instance).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(CREATE_BODY)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 409);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "table_already_exists");
}

#[tokio::test]
async fn create_table_bad_type_maps_to_400() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(r#"{"table_name":"t","columns":[{"name":"c","type":"NOTATYPE"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "invalid_argument");
}

#[tokio::test]
async fn create_table_requires_auth() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables", server.base_url()))
        .header("Content-Type", JSON)
        .body(CREATE_BODY)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
    assert!(server.instance.created_tables.lock().unwrap().is_empty());
}

#[tokio::test]
async fn drop_table_succeeds() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .delete(format!("{}/databases/db/tables/gw_kv", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204);
    let dropped = server.instance.dropped_tables.lock().unwrap();
    assert_eq!(dropped.as_slice(), ["gw_kv"]);
}
