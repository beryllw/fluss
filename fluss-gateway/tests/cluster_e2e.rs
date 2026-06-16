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

//! T1-T4 — real-cluster end-to-end verification (DESIGN.md §3.3 integration
//! model; sql-path.md §P3.3 assembly order; direct-path.md §P5 at-least-once).
//!
//! This is the final executable evidence that "data is really written, and SQL
//! really reads it back" against a live Apache Fluss cluster. The whole flow
//! runs once against one cluster (bringing one up is expensive), reusing a single
//! shared `FlussConnection`:
//!
//! 1. Assemble a REAL [`GatewayInstanceImpl`] over the cluster:
//!    `SharedProxyConnectionProvider` -> `Arc<FlussConnection>` ->
//!    `FlussDatafusion::new(conn)` -> `FlussDatafusionCatalogInstaller` ->
//!    `PgSqlEnvironmentProvider` -> `SqlEnvironmentRegistry`; the same connection
//!    backs a `FlussBackendFacade`; `GatewayInstanceImpl::new(...)` ties them.
//! 2. (b) REST WRITE: drive the spawned `RestServer` with `reqwest` — a KV upsert
//!    (JSON body) and a Log append (Arrow IPC body) — then read the rows back
//!    with the Fluss client (KV `Lookuper`) to prove the write actually landed.
//! 3. (c) PG SELECT: drive the spawned `PgServer` with `tokio-postgres` — a
//!    full-PK point lookup on the KV table, a KV bounded `LIMIT` scan, a KV
//!    prefix lookup (`WHERE c1 = ...` on a composite-PK table, datafusion-v0.2.4),
//!    and a `LIMIT` bounded scan on the Log table — asserting the just-written
//!    rows come back through the gateway's own SQL catalog path.
//! 4. (d) REST METADATA: list databases, list the tables in the database, and
//!    fetch each table's schema (getMetadata) straight from the live Fluss
//!    catalog through the gateway's metadata surface.
//! 5. T4 semantics that need a cluster: at-least-once (REST 2xx == backend ack)
//!    and "direct path opens no session" are asserted here against the real
//!    instance. The cluster-free T4 semantics (SessionContext dirty/rebuild,
//!    cooperative cancel, operation state machine) are covered by the in-crate
//!    unit tests (`session::session`, `session::operation`,
//!    `sql::environment::pg`) and the fake-backed `tests/integration.rs` /
//!    `tests/rest_integration.rs`; they are not duplicated here.
//!
//! Gated by `integration_tests` (needs a docker runtime + the
//! `apache/fluss:0.9.1-incubating` image). Default `cargo test` is unaffected.
//! Run with:
//!   cargo test --features integration_tests --manifest-path \
//!     fluss-gateway/Cargo.toml -- --nocapture

#![cfg(feature = "integration_tests")]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use fluss::client::FlussConnection;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::{DataGetters, GenericRow};
use fluss::rpc::message::OffsetSpec;
use fluss_test_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};

use fluss_gateway::auth::TrustAuthenticator;
use fluss_gateway::backend::FlussBackendFacade;
use fluss_gateway::cluster::{ClusterConfig, ClusterRegistry};
use fluss_gateway::connection::{FlussConnectionProvider, SharedProxyConnectionProvider};
use fluss_gateway::instance::{GatewayInstance, GatewayInstanceImpl};
use fluss_gateway::server::postgres::PgServer;
use fluss_gateway::server::rest::RestServer;
use fluss_gateway::session::manager::{SessionManager, SessionManagerConfig};
use fluss_gateway::sql::environment::{
    FlussDatafusionCatalogInstaller, PgSqlEnvironmentProvider, SqlEnvironmentRegistry,
    StubPgCatalogOverlayInstaller,
};
use fluss_gateway::types::{ClusterId, Principal, SqlEnvironmentId};

/// Dedicated name/port so this binary's cluster never collides with another.
const CLUSTER_NAME: &str = "gw-e2e";
const CLUSTER_PORT: u16 = 9143;
const READY_TIMEOUT: Duration = Duration::from_secs(30);

const DATABASE: &str = "fluss";
const KV_TABLE: &str = "gw_kv";
const LOG_TABLE: &str = "gw_log";
// Composite-PK KV table whose bucket key (`c1`) is a STRICT prefix of the PK
// `(c1, c2)`, so a `WHERE c1 = ...` predicate exercises KV prefix lookup
// (datafusion-v0.2.4). Seeded so one `c1` matches several rows.
const KV_PREFIX_TABLE: &str = "gw_kv_prefix";

const JSON: &str = "application/json";
const ARROW: &str = "application/vnd.apache.arrow.stream";

// ---------------------------------------------------------------------------
// cluster bring-up (mirrors the fluss-datafusion integration setup template)
// ---------------------------------------------------------------------------

async fn start_cluster() -> FlussTestingCluster {
    let cluster = FlussTestingClusterBuilder::new(CLUSTER_NAME)
        .with_port(CLUSTER_PORT)
        .build()
        .await;
    wait_for_ready(&cluster).await;
    cluster
}

async fn wait_for_ready(cluster: &FlussTestingCluster) {
    let start = std::time::Instant::now();
    loop {
        let connection = cluster.get_fluss_connection().await;
        if connection
            .get_metadata()
            .get_cluster()
            .get_one_available_server()
            .is_some()
        {
            return;
        }
        if start.elapsed() >= READY_TIMEOUT {
            panic!("cluster did not become ready in {READY_TIMEOUT:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Single-PK KV table `(id int PK, name string)`. Created empty; the E2E flows populate it through the REST path.
async fn create_kv_table(conn: &FlussConnection) {
    let path = TablePath::new(DATABASE, KV_TABLE);
    let admin = conn.get_admin().unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .primary_key(vec!["id"])
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    admin.create_table(&path, &descriptor, true).await.unwrap();
}

/// Single-bucket Log table `(id int, name string)`. One bucket keeps the bounded
/// scan deterministic. Created empty; the E2E flows populate it through the append path.
async fn create_log_table(conn: &FlussConnection) {
    let path = TablePath::new(DATABASE, LOG_TABLE);
    let admin = conn.get_admin().unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .build()
                .unwrap(),
        )
        .distributed_by(Some(1), vec![])
        .build()
        .unwrap();
    admin.create_table(&path, &descriptor, true).await.unwrap();
}

/// Composite-PK KV table `(c1 int, c2 int, name string)` with `PRIMARY KEY
/// (c1, c2)` and bucket key `c1` (a strict prefix of the PK). A `WHERE c1 = ...`
/// query then routes to KV prefix lookup. Created empty; populated through REST.
async fn create_kv_prefix_table(conn: &FlussConnection) {
    let path = TablePath::new(DATABASE, KV_PREFIX_TABLE);
    let admin = conn.get_admin().unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("c1", DataTypes::int())
                .column("c2", DataTypes::int())
                .column("name", DataTypes::string())
                .primary_key(vec!["c1", "c2"])
                .build()
                .unwrap(),
        )
        .distributed_by(Some(1), vec!["c1".to_string()])
        .build()
        .unwrap();
    admin.create_table(&path, &descriptor, true).await.unwrap();
}

/// Wait until bucket 0 of `table` can serve reads (REST append acked != bucket
/// readable; a bounded scan needs the offsets to exist first).
async fn wait_for_log_offsets(conn: &FlussConnection) {
    wait_for_bucket0(conn, LOG_TABLE).await;
}

/// Wait until bucket 0 of `table_name` (under `DATABASE`) can serve reads.
async fn wait_for_bucket0(conn: &FlussConnection, table_name: &str) {
    let admin = conn.get_admin().unwrap();
    let path = TablePath::new(DATABASE, table_name);
    let start = std::time::Instant::now();
    loop {
        if admin
            .list_offsets(&path, &[0], OffsetSpec::Latest)
            .await
            .is_ok()
        {
            return;
        }
        if start.elapsed() >= READY_TIMEOUT {
            panic!("{table_name} bucket not ready in {READY_TIMEOUT:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ---------------------------------------------------------------------------
// real GatewayInstanceImpl assembly (DESIGN.md §3.3 integration model)
// ---------------------------------------------------------------------------

/// Assemble the production facade over the live cluster. Returns the instance
/// plus the shared connection used for client-side readback assertions.
async fn assemble_instance(bootstrap: &str) -> (Arc<GatewayInstanceImpl>, Arc<FlussConnection>) {
    let cluster = ClusterId("default".into());
    let principal = Principal { name: "alice".into() };

    // Connection provider points the `default` cluster at the test cluster; the
    // provider returns ONE shared proxy connection, reused by SQL and direct.
    let registry = ClusterRegistry::single_default(ClusterConfig {
        bootstrap_servers: bootstrap.to_string(),
    });
    let conn_provider = SharedProxyConnectionProvider::new(registry);
    let connection = conn_provider.resolve(&cluster, &principal).await.unwrap();

    // SQL path: real Fluss catalog installer behind PgSqlEnvironmentProvider.
    // The overlay stays a stub (P6.4 deferred); it does not affect SELECT from
    // the real Fluss catalog.
    let fluss_df = Arc::new(
        fluss_datafusion::FlussDatafusion::new(
            Arc::clone(&connection),
            fluss_datafusion::FlussDatafusionOptions::default(),
        )
        .await
        .expect("FlussDatafusion::new over the live connection"),
    );
    let pg_provider = PgSqlEnvironmentProvider::new(
        Arc::new(FlussDatafusionCatalogInstaller::new(fluss_df)),
        Arc::new(StubPgCatalogOverlayInstaller),
    );
    let mut sql_environments = SqlEnvironmentRegistry::new();
    sql_environments.register(SqlEnvironmentId("postgres".into()), Arc::new(pg_provider));

    // Direct path: a backend over the SAME shared connection.
    let backend = Arc::new(FlussBackendFacade::new(Arc::clone(&connection)));
    let sessions = Arc::new(SessionManager::new(SessionManagerConfig::default()));

    let instance = Arc::new(GatewayInstanceImpl::new(
        sessions,
        backend,
        Arc::new(sql_environments),
    ));
    (instance, connection)
}

// ---------------------------------------------------------------------------
// the single end-to-end test
// ---------------------------------------------------------------------------

// Multi-threaded runtime: the in-process PG/REST servers and the wire clients
// run concurrently against each other on the same runtime, and the Fluss catalog
// bridges sync DataFusion callbacks across threads — a current-thread runtime can
// starve one side. This mirrors how the gateway actually runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn cluster_rest_kv_and_log_then_pg_selects() {
    let cluster = start_cluster().await;
    let bootstrap = cluster.plaintext_bootstrap_servers().to_string();
    let connection = Arc::new(cluster.get_fluss_connection().await);

    create_kv_table(&connection).await;
    create_log_table(&connection).await;
    create_kv_prefix_table(&connection).await;

    let (instance, gw_conn) = assemble_instance(&bootstrap).await;

    let rest = RestServer::new(
        instance.clone() as Arc<dyn GatewayInstance>,
        Arc::new(TrustAuthenticator::new()),
    );
    let (rest_listener, rest_addr) = RestServer::bind("127.0.0.1:0").await.unwrap();
    tokio::spawn(async move {
        let _ = rest.serve(rest_listener).await;
    });

    let pg = PgServer::new(
        instance.clone() as Arc<dyn GatewayInstance>,
        Arc::new(TrustAuthenticator::new()),
    );
    let (pg_listener, pg_addr) = PgServer::bind("127.0.0.1:0").await.unwrap();
    tokio::spawn(async move {
        let _ = pg.serve(pg_listener).await;
    });

    let rest_base = format!("http://{rest_addr}/v1/clusters/default");
    let http = reqwest::Client::new();
    let auth = format!("Basic {}", basic_auth("alice"));

    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        http.post(format!("{rest_base}/databases/{DATABASE}/tables/{KV_TABLE}/records"))
            .header("Authorization", &auth)
            .header("Content-Type", JSON)
            .body(r#"[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]"#)
            .send(),
    )
    .await
    .expect("REST KV upsert timed out before the gateway replied")
    .unwrap();
    assert_eq!(resp.status(), 200, "KV upsert REST status");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows_written"], 2, "KV upsert acked 2 rows");

    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        http.post(format!("{rest_base}/databases/{DATABASE}/tables/{LOG_TABLE}/records"))
            .header("Authorization", &auth)
            .header("Content-Type", ARROW)
            .body(log_arrow_body())
            .send(),
    )
    .await
    .expect("REST log append timed out before the gateway replied")
    .unwrap();
    assert_eq!(resp.status(), 200, "Log append REST status");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows_written"], 3, "Log append acked 3 rows");

    // (b3) Populate the composite-PK prefix table via REST: three rows share
    // `c1 = 10` so a later `WHERE c1 = 10` prefix lookup returns exactly three.
    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        http.post(format!("{rest_base}/databases/{DATABASE}/tables/{KV_PREFIX_TABLE}/records"))
            .header("Authorization", &auth)
            .header("Content-Type", JSON)
            .body(
                r#"[{"c1":10,"c2":1,"name":"a"},{"c1":10,"c2":2,"name":"b"},{"c1":10,"c2":3,"name":"c"},{"c1":20,"c2":1,"name":"d"}]"#,
            )
            .send(),
    )
    .await
    .expect("REST prefix-table upsert timed out before the gateway replied")
    .unwrap();
    assert_eq!(resp.status(), 200, "prefix-table upsert REST status");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows_written"], 4, "prefix-table upsert acked 4 rows");

    let (name1, name2) = kv_lookup_names(&gw_conn).await;
    assert_eq!(name1.as_deref(), Some("alice"), "client lookup id=1");
    assert_eq!(name2.as_deref(), Some("bob"), "client lookup id=2");

    tokio::time::timeout(Duration::from_secs(30), wait_for_log_offsets(&gw_conn))
        .await
        .expect("log append acked but bucket offsets never became readable");

    let (pg_client, pg_conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=alice password=ignored dbname=fluss", pg_addr.port()),
        tokio_postgres::NoTls,
    )
    .await
    .expect("PG connect");
    tokio::spawn(async move {
        let _ = pg_conn.await;
    });

    let rows = tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!(
            "SELECT id, name FROM fluss.{DATABASE}.{KV_TABLE} WHERE id = 2"
        )),
    )
    .await
    .expect("PG KV point lookup timed out")
    .expect("PG KV point lookup");
    let kv_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(kv_rows.len(), 1, "KV point lookup returns exactly one row");
    assert_eq!(kv_rows[0].get("id"), Some("2"));
    assert_eq!(kv_rows[0].get("name"), Some("bob"));

    // (c2) KV bounded scan (datafusion-v0.2.4): `SELECT ... LIMIT n` on a KV table
    // without a primary-key predicate now returns up to n rows (previously a clear
    // "unsupported" error). gw_kv has 2 rows; LIMIT 1 must bound to exactly one.
    let rows = tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!(
            "SELECT id, name FROM fluss.{DATABASE}.{KV_TABLE} LIMIT 1"
        )),
    )
    .await
    .expect("PG KV bounded scan timed out")
    .expect("PG KV bounded scan");
    let kv_scan_rows = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(kv_scan_rows, 1, "KV bounded scan respects LIMIT 1");

    // (c3) KV prefix lookup (datafusion-v0.2.4): a `WHERE c1 = 10` predicate on the
    // bucket-key prefix returns all matching rows (three share c1 = 10), not just one.
    tokio::time::timeout(Duration::from_secs(30), wait_for_bucket0(&gw_conn, KV_PREFIX_TABLE))
        .await
        .expect("prefix table bucket never became readable");
    let rows = tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!(
            "SELECT c1, c2, name FROM fluss.{DATABASE}.{KV_PREFIX_TABLE} WHERE c1 = 10"
        )),
    )
    .await
    .expect("PG KV prefix lookup timed out")
    .expect("PG KV prefix lookup");
    let prefix_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(prefix_rows.len(), 3, "KV prefix lookup on c1=10 returns three rows");
    assert!(
        prefix_rows.iter().all(|r| r.get("c1") == Some("10")),
        "every prefix-lookup row has c1 = 10"
    );

    let rows = tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!(
            "SELECT id, name FROM fluss.{DATABASE}.{LOG_TABLE} LIMIT 3"
        )),
    )
    .await
    .expect("PG Log bounded scan timed out")
    .expect("PG Log bounded scan");
    let log_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(log_rows.len(), 3, "Log LIMIT 3 returns three rows");
    let mut names: Vec<String> = log_rows
        .iter()
        .filter_map(|r| r.get("name").map(|s| s.to_string()))
        .collect();
    names.sort();
    assert_eq!(names, vec!["x", "y", "z"], "Log rows read back via PG SELECT");

    // (d) METADATA over REST against the live catalog: list databases, list the
    // tables in our database, and fetch each table's schema (getMetadata). These
    // read straight from the real Fluss catalog through the gateway's metadata
    // surface, so they prove list/describe work end-to-end, not just write/query.

    // list databases: the database we wrote to must be visible.
    let dbs = http
        .get(format!("{rest_base}/databases"))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert!(
        dbs["databases"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d == DATABASE),
        "metadata lists the database we wrote to"
    );

    // list tables: both the KV and Log tables we created must be listed.
    let tables = http
        .get(format!("{rest_base}/databases/{DATABASE}/tables"))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let table_names: Vec<&str> = tables["tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t.as_str())
        .collect();
    assert!(
        table_names.contains(&KV_TABLE),
        "metadata lists the KV table (got {table_names:?})"
    );
    assert!(
        table_names.contains(&LOG_TABLE),
        "metadata lists the Log table (got {table_names:?})"
    );

    // getMetadata: fetch each table's schema and assert the (id, name) columns
    // come back from the real catalog.
    for table in [KV_TABLE, LOG_TABLE] {
        let info = http
            .get(format!("{rest_base}/databases/{DATABASE}/tables/{table}"))
            .header("Authorization", &auth)
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert_eq!(info["database"], DATABASE, "{table} metadata database");
        assert_eq!(info["table"], table, "{table} metadata table name");
        let columns: Vec<&str> = info["columns"]
            .as_array()
            .unwrap_or_else(|| panic!("{table} metadata has no columns array"))
            .iter()
            .filter_map(|c| c["name"].as_str())
            .collect();
        assert_eq!(
            columns,
            vec!["id", "name"],
            "{table} metadata reports the (id, name) schema"
        );
    }

    drop(pg_client);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        instance.sessions().len(),
        0,
        "direct REST path must not open any GatewaySession (direct-path.md §7)"
    );

    let admin = gw_conn.get_admin().unwrap();
    let _ = admin.drop_table(&TablePath::new(DATABASE, KV_TABLE), true).await;
    let _ = admin.drop_table(&TablePath::new(DATABASE, LOG_TABLE), true).await;
    let _ = admin.drop_table(&TablePath::new(DATABASE, KV_PREFIX_TABLE), true).await;
    cluster.stop();
}


// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Three-row Arrow IPC stream for the Log table: (id, name) = (10,x),(20,y),(30,z).
fn log_arrow_body() -> Vec<u8> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
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

/// Look up id=1 and id=2 in the KV table with the Fluss client, returning their
/// `name` values. Proves the REST upsert actually reached storage.
async fn kv_lookup_names(conn: &FlussConnection) -> (Option<String>, Option<String>) {
    let path = TablePath::new(DATABASE, KV_TABLE);
    let table = conn.get_table(&path).await.unwrap();
    let mut lookuper = table.new_lookup().unwrap().create_lookuper().unwrap();

    let read = |result: fluss::client::LookupResult| -> Option<String> {
        result
            .get_single_row()
            .unwrap()
            .map(|row| row.get_string(1).unwrap().to_string())
    };

    let mut key1 = GenericRow::new(1);
    key1.set_field(0, 1i32);
    let r1 = read(lookuper.lookup(&key1).await.unwrap());

    let mut key2 = GenericRow::new(1);
    key2.set_field(0, 2i32);
    let r2 = read(lookuper.lookup(&key2).await.unwrap());

    (r1, r2)
}

/// `base64("user:ignored")` for the REST Basic-auth header (Phase 1 trust).
fn basic_auth(user: &str) -> String {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let raw = format!("{user}:ignored");
    let input = raw.as_bytes();
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
