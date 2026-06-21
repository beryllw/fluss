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

//! Real-cluster end-to-end verification (DESIGN.md §3.3 integration
//! model; sql-path.md assembly order; direct-path.md at-least-once).
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
//!    prefix lookup (`WHERE c1 = ...` on a composite-PK table),
//!    and a `LIMIT` bounded scan on the Log table — asserting the just-written
//!    rows come back through the gateway's own SQL catalog path.
//! 4. (d) REST METADATA: list databases, list the tables in the database, and
//!    fetch each table's schema (getMetadata) straight from the live Fluss
//!    catalog through the gateway's metadata surface.
//! 4b. (e) psql `\d` introspection: drive the exact SQL psql `\d <table>` emits
//!    and assert it executes against the real catalog (pins the PG introspection
//!    rewrite: OPERATOR/COLLATE/pg_table_is_visible + regtype cast + correlated
//!    scalar subqueries).
//! 4c. (f) REST DDL: create a table via `POST .../tables`, confirm it is listed,
//!    a duplicate is rejected 409, the table is writable + PG-readable, and
//!    `DELETE` drops it (204).
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

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
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
use fluss_gateway::server::mcp::McpServer;
use fluss_gateway::server::postgres::PgServer;
use fluss_gateway::server::rest::{OtlpConfig, RestServer};
use fluss_gateway::session::manager::{SessionManager, SessionManagerConfig};
use fluss_gateway::sql::environment::{
    FlussDatafusionCatalogInstaller, PgSqlEnvironmentProvider, SqlEnvironmentRegistry,
    StubPgCatalogOverlayInstaller,
};
use fluss_gateway::types::{ClusterId, Principal, SqlEnvironmentId, TableRef};

use reqwest::header::{HeaderValue, AUTHORIZATION};
use rmcp::model::{object, CallToolRequestParams};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use rmcp::ServiceExt;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value as OtlpValue, AnyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber};
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Dedicated ports per e2e so the two live tests can run independently.
const REST_CLUSTER_PORT: u16 = 9143;
const OTLP_CLUSTER_PORT: u16 = 9144;
const MCP_CLUSTER_PORT: u16 = 9145;
const READY_TIMEOUT: Duration = Duration::from_secs(30);

const DATABASE: &str = "fluss";
const KV_TABLE: &str = "gw_kv";
const LOG_TABLE: &str = "gw_log";
// Composite-PK KV table whose bucket key (`c1`) is a STRICT prefix of the PK
// `(c1, c2)`, so a `WHERE c1 = ...` predicate exercises KV prefix lookup.
// Seeded so one `c1` matches several rows.
const KV_PREFIX_TABLE: &str = "gw_kv_prefix";
/// Append-only OTLP logs landing table (subset of the telemetry column contract).
const OTLP_LOGS_TABLE: &str = "gw_otlp_logs";

const JSON: &str = "application/json";
const ARROW: &str = "application/vnd.apache.arrow.stream";
const PROTOBUF: &str = "application/x-protobuf";

// The two live tests in this binary both start dockerized Fluss clusters on
// fixed host ports. The Rust test harness runs them in parallel by default, so
// serialize cluster bring-up/teardown here to avoid cross-test port/container
// collisions while keeping the rest of the workspace tests parallel.
static CLUSTER_E2E_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

async fn cluster_e2e_permit() -> OwnedSemaphorePermit {
    CLUSTER_E2E_SEMAPHORE
        .get_or_init(|| Arc::new(Semaphore::new(1)))
        .clone()
        .acquire_owned()
        .await
        .expect("cluster e2e semaphore closed")
}

// ---------------------------------------------------------------------------
// cluster bring-up (mirrors the fluss-datafusion integration setup template)
// ---------------------------------------------------------------------------

async fn start_cluster(name: &str, port: u16) -> FlussTestingCluster {
    let cluster = FlussTestingClusterBuilder::new(name)
        .with_port(port)
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
    // The pg_catalog overlay stays a stub; it does not affect SELECT from
    // the real Fluss catalog.
    let fluss_df = Arc::new(
        fluss_datafusion::FlussDatafusion::new(
            Arc::clone(&connection),
            fluss_datafusion::FlussDatafusionOptions::default(),
        )
        .await
        .expect("FlussDatafusion::new over the live connection"),
    );
    // Connection-recovery manager: owns the shared connection, hot-swaps it into
    // FlussDatafusion on rebuild; the backend reads the live connection from it.
    let fluss_df_for_swap = Arc::clone(&fluss_df);
    let conn_manager = Arc::new(fluss_gateway::connection::ConnectionManager::new(
        Arc::clone(&connection),
        fluss_gateway::connection::build_fluss_config(&ClusterConfig {
            bootstrap_servers: bootstrap.to_string(),
        }),
        Box::new(move |new| {
            fluss_df_for_swap
                .swap_connection(Arc::clone(new))
                .map_err(|e| fluss_gateway::error::GatewayError::Backend(format!("swap: {e}")))
        }),
    ));

    let pg_provider = PgSqlEnvironmentProvider::new(
        Arc::new(FlussDatafusionCatalogInstaller::new(fluss_df)),
        Arc::new(StubPgCatalogOverlayInstaller),
    );
    let mut sql_environments = SqlEnvironmentRegistry::new();
    sql_environments.register(SqlEnvironmentId("postgres".into()), Arc::new(pg_provider));

    // Direct path: a backend that reads the live connection from the manager.
    let backend = Arc::new(FlussBackendFacade::new(Arc::clone(&conn_manager)));
    let sessions = Arc::new(SessionManager::new(SessionManagerConfig::default()));

    let instance = Arc::new(
        GatewayInstanceImpl::new(sessions, backend, Arc::new(sql_environments))
            .with_recovery(conn_manager),
    );
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
    let _permit = cluster_e2e_permit().await;
    let cluster = start_cluster("gw-e2e-rest", REST_CLUSTER_PORT).await;
    let bootstrap = cluster.plaintext_bootstrap_servers().to_string();
    let connection = Arc::new(cluster.get_fluss_connection().await);

    create_kv_table(&connection).await;
    create_log_table(&connection).await;
    create_kv_prefix_table(&connection).await;

    let (instance, gw_conn) = assemble_instance(&bootstrap).await;

    let rest = RestServer::new(
        instance.clone() as Arc<dyn GatewayInstance>,
        Arc::new(TrustAuthenticator::new()),
        None,
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
    // Wait for the bucket to be readable first; composite/bucket-key metadata can
    // lag just after CREATE TABLE, and the REST write would otherwise time out.
    tokio::time::timeout(Duration::from_secs(30), wait_for_bucket0(&gw_conn, KV_PREFIX_TABLE))
        .await
        .expect("prefix table bucket never became readable before REST upsert");
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

    // (c2) KV bounded scan: `SELECT ... LIMIT n` on a KV table without a
    // primary-key predicate returns up to n rows (previously a clear "unsupported"
    // error). gw_kv has 2 rows; LIMIT 1 must bound to exactly one.
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

    // (c2b) datafusion-v0.3.2: a non-lake KV full scan without a primary-key /
    // bucket-key predicate or LIMIT is rejected cleanly rather than falling back
    // to an unbounded scan. The shared connection must remain usable afterward.
    let full_scan = tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!(
            "SELECT id, name FROM fluss.{DATABASE}.{KV_TABLE}"
        )),
    )
    .await
    .expect("PG KV full-scan rejection timed out");
    let err = full_scan.expect_err("non-lake KV full scan should be rejected");
    let msg = err.to_string();
    assert!(
        !msg.trim().is_empty(),
        "rejected full-scan error should surface a non-empty message"
    );

    let rows = tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!(
            "SELECT id, name FROM fluss.{DATABASE}.{KV_TABLE} WHERE id = 1"
        )),
    )
    .await
    .expect("PG KV point lookup after rejected full scan timed out")
    .expect("PG KV point lookup after rejected full scan");
    let after_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(after_rows.len(), 1, "connection stays usable after rejected full scan");
    assert_eq!(after_rows[0].get("name"), Some("alice"));

    // (c3) KV prefix lookup: a `WHERE c1 = 10` predicate on the bucket-key prefix
    // (a strict prefix of the PK) returns all matching rows (three share c1 = 10),
    // not just one.
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

    // (e) psql `\d` introspection over PG: `\d` is a psql client macro, but the SQL
    // it emits flows through the gateway's introspection rewrite. Drive the exact
    // queries psql sends and assert they execute against the real catalog — this
    // pins the OPERATOR(pg_catalog.~) / COLLATE / pg_table_is_visible rewrite (step
    // 1) and the regtype-cast + correlated-scalar-subquery rewrite (column query).
    let rows = tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!(
            "SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c \
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname OPERATOR(pg_catalog.~) '^({KV_TABLE})$' COLLATE pg_catalog.default \
             AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 3"
        )),
    )
    .await
    .expect("psql \\d name query timed out")
    .expect("psql \\d name query (OPERATOR/COLLATE/pg_table_is_visible rewrite)");
    let oid_row = rows
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) if r.get("relname") == Some(KV_TABLE) => {
                Some(r.get("oid").map(|s| s.to_string()))
            }
            _ => None,
        })
        .flatten()
        .expect("psql \\d name query must resolve the KV table oid");

    let rows = tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!(
            "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), \
             (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d \
              WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), \
             a.attnotnull, \
             (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t \
              WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation, \
             a.attidentity, a.attgenerated \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = '{oid_row}' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
        )),
    )
    .await
    .expect("psql \\d column query timed out")
    .expect("psql \\d column query (regtype cast + correlated subquery rewrite)");
    let d_cols: Vec<String> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get("attname").map(|s| s.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(d_cols, vec!["id", "name"], "psql \\d lists the KV table columns");

    // psql `\d`'s foreign-key query casts `conrelid::pg_catalog.regclass` — an
    // OID-alias type DataFusion can't execute; the rewrite degrades it to ::text.
    // (No FKs on a Fluss table, so this returns no rows — it just has to plan.)
    tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!(
            "SELECT true as sametable, conname, \
             pg_catalog.pg_get_constraintdef(r.oid, true) as condef, \
             conrelid::pg_catalog.regclass AS ontable \
             FROM pg_catalog.pg_constraint r \
             WHERE r.conrelid = '{oid_row}' AND r.contype = 'f' AND conparentid = 0 \
             ORDER BY conname"
        )),
    )
    .await
    .expect("psql \\d FK query timed out")
    .expect("psql \\d FK query (regclass cast rewrite)");

    // The remaining psql `\d` / `\d+` section probes target catalogs that are
    // always empty for a Fluss table but are built from PostgreSQL-only constructs
    // (ARRAY constructors, `'x' = any(stxkind)`, ARRAY indexing) that DataFusion
    // cannot otherwise plan. Replay the exact SQL psql emits and assert each one
    // executes — this pins the policy / statistics_ext / publication / NOT NULL
    // rewrites so `\d` and `\d+` keep working against the real catalog.
    let d_probes = [
        // RLS-policy probe — array(SELECT ...) role aggregation -> NULL.
        format!(
            "SELECT pol.polname, pol.polpermissive, \
             CASE WHEN pol.polroles = '{{0}}' THEN NULL ELSE \
             pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles \
             where oid = any (pol.polroles) order by 1),',') END, \
             pg_catalog.pg_get_expr(pol.polqual, pol.polrelid), \
             pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid) \
             FROM pg_catalog.pg_policy pol WHERE pol.polrelid = '{oid_row}' ORDER BY 1"
        ),
        // Extended-statistics probe — 'd' = any(stxkind) -> false.
        format!(
            "SELECT oid, stxname, 'd' = any(stxkind) AS ndist_enabled, \
             'f' = any(stxkind) AS deps_enabled, 'm' = any(stxkind) AS mcv_enabled \
             FROM pg_catalog.pg_statistic_ext WHERE stxrelid = '{oid_row}' ORDER BY stxname"
        ),
        // Publication probe — 3-way UNION of PG-only constructs (short-circuited).
        format!(
            "SELECT pubname, NULL, NULL FROM pg_catalog.pg_publication p \
             WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('{oid_row}') ORDER BY 1"
        ),
        // NOT NULL constraint probe — conkey[1] array indexing (short-circuited).
        format!(
            "SELECT c.conname, a.attname, c.connoinherit, c.conislocal, \
             c.coninhcount <> 0, c.convalidated \
             FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_attribute a ON \
             (a.attrelid = c.conrelid AND a.attnum = c.conkey[1]) \
             WHERE c.contype = 'n' AND c.conrelid = '{oid_row}'::pg_catalog.regclass ORDER BY a.attnum"
        ),
    ];
    for probe in &d_probes {
        tokio::time::timeout(Duration::from_secs(30), pg_client.simple_query(probe))
            .await
            .expect("psql \\d section probe timed out")
            .expect("psql \\d/\\d+ section probe must plan against the real catalog");
    }

    // (f) DDL over REST (design/direct-path.md "表管理（DDL）API"): create a table
    // through the gateway's own POST, prove it is visible + usable, that a
    // duplicate is rejected 409, and that DELETE drops it.
    let rest_created = "gw_rest_kv";
    let create_body = format!(
        r#"{{"table_name":"{rest_created}","columns":[{{"name":"id","type":"INT","nullable":false}},{{"name":"name","type":"STRING"}}],"primary_key":["id"],"distribution":{{"bucket_keys":["id"],"bucket_count":1}}}}"#
    );
    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        http.post(format!("{rest_base}/databases/{DATABASE}/tables"))
            .header("Authorization", &auth)
            .header("Content-Type", JSON)
            .body(create_body.clone())
            .send(),
    )
    .await
    .expect("REST create table timed out")
    .unwrap();
    assert_eq!(resp.status(), 201, "REST create returns 201");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["table"], rest_created, "create returns the new table metadata");
    let cols: Vec<&str> = body["columns"].as_array().unwrap().iter().filter_map(|c| c["name"].as_str()).collect();
    assert_eq!(cols, vec!["id", "name"], "created table has the (id, name) schema");

    // list now includes the just-created table
    let tables = http
        .get(format!("{rest_base}/databases/{DATABASE}/tables"))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert!(
        tables["tables"].as_array().unwrap().iter().any(|t| t == rest_created),
        "REST-created table appears in the table list"
    );

    // duplicate create -> 409
    let resp = http
        .post(format!("{rest_base}/databases/{DATABASE}/tables"))
        .header("Authorization", &auth)
        .header("Content-Type", JSON)
        .body(create_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409, "duplicate create returns 409");

    // the created table is usable: REST write + PG read back
    let resp = http
        .post(format!("{rest_base}/databases/{DATABASE}/tables/{rest_created}/records"))
        .header("Authorization", &auth)
        .header("Content-Type", JSON)
        .body(r#"[{"id":7,"name":"zoe"}]"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "write into the REST-created table");

    let rows = tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!(
            "SELECT id, name FROM fluss.{DATABASE}.{rest_created} WHERE id = 7"
        )),
    )
    .await
    .expect("PG read of REST-created table timed out")
    .expect("PG read of REST-created table");
    let n = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) if r.get("name") == Some("zoe") => Some(()),
            _ => None,
        })
        .count();
    assert_eq!(n, 1, "row written into the REST-created table reads back via PG");

    // DELETE drops it -> 204
    let resp = http
        .delete(format!("{rest_base}/databases/{DATABASE}/tables/{rest_created}"))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204, "REST drop returns 204");

    // (g) comprehensive column-type coverage: create a wide table exercising every
    // `ColumnType` variant (incl. precision/scale and not-null) via REST, assert
    // the Arrow data_type each maps to, then replay the psql `\d` column query
    // against it so `format_type` renders every type without a planning error.
    let wide = "gw_types_e2e";
    let wide_body = format!(
        r#"{{"table_name":"{wide}","columns":[
          {{"name":"id","type":"INT","nullable":false}},
          {{"name":"c_bool","type":"BOOLEAN"}},
          {{"name":"c_tinyint","type":"TINYINT"}},
          {{"name":"c_smallint","type":"SMALLINT"}},
          {{"name":"c_bigint","type":"BIGINT"}},
          {{"name":"c_float","type":"FLOAT"}},
          {{"name":"c_double","type":"DOUBLE"}},
          {{"name":"c_decimal","type":"DECIMAL(10,2)"}},
          {{"name":"c_char","type":"CHAR(8)"}},
          {{"name":"c_string","type":"STRING"}},
          {{"name":"c_binary","type":"BINARY(16)"}},
          {{"name":"c_bytes","type":"BYTES"}},
          {{"name":"c_date","type":"DATE"}},
          {{"name":"c_time","type":"TIME(3)"}},
          {{"name":"c_timestamp","type":"TIMESTAMP(6)"}},
          {{"name":"c_notnull","type":"BIGINT","nullable":false}}
        ],"primary_key":["id"],"distribution":{{"bucket_keys":["id"],"bucket_count":1}}}}"#
    );
    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        http.post(format!("{rest_base}/databases/{DATABASE}/tables"))
            .header("Authorization", &auth)
            .header("Content-Type", JSON)
            .body(wide_body)
            .send(),
    )
    .await
    .expect("REST create wide table timed out")
    .unwrap();
    assert_eq!(resp.status(), 201, "REST create wide-type table returns 201");
    let wbody: serde_json::Value = resp.json().await.unwrap();
    let dtype = |name: &str| -> String {
        wbody["columns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == name)
            .and_then(|c| c["data_type"].as_str())
            .unwrap_or("")
            .to_string()
    };
    assert_eq!(dtype("c_bool"), "Boolean");
    assert_eq!(dtype("c_tinyint"), "Int8");
    assert_eq!(dtype("c_smallint"), "Int16");
    assert_eq!(dtype("id"), "Int32");
    assert_eq!(dtype("c_bigint"), "Int64");
    assert_eq!(dtype("c_float"), "Float32");
    assert_eq!(dtype("c_double"), "Float64");
    assert_eq!(dtype("c_decimal"), "Decimal128(10, 2)");
    assert_eq!(dtype("c_char"), "Utf8");
    assert_eq!(dtype("c_string"), "Utf8");
    assert_eq!(dtype("c_binary"), "FixedSizeBinary(16)");
    assert_eq!(dtype("c_bytes"), "Binary");
    assert_eq!(dtype("c_date"), "Date32");
    assert!(dtype("c_time").starts_with("Time32"), "TIME(3) -> {}", dtype("c_time"));
    assert!(dtype("c_timestamp").starts_with("Timestamp"), "TIMESTAMP(6) -> {}", dtype("c_timestamp"));
    let wide_not_null = wbody["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "c_notnull")
        .unwrap()["nullable"]
        .as_bool()
        .unwrap();
    assert!(!wide_not_null, "explicit nullable:false is preserved");

    // psql `\d <wide>` column query must plan and render every type via format_type.
    let wide_oid = tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!(
            "SELECT c.oid, c.relname FROM pg_catalog.pg_class c \
             WHERE c.relname OPERATOR(pg_catalog.~) '^({wide})$' COLLATE pg_catalog.default \
             AND pg_catalog.pg_table_is_visible(c.oid)"
        )),
    )
    .await
    .expect("wide \\d name query timed out")
    .expect("wide \\d name query")
    .iter()
    .find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) if r.get("relname") == Some(wide) => {
            r.get("oid").map(|s| s.to_string())
        }
        _ => None,
    })
    .expect("wide \\d name query resolves oid");
    let wide_cols = tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!(
            "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), \
             (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d \
              WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), \
             a.attnotnull, a.attidentity, a.attgenerated \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = '{wide_oid}' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
        )),
    )
    .await
    .expect("wide \\d column query timed out")
    .expect("wide \\d column query renders all types")
    .iter()
    .filter_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get("attname").map(|s| s.to_string()),
        _ => None,
    })
    .count();
    assert_eq!(wide_cols, 16, "psql \\d lists all 16 columns of the wide-type table");

    // REST write covering EVERY writable column type (integers, floats, boolean,
    // string, decimal, date/time/timestamp as JSON strings, and binary as a hex
    // string — arrow-json's binary encoding), then read the row back via PG to
    // prove the full Arrow -> Fluss `GenericRow` conversion round-trips.
    let wide_row = r#"[{"id":1,"c_bool":true,"c_tinyint":7,"c_smallint":300,
        "c_bigint":9000000000,"c_float":1.5,"c_double":2.25,"c_decimal":3.14,
        "c_char":"abcd","c_string":"hello",
        "c_binary":"000102030405060708090a0b0c0d0e0f","c_bytes":"aabbcc",
        "c_date":"2024-03-15","c_time":"12:34:56","c_timestamp":"2024-03-15T12:34:56.789",
        "c_notnull":42}]"#;
    let resp = http
        .post(format!("{rest_base}/databases/{DATABASE}/tables/{wide}/records"))
        .header("Authorization", &auth)
        .header("Content-Type", JSON)
        .body(wide_row)
        .send()
        .await
        .unwrap();
    // The write returning rows_written:1 proves the full Arrow -> Fluss row
    // conversion succeeded for every column. The `SELECT *` read-back then proves
    // each type round-trips over PG — including the binary columns: `BYTES`
    // (variable `Binary`) and `BINARY(16)` (`FixedSizeBinary`, which arrow-pg
    // can't encode directly and which the adapter normalizes to `Binary`/bytea).
    assert_eq!(resp.status(), 200, "write of an all-types row succeeds");
    assert_eq!(resp.json::<serde_json::Value>().await.unwrap()["rows_written"], 1);

    let rows = tokio::time::timeout(
        Duration::from_secs(30),
        pg_client.simple_query(&format!("SELECT * FROM fluss.{DATABASE}.{wide} WHERE id = 1")),
    )
    .await
    .expect("PG read of all-types row timed out")
    .expect("PG read of all-types row");
    let row = rows
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) if r.get("id") == Some("1") => Some(r),
            _ => None,
        })
        .expect("the all-types row reads back via PG");
    let g = |c: &str| row.get(c).unwrap_or("").to_string();
    assert_eq!(g("c_tinyint"), "7", "TINYINT round-trips");
    assert_eq!(g("c_smallint"), "300", "SMALLINT round-trips");
    assert_eq!(g("c_bigint"), "9000000000", "BIGINT round-trips");
    assert_eq!(g("c_bool"), "t", "BOOLEAN round-trips");
    assert_eq!(g("c_string"), "hello", "STRING round-trips");
    assert_eq!(g("c_decimal"), "3.14", "DECIMAL round-trips");
    assert_eq!(g("c_date"), "2024-03-15", "DATE round-trips");
    assert!(g("c_time").starts_with("12:34:56"), "TIME round-trips: {}", g("c_time"));
    assert!(g("c_timestamp").starts_with("2024-03-15 12:34:56.789"), "TIMESTAMP round-trips: {}", g("c_timestamp"));
    assert_eq!(g("c_bytes"), "\\xaabbcc", "BYTES round-trips as bytea");
    assert_eq!(
        g("c_binary"), "\\x000102030405060708090a0b0c0d0e0f",
        "BINARY(16)/FixedSizeBinary round-trips as bytea (adapter normalization)"
    );
    assert_eq!(g("c_notnull"), "42", "explicit NOT NULL column round-trips");

    http.delete(format!("{rest_base}/databases/{DATABASE}/tables/{wide}"))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();

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


/// OTLP-over-HTTP end-to-end: POST a protobuf logs request to the gateway, then
/// read the flattened rows back through the PG SQL path. Proves the OTLP HTTP
/// adapter lands telemetry as a real Fluss `LogAppend` against a live cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn cluster_otlp_logs_lands_and_reads_back() {
    let _permit = cluster_e2e_permit().await;
    let cluster = start_cluster("gw-e2e-otlp", OTLP_CLUSTER_PORT).await;
    let bootstrap = cluster.plaintext_bootstrap_servers().to_string();
    let connection = Arc::new(cluster.get_fluss_connection().await);

    create_otlp_logs_table(&connection).await;

    let (instance, gw_conn) = assemble_instance(&bootstrap).await;

    // All three signals must be configured; this test only exercises logs, so the
    // metrics/traces refs point at the same landing table (never written here).
    let logs_table = TableRef {
        database: DATABASE.into(),
        table: OTLP_LOGS_TABLE.into(),
    };
    let otlp = OtlpConfig {
        logs_table: logs_table.clone(),
        metrics_table: logs_table.clone(),
        traces_table: logs_table,
    };
    let rest = RestServer::new(
        instance.clone() as Arc<dyn GatewayInstance>,
        Arc::new(TrustAuthenticator::new()),
        Some(otlp),
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
        http.post(format!("{rest_base}/otlp/v1/logs"))
            .header("Authorization", &auth)
            .header("Content-Type", PROTOBUF)
            .body(otlp_logs_body())
            .send(),
    )
    .await
    .expect("OTLP logs POST timed out before the gateway replied")
    .unwrap();
    assert_eq!(resp.status(), 200, "OTLP logs ingest status");

    tokio::time::timeout(Duration::from_secs(30), wait_for_bucket0(&gw_conn, OTLP_LOGS_TABLE))
        .await
        .expect("OTLP logs bucket never became readable");

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
            "SELECT signal, severity_text, body FROM fluss.{DATABASE}.{OTLP_LOGS_TABLE} LIMIT 10"
        )),
    )
    .await
    .expect("PG read of OTLP logs timed out")
    .expect("PG read of OTLP logs");
    let log_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(log_rows.len(), 2, "both OTLP log records land and read back");
    assert!(
        log_rows.iter().all(|r| r.get("signal") == Some("logs")),
        "every landed row is tagged with the logs signal"
    );
    // `body` is stored as the JSON serialization of OTLP AnyValue, so a plain
    // string log body reads back with JSON quotes preserved.
    let bodies: Vec<&str> = log_rows.iter().filter_map(|r| r.get("body")).collect();
    assert!(bodies.contains(&"\"first\""), "first log body reads back as OTLP JSON");
    assert!(bodies.contains(&"\"second\""), "second log body reads back as OTLP JSON");

    let admin = gw_conn.get_admin().unwrap();
    let _ = admin.drop_table(&TablePath::new(DATABASE, OTLP_LOGS_TABLE), true).await;
    cluster.stop();
}

/// MCP end-to-end: drive the four read-only MCP tools with the REAL rmcp client
/// (Streamable HTTP) against a live cluster. Seeds a KV table via REST, then uses
/// the MCP tools an agent would call: `list_databases`, `list_tables`,
/// `describe_table`, and `query` (which borrows the SQL path through an ephemeral
/// session). Also asserts the read-only guard rejects DDL end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn cluster_mcp_tools_against_live_fluss() {
    let _permit = cluster_e2e_permit().await;
    let cluster = start_cluster("gw-e2e-mcp", MCP_CLUSTER_PORT).await;
    let bootstrap = cluster.plaintext_bootstrap_servers().to_string();
    let connection = Arc::new(cluster.get_fluss_connection().await);

    create_kv_table(&connection).await;

    let (instance, gw_conn) = assemble_instance(&bootstrap).await;

    // REST only seeds data (writes go through the direct path); reads are MCP.
    let rest = RestServer::new(
        instance.clone() as Arc<dyn GatewayInstance>,
        Arc::new(TrustAuthenticator::new()),
        None,
    );
    let (rest_listener, rest_addr) = RestServer::bind("127.0.0.1:0").await.unwrap();
    tokio::spawn(async move {
        let _ = rest.serve(rest_listener).await;
    });

    let mcp = McpServer::new(
        instance.clone() as Arc<dyn GatewayInstance>,
        Arc::new(TrustAuthenticator::new()),
    );
    let (mcp_listener, mcp_addr) = McpServer::bind("127.0.0.1:0").await.unwrap();
    tokio::spawn(async move {
        let _ = mcp.serve(mcp_listener).await;
    });

    let http = reqwest::Client::new();
    let auth = format!("Basic {}", basic_auth("alice"));
    let rest_base = format!("http://{rest_addr}/v1/clusters/default");

    // Seed two rows into the KV table via REST.
    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        http.post(format!("{rest_base}/databases/{DATABASE}/tables/{KV_TABLE}/records"))
            .header("Authorization", &auth)
            .header("Content-Type", JSON)
            .body(r#"[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]"#)
            .send(),
    )
    .await
    .expect("REST seed timed out before the gateway replied")
    .unwrap();
    assert_eq!(resp.status(), 200, "KV seed REST status");

    // Connect the real MCP client (Basic auth via custom header — rmcp's
    // `auth_header` would force a Bearer scheme).
    let mut headers = HashMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_str(&auth).unwrap());
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{mcp_addr}/mcp"))
            .custom_headers(headers),
    );
    let client = tokio::time::timeout(Duration::from_secs(30), ().serve(transport))
        .await
        .expect("MCP initialize timed out")
        .expect("MCP initialize handshake");

    // tools/list advertises exactly the four read-only tools.
    let tools = client.list_tools(Default::default()).await.unwrap();
    let mut names: Vec<String> = tools.tools.iter().map(|t| t.name.to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["describe_table", "list_databases", "list_tables", "query"],
        "MCP advertises the four read-only tools"
    );

    // list_databases includes the database we seeded.
    let v = client
        .call_tool(CallToolRequestParams::new("list_databases"))
        .await
        .unwrap()
        .structured_content
        .expect("list_databases structured content");
    assert!(
        v["databases"].as_array().unwrap().iter().any(|d| d == DATABASE),
        "MCP list_databases includes {DATABASE}: {v}"
    );

    // list_tables includes the KV table.
    let v = client
        .call_tool(
            CallToolRequestParams::new("list_tables")
                .with_arguments(object(serde_json::json!({ "database": DATABASE }))),
        )
        .await
        .unwrap()
        .structured_content
        .expect("list_tables structured content");
    assert!(
        v["tables"].as_array().unwrap().iter().any(|t| t == KV_TABLE),
        "MCP list_tables includes {KV_TABLE}: {v}"
    );

    // describe_table reports the (id, name) schema from the live catalog.
    let v = client
        .call_tool(
            CallToolRequestParams::new("describe_table").with_arguments(object(serde_json::json!({
                "database": DATABASE,
                "table": KV_TABLE,
            }))),
        )
        .await
        .unwrap()
        .structured_content
        .expect("describe_table structured content");
    let cols: Vec<&str> = v["columns"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c["name"].as_str())
        .collect();
    assert_eq!(cols, vec!["id", "name"], "MCP describe_table columns");

    // query borrows the SQL path: a PK point lookup reads back the seeded row.
    let v = tokio::time::timeout(
        Duration::from_secs(30),
        client.call_tool(CallToolRequestParams::new("query").with_arguments(object(
            serde_json::json!({
                "sql": format!("SELECT id, name FROM fluss.{DATABASE}.{KV_TABLE} WHERE id = 2"),
            }),
        ))),
    )
    .await
    .expect("MCP query timed out")
    .unwrap()
    .structured_content
    .expect("query structured content");
    assert_eq!(v["row_count"], 1, "MCP query returns one row: {v}");
    assert_eq!(v["truncated"], false, "row fits under the cap");
    assert_eq!(v["rows"][0]["name"], "bob", "MCP query reads back the seeded row");

    // The read-only guard rejects DDL before it reaches the SQL path.
    let rejected = client
        .call_tool(
            CallToolRequestParams::new("query")
                .with_arguments(object(serde_json::json!({ "sql": "DROP TABLE x" }))),
        )
        .await;
    assert!(rejected.is_err(), "MCP query rejects DDL end-to-end");

    let _ = client.cancel().await;

    let admin = gw_conn.get_admin().unwrap();
    let _ = admin.drop_table(&TablePath::new(DATABASE, KV_TABLE), true).await;
    cluster.stop();
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Append-only OTLP logs landing table. A subset of the telemetry column
/// contract is enough to prove the adapter writes by column name; columns the
/// adapter does not fill are simply absent here.
async fn create_otlp_logs_table(conn: &FlussConnection) {
    let path = TablePath::new(DATABASE, OTLP_LOGS_TABLE);
    let admin = conn.get_admin().unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("signal", DataTypes::string())
                .column("severity_text", DataTypes::string())
                .column("body", DataTypes::string())
                .build()
                .unwrap(),
        )
        .distributed_by(Some(1), vec![])
        .build()
        .unwrap();
    admin.create_table(&path, &descriptor, true).await.unwrap();
}

/// Two-record OTLP logs export request, protobuf-encoded.
fn otlp_logs_body() -> Vec<u8> {
    let record = |severity: &str, body: &str| LogRecord {
        time_unix_nano: 1,
        observed_time_unix_nano: 1,
        severity_number: SeverityNumber::Info as i32,
        severity_text: severity.into(),
        body: Some(AnyValue {
            value: Some(OtlpValue::StringValue(body.into())),
        }),
        attributes: vec![],
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: vec![],
        span_id: vec![],
        event_name: String::new(),
    };
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![record("INFO", "first"), record("WARN", "second")],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec()
}

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

/// `base64("user:ignored")` for the REST Basic-auth header (trust mode).
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
