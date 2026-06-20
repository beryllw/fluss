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

//! PostgreSQL protocol integration tests.
//!
//! Drives the spawned PG frontend with the real `tokio-postgres` wire client
//! over loopback TCP, against the `FakeInstance` in `harness`. Covers the P4
//! completion criteria: connect (cleartext-then-trust) -> probe -> SELECT
//! (simple + extended/decoded rows) -> SET/SHOW -> write rejected -> out-of-band
//! CancelRequest with secret verification. No Fluss cluster is required.

mod harness;

use harness::PgTestServer;

/// Connect (exercises cleartext-then-trust startup + BackendKeyData), then run a
/// simple-protocol SELECT and decode the deterministic rows.
#[tokio::test]
async fn connect_and_simple_select() {
    let server = PgTestServer::start().await;
    let (client, conn) = tokio_postgres::connect(&server.conn_string(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // `simple_query` uses the simple (Q) protocol path.
    let rows = client.simple_query("SELECT id, name FROM whatever").await.unwrap();
    // SimpleQueryMessage::Row entries + a CommandComplete.
    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(data_rows.len(), 2);
    assert_eq!(data_rows[0].get("id"), Some("1"));
    assert_eq!(data_rows[0].get("name"), Some("alice"));
    assert_eq!(data_rows[1].get("name"), Some("bob"));
}

/// Extended-protocol SELECT: `query` sends Parse/Bind/Describe/Execute/Sync and
/// decodes typed values via the RowDescription OIDs.
#[tokio::test]
async fn extended_select_decodes_typed_rows() {
    let server = PgTestServer::start().await;
    let (client, conn) = tokio_postgres::connect(&server.conn_string(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let rows = client.query("SELECT id, name FROM whatever", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let id: i32 = rows[0].get("id");
    let name: &str = rows[0].get("name");
    assert_eq!(id, 1);
    assert_eq!(name, "alice");
}

/// Extended-protocol parameterized SELECT: the client sends bound `$1`/`$2`
/// values, which must be decoded from the PG wire (text/binary) into
/// `ScalarValue`/`ParamValues` and reach `Instance.execute_sql`. The
/// `FakeInstance` echoes the decoded params back as the result row, so a wrong /
/// dropped parameter would surface as a wrong value here.
#[tokio::test]
async fn extended_select_with_bound_parameters() {
    let server = PgTestServer::start().await;
    let (client, conn) = tokio_postgres::connect(&server.conn_string(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // tokio-postgres binds these via Parse/Bind (extended protocol), sending the
    // values as wire parameters rather than inlining them into the SQL text.
    let id_param: i32 = 42;
    let name_param: &str = "zoe";
    let rows = client
        .query(
            "SELECT id, name FROM whatever WHERE id = $1 AND name = $2",
            &[&id_param, &name_param],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get("id");
    let name: &str = rows[0].get("name");
    assert_eq!(id, 42, "decoded $1 must reach execute_sql");
    assert_eq!(name, "zoe", "decoded $2 must reach execute_sql");
}

/// `SET` returns a SET tag and `SHOW` reads the value back from session vars.
#[tokio::test]
async fn set_then_show_roundtrips() {
    let server = PgTestServer::start().await;
    let (client, conn) = tokio_postgres::connect(&server.conn_string(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    client.batch_execute("SET TimeZone = 'Asia/Shanghai'").await.unwrap();
    let rows = client.simple_query("SHOW timezone").await.unwrap();
    let value = rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get("timezone").map(|s| s.to_string()),
        _ => None,
    });
    assert_eq!(value.as_deref(), Some("Asia/Shanghai"));
}

/// `SET TimeZone = DEFAULT` clears the session override and makes `SHOW timezone`
/// fall back to the default value again.
#[tokio::test]
async fn set_timezone_default_clears_override() {
    let server = PgTestServer::start().await;
    let (client, conn) = tokio_postgres::connect(&server.conn_string(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    client.batch_execute("SET TimeZone = 'Asia/Shanghai'").await.unwrap();
    client.batch_execute("SET TimeZone = DEFAULT").await.unwrap();
    let rows = client.simple_query("SHOW timezone").await.unwrap();
    let value = rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get("timezone").map(|s| s.to_string()),
        _ => None,
    });
    assert_eq!(value.as_deref(), Some("UTC"));
}

/// BEGIN/COMMIT are accepted as autocommit no-ops (BI tools must not break).
#[tokio::test]
async fn transaction_control_is_noop() {
    let server = PgTestServer::start().await;
    let (client, conn) = tokio_postgres::connect(&server.conn_string(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    client.batch_execute("BEGIN").await.unwrap();
    client.batch_execute("COMMIT").await.unwrap();
    client.batch_execute("ROLLBACK").await.unwrap();
}

/// A write statement is rejected with a feature-not-supported error whose
/// message points at the REST write path (read-only).
#[tokio::test]
async fn write_is_rejected_as_unsupported() {
    let server = PgTestServer::start().await;
    let (client, conn) = tokio_postgres::connect(&server.conn_string(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let err = client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .unwrap_err();
    let db_err = err.as_db_error().expect("a structured PG error");
    // SQLSTATE 0A000 = feature_not_supported.
    assert_eq!(db_err.code().code(), "0A000");
    assert!(db_err.message().contains("REST"), "msg: {}", db_err.message());
}

/// Out-of-band CancelRequest handling at the real wire level:
/// - a forged CancelRequest with an unknown pid / wrong secret is rejected and
///   never reaches `Instance.cancel_operation`;
/// - the client's own (correct-key) cancel token with no query running is a
///   no-op that likewise does not reach the instance.
///
/// The CancelRequest packet is sent as raw bytes over a fresh TCP connection,
/// exactly as a real client's second connection would.
#[tokio::test]
async fn cancel_request_secret_verification() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let server = PgTestServer::start().await;
    let (client, conn) = tokio_postgres::connect(&server.conn_string(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    // Establish a session and let any canned query finish.
    let _ = client.simple_query("SELECT id, name FROM whatever").await.unwrap();

    // Forge a CancelRequest with an unknown pid and a wrong secret over raw TCP.
    // Packet: int32 len(16), int32 code(80877102), int32 pid, int32 secret.
    let mut sock = TcpStream::connect(("127.0.0.1", server.port)).await.unwrap();
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(&16i32.to_be_bytes());
    buf.extend_from_slice(&80877102i32.to_be_bytes());
    buf.extend_from_slice(&424242i32.to_be_bytes()); // unknown pid
    buf.extend_from_slice(&0i32.to_be_bytes()); // wrong secret
    sock.write_all(&buf).await.unwrap();
    let _ = sock.flush().await;
    drop(sock);

    // The client's own cancel token (correct key) with nothing running.
    let _ = client.cancel_token().cancel_query(tokio_postgres::NoTls).await;

    // Give the server a moment to process the out-of-band requests.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert!(
        server.instance.cancelled.lock().unwrap().is_empty(),
        "no operation was running, so no cancel should reach the instance"
    );
}

/// Accepted cancel path: a long-running query is cancelled out-of-band with the
/// connection's correct backend key, and `Instance.cancel_operation` is reached.
#[tokio::test]
async fn cancel_request_accepted_reaches_instance() {
    let server = PgTestServer::start().await;
    let (client, conn) = tokio_postgres::connect(&server.conn_string(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Capture the backend key for this connection before launching the query.
    let cancel_token = client.cancel_token();

    // Launch a query that hangs (mentions SLEEP) so it stays "running".
    let query = tokio::spawn(async move {
        let _ = client.simple_query("SELECT id FROM t WHERE pg_sleep(60)").await;
    });

    // Let the server start draining (publishes the running operation).
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    cancel_token.cancel_query(tokio_postgres::NoTls).await.unwrap();

    // Wait until the instance records the cancel.
    let mut reached = false;
    for _ in 0..50 {
        if !server.instance.cancelled.lock().unwrap().is_empty() {
            reached = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    query.abort();
    assert!(reached, "cancel of a running operation must reach the instance");
}
