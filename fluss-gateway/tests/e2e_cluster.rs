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

//! End-to-end metadata API tests against a real Dockerized Fluss cluster.
//!
//! The test crosses every production boundary: a real HTTP client calls the compiled Gateway binary,
//! which reaches the Docker cluster through the native `fluss-rs` backend. Run it with
//! `just test-e2e`.

mod support;

use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use support::{Api, ChildGuard, await_http_ok, binary, free_port};

const DATABASE: &str = "gateway_e2e";
const TABLE: &str = "events";
const SERVICE_ACCOUNT: &str = "admin";
const SERVICE_SECRET: &str = "admin-secret";

/// A detached Docker cluster managed through the same helper used by the other language bindings.
///
/// Drop is a best-effort backstop for assertions that panic; the happy path checks cleanup explicitly.
struct FlussCluster {
    helper: PathBuf,
    name: String,
    plaintext_bootstrap_servers: String,
    sasl_bootstrap_servers: String,
    stopped: bool,
}

impl FlussCluster {
    fn start(port: u16) -> Self {
        let helper = std::env::var_os("FLUSS_TEST_CLUSTER_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("../fluss-rust/target/debug/fluss-test-cluster"));
        assert!(
            helper.is_file(),
            "Fluss test cluster helper does not exist at {}; run `just test-e2e`",
            helper.display()
        );
        let name = format!("gateway-e2e-{}-{port}", std::process::id());
        // Construct the guard before invoking the helper, so partial startup is cleaned on any panic.
        let mut cluster = Self {
            helper,
            name,
            plaintext_bootstrap_servers: String::new(),
            sasl_bootstrap_servers: String::new(),
            stopped: false,
        };
        let output = Command::new(&cluster.helper)
            .args(["start", "--name"])
            .arg(&cluster.name)
            .arg("--sasl")
            .args(["--port", &port.to_string()])
            .output()
            .expect("start the Fluss test cluster helper");
        assert!(
            output.status.success(),
            "Fluss test cluster failed to start:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("cluster helper stdout is UTF-8");
        let json = stdout
            .lines()
            .find_map(|line| line.strip_prefix("CLUSTER_JSON: "))
            .expect("cluster helper returns CLUSTER_JSON");
        let info: serde_json::Value = serde_json::from_str(json).expect("valid cluster JSON");
        cluster.plaintext_bootstrap_servers = info["bootstrap_servers"]
            .as_str()
            .expect("cluster JSON contains bootstrap_servers")
            .to_string();
        cluster.sasl_bootstrap_servers = info["sasl_bootstrap_servers"]
            .as_str()
            .expect("SASL cluster JSON contains sasl_bootstrap_servers")
            .to_string();
        cluster
    }

    fn stop(mut self) {
        let output =
            stop_cluster(&self.helper, &self.name).expect("stop the Fluss test cluster helper");
        assert!(
            output.status.success(),
            "Fluss test cluster failed to stop:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        self.stopped = true;
    }
}

impl Drop for FlussCluster {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = stop_cluster(&self.helper, &self.name);
        }
    }
}

fn stop_cluster(helper: &Path, name: &str) -> std::io::Result<std::process::Output> {
    Command::new(helper).args(["stop", "--name", name]).output()
}

/// Finds the SASL and plaintext host-port pairs for the coordinator and single tablet server.
fn free_cluster_port_pair() -> u16 {
    for _ in 0..100 {
        let candidate = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a candidate port");
        let port = candidate.local_addr().expect("candidate address").port();
        drop(candidate);
        let Some(sasl_tablet) = port.checked_add(1) else {
            continue;
        };
        let Some(plaintext_coordinator) = port.checked_add(100) else {
            continue;
        };
        let Some(plaintext_tablet) = port.checked_add(101) else {
            continue;
        };
        let mut reservations = Vec::new();
        for candidate in [port, sasl_tablet, plaintext_coordinator, plaintext_tablet] {
            match std::net::TcpListener::bind(("127.0.0.1", candidate)) {
                Ok(listener) => reservations.push(listener),
                Err(_) => break,
            }
        }
        if reservations.len() == 4 {
            return port;
        }
    }
    panic!("failed to find the four ports for a SASL Fluss test cluster");
}

/// Writes the production Gateway configuration that points its default cluster at the test cluster.
fn write_gateway_config(
    directory: &tempfile::TempDir,
    port: u16,
    bootstrap_servers: &str,
    security_protocol: &str,
) -> std::path::PathBuf {
    let path = directory.path().join("gateway.yaml");
    let mut file = std::fs::File::create(&path).expect("create gateway config");
    writeln!(file, "gateway.rest.listen: 127.0.0.1:{port}").expect("write REST listener");
    writeln!(file, "gateway.metrics.enabled: false").expect("disable metrics listener");
    writeln!(
        file,
        "gateway.cluster.default.bootstrap.servers: {bootstrap_servers}"
    )
    .expect("write Fluss bootstrap servers");
    writeln!(
        file,
        "gateway.cluster.default.connection.security.protocol: {security_protocol}"
    )
    .expect("write Fluss security protocol");
    if security_protocol == "sasl" {
        writeln!(
            file,
            "gateway.cluster.default.connection.service.account: {SERVICE_ACCOUNT}"
        )
        .expect("write Gateway service account");
        writeln!(
            file,
            "gateway.cluster.default.connection.service.secret: {SERVICE_SECRET}"
        )
        .expect("write Gateway service secret");
    }
    path
}

async fn assert_metadata_apis(bootstrap_servers: &str, security_protocol: &str) {
    let gateway_port = free_port();
    let directory = tempfile::tempdir().expect("temporary Gateway config directory");
    let config = write_gateway_config(
        &directory,
        gateway_port,
        bootstrap_servers,
        security_protocol,
    );
    let child = binary()
        .arg("--config")
        .arg(config)
        .spawn()
        .expect("start the Gateway binary");
    let mut gateway = ChildGuard(child);
    let base = format!("http://127.0.0.1:{gateway_port}");
    assert!(
        await_http_ok(&format!("{base}/ready"), Duration::from_secs(15)).await,
        "Gateway becomes ready"
    );

    let api = Api::new(base);
    assert_eq!(
        api.get_ok("/v1/clusters").await,
        serde_json::json!({"clusters": ["default"]})
    );

    let databases = api.get_ok("/v1/clusters/default/databases").await;
    assert!(
        databases["databases"]
            .as_array()
            .expect("database array")
            .iter()
            .any(|database| database == DATABASE),
        "created database is returned: {databases}"
    );
    assert_eq!(
        api.get_ok(&format!("/v1/clusters/default/databases/{DATABASE}/tables"))
            .await,
        serde_json::json!({"tables": [TABLE]})
    );

    gateway.send_sigterm();
    let status = gateway.wait_for_exit(Duration::from_secs(35)).await;
    assert_eq!(status.code(), Some(0), "Gateway drains cleanly");
}

#[tokio::test]
async fn metadata_apis_support_plaintext_and_sasl_fluss_clusters() {
    let cluster = tokio::task::spawn_blocking(|| FlussCluster::start(free_cluster_port_pair()))
        .await
        .expect("cluster startup task");

    let connection = FlussConnection::new(Config {
        bootstrap_servers: cluster.sasl_bootstrap_servers.clone(),
        security_protocol: "sasl".to_string(),
        security_sasl_mechanism: "PLAIN".to_string(),
        security_sasl_username: SERVICE_ACCOUNT.to_string(),
        security_sasl_password: SERVICE_SECRET.to_string(),
        ..Default::default()
    })
    .await
    .expect("connect the catalog setup client");
    let admin = connection.get_admin().expect("get Fluss admin client");
    admin
        .create_database(DATABASE, None, false)
        .await
        .expect("create the E2E database");
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("payload", DataTypes::string())
                .build()
                .expect("build the E2E schema"),
        )
        .build()
        .expect("build the E2E table descriptor");
    admin
        .create_table(&TablePath::new(DATABASE, TABLE), &descriptor, false)
        .await
        .expect("create the E2E table");

    assert_metadata_apis(&cluster.plaintext_bootstrap_servers, "plaintext").await;
    assert_metadata_apis(&cluster.sasl_bootstrap_servers, "sasl").await;

    connection
        .close(Duration::from_secs(10))
        .await
        .expect("close the setup client");
    tokio::task::spawn_blocking(|| cluster.stop())
        .await
        .expect("cluster cleanup task");
}
