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

//! End-to-end suite: the compiled gateway running next to a real Fluss cluster.
//!
//! The cluster is the dockerized fixed-version fixture of the Rust client
//! (`fluss_test_cluster::FlussTestingClusterBuilder`), reused rather than reimplemented, so both projects
//! start a test cluster the same way. `FLUSS_IMAGE` and `FLUSS_VERSION` override the image.
//!
//! # Why this is gated and not enabled yet
//!
//! The suite needs a Docker daemon, so it sits behind the `integration_tests` feature (the name FIP-49's
//! test plan uses) and is compiled away by default. CI has a matching job that only runs on
//! `workflow_dispatch`; enabling it on every pull request is a one-line change there once the gateway has
//! behaviour worth checking against a cluster.
//!
//! # What arrives here next
//!
//! The gateway cannot be pointed at the cluster yet: `gateway.cluster.<id>.bootstrap.servers` is part of
//! the FlussBackend capability, so the configuration schema has no key for it. This suite therefore proves
//! today that the fixture and the gateway process come up together and shut down cleanly, and it is where
//! the FIP-49 cluster scenarios land as the capabilities arrive — REST write against a real cluster,
//! reconnection after a TabletServer is killed, partitioned-table lifecycle, dual-cluster routing, and KV
//! backpressure. Each of those is a test function added below, not new infrastructure.

mod support;

use fluss_test_cluster::FlussTestingClusterBuilder;
use std::time::Duration;
use support::{Api, ChildGuard, await_http_ok, binary, free_port, write_config};

/// Port 19123 keeps the fixture clear of the fluss-rs integration suite's default cluster on 9123: the
/// fixture binds fixed host ports, not ephemeral ones.
const CLUSTER_PORT: u16 = 19123;

#[tokio::test]
async fn the_gateway_serves_alongside_a_real_cluster_and_shuts_down_cleanly() {
    let mut builder =
        FlussTestingClusterBuilder::new("gateway-e2e-cluster").with_port(CLUSTER_PORT);
    let cluster = builder.build().await;

    // Bare host:port, e.g. "127.0.0.1:19123" — no scheme prefix to strip. Connect with the async client:
    // every wait in this test has to yield, or it stalls the runtime the fixture also runs on.
    let bootstrap = cluster.plaintext_bootstrap_servers().to_string();
    let reachable = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::net::TcpStream::connect(&bootstrap),
    )
    .await;
    assert!(
        matches!(reachable, Ok(Ok(_))),
        "fixture bootstrap {bootstrap} accepts TCP connections"
    );

    let directory = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    // TODO: pass `bootstrap` through `gateway.cluster.default.bootstrap.servers` once the FlussBackend
    // capability adds that option, and assert a write lands in the cluster.
    let config = write_config(&directory, port);
    let mut gateway = ChildGuard(
        binary()
            .arg("--config")
            .arg(&config)
            .spawn()
            .expect("spawn the gateway"),
    );

    let base = format!("http://127.0.0.1:{port}");
    assert!(
        await_http_ok(&format!("{base}/health"), Duration::from_secs(15)).await,
        "the gateway serves /health while the cluster runs"
    );
    let health = Api::new(base).get_ok("/health").await;
    assert_eq!(health["status"], "ok");

    gateway.send_sigterm();
    assert_eq!(
        gateway.wait_for_exit(Duration::from_secs(35)).await.code(),
        Some(0),
        "the gateway drains and exits 0 with the cluster still up"
    );
    cluster.stop();
}
