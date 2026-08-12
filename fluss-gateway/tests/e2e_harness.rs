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

//! Self-test of the shared end-to-end harness: starts the fixed-version dockerized Fluss cluster
//! and proves the test environment can reach it. The gateway's own production connection to Fluss
//! is exercised by the authentication/service-identity capability, not here.
//!
//! Gated behind `integration_tests` because it needs Docker. The CI gate runs it with
//! `cargo test --features integration_tests --test e2e_harness` and fails loudly when the fixture
//! cannot start — a selected scenario must never skip silently.
//!
//! TODO: once the gateway connects to Fluss, move its integration suites onto the server image that
//! `client-integration.yml` builds from the current source tree, so they verify the gateway against
//! this revision of the server rather than only against a released fixture image.

use fluss_test_cluster::FlussTestingClusterBuilder;
use std::net::TcpStream;
use std::time::Duration;

#[tokio::test]
async fn the_fixed_version_fluss_fixture_starts_and_is_reachable() {
    // Port 19123 keeps the fixture clear of the fluss-rs integration suite's
    // default cluster on 9123 (host ports are fixed, not ephemeral).
    let mut builder = FlussTestingClusterBuilder::new("gateway-harness-selftest").with_port(19123);
    let cluster = builder.build().await;
    // Bare host:port, e.g. "127.0.0.1:19123" — no scheme prefix to strip.
    let address = cluster.plaintext_bootstrap_servers().to_string();
    let reachable = TcpStream::connect_timeout(
        &address.parse().expect("bootstrap address parses"),
        Duration::from_secs(10),
    );
    assert!(
        reachable.is_ok(),
        "fixture bootstrap {address} accepts TCP connections"
    );
    cluster.stop();
}
