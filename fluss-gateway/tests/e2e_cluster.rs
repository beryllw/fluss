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
//! The journeys this file will cover: DDL round trips, writes with per-row delivery outcomes, primary-key and
//! prefix lookups against real data, partitioned tables, multi-cluster routing, and TabletServer
//! restart/reconnect.

#[test]
fn placeholder_until_the_cluster_journeys_land() {
    // Keeps the gated target meaningful: enabling the feature must produce a suite that runs.
}
