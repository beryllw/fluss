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

//! The statelessness contract: any instance can serve any request.
//!
//! This suite is deliberately **not** feature-gated behind `integration_tests`. Statelessness is the gateway's
//! first hard constraint, so the default `cargo test` run must verify it. It needs no cluster: two independent
//! `RunningGateway` instances are started over one shared fixture backend, and every operation is interleaved
//! across both. Any difference in what they answer would mean an instance is holding request-spanning state.
//!
//! In this phase the harness plus one smoke assertion are in place. The full round-robin journey — write on A,
//! look up on B, page a listing across both, run DDL on one and observe it on the other — arrives with the
//! endpoints it exercises.

mod support;

use fluss_gateway::backend::testing::TestBackend;
use std::sync::Arc;
use support::{Instance, single_cluster, start_instance};

/// Starts two independent gateway instances over one shared Fluss backend.
///
/// Each instance gets its **own** cluster registry, exactly as two deployed processes would: the registry is a
/// per-process connection pool, not shared infrastructure. Only the backend behind it — standing in for the
/// Fluss cluster — is common. Sharing one registry would couple the instances through the pool and mask the very
/// property this suite exists to prove.
async fn two_instances() -> (Instance, Instance, Arc<TestBackend>) {
    let backend = Arc::new(TestBackend::new());
    let first = start_instance(single_cluster(backend.clone())).await;
    let second = start_instance(single_cluster(backend.clone())).await;
    (first, second, backend)
}

#[tokio::test]
async fn two_instances_serve_identical_cluster_views() {
    let (first, second, _backend) = two_instances().await;

    assert_ne!(
        first.gateway.local_addr(),
        second.gateway.local_addr(),
        "the instances must be independently bound"
    );

    let from_first = first.api.get_ok("/v1/clusters").await;
    let from_second = second.api.get_ok("/v1/clusters").await;
    assert_eq!(from_first, from_second);

    // Interleaving requests across the instances changes nothing: the answer depends only on the request and the
    // current cluster state, never on which instance answered a previous one.
    for _ in 0..3 {
        assert_eq!(first.api.get_ok("/v1/clusters").await, from_first);
        assert_eq!(second.api.get_ok("/v1/clusters").await, from_first);
    }

    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn losing_one_instance_does_not_affect_the_other() {
    let (first, second, backend) = two_instances().await;
    let expected = first.api.get_ok("/v1/clusters").await;

    // A stateless instance owns nothing another instance needs, so terminating it mid-sequence is invisible.
    first.shutdown().await;

    assert_eq!(second.api.get_ok("/v1/clusters").await, expected);
    assert_eq!(second.api.get_ok("/health/ready").await["status"], "ready");
    // Shutting the first instance down released only that instance's connection pool. The survivor still resolves
    // a connected backend of its own, which is exactly what "no shared per-request state" has to mean in practice.
    assert!(
        second.clusters.backend("default").is_ok(),
        "the survivor keeps its own connection to the shared cluster"
    );
    let _ = &backend;

    second.shutdown().await;
}

#[tokio::test]
async fn both_instances_serve_the_same_openapi_contract() {
    let (first, second, _backend) = two_instances().await;

    let from_first = first.api.get_ok("/v1/openapi.json").await;
    let from_second = second.api.get_ok("/v1/openapi.json").await;
    assert_eq!(from_first, from_second);

    // No path in the contract can be instance-affine: there are no cursors, sessions, or handles to route back.
    let paths = from_first["paths"].as_object().expect("paths object");
    assert!(!paths.is_empty());
    for path in paths.keys() {
        assert!(!path.contains("cursor"), "instance-affine path {path}");
        assert!(!path.contains("session"), "instance-affine path {path}");
    }

    first.shutdown().await;
    second.shutdown().await;
}
