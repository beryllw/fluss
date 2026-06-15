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

//! P6 — ClusterRegistry.
//!
//! Knows which clusters exist and their connection config (Phase 1: `default`
//! only). Bottom of the `BackendFacade -> FlussConnectionProvider ->
//! ClusterRegistry` chain. Intentionally not exposed to upper layers
//! (SQL / direct only see `BackendFacade` and connections), so it stays a
//! private routing detail consumed by `FlussConnectionProvider`.
//! Design: `design/infra.md` §P6.1.

use std::collections::BTreeMap;

use crate::error::{GatewayError, GatewayResult};
use crate::types::ClusterId;

/// Connection configuration for a single cluster. Phase 1 keeps this minimal:
/// just the bootstrap endpoint(s) used to construct the shared proxy connection.
/// Per-user credentials / doAs are explicitly out of scope (DESIGN.md §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    /// Bootstrap server list, e.g. `"127.0.0.1:9123"`. The exact wiring into the
    /// fluss-rs `Config` happens in the connection provider, not here.
    pub bootstrap_servers: String,
}

/// Minimal cluster registry. Phase 1 is single-cluster: exactly one entry keyed
/// by `ClusterId("default")`. The shape (a map) is kept so multi-cluster routing
/// can be added later without changing the lookup call site, but no multi-cluster
/// routing table is built now (DESIGN.md §2: real multi-cluster is out of scope).
#[derive(Debug, Clone)]
pub struct ClusterRegistry {
    clusters: BTreeMap<String, ClusterConfig>,
    default: ClusterId,
}

impl ClusterRegistry {
    /// Build a single-cluster registry whose only (and default) cluster is
    /// `ClusterId("default")` with the supplied config.
    pub fn single_default(config: ClusterConfig) -> Self {
        let default = ClusterId("default".to_string());
        let mut clusters = BTreeMap::new();
        clusters.insert(default.0.clone(), config);
        Self { clusters, default }
    }

    /// The cluster used when a request does not name one.
    pub fn default_cluster(&self) -> &ClusterId {
        &self.default
    }

    /// Resolve a cluster's connection config, or `DatabaseNotFound`-adjacent
    /// `InvalidArgument` if the cluster id is unknown. (No dedicated
    /// `ClusterNotFound` variant in Phase 1; an unknown cluster is a bad request.)
    pub fn config(&self, cluster: &ClusterId) -> GatewayResult<&ClusterConfig> {
        self.clusters
            .get(&cluster.0)
            .ok_or_else(|| GatewayError::InvalidArgument(format!("unknown cluster: {}", cluster.0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> ClusterRegistry {
        ClusterRegistry::single_default(ClusterConfig {
            bootstrap_servers: "127.0.0.1:9123".into(),
        })
    }

    #[test]
    fn default_cluster_is_default() {
        let r = registry();
        assert_eq!(r.default_cluster(), &ClusterId("default".into()));
    }

    #[test]
    fn config_resolves_default() {
        let r = registry();
        let cfg = r.config(&ClusterId("default".into())).unwrap();
        assert_eq!(cfg.bootstrap_servers, "127.0.0.1:9123");
    }

    #[test]
    fn unknown_cluster_is_invalid_argument() {
        let r = registry();
        let err = r.config(&ClusterId("other".into())).unwrap_err();
        assert!(matches!(err, GatewayError::InvalidArgument(_)));
        assert!(err.to_string().contains("other"));
    }
}
