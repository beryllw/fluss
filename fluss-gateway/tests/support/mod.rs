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

//! HTTP helpers shared by the gateway test suites.
//!
//! The suites differ only in what serves the requests — an in-process gateway over the fixture backend, two such
//! gateways at once, or a full gateway in front of a real cluster — so the client side lives here once.

// Each test binary uses a different subset of these helpers.
#![allow(dead_code)]

use fluss_gateway::backend::registry::ClusterRegistry;
use fluss_gateway::backend::testing::TestBackend;
use fluss_gateway::config::{AuthenticationMode, CliOverrides, GatewayConfig, load};
use fluss_gateway::lifecycle::{RunningGateway, start_with_clusters};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A thin REST client bound to one gateway base URL.
pub struct Api {
    client: reqwest::Client,
    base: String,
}

impl Api {
    /// Creates a test client bound to `base_url`.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base: base_url.into(),
        }
    }

    /// Resolves one absolute request URL against the configured base URL.
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Sends a GET request and returns the raw response.
    pub async fn get(&self, path: &str) -> reqwest::Response {
        self.client
            .get(self.url(path))
            .send()
            .await
            .expect("GET request")
    }

    /// GET expecting 200, returning the parsed body.
    pub async fn get_ok(&self, path: &str) -> Value {
        let response = self.get(path).await;
        assert_eq!(response.status(), 200, "GET {path}");
        response.json().await.expect("JSON body")
    }

    /// Sends a GET request carrying HTTP basic credentials.
    pub async fn get_with_basic(&self, path: &str, user: &str, pass: &str) -> reqwest::Response {
        self.client
            .get(self.url(path))
            .basic_auth(user, Some(pass))
            .send()
            .await
            .expect("GET request")
    }

    /// Sends a GET request with one explicit raw header, for malformed-credential cases.
    pub async fn get_with_header(
        &self,
        path: &str,
        name: &'static str,
        value: &str,
    ) -> reqwest::Response {
        self.client
            .get(self.url(path))
            .header(name, value)
            .send()
            .await
            .expect("GET request")
    }

    /// Sends a JSON POST request accepting a JSON response.
    pub async fn post_json(&self, path: &str, body: &Value) -> reqwest::Response {
        self.post_json_accept(path, body, "application/json").await
    }

    /// Sends a JSON POST request with an explicit response media type.
    pub async fn post_json_accept(
        &self,
        path: &str,
        body: &Value,
        accept: &str,
    ) -> reqwest::Response {
        self.client
            .post(self.url(path))
            .header("accept", accept)
            .json(body)
            .send()
            .await
            .expect("POST request")
    }

    /// Sends a JSON PATCH request accepting a JSON response.
    pub async fn patch_json(&self, path: &str, body: &Value) -> reqwest::Response {
        self.client
            .patch(self.url(path))
            .header("accept", "application/json")
            .json(body)
            .send()
            .await
            .expect("PATCH request")
    }

    /// Sends a DELETE request and returns the raw response.
    pub async fn delete(&self, path: &str) -> reqwest::Response {
        self.client
            .delete(self.url(path))
            .send()
            .await
            .expect("DELETE request")
    }
}

/// Configuration for an in-process gateway on ephemeral ports with the metrics listener disabled.
pub fn ephemeral_config() -> GatewayConfig {
    let mut config = load(None, &BTreeMap::new(), &CliOverrides::default()).expect("defaults");
    config.server.rest.bind_address = "127.0.0.1:0".parse().expect("valid");
    config.server.metrics.enabled = false;
    config.validate().expect("ephemeral configuration is valid");
    config
}

/// Ephemeral-port configuration enforcing password authentication over the given user table.
pub fn password_config(users: &str) -> GatewayConfig {
    let mut config = ephemeral_config();
    config.security.authentication = AuthenticationMode::Password;
    config.security.users = Some(users.to_string());
    config.validate().expect("password configuration is valid");
    config
}

/// One in-process gateway plus a client bound to its address.
pub struct Instance {
    pub gateway: RunningGateway,
    pub api: Api,
    /// The registry this instance was started with: its own per-process connection pool.
    pub clusters: Arc<ClusterRegistry>,
}

impl Instance {
    /// Stops the instance and asserts it shut down cleanly.
    pub async fn shutdown(self) {
        self.gateway.shutdown().await.expect("clean shutdown");
    }
}

/// Starts one gateway over the given registry and binds a client to it.
pub async fn start_instance(clusters: Arc<ClusterRegistry>) -> Instance {
    start_instance_with_config(ephemeral_config(), clusters).await
}

/// Starts one gateway with explicit configuration over the given registry.
pub async fn start_instance_with_config(
    config: GatewayConfig,
    clusters: Arc<ClusterRegistry>,
) -> Instance {
    let gateway = start_with_clusters(config, clusters.clone())
        .await
        .expect("the gateway starts");
    let api = Api::new(format!("http://{}", gateway.local_addr()));
    Instance {
        gateway,
        api,
        clusters,
    }
}

/// A single connected fixture cluster named `default`.
pub fn single_cluster(backend: Arc<TestBackend>) -> Arc<ClusterRegistry> {
    Arc::new(ClusterRegistry::from_test_entries(vec![(
        "default".to_string(),
        Some(backend as Arc<dyn fluss_gateway::backend::GatewayBackend>),
        Some(green_report()),
    )]))
}

/// A green cluster health report for a reachable fixture cluster.
pub fn green_report() -> fluss_gateway::backend::model::ClusterHealthReport {
    fluss_gateway::backend::model::ClusterHealthReport {
        status: fluss_gateway::backend::model::ClusterStatus::Green,
        num_replicas: 6,
        in_sync_replicas: 6,
        num_leader_replicas: 3,
        active_leader_replicas: 3,
    }
}
