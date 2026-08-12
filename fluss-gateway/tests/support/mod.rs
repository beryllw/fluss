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
//! The suites differ only in what serves the requests — an in-process gateway or the compiled binary —
//! so the client side lives here once.

// Each test binary uses a different subset of these helpers.
#![allow(dead_code)]

use fluss_gateway::config::GatewayConfig;
use fluss_gateway::lifecycle::RunningGateway;
use serde_json::Value;

/// A thin REST client bound to one gateway base URL.
///
/// The gateway has no authentication yet, so every request is sent bare.
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
}

/// Starts an in-process gateway over `lifecycle::start` with an ephemeral port and no metrics listener.
pub async fn start_gateway() -> RunningGateway {
    let mut config = GatewayConfig::default();
    config.server.rest.bind_address = "127.0.0.1:0".parse().expect("valid");
    config.server.metrics.enabled = false;
    fluss_gateway::lifecycle::start(config)
        .await
        .expect("gateway starts")
}
