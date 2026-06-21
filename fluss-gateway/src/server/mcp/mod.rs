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

//! MCP (Model Context Protocol) frontend — read-only tools for AI agents.
//!
//! A separate listener that serves the official `rmcp` Streamable HTTP transport
//! at `/mcp`, exposing four read-only tools (`list_databases`, `list_tables`,
//! `describe_table`, `query`) that map onto the [`GatewayInstance`] facade. It is
//! its own server (mirroring [`PgServer`](crate::server::PgServer) /
//! [`RestServer`](crate::server::RestServer)) rather than nested on the REST
//! router, because rmcp owns its own tower service + session manager; keeping it
//! standalone avoids entangling the hand-written REST router with rmcp's session
//! lifecycle.
//!
//! Auth reuses the shared Basic-auth seam: an axum middleware wraps `/mcp`,
//! authenticates `Authorization: Basic` to a [`Principal`], and injects it into
//! the request extensions. The Streamable HTTP transport copies the request
//! `Parts` (including those extensions) into each tool call, where the handler
//! reads the principal back. Unauthenticated requests get 401 before any tool runs.
//!
//! The DNS-rebinding `Host` allow-list (an rmcp default for locally-run servers)
//! is disabled: this gateway is a single-tenant service reached by arbitrary agent
//! hosts inside a trusted VPC and is already protected by authentication, so a
//! loopback-only Host check would only break legitimate access.

mod handler;
mod tools;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use tokio::net::TcpListener;

use crate::auth::{credential_from_userpass, Authenticator};
use crate::error::GatewayError;
use crate::instance::GatewayInstance;
use crate::server::rest::parse_basic_auth;
use crate::types::{ClusterId, Principal};

use handler::McpHandler;

/// The MCP frontend: builds the axum router (rmcp service under `/mcp` behind the
/// auth middleware) and owns bind/serve, mirroring `RestServer` so tests can learn
/// the ephemeral port before driving traffic.
#[derive(Clone)]
pub struct McpServer {
    instance: Arc<dyn GatewayInstance>,
    authenticator: Arc<dyn Authenticator>,
    cluster: ClusterId,
}

impl McpServer {
    pub fn new(instance: Arc<dyn GatewayInstance>, authenticator: Arc<dyn Authenticator>) -> Self {
        Self {
            instance,
            authenticator,
            // Single-cluster deployment; the REST path likewise routes only "default".
            cluster: ClusterId("default".to_string()),
        }
    }

    /// Build the axum [`Router`]: the rmcp Streamable HTTP service mounted at
    /// `/mcp`, wrapped by the Basic-auth middleware.
    pub fn router(&self) -> Router {
        let instance = self.instance.clone();
        let cluster = self.cluster.clone();

        let service = StreamableHttpService::new(
            move || Ok(McpHandler::new(instance.clone(), cluster.clone())),
            Arc::new(LocalSessionManager::default()),
            // Disable the loopback-only Host allow-list (see module docs).
            StreamableHttpServerConfig::default().disable_allowed_hosts(),
        );

        Router::new()
            .nest_service("/mcp", service)
            .layer(middleware::from_fn_with_state(
                self.authenticator.clone(),
                auth_middleware,
            ))
    }

    /// Bind a TCP listener and return it with the resolved local address. Tests
    /// bind `127.0.0.1:0` to learn the ephemeral port.
    pub async fn bind(addr: &str) -> std::io::Result<(TcpListener, SocketAddr)> {
        let listener = TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        Ok((listener, local))
    }

    /// Serve the router on an already-bound listener until the process ends.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        axum::serve(listener, self.router().into_make_service()).await
    }
}

/// Authenticate `Authorization: Basic` and stash the resulting [`Principal`] in
/// the request extensions for the tool handlers; reject with 401 on failure.
async fn auth_middleware(
    State(authenticator): State<Arc<dyn Authenticator>>,
    mut req: Request,
    next: Next,
) -> Response {
    match authenticate(&authenticator, req.headers()).await {
        Ok(principal) => {
            req.extensions_mut().insert(principal);
            next.run(req).await
        }
        Err(err) => unauthorized(err),
    }
}

async fn authenticate(
    authenticator: &Arc<dyn Authenticator>,
    headers: &HeaderMap,
) -> Result<Principal, GatewayError> {
    let (username, password) = parse_basic_auth(headers)?;
    let credential = credential_from_userpass(username, password);
    authenticator
        .authenticate(credential)
        .await
        .map_err(GatewayError::from)
}

fn unauthorized(err: GatewayError) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({
            "error": { "code": "unauthenticated", "message": err.to_string() }
        })),
    )
        .into_response()
}
