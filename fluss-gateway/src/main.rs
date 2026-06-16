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

//! `fluss-gateway` server binary.
//!
//! Assembles the production gateway facade over a single Fluss cluster and serves
//! both frontends: the PostgreSQL wire protocol (read-only SQL) and the REST API
//! (direct write + metadata). This mirrors the assembly used by the cluster e2e
//! test (`tests/cluster_e2e.rs::assemble_instance`) but is driven by environment
//! configuration so it can run as a container.
//!
//! Configuration (all env-driven, with defaults suited to local runs):
//! - `FLUSS_BOOTSTRAP_SERVERS` (default `127.0.0.1:9123`) — the Fluss cluster.
//! - `GATEWAY_PG_LISTEN`       (default `0.0.0.0:5432`)   — PostgreSQL bind addr.
//! - `GATEWAY_REST_LISTEN`     (default `0.0.0.0:8080`)   — REST bind addr.
//! - `FLUSS_CLUSTER`           (default `default`)        — logical cluster id.
//! - `RUST_LOG`                (default `info`)           — tracing filter.
//!
//! Phase note: authentication is `TrustAuthenticator` (the username is trusted,
//! the password is not verified) and there is no config file yet — both are
//! deferred. See `auth/mod.rs` for the verifying authenticator seam.

use std::sync::Arc;
use std::time::Duration;

use fluss_gateway::auth::TrustAuthenticator;
use fluss_gateway::backend::FlussBackendFacade;
use fluss_gateway::cluster::{ClusterConfig, ClusterRegistry};
use fluss_gateway::connection::{FlussConnectionProvider, SharedProxyConnectionProvider};
use fluss_gateway::instance::{GatewayInstance, GatewayInstanceImpl};
use fluss_gateway::server::postgres::PgServer;
use fluss_gateway::server::rest::RestServer;
use fluss_gateway::session::manager::{SessionManager, SessionManagerConfig};
use fluss_gateway::sql::environment::{
    FlussDatafusionCatalogInstaller, PgSqlEnvironmentProvider, SqlEnvironmentRegistry,
    StubPgCatalogOverlayInstaller,
};
use fluss_gateway::types::{ClusterId, Principal, SqlEnvironmentId};

/// Resolved runtime configuration, read once from the environment at startup.
struct GatewayConfig {
    bootstrap_servers: String,
    pg_listen: String,
    rest_listen: String,
    cluster: String,
}

impl GatewayConfig {
    fn from_env() -> Self {
        let env_or = |key: &str, default: &str| {
            std::env::var(key)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| default.to_string())
        };
        Self {
            bootstrap_servers: env_or("FLUSS_BOOTSTRAP_SERVERS", "127.0.0.1:9123"),
            pg_listen: env_or("GATEWAY_PG_LISTEN", "0.0.0.0:5432"),
            rest_listen: env_or("GATEWAY_REST_LISTEN", "0.0.0.0:8080"),
            cluster: env_or("FLUSS_CLUSTER", "default"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tracing: honor RUST_LOG, defaulting to `info` if unset.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = GatewayConfig::from_env();
    tracing::info!(
        bootstrap_servers = %config.bootstrap_servers,
        cluster = %config.cluster,
        pg_listen = %config.pg_listen,
        rest_listen = %config.rest_listen,
        "starting fluss-gateway"
    );

    let instance = assemble_instance(&config).await?;

    // Bind both frontends before serving so a bind failure (e.g. port in use)
    // surfaces immediately rather than after the cluster handshake.
    let (pg_listener, pg_addr) = PgServer::bind(&config.pg_listen).await?;
    let (rest_listener, rest_addr) = RestServer::bind(&config.rest_listen).await?;
    tracing::info!(%pg_addr, %rest_addr, "listening (postgres + rest)");

    let pg = PgServer::new(
        Arc::clone(&instance) as Arc<dyn GatewayInstance>,
        Arc::new(TrustAuthenticator::new()),
    );
    let rest = RestServer::new(
        Arc::clone(&instance) as Arc<dyn GatewayInstance>,
        Arc::new(TrustAuthenticator::new()),
    );

    let mut pg_task = tokio::spawn(async move { pg.serve(pg_listener).await });
    let mut rest_task = tokio::spawn(async move { rest.serve(rest_listener).await });

    // Run until either frontend exits (an error) or a shutdown signal arrives.
    tokio::select! {
        res = &mut pg_task => {
            tracing::error!(?res, "postgres frontend exited");
        }
        res = &mut rest_task => {
            tracing::error!(?res, "rest frontend exited");
        }
        _ = shutdown_signal() => {
            tracing::info!("shutdown signal received, stopping");
            pg_task.abort();
            rest_task.abort();
        }
    }

    Ok(())
}

/// Build the production gateway facade over the configured cluster. The same
/// shared `FlussConnection` backs both the SQL path (via `FlussDatafusion`) and
/// the direct path (via `FlussBackendFacade`).
async fn assemble_instance(
    config: &GatewayConfig,
) -> Result<Arc<GatewayInstanceImpl>, Box<dyn std::error::Error>> {
    let cluster = ClusterId(config.cluster.clone());
    // Phase 1 uses a shared proxy account; the principal is carried through the
    // chain but not consumed (no doAs).
    let principal = Principal {
        name: "gateway".to_string(),
    };

    let registry = ClusterRegistry::single_default(ClusterConfig {
        bootstrap_servers: config.bootstrap_servers.clone(),
    });
    let conn_provider = SharedProxyConnectionProvider::new(registry);

    // The cluster may not be reachable the instant the container starts; retry a
    // few times with backoff so `docker run` before the cluster is up recovers.
    let connection = resolve_with_retry(&conn_provider, &cluster, &principal).await?;

    // SQL path: real Fluss catalog behind the PostgreSQL SQL environment provider.
    let fluss_df = Arc::new(
        fluss_datafusion::FlussDatafusion::new(
            Arc::clone(&connection),
            fluss_datafusion::FlussDatafusionOptions::default(),
        )
        .await?,
    );
    let pg_provider = PgSqlEnvironmentProvider::new(
        Arc::new(FlussDatafusionCatalogInstaller::new(fluss_df)),
        Arc::new(StubPgCatalogOverlayInstaller),
    );
    let mut sql_environments = SqlEnvironmentRegistry::new();
    sql_environments.register(SqlEnvironmentId("postgres".into()), Arc::new(pg_provider));

    // Direct path: a backend over the SAME shared connection.
    let backend = Arc::new(FlussBackendFacade::new(Arc::clone(&connection)));
    let sessions = Arc::new(SessionManager::new(SessionManagerConfig::default()));

    Ok(Arc::new(GatewayInstanceImpl::new(
        sessions,
        backend,
        Arc::new(sql_environments),
    )))
}

/// Resolve the shared connection, retrying a bounded number of times so a
/// not-yet-ready cluster at container start does not immediately kill the process.
async fn resolve_with_retry(
    provider: &SharedProxyConnectionProvider,
    cluster: &ClusterId,
    principal: &Principal,
) -> Result<Arc<fluss::client::FlussConnection>, Box<dyn std::error::Error>> {
    const ATTEMPTS: u32 = 10;
    const BACKOFF: Duration = Duration::from_secs(3);
    let mut last_err = None;
    for attempt in 1..=ATTEMPTS {
        match provider.resolve(cluster, principal).await {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                tracing::warn!(attempt, max = ATTEMPTS, error = %e, "cluster not reachable yet, retrying");
                last_err = Some(e);
                tokio::time::sleep(BACKOFF).await;
            }
        }
    }
    Err(last_err
        .map(|e| Box::new(e) as Box<dyn std::error::Error>)
        .unwrap_or_else(|| "failed to resolve Fluss connection".into()))
}

/// Resolve on Ctrl-C (SIGINT) or, on Unix, SIGTERM (`docker stop`).
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
