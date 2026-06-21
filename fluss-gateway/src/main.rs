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
//! - `GATEWAY_MCP_ENABLED`     (default `false`)          — serve the MCP frontend.
//! - `GATEWAY_MCP_LISTEN`      (default `0.0.0.0:8000`)   — MCP bind addr (when enabled).
//! - `FLUSS_CLUSTER`           (default `default`)        — logical cluster id.
//! - `GATEWAY_CONFIG`          (optional)                 — YAML config file.
//! - `GATEWAY_USERS`           (optional)                 — `user:secret,...` auth override.
//! - `RUST_LOG`                (default `info`)           — tracing filter.
//!
//! Auth note: when users are configured (YAML file and/or env override), the
//! gateway verifies username/password on both PG and REST. With no configured
//! users, it falls back to trust mode (username becomes the principal,
//! password ignored) and logs a warning.

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use fluss_gateway::auth::config::{parse_users_env, parse_yaml};
use fluss_gateway::auth::{Authenticator, ConfigUserStoreAuthenticator, TrustAuthenticator};
use fluss_gateway::backend::FlussBackendFacade;
use fluss_gateway::cluster::{ClusterConfig, ClusterRegistry};
use fluss_gateway::connection::{
    build_fluss_config, ConnectionManager, FlussConnectionProvider, SharedProxyConnectionProvider,
};
use fluss_gateway::error::GatewayError;
use fluss_gateway::instance::{GatewayInstance, GatewayInstanceImpl};
use fluss_gateway::server::mcp::McpServer;
use fluss_gateway::server::postgres::PgServer;
use fluss_gateway::server::rest::{OtlpConfig, RestServer};
use fluss_gateway::session::manager::{SessionManager, SessionManagerConfig};
use fluss_gateway::sql::environment::{
    FlussDatafusionCatalogInstaller, PgSqlEnvironmentProvider, SqlEnvironmentRegistry,
    StubPgCatalogOverlayInstaller,
};
use fluss_gateway::types::{ClusterId, Principal, SqlEnvironmentId, TableRef};

/// Resolved runtime configuration, read once from the environment at startup.
struct GatewayConfig {
    bootstrap_servers: String,
    pg_listen: String,
    rest_listen: String,
    mcp_enabled: bool,
    mcp_listen: String,
    cluster: String,
    config_path: Option<String>,
    users_env: Option<String>,
    otlp_enabled: bool,
    otlp_logs_table: Option<TableRef>,
    otlp_metrics_table: Option<TableRef>,
    otlp_traces_table: Option<TableRef>,
}

impl GatewayConfig {
    fn from_env() -> Result<Self, GatewayError> {
        let env_or = |key: &str, default: &str| {
            std::env::var(key)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| default.to_string())
        };
        let env_opt = |key: &str| std::env::var(key).ok().filter(|v| !v.trim().is_empty());
        let env_flag = |key: &str| {
            std::env::var(key)
                .ok()
                .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
                .unwrap_or(false)
        };
        let otlp_enabled = env_flag("GATEWAY_OTLP_ENABLED");
        let otlp_logs_table = env_opt("GATEWAY_OTLP_LOGS_TABLE")
            .map(|v| parse_table_ref("GATEWAY_OTLP_LOGS_TABLE", &v))
            .transpose()?;
        let otlp_metrics_table = env_opt("GATEWAY_OTLP_METRICS_TABLE")
            .map(|v| parse_table_ref("GATEWAY_OTLP_METRICS_TABLE", &v))
            .transpose()?;
        let otlp_traces_table = env_opt("GATEWAY_OTLP_TRACES_TABLE")
            .map(|v| parse_table_ref("GATEWAY_OTLP_TRACES_TABLE", &v))
            .transpose()?;
        let cfg = Self {
            bootstrap_servers: env_or("FLUSS_BOOTSTRAP_SERVERS", "127.0.0.1:9123"),
            pg_listen: env_or("GATEWAY_PG_LISTEN", "0.0.0.0:5432"),
            rest_listen: env_or("GATEWAY_REST_LISTEN", "0.0.0.0:8080"),
            mcp_enabled: env_flag("GATEWAY_MCP_ENABLED"),
            mcp_listen: env_or("GATEWAY_MCP_LISTEN", "0.0.0.0:8000"),
            cluster: env_or("FLUSS_CLUSTER", "default"),
            config_path: env_opt("GATEWAY_CONFIG"),
            users_env: env_opt("GATEWAY_USERS"),
            otlp_enabled,
            otlp_logs_table,
            otlp_metrics_table,
            otlp_traces_table,
        };
        cfg.validate_otlp()?;
        Ok(cfg)
    }

    fn validate_otlp(&self) -> Result<(), GatewayError> {
        if !self.otlp_enabled {
            return Ok(());
        }
        for (name, value) in [
            ("GATEWAY_OTLP_LOGS_TABLE", &self.otlp_logs_table),
            ("GATEWAY_OTLP_METRICS_TABLE", &self.otlp_metrics_table),
            ("GATEWAY_OTLP_TRACES_TABLE", &self.otlp_traces_table),
        ] {
            if value.is_none() {
                return Err(GatewayError::InvalidArgument(format!(
                    "{name} is required when GATEWAY_OTLP_ENABLED=true"
                )));
            }
        }
        Ok(())
    }

    fn otlp_config(&self) -> Option<OtlpConfig> {
        self.otlp_enabled.then(|| OtlpConfig {
            logs_table: self.otlp_logs_table.clone().expect("validated logs table"),
            metrics_table: self
                .otlp_metrics_table
                .clone()
                .expect("validated metrics table"),
            traces_table: self
                .otlp_traces_table
                .clone()
                .expect("validated traces table"),
        })
    }
}

fn parse_table_ref(name: &str, raw: &str) -> Result<TableRef, GatewayError> {
    let (database, table) = raw.split_once('.').ok_or_else(|| {
        GatewayError::InvalidArgument(format!("{name} must be '<database>.<table>'"))
    })?;
    let database = database.trim();
    let table = table.trim();
    if database.is_empty() || table.is_empty() || table.contains('.') {
        return Err(GatewayError::InvalidArgument(format!(
            "{name} must be '<database>.<table>'"
        )));
    }
    Ok(TableRef {
        database: database.to_string(),
        table: table.to_string(),
    })
}

fn build_authenticator(
    config: &GatewayConfig,
) -> Result<Arc<dyn Authenticator>, Box<dyn std::error::Error>> {
    let mut users = HashMap::<String, String>::new();

    if let Some(path) = &config.config_path {
        let text = fs::read_to_string(path)?;
        for (username, secret) in parse_yaml(&text)? {
            users.insert(username, secret);
        }
    }
    if let Some(spec) = &config.users_env {
        for (username, secret) in parse_users_env(spec) {
            users.insert(username, secret);
        }
    }

    if users.is_empty() {
        tracing::warn!(
            "no auth users configured; falling back to trust mode (username is trusted, password ignored)"
        );
        Ok(Arc::new(TrustAuthenticator::new()))
    } else {
        let auth = ConfigUserStoreAuthenticator::from_pairs(users)?;
        tracing::info!(users = auth.user_count(), "configured password authenticator");
        Ok(Arc::new(auth))
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

    let config = GatewayConfig::from_env()?;
    tracing::info!(
        bootstrap_servers = %config.bootstrap_servers,
        cluster = %config.cluster,
        pg_listen = %config.pg_listen,
        rest_listen = %config.rest_listen,
        mcp_enabled = config.mcp_enabled,
        otlp_enabled = config.otlp_enabled,
        "starting fluss-gateway"
    );

    let instance = assemble_instance(&config).await?;
    let authenticator = build_authenticator(&config)?;

    // Bind the frontends before serving so a bind failure (e.g. port in use)
    // surfaces immediately rather than after the cluster handshake.
    let (pg_listener, pg_addr) = PgServer::bind(&config.pg_listen).await?;
    let (rest_listener, rest_addr) = RestServer::bind(&config.rest_listen).await?;
    tracing::info!(%pg_addr, %rest_addr, "listening (postgres + rest)");

    let pg = PgServer::new(
        Arc::clone(&instance) as Arc<dyn GatewayInstance>,
        Arc::clone(&authenticator),
    );
    let rest = RestServer::new(
        Arc::clone(&instance) as Arc<dyn GatewayInstance>,
        Arc::clone(&authenticator),
        config.otlp_config(),
    );

    let mut pg_task = tokio::spawn(async move { pg.serve(pg_listener).await });
    let mut rest_task = tokio::spawn(async move { rest.serve(rest_listener).await });

    // MCP is opt-in. When disabled, a never-resolving placeholder keeps the
    // select! arm uniform (same JoinHandle<io::Result<()>> type as pg/rest).
    let mut mcp_task = if config.mcp_enabled {
        let (mcp_listener, mcp_addr) = McpServer::bind(&config.mcp_listen).await?;
        tracing::info!(%mcp_addr, "listening (mcp)");
        let mcp = McpServer::new(
            Arc::clone(&instance) as Arc<dyn GatewayInstance>,
            Arc::clone(&authenticator),
        );
        tokio::spawn(async move { mcp.serve(mcp_listener).await })
    } else {
        tokio::spawn(async move { std::future::pending::<std::io::Result<()>>().await })
    };

    // Run until a frontend exits (an error) or a shutdown signal arrives.
    tokio::select! {
        res = &mut pg_task => {
            tracing::error!(?res, "postgres frontend exited");
        }
        res = &mut rest_task => {
            tracing::error!(?res, "rest frontend exited");
        }
        res = &mut mcp_task => {
            tracing::error!(?res, "mcp frontend exited");
        }
        _ = shutdown_signal() => {
            tracing::info!("shutdown signal received, stopping");
            pg_task.abort();
            rest_task.abort();
            mcp_task.abort();
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
    // Uses a shared proxy account; the principal is carried through the
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

    // Connection recovery: a manager owns the shared connection and, on death,
    // rebuilds it (bounded) and hot-swaps it into FlussDatafusion (SQL path) while
    // the backend reads the live connection from the manager (direct path).
    let fluss_df_for_swap = Arc::clone(&fluss_df);
    let conn_manager = Arc::new(ConnectionManager::new(
        Arc::clone(&connection),
        build_fluss_config(&ClusterConfig {
            bootstrap_servers: config.bootstrap_servers.clone(),
        }),
        Box::new(move |new| {
            fluss_df_for_swap
                .swap_connection(Arc::clone(new))
                .map_err(|e| GatewayError::Backend(format!("swap_connection: {e}")))
        }),
    ));

    let pg_provider = PgSqlEnvironmentProvider::new(
        Arc::new(FlussDatafusionCatalogInstaller::new(fluss_df)),
        Arc::new(StubPgCatalogOverlayInstaller),
    );
    let mut sql_environments = SqlEnvironmentRegistry::new();
    sql_environments.register(SqlEnvironmentId("postgres".into()), Arc::new(pg_provider));

    // Direct path: a backend that reads the live connection from the manager.
    let backend = Arc::new(FlussBackendFacade::new(Arc::clone(&conn_manager)));
    let sessions = Arc::new(SessionManager::new(SessionManagerConfig::default()));

    Ok(Arc::new(
        GatewayInstanceImpl::new(sessions, backend, Arc::new(sql_environments))
            .with_recovery(conn_manager),
    ))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_table_ref_accepts_database_dot_table() {
        let table = parse_table_ref("GATEWAY_OTLP_LOGS_TABLE", "obs.logs").unwrap();
        assert_eq!(table.database, "obs");
        assert_eq!(table.table, "logs");
    }

    #[test]
    fn parse_table_ref_rejects_invalid_shapes() {
        for raw in ["logs", ".logs", "obs.", "obs.logs.more"] {
            let err = parse_table_ref("GATEWAY_OTLP_LOGS_TABLE", raw).unwrap_err();
            assert!(matches!(err, GatewayError::InvalidArgument(_)));
        }
    }

    #[test]
    fn validate_otlp_requires_all_three_tables_when_enabled() {
        let cfg = GatewayConfig {
            bootstrap_servers: "127.0.0.1:9123".into(),
            pg_listen: "0.0.0.0:5432".into(),
            rest_listen: "0.0.0.0:8080".into(),
            mcp_enabled: false,
            mcp_listen: "0.0.0.0:8000".into(),
            cluster: "default".into(),
            config_path: None,
            users_env: None,
            otlp_enabled: true,
            otlp_logs_table: Some(TableRef {
                database: "obs".into(),
                table: "logs".into(),
            }),
            otlp_metrics_table: None,
            otlp_traces_table: Some(TableRef {
                database: "obs".into(),
                table: "traces".into(),
            }),
        };
        let err = cfg.validate_otlp().unwrap_err();
        assert!(matches!(err, GatewayError::InvalidArgument(_)));
        assert!(err.to_string().contains("GATEWAY_OTLP_METRICS_TABLE"));
    }
}
