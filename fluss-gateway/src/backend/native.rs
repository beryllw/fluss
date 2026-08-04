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

//! Native [`GatewayBackend`] over `fluss-rs`.
//!
//! Owns one long-lived [`FlussConnection`] for the process lifetime. Connection setup, cluster health, error
//! mapping, and shutdown are implemented here. The catalog operations report an unsupported operation until the
//! metadata and DDL endpoints land; the data-plane operations delegate to
//! the crate-private `native_write` and `native_lookup` modules.

use crate::application::ddl::{
    AlterTableRequest, CreateDatabaseRequest, CreateTableRequest, PartitionMutationRequest,
};
use crate::backend::GatewayBackend;
use crate::backend::model::{
    ClusterHealthReport, ClusterStatus, DatabaseDescription, LookupKey, LookupOutcome,
    PartitionDescription, PrefixLookupOutcome, PrefixLookupRequest, PreparedWriteRequest,
    TableDescription, TableRef, WriteResult,
};
use crate::config::{ClusterConfig, LookupConfig};
use crate::error::GatewayError;
use async_trait::async_trait;
use fluss::client::{FlussAdmin, FlussConnection};
use fluss::error::{Error as FlussClientError, FlussError};
use fluss::metadata::ClusterHealthStatus;
use std::sync::Arc;
use std::time::Duration;

/// Native backend implementation over the Fluss Rust client.
pub struct NativeGatewayBackend {
    connection: Arc<FlussConnection>,
    /// Bound on concurrently in-flight native lookups per request.
    lookup_concurrency: usize,
}

impl NativeGatewayBackend {
    /// Connects to one entry from `[clusters.<id>]`. Fails as unavailable when the bootstrap servers cannot be
    /// reached.
    pub async fn connect(
        cluster: &ClusterConfig,
        lookup: &LookupConfig,
    ) -> Result<Self, GatewayError> {
        let connection = FlussConnection::new_with_request_timeout(
            client_config(cluster, lookup),
            cluster.request_timeout.get(),
        )
        .await
        .map_err(|e| map_fluss_error("connect to Fluss", e))?;
        Ok(Self {
            connection: Arc::new(connection),
            lookup_concurrency: lookup.max_concurrent.max(1) as usize,
        })
    }

    /// Closes the underlying connection, bounding the drain by `timeout`. Idempotent, called from lifecycle
    /// shutdown.
    pub async fn close(&self, timeout: Duration) -> Result<(), GatewayError> {
        self.connection
            .close(timeout)
            .await
            .map_err(|e| map_fluss_error("close Fluss connection", e))
    }

    fn admin(&self) -> Result<Arc<FlussAdmin>, GatewayError> {
        self.connection
            .get_admin()
            .map_err(|e| map_fluss_error("create admin client", e))
    }
}

#[async_trait]
impl GatewayBackend for NativeGatewayBackend {
    async fn list_databases(&self) -> Result<Vec<String>, GatewayError> {
        Err(unimplemented_catalog("list databases"))
    }

    async fn describe_database(
        &self,
        _database: &str,
    ) -> Result<DatabaseDescription, GatewayError> {
        Err(unimplemented_catalog("describe a database"))
    }

    async fn create_database(&self, _request: &CreateDatabaseRequest) -> Result<(), GatewayError> {
        Err(unimplemented_catalog("create a database"))
    }

    async fn drop_database(&self, _database: &str) -> Result<(), GatewayError> {
        Err(unimplemented_catalog("drop a database"))
    }

    async fn list_tables(&self, _database: &str) -> Result<Vec<String>, GatewayError> {
        Err(unimplemented_catalog("list tables"))
    }

    async fn describe_table(
        &self,
        _table: &TableRef,
    ) -> Result<Arc<TableDescription>, GatewayError> {
        Err(unimplemented_catalog("describe a table"))
    }

    async fn create_table(&self, _request: &CreateTableRequest) -> Result<(), GatewayError> {
        Err(unimplemented_catalog("create a table"))
    }

    async fn alter_table(&self, _request: &AlterTableRequest) -> Result<(), GatewayError> {
        Err(unimplemented_catalog("alter a table"))
    }

    async fn drop_table(&self, _table: &TableRef) -> Result<(), GatewayError> {
        Err(unimplemented_catalog("drop a table"))
    }

    async fn list_partitions(
        &self,
        _table: &TableRef,
    ) -> Result<Vec<PartitionDescription>, GatewayError> {
        Err(unimplemented_catalog("list partitions"))
    }

    async fn create_partition(
        &self,
        _request: &PartitionMutationRequest,
    ) -> Result<(), GatewayError> {
        Err(unimplemented_catalog("create a partition"))
    }

    async fn drop_partition(
        &self,
        _request: &PartitionMutationRequest,
    ) -> Result<(), GatewayError> {
        Err(unimplemented_catalog("drop a partition"))
    }

    async fn lookup(
        &self,
        table: &TableRef,
        keys: Vec<LookupKey>,
    ) -> Result<Vec<LookupOutcome>, GatewayError> {
        crate::backend::native_lookup::lookup(
            &self.connection,
            table,
            keys,
            self.lookup_concurrency,
        )
        .await
    }

    async fn prefix_lookup(
        &self,
        table: &TableRef,
        request: PrefixLookupRequest,
    ) -> Result<Vec<PrefixLookupOutcome>, GatewayError> {
        crate::backend::native_lookup::prefix_lookup(
            &self.connection,
            table,
            request,
            self.lookup_concurrency,
        )
        .await
    }

    async fn write(&self, request: PreparedWriteRequest) -> Result<WriteResult, GatewayError> {
        crate::backend::native_write::execute(&self.connection, request).await
    }

    /// Reports cluster health, tolerating servers without the health RPC.
    ///
    /// Released Fluss servers up to 0.9.x do not implement `GetClusterHealth` and answer with an
    /// unsupported-api-version error even though every data RPC works. In that case reachability is established
    /// with the cheap `get_server_nodes` call and the health status is reported as `UNKNOWN` instead of failing
    /// readiness. Genuine transport failures still surface as errors.
    async fn cluster_health(&self) -> Result<ClusterHealthReport, GatewayError> {
        let admin = self.admin()?;
        let health = match admin.get_cluster_health().await {
            Ok(health) => health,
            Err(error) if is_unsupported_api(&error) => {
                log::debug!(
                    "server lacks the cluster health RPC, probing reachability instead: {error}"
                );
                admin
                    .get_server_nodes()
                    .await
                    .map_err(|e| map_fluss_error("probe server nodes", e))?;
                return Ok(ClusterHealthReport {
                    status: ClusterStatus::Unknown,
                    num_replicas: 0,
                    in_sync_replicas: 0,
                    num_leader_replicas: 0,
                    active_leader_replicas: 0,
                });
            }
            Err(error) => return Err(map_fluss_error("get cluster health", error)),
        };
        Ok(ClusterHealthReport {
            status: match health.status {
                ClusterHealthStatus::Green => ClusterStatus::Green,
                ClusterHealthStatus::Yellow => ClusterStatus::Yellow,
                ClusterHealthStatus::Red => ClusterStatus::Red,
                ClusterHealthStatus::Unknown => ClusterStatus::Unknown,
            },
            num_replicas: health.num_replicas,
            in_sync_replicas: health.in_sync_replicas,
            num_leader_replicas: health.num_leader_replicas,
            active_leader_replicas: health.active_leader_replicas,
        })
    }

    async fn close(&self, timeout: Duration) -> Result<(), GatewayError> {
        NativeGatewayBackend::close(self, timeout).await
    }
}

/// Placeholder failure for the catalog operations that are not wired to the client yet.
fn unimplemented_catalog(operation: &str) -> GatewayError {
    GatewayError::unsupported(format!(
        "the gateway cannot {operation} yet: the catalog backend is not implemented"
    ))
}

/// True when the server rejected an RPC because it does not know the API or its version, as older releases do for
/// newer admin calls.
fn is_unsupported_api(error: &FlussClientError) -> bool {
    matches!(
        error,
        FlussClientError::UnsupportedVersion { .. } | FlussClientError::UnsupportedOperation { .. }
    )
}

/// Builds the native client configuration for one cluster.
///
/// The write invariants are pinned by the gateway and are not operator-configurable: idempotent writer sessions,
/// `acks=all`, and a short batch timeout. Retries are effectively unbounded because the per-entry delivery
/// deadline, not an attempt count, is what terminates a write.
fn client_config(cluster: &ClusterConfig, lookup: &LookupConfig) -> fluss::config::Config {
    fluss::config::Config {
        bootstrap_servers: cluster.bootstrap_servers.join(","),
        connect_timeout_ms: duration_millis(cluster.connect_timeout.get()),
        lookup_queue_size: lookup.queue_size as usize,
        lookup_max_retries: lookup.max_retries as i32,
        writer_enable_idempotence: true,
        writer_acks: "all".to_string(),
        writer_retries: i32::MAX,
        writer_batch_timeout_ms: 10,
        ..fluss::config::Config::default()
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Maps a `fluss-rs` client error onto the gateway taxonomy.
///
/// Client-safe messages only: for infrastructure failures the detail (which may contain addresses) goes to the
/// log, not the envelope. The native `is_retriable()` verdict is recorded on every mapped error so the REST
/// envelope's `retryable` field reflects what the client actually knows.
pub(crate) fn map_fluss_error(context: &str, error: FlussClientError) -> GatewayError {
    let retriable = error.is_retriable();
    map_fluss_error_kind(context, error).with_retryable(retriable)
}

fn map_fluss_error_kind(context: &str, error: FlussClientError) -> GatewayError {
    if let Some(api_error) = error.api_error() {
        match api_error {
            FlussError::DatabaseNotExist => {
                return GatewayError::not_found("the requested database does not exist");
            }
            FlussError::TableNotExist | FlussError::UnknownTableOrBucketException => {
                return GatewayError::not_found("the requested table does not exist");
            }
            FlussError::SchemaNotExist => {
                return GatewayError::not_found("the requested schema does not exist");
            }
            FlussError::PartitionNotExists => {
                return GatewayError::not_found("the requested partition does not exist");
            }
            FlussError::DatabaseAlreadyExist => {
                return GatewayError::already_exists("the database already exists");
            }
            FlussError::TableAlreadyExist => {
                return GatewayError::already_exists("the table already exists");
            }
            FlussError::PartitionAlreadyExists => {
                return GatewayError::already_exists("the partition already exists");
            }
            FlussError::DatabaseNotEmpty => {
                return GatewayError::failed_precondition("the database is not empty");
            }
            FlussError::InvalidTableException
            | FlussError::InvalidDatabaseException
            | FlussError::TableNotPartitionedException
            | FlussError::NonPrimaryKeyTableException
            | FlussError::PartitionSpecInvalidException
            | FlussError::InvalidTimestampException
            | FlussError::InvalidColumnProjection
            | FlussError::InvalidConfigException => {
                return GatewayError::invalid_argument(format!(
                    "Fluss rejected the request while trying to {context}"
                ));
            }
            FlussError::RequestTimeOut => {
                return GatewayError::deadline_exceeded(format!(
                    "Fluss did not answer in time while trying to {context}"
                ));
            }
            _ => {}
        }
    }

    match &error {
        FlussClientError::UnsupportedOperation { .. }
        | FlussClientError::UnsupportedVersion { .. } => {
            GatewayError::unsupported(format!("{error}"))
        }
        FlussClientError::IllegalArgument { .. } => GatewayError::invalid_argument(format!(
            "Fluss rejected the request while trying to {context}"
        )),
        _ if error.is_retriable() => {
            log::warn!("Fluss backend temporarily unavailable while trying to {context}: {error}");
            GatewayError::unavailable(format!("Fluss is unavailable while trying to {context}"))
        }
        _ => {
            log::error!("Fluss request failed while trying to {context}: {error}");
            GatewayError::internal(format!("the gateway failed to {context}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigDuration;
    use crate::error::ErrorKind;

    #[test]
    fn pinned_write_invariants_are_not_configurable() {
        let cluster = ClusterConfig {
            bootstrap_servers: vec!["a:9123".to_string(), "b:9123".to_string()],
            connect_timeout: ConfigDuration::from_secs(7),
            request_timeout: ConfigDuration::from_secs(9),
        };
        let lookup = LookupConfig::default();
        let config = client_config(&cluster, &lookup);

        assert_eq!(config.bootstrap_servers, "a:9123,b:9123");
        assert_eq!(config.connect_timeout_ms, 7_000);
        assert!(config.writer_enable_idempotence);
        assert_eq!(config.writer_acks, "all");
        assert_eq!(config.lookup_queue_size, lookup.queue_size as usize);
        assert_eq!(config.lookup_max_retries, lookup.max_retries as i32);
    }

    #[test]
    fn catalog_operations_report_unsupported_rather_than_failing_internally() {
        let error = unimplemented_catalog("list databases");
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert!(!error.retryable());
    }
}
