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
//! Owns one long-lived [`FlussConnection`] for the process lifetime. Connection setup, the catalog half of the
//! trait, cluster health, error mapping, and shutdown are implemented here; the data-plane operations delegate to
//! the crate-private `native_write` and `native_lookup` modules.
//!
//! Every catalog call goes through `FlussAdmin`, which is obtained from the connection per request rather than
//! cached, because the admin client is a thin handle over the shared RPC client. Nothing in this module outlives a
//! request except the connection itself.

use crate::backend::GatewayBackend;
use crate::backend::model::{
    AlterTableRequest, CreateDatabaseRequest, CreateTableRequest, PartitionMutationRequest,
    TableChange,
};
use crate::backend::model::{
    ClusterHealthReport, ClusterStatus, ColumnDescription, DatabaseDescription, LookupKey,
    LookupOutcome, PartitionDescription, PrefixLookupOutcome, PrefixLookupRequest,
    PreparedWriteRequest, TableCapabilities, TableDescription, TableKind, TableRef, WriteResult,
};
use crate::config::{ClusterConfig, LookupConfig};
use crate::error::{ErrorKind, GatewayError};
use async_trait::async_trait;
use fluss::client::{FlussAdmin, FlussConnection};
use fluss::error::{Error as FlussClientError, FlussError};
use fluss::metadata::{
    AddColumn, AlterConfig, AlterConfigOpType, AlterTableChanges, ClusterHealthStatus,
    ColumnPositionType, DatabaseDescriptor, JsonSerde, PartitionSpec, Schema, TableDescriptor,
    TableInfo, TablePath,
};
use fluss::record::to_arrow_schema;
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
        Self::connect_with(client_config(cluster, lookup), cluster, lookup)
            .await
            .map_err(|e| map_fluss_error("connect to Fluss", e))
    }

    /// Connects acting as `act_as`: the connection authenticates with the service account and
    /// carries the principal name as the SASL authorization id, so Fluss authorizes every call
    /// on it as the impersonated end user (FIP-49 user identity mode).
    pub async fn connect_as(
        cluster: &ClusterConfig,
        lookup: &LookupConfig,
        act_as: &str,
    ) -> Result<Self, GatewayError> {
        let mut config = client_config(cluster, lookup);
        config.security_sasl_authorization_id = act_as.to_string();
        Self::connect_with(config, cluster, lookup)
            .await
            .map_err(|error| map_act_as_connect_error(act_as, error))
    }

    async fn connect_with(
        config: fluss::config::Config,
        cluster: &ClusterConfig,
        lookup: &LookupConfig,
    ) -> Result<Self, FlussClientError> {
        let connection =
            FlussConnection::new_with_request_timeout(config, cluster.request_timeout.get())
                .await?;
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
        self.admin()?
            .list_databases()
            .await
            .map_err(|e| map_fluss_error("list databases", e))
    }

    async fn describe_database(&self, database: &str) -> Result<DatabaseDescription, GatewayError> {
        let info = self
            .admin()?
            .get_database_info(database)
            .await
            .map_err(|error| {
                map_fluss_resource_error(
                    "describe database",
                    error,
                    ErrorResources::database(database),
                )
            })?;
        Ok(DatabaseDescription {
            name: info.database_name().to_string(),
            comment: info.database_descriptor().comment().map(str::to_string),
            custom_properties: info.database_descriptor().custom_properties().clone(),
            created_time: info.created_time(),
            modified_time: info.modified_time(),
        })
    }

    /// Creates the database, never tolerating an existing one, so a conflict surfaces as `already_exists`.
    async fn create_database(&self, request: &CreateDatabaseRequest) -> Result<(), GatewayError> {
        let mut builder =
            DatabaseDescriptor::builder().custom_properties(request.custom_properties.clone());
        if let Some(comment) = &request.comment {
            builder = builder.comment(comment);
        }
        self.admin()?
            .create_database(&request.name, Some(&builder.build()), false)
            .await
            .map_err(|error| {
                map_fluss_resource_error(
                    "create database",
                    error,
                    ErrorResources::database(&request.name),
                )
            })
    }

    /// Drops the database without cascade, so a non-empty database is rejected as `failed_precondition` rather
    /// than silently dropping its tables.
    async fn drop_database(&self, database: &str) -> Result<(), GatewayError> {
        self.admin()?
            .drop_database(database, false, false)
            .await
            .map_err(|error| {
                map_fluss_resource_error("drop database", error, ErrorResources::database(database))
            })
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>, GatewayError> {
        self.admin()?.list_tables(database).await.map_err(|error| {
            map_fluss_resource_error("list tables", error, ErrorResources::database(database))
        })
    }

    async fn describe_table(
        &self,
        table: &TableRef,
    ) -> Result<Arc<TableDescription>, GatewayError> {
        let info = self
            .admin()?
            .get_table_info(&table_path(table))
            .await
            .map_err(|error| {
                map_fluss_resource_error("describe table", error, ErrorResources::table(table))
            })?;
        Ok(Arc::new(to_table_description(table.clone(), &info)?))
    }

    async fn create_table(&self, request: &CreateTableRequest) -> Result<(), GatewayError> {
        let mut schema_builder = Schema::builder();
        for column in &request.columns {
            schema_builder = schema_builder.column(
                &column.name,
                fluss::metadata::DataType::try_from(&column.data_type)?,
            );
            if let Some(comment) = &column.comment {
                schema_builder = schema_builder.with_comment(comment);
            }
        }
        if !request.primary_key.is_empty() {
            schema_builder = schema_builder.primary_key(request.primary_key.clone());
        }
        let schema = schema_builder
            .build()
            .map_err(|error| map_fluss_error("validate table schema", error))?;
        let mut descriptor = TableDescriptor::builder()
            .schema(schema)
            .properties(request.configs.clone())
            .custom_properties(request.custom_properties.clone())
            .partitioned_by(request.partitioned_by.clone());
        if let Some(distribution) = &request.distribution {
            descriptor = descriptor.distributed_by(
                Some(distribution.bucket_count),
                distribution.bucket_keys.clone(),
            );
        }
        if let Some(comment) = &request.comment {
            descriptor = descriptor.comment(comment);
        }
        let descriptor = descriptor
            .build()
            .map_err(|error| map_fluss_error("validate table definition", error))?;
        self.admin()?
            .create_table(&table_path(&request.table), &descriptor, false)
            .await
            .map_err(|error| {
                map_fluss_resource_error(
                    "create table",
                    error,
                    ErrorResources::table(&request.table),
                )
            })
    }

    /// Translates every requested change into one native [`AlterTableChanges`] batch so the server applies them
    /// atomically. Column additions are always appended last, which is the only position the server accepts today.
    async fn alter_table(&self, request: &AlterTableRequest) -> Result<(), GatewayError> {
        let mut changes = AlterTableChanges::default();
        for change in &request.changes {
            match change {
                TableChange::AddColumn(column) => {
                    let data_type = fluss::metadata::DataType::try_from(&column.data_type)?;
                    let data_type_json = serde_json::to_vec(
                        &data_type
                            .serialize_json()
                            .map_err(|error| map_fluss_error("serialize column type", error))?,
                    )
                    .map_err(|error| {
                        log::error!("failed to serialize native column type: {error}");
                        GatewayError::internal("the gateway failed to encode the new column type")
                    })?;
                    changes.add_columns.push(AddColumn {
                        column_name: column.name.clone(),
                        data_type_json,
                        comment: column.comment.clone(),
                        position: ColumnPositionType::Last,
                    });
                }
                TableChange::SetConfig { key, value } => {
                    changes.config_changes.push(AlterConfig::new(
                        key,
                        Some(value.clone()),
                        AlterConfigOpType::Set,
                    ));
                }
                TableChange::ResetConfig { key } => {
                    changes.config_changes.push(AlterConfig::new(
                        key,
                        None,
                        AlterConfigOpType::Delete,
                    ));
                }
            }
        }
        self.admin()?
            .alter_table(&table_path(&request.table), false, changes)
            .await
            .map_err(|error| {
                map_fluss_resource_error(
                    "alter table",
                    error,
                    ErrorResources::table(&request.table),
                )
            })
    }

    async fn drop_table(&self, table: &TableRef) -> Result<(), GatewayError> {
        self.admin()?
            .drop_table(&table_path(table), false)
            .await
            .map_err(|error| {
                map_fluss_resource_error("drop table", error, ErrorResources::table(table))
            })
    }

    async fn list_partitions(
        &self,
        table: &TableRef,
    ) -> Result<Vec<PartitionDescription>, GatewayError> {
        let partitions = self
            .admin()?
            .list_partition_infos(&table_path(table))
            .await
            .map_err(|error| {
                map_fluss_resource_error("list partitions", error, ErrorResources::table(table))
            })?;
        Ok(partitions
            .iter()
            .map(|partition| {
                let spec = partition.get_resolved_partition_spec();
                PartitionDescription {
                    partition_id: partition.get_partition_id(),
                    partition_name: partition.get_partition_name(),
                    spec: spec
                        .get_partition_keys()
                        .iter()
                        .cloned()
                        .zip(spec.get_partition_values().iter().cloned())
                        .collect(),
                }
            })
            .collect())
    }

    async fn create_partition(
        &self,
        request: &PartitionMutationRequest,
    ) -> Result<(), GatewayError> {
        let spec = partition_spec(request);
        self.admin()?
            .create_partition(&table_path(&request.table), &spec, false)
            .await
            .map_err(|error| {
                map_fluss_resource_error(
                    "create partition",
                    error,
                    ErrorResources::partition(request),
                )
            })
    }

    async fn drop_partition(&self, request: &PartitionMutationRequest) -> Result<(), GatewayError> {
        let spec = partition_spec(request);
        self.admin()?
            .drop_partition(&table_path(&request.table), &spec, false)
            .await
            .map_err(|error| {
                map_fluss_resource_error(
                    "drop partition",
                    error,
                    ErrorResources::partition(request),
                )
            })
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

/// Builds the native table path for one gateway table reference.
fn table_path(table: &TableRef) -> TablePath {
    TablePath::new(table.database.clone(), table.table.clone())
}

/// Builds the native partition spec from an already validated, ordered request spec.
///
/// The native type is a map, so request order is not carried through it. Ordering against the table's declared
/// partition keys is enforced before dispatch by `crate::backend::model::validate_partition_spec`.
fn partition_spec(request: &PartitionMutationRequest) -> PartitionSpec {
    PartitionSpec::new(
        request
            .spec
            .iter()
            .map(|entry| (entry.key.clone(), entry.value.clone()))
            .collect::<std::collections::HashMap<String, String>>(),
    )
}

/// Converts a `fluss-rs` [`TableInfo`] into the HTTP-independent description, deriving the table kind and the
/// lookup capabilities.
///
/// Capability derivation mirrors what the native client will actually accept, so the REST layer never rejects an
/// operation the client would have run and never dispatches one it would have refused:
///
/// - exact lookup needs a primary key;
/// - prefix lookup additionally needs non-empty bucket keys that form a prefix of the physical primary key, which
///   is precisely the client's own `validate_prefix_lookup` contract; only then does a prefix covering the bucket
///   keys route to a single bucket.
fn to_table_description(
    table: TableRef,
    info: &TableInfo,
) -> Result<TableDescription, GatewayError> {
    let columns = info
        .schema
        .columns()
        .iter()
        .map(|column| {
            Ok(ColumnDescription {
                name: column.name().to_string(),
                data_type: crate::backend::types::DataType::try_from(column.data_type())?,
                comment: column.comment().map(str::to_string),
            })
        })
        .collect::<Result<Vec<_>, GatewayError>>()?;

    let kind = if info.primary_keys.is_empty() {
        TableKind::Log
    } else {
        TableKind::PrimaryKey
    };

    let (kv_format, log_format) = match kind {
        TableKind::PrimaryKey => {
            let kv_format = info.table_config.get_kv_format().map_err(|e| {
                log::error!("invalid kv format for table `{table}`: {e}");
                GatewayError::internal(format!("table `{table}` declares an unusable kv format"))
            })?;
            (Some(kv_format.to_string()), None)
        }
        TableKind::Log => {
            let log_format = info.table_config.get_log_format().map_err(|e| {
                log::error!("invalid log format for table `{table}`: {e}");
                GatewayError::internal(format!("table `{table}` declares an unusable log format"))
            })?;
            (None, Some(log_format.to_string()))
        }
    };

    let is_primary_key = kind == TableKind::PrimaryKey;
    let capabilities = TableCapabilities {
        exact_lookup_supported: is_primary_key,
        prefix_lookup_supported: is_primary_key
            && !info.bucket_keys.is_empty()
            && info.physical_primary_keys.starts_with(&info.bucket_keys),
    };

    let arrow_schema = to_arrow_schema(info.row_type()).map_err(|e| {
        log::error!("cannot derive the Arrow schema for table `{table}`: {e}");
        GatewayError::internal(format!(
            "the gateway cannot represent the schema of table `{table}`"
        ))
    })?;

    Ok(TableDescription {
        table,
        table_id: info.table_id,
        schema_id: info.schema_id,
        kind,
        columns,
        primary_keys: info.primary_keys.clone(),
        physical_primary_keys: info.physical_primary_keys.clone(),
        bucket_keys: info.bucket_keys.clone(),
        partition_keys: info.partition_keys.to_vec(),
        auto_increment_columns: info.schema.auto_increment_col_names().clone(),
        num_buckets: info.num_buckets,
        log_format,
        kv_format,
        comment: info.comment.clone(),
        properties: info.properties.clone(),
        custom_properties: info.custom_properties.clone(),
        created_time: info.created_time,
        modified_time: info.modified_time,
        capabilities,
        arrow_schema,
    })
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
    let mut config = fluss::config::Config {
        bootstrap_servers: cluster.bootstrap_servers.join(","),
        connect_timeout_ms: duration_millis(cluster.connect_timeout.get()),
        lookup_queue_size: lookup.queue_size as usize,
        lookup_max_retries: lookup.max_retries as i32,
        writer_enable_idempotence: true,
        writer_acks: "all".to_string(),
        writer_retries: i32::MAX,
        writer_batch_timeout_ms: 10,
        ..fluss::config::Config::default()
    };
    // Service credentials switch the connection to SASL/PLAIN with the account acting as
    // itself — the super-user non-propagating transition mode. The authorization id stays
    // empty here; per-user act-as connections install it per identity.
    if let (Some(account), Some(password)) = (&cluster.service_account, &cluster.service_password) {
        config.security_protocol = "sasl".to_string();
        config.security_sasl_username = account.clone();
        config.security_sasl_password = password.expose().to_string();
    }
    config
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The resource names a single catalog call could possibly be talking about.
///
/// Populated by the caller, which is the only place that knows which database, table, or partition the request
/// named. The mapper picks at most one of them based on the native error code, so a not-found answer never
/// attributes itself to the wrong resource.
#[derive(Default)]
struct ErrorResources {
    database: Option<String>,
    table: Option<String>,
    partition: Option<String>,
}

impl ErrorResources {
    fn database(database: &str) -> Self {
        Self {
            database: Some(database.to_string()),
            ..Self::default()
        }
    }

    fn table(table: &TableRef) -> Self {
        Self {
            database: Some(table.database.clone()),
            table: Some(table.to_string()),
            ..Self::default()
        }
    }

    /// Builds the `database.table/value1$value2` partition name, matching what the application layer attaches for
    /// the same partition so both sources of error details agree.
    fn partition(request: &PartitionMutationRequest) -> Self {
        let partition_name = request
            .spec
            .iter()
            .map(|entry| entry.value.as_str())
            .collect::<Vec<_>>()
            .join("$");
        Self {
            database: Some(request.table.database.clone()),
            table: Some(request.table.to_string()),
            partition: Some(format!("{}/{partition_name}", request.table)),
        }
    }
}

/// Maps a native error and attaches the resource the caller named, when the error is about a resource at all.
fn map_fluss_resource_error(
    context: &str,
    error: FlussClientError,
    resources: ErrorResources,
) -> GatewayError {
    let api_error = error.api_error();
    let mapped = map_fluss_error(context, error);
    let resource = match api_error {
        Some(
            FlussError::DatabaseNotExist
            | FlussError::DatabaseAlreadyExist
            | FlussError::DatabaseNotEmpty,
        ) => resources.database.map(|name| ("database", name)),
        Some(
            FlussError::TableNotExist
            | FlussError::TableAlreadyExist
            | FlussError::SchemaNotExist
            | FlussError::UnknownTableOrBucketException,
        ) => resources.table.map(|name| ("table", name)),
        Some(FlussError::PartitionNotExists | FlussError::PartitionAlreadyExists) => {
            resources.partition.map(|name| ("partition", name))
        }
        _ => None,
    };
    match (mapped.kind(), resource) {
        (
            ErrorKind::NotFound | ErrorKind::AlreadyExists | ErrorKind::FailedPrecondition,
            Some((resource_kind, resource_name)),
        ) => mapped.with_resource(resource_kind, Some(resource_name)),
        _ => mapped,
    }
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

/// Maps a failure to dial an act-as connection (FIP-49 user identity mode).
///
/// A definitive authentication rejection here means the server refused to let the service
/// account act as this principal — the caller's identity is outside the server-side
/// impersonation allowlist — which is the caller's 403, not an internal gateway failure.
/// Every other dial failure keeps the shared connect mapping.
fn map_act_as_connect_error(act_as: &str, error: FlussClientError) -> GatewayError {
    if error.api_error() == Some(FlussError::AuthenticateException) {
        log::warn!("Fluss refused an act-as connection for principal {act_as:?}: {error}");
        return GatewayError::unauthorized(format!(
            "Fluss refused to authorize acting as `{act_as}`"
        ));
    }
    map_fluss_error("connect to Fluss", error)
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
    use crate::backend::model::PartitionSpecEntry;
    use crate::config::ConfigDuration;
    use crate::error::ErrorDetails;
    use fluss::metadata::{DataTypes, Schema, TableConfig};
    use std::collections::HashMap;

    /// A definitive authentication rejection on an act-as dial is the caller's 403 — the
    /// principal is outside the server-side impersonation allowlist — while every other dial
    /// failure keeps the shared connect mapping.
    #[test]
    fn act_as_authentication_rejection_maps_to_unauthorized() {
        let refused = map_act_as_connect_error(
            "carol",
            FlussClientError::FlussAPIError {
                api_error: FlussError::AuthenticateException
                    .to_api_error(Some("not authorized to impersonate".to_string())),
            },
        );
        assert_eq!(refused.kind(), ErrorKind::Unauthorized);
        assert!(!refused.retryable());
        assert!(refused.message().contains("carol"), "{refused:?}");

        // Any other dial failure stays on the shared connect mapping.
        let transient = map_act_as_connect_error(
            "carol",
            FlussClientError::FlussAPIError {
                api_error: FlussError::NetworkException.to_api_error(None),
            },
        );
        assert_eq!(transient.kind(), ErrorKind::Unavailable);
        assert!(transient.retryable());
    }

    /// Without service credentials the native client keeps today's plaintext connection.
    #[test]
    fn client_config_defaults_to_plaintext_without_service_credentials() {
        let cluster = ClusterConfig::default();
        let config = client_config(&cluster, &crate::config::LookupConfig::default());
        assert!(!config.is_sasl_enabled());
        assert!(config.security_sasl_username.is_empty());
        assert!(config.security_sasl_password.is_empty());
    }

    /// Configured service credentials turn on SASL/PLAIN with the account acting as itself:
    /// no authorization id is set, per the super-user non-propagating transition mode.
    #[test]
    fn client_config_enables_sasl_with_service_credentials() {
        let cluster = ClusterConfig {
            service_account: Some("gateway_svc".to_string()),
            service_password: Some(crate::auth::Secret::new("sup3r")),
            ..ClusterConfig::default()
        };
        let config = client_config(&cluster, &crate::config::LookupConfig::default());
        assert!(config.is_sasl_enabled());
        assert!(
            config.validate_security().is_ok(),
            "security config invalid"
        );
        assert_eq!(config.security_sasl_username, "gateway_svc");
        assert_eq!(config.security_sasl_password, "sup3r");
        assert!(config.security_sasl_authorization_id.is_empty());
    }

    /// Builds a `TableInfo` whose only interesting parts are the key layout and the table config, which is what
    /// `to_table_description` derives kind, formats, and capabilities from.
    fn table_info(
        primary_keys: &[&str],
        physical_primary_keys: &[&str],
        bucket_keys: &[&str],
        partition_keys: &[&str],
        properties: HashMap<String, String>,
    ) -> TableInfo {
        let schema = Schema::builder()
            .column("region", DataTypes::string())
            .with_comment("the sales region")
            .column("id", DataTypes::bigint())
            .column("amount", DataTypes::bigint())
            .build()
            .expect("schema builds");
        let row_type = schema.row_type().clone();
        TableInfo {
            table_path: TablePath::new("fluss".to_string(), "orders".to_string()),
            table_id: 42,
            schema_id: 7,
            schema,
            row_type,
            primary_keys: to_strings(primary_keys),
            physical_primary_keys: to_strings(physical_primary_keys),
            bucket_keys: to_strings(bucket_keys),
            partition_keys: to_strings(partition_keys).into(),
            num_buckets: 3,
            properties: properties.clone(),
            table_config: TableConfig::from_properties(properties),
            custom_properties: HashMap::new(),
            comment: Some("orders".to_string()),
            created_time: 111,
            modified_time: 222,
        }
    }

    fn to_strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn describe(
        primary_keys: &[&str],
        physical_primary_keys: &[&str],
        bucket_keys: &[&str],
        partition_keys: &[&str],
    ) -> TableDescription {
        to_table_description(
            TableRef::new("fluss", "orders"),
            &table_info(
                primary_keys,
                physical_primary_keys,
                bucket_keys,
                partition_keys,
                HashMap::new(),
            ),
        )
        .expect("description maps")
    }

    #[test]
    fn pinned_write_invariants_are_not_configurable() {
        let cluster = ClusterConfig {
            bootstrap_servers: vec!["a:9123".to_string(), "b:9123".to_string()],
            connect_timeout: ConfigDuration::from_secs(7),
            request_timeout: ConfigDuration::from_secs(9),
            ..ClusterConfig::default()
        };
        let lookup = LookupConfig::default();
        let config = client_config(&cluster, &lookup);

        assert_eq!(config.bootstrap_servers, "a:9123,b:9123");
        assert_eq!(config.connect_timeout_ms, 7_000);
        assert!(config.writer_enable_idempotence);
        assert_eq!(config.writer_acks, "all");
        assert_eq!(config.writer_retries, i32::MAX);
        assert_eq!(config.writer_batch_timeout_ms, 10);
        assert_eq!(config.lookup_queue_size, lookup.queue_size as usize);
        assert_eq!(config.lookup_max_retries, lookup.max_retries as i32);
    }

    #[test]
    fn maps_table_metadata_and_columns_verbatim() {
        let description = describe(&["region", "id"], &["id"], &["id"], &["region"]);

        assert_eq!(description.table, TableRef::new("fluss", "orders"));
        assert_eq!(description.table_id, 42);
        assert_eq!(description.schema_id, 7);
        assert_eq!(description.num_buckets, 3);
        assert_eq!(description.created_time, 111);
        assert_eq!(description.modified_time, 222);
        assert_eq!(description.comment.as_deref(), Some("orders"));
        assert_eq!(
            description
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["region", "id", "amount"]
        );
        assert_eq!(
            description.columns[0].comment.as_deref(),
            Some("the sales region")
        );
        assert_eq!(description.columns[1].comment, None);
        assert_eq!(
            description
                .arrow_schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["region", "id", "amount"]
        );
    }

    #[test]
    fn classifies_a_partitioned_primary_key_table() {
        let description = describe(&["region", "id"], &["id"], &["id"], &["region"]);

        assert_eq!(description.kind, TableKind::PrimaryKey);
        assert!(description.is_partitioned());
        assert_eq!(description.partition_keys, vec!["region".to_string()]);
        assert_eq!(description.kv_format.as_deref(), Some("COMPACTED"));
        assert_eq!(description.log_format, None);
        assert_eq!(
            description.capabilities,
            TableCapabilities {
                exact_lookup_supported: true,
                prefix_lookup_supported: true,
            }
        );
    }

    #[test]
    fn classifies_a_log_table_as_lookup_incapable() {
        let description = describe(&[], &[], &["id"], &[]);

        assert_eq!(description.kind, TableKind::Log);
        assert!(!description.is_partitioned());
        assert_eq!(description.log_format.as_deref(), Some("ARROW"));
        assert_eq!(description.kv_format, None);
        assert_eq!(
            description.capabilities,
            TableCapabilities {
                exact_lookup_supported: false,
                prefix_lookup_supported: false,
            }
        );
    }

    #[test]
    fn reads_the_declared_formats_rather_than_assuming_defaults() {
        let mut properties = HashMap::new();
        properties.insert("table.log.format".to_string(), "INDEXED".to_string());
        properties.insert("table.kv.format".to_string(), "INDEXED".to_string());

        let log = to_table_description(
            TableRef::new("fluss", "orders"),
            &table_info(&[], &[], &[], &[], properties.clone()),
        )
        .expect("log description maps");
        assert_eq!(log.log_format.as_deref(), Some("INDEXED"));

        let primary_key = to_table_description(
            TableRef::new("fluss", "orders"),
            &table_info(&["id"], &["id"], &["id"], &[], properties),
        )
        .expect("pk description maps");
        assert_eq!(primary_key.kv_format.as_deref(), Some("INDEXED"));
        // An INDEXED kv table is still exactly and prefix lookupable.
        assert!(primary_key.capabilities.prefix_lookup_supported);
    }

    /// The capability must predict `TablePrefixLookup::create_lookuper`, which requires the bucket keys to be a
    /// non-empty prefix of the physical primary keys. Reporting otherwise would make the REST layer refuse a
    /// working lookup or dispatch one the client rejects.
    #[test]
    fn prefix_lookup_needs_bucket_keys_that_prefix_the_physical_primary_key() {
        // Bucket keys are the second physical primary key column, not a prefix of it.
        let not_a_prefix = describe(&["a", "b"], &["a", "b"], &["b"], &[]);
        assert!(not_a_prefix.capabilities.exact_lookup_supported);
        assert!(!not_a_prefix.capabilities.prefix_lookup_supported);

        // No bucket keys at all: nothing to route a prefix to a single bucket with.
        let no_bucket_keys = describe(&["a", "b"], &["a", "b"], &[], &[]);
        assert!(no_bucket_keys.capabilities.exact_lookup_supported);
        assert!(!no_bucket_keys.capabilities.prefix_lookup_supported);

        // Bucket keys equal to the physical primary key are still a prefix, and the client accepts them.
        let whole_key = describe(&["a", "b"], &["a", "b"], &["a", "b"], &[]);
        assert!(whole_key.capabilities.prefix_lookup_supported);

        // A strict prefix is the ordinary case.
        let strict_prefix = describe(&["a", "b"], &["a", "b"], &["a"], &[]);
        assert!(strict_prefix.capabilities.prefix_lookup_supported);
    }

    #[test]
    fn builds_the_native_partition_spec_from_the_request_pairs() {
        let request = PartitionMutationRequest {
            table: TableRef::new("fluss", "orders"),
            spec: vec![
                PartitionSpecEntry {
                    key: "region".to_string(),
                    value: "eu".to_string(),
                },
                PartitionSpecEntry {
                    key: "day".to_string(),
                    value: "2026-08-04".to_string(),
                },
            ],
        };

        // The native spec is a map, so pairing rather than ordering is what survives the conversion.
        let spec = partition_spec(&request);
        let entries = spec.get_spec_map();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries.get("region"), Some(&"eu".to_string()));
        assert_eq!(entries.get("day"), Some(&"2026-08-04".to_string()));
    }

    #[test]
    fn table_not_found_carries_the_table_resource_without_leaking_detail() {
        let table = TableRef::new("fluss", "missing");
        let error = map_fluss_resource_error(
            "describe table",
            FlussClientError::table_not_exist("Table not found: fluss.missing at 10.0.0.7:9123"),
            ErrorResources::table(&table),
        );

        assert_eq!(error.kind(), ErrorKind::NotFound);
        assert_eq!(error.message(), "the requested table does not exist");
        assert!(!error.message().contains("10.0.0.7"));
        assert!(!error.retryable());
        assert_eq!(
            error.details(),
            Some(&ErrorDetails {
                resource_kind: Some("table".to_string()),
                resource_name: Some("fluss.missing".to_string()),
            })
        );
    }

    #[test]
    fn database_errors_keep_their_taxonomy_and_resource() {
        let already_exists = map_fluss_resource_error(
            "create database",
            FlussClientError::from(
                FlussError::DatabaseAlreadyExist
                    .to_api_error(Some("internal database response".to_string())),
            ),
            ErrorResources::database("fluss"),
        );
        assert_eq!(already_exists.kind(), ErrorKind::AlreadyExists);
        assert_eq!(already_exists.message(), "the database already exists");
        assert!(!already_exists.message().contains("internal"));
        assert_eq!(
            already_exists.details(),
            Some(&ErrorDetails {
                resource_kind: Some("database".to_string()),
                resource_name: Some("fluss".to_string()),
            })
        );

        let not_found = map_fluss_resource_error(
            "describe database",
            FlussClientError::from(FlussError::DatabaseNotExist.to_api_error(None)),
            ErrorResources::database("fluss"),
        );
        assert_eq!(not_found.kind(), ErrorKind::NotFound);
        assert_eq!(not_found.details(), already_exists.details());

        // A non-empty database is a state conflict, not a missing resource, and still names the database.
        let not_empty = map_fluss_resource_error(
            "drop database",
            FlussClientError::from(
                FlussError::DatabaseNotEmpty
                    .to_api_error(Some("table names must stay private".to_string())),
            ),
            ErrorResources::database("fluss"),
        );
        assert_eq!(not_empty.kind(), ErrorKind::FailedPrecondition);
        assert_eq!(not_empty.message(), "the database is not empty");
        assert!(!not_empty.message().contains("table names"));
        assert_eq!(not_empty.details(), already_exists.details());
    }

    #[test]
    fn partition_errors_name_the_qualified_partition() {
        let request = PartitionMutationRequest {
            table: TableRef::new("fluss", "orders"),
            spec: vec![
                PartitionSpecEntry {
                    key: "region".to_string(),
                    value: "eu".to_string(),
                },
                PartitionSpecEntry {
                    key: "day".to_string(),
                    value: "2026-08-04".to_string(),
                },
            ],
        };

        let already_exists = map_fluss_resource_error(
            "create partition",
            FlussClientError::from(FlussError::PartitionAlreadyExists.to_api_error(None)),
            ErrorResources::partition(&request),
        );
        assert_eq!(already_exists.kind(), ErrorKind::AlreadyExists);
        assert_eq!(
            already_exists.details(),
            Some(&ErrorDetails {
                resource_kind: Some("partition".to_string()),
                resource_name: Some("fluss.orders/eu$2026-08-04".to_string()),
            })
        );

        let not_found = map_fluss_resource_error(
            "drop partition",
            FlussClientError::from(FlussError::PartitionNotExists.to_api_error(None)),
            ErrorResources::partition(&request),
        );
        assert_eq!(not_found.kind(), ErrorKind::NotFound);
        assert_eq!(not_found.details(), already_exists.details());
    }

    #[test]
    fn non_resource_errors_carry_no_resource_details() {
        let invalid = map_fluss_resource_error(
            "create partition",
            FlussClientError::invalid_partition("bad spec"),
            ErrorResources::table(&TableRef::new("fluss", "orders")),
        );
        assert_eq!(invalid.kind(), ErrorKind::InvalidArgument);
        assert!(!invalid.retryable());
        assert_eq!(invalid.details(), None);

        let timed_out = map_fluss_resource_error(
            "describe table",
            FlussClientError::from(FlussError::RequestTimeOut.to_api_error(None)),
            ErrorResources::table(&TableRef::new("fluss", "orders")),
        );
        assert_eq!(timed_out.kind(), ErrorKind::DeadlineExceeded);
        assert_eq!(timed_out.details(), None);
    }

    #[test]
    fn retriable_transport_errors_map_to_unavailable_without_addresses() {
        let error = map_fluss_resource_error(
            "list tables",
            FlussClientError::leader_not_available("leader for bucket 0 on 10.0.0.7:9123"),
            ErrorResources::database("fluss"),
        );

        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert!(error.retryable());
        assert!(!error.message().contains("10.0.0.7"));
        assert_eq!(error.details(), None);
    }

    #[test]
    fn unclassified_errors_map_to_internal_without_detail() {
        let error = map_fluss_error(
            "describe table",
            FlussClientError::UnexpectedError {
                message: "socket at 10.0.0.9:9123 broke".to_string(),
                source: None,
            },
        );

        assert_eq!(error.kind(), ErrorKind::Internal);
        assert!(!error.retryable());
        assert!(!error.message().contains("10.0.0.9"));
    }

    #[test]
    fn unsupported_api_version_is_classified_for_the_health_fallback() {
        assert!(is_unsupported_api(&FlussClientError::UnsupportedVersion {
            message: "The server does not support GetClusterHealth".to_string(),
        }));
        assert!(is_unsupported_api(
            &FlussClientError::UnsupportedOperation {
                message: "GetClusterHealth".to_string(),
            }
        ));
        assert!(!is_unsupported_api(
            &FlussClientError::leader_not_available("leader moved")
        ));
    }
}
