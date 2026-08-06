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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Catalog reads, catalog mutation models, and their protocol-neutral validation.
//!
//! This module owns the metadata and DDL half of [`GatewayService`]. The models and validators below are
//! complete; the service methods in the inherent `impl` block at the end of the file report an unsupported
//! operation until the metadata and DDL endpoints are wired to the backend.

use crate::application::service::{cache_table, load_table, resource_error};
use crate::application::validate_table_schema;
use crate::application::{
    ClusterHealthReport, DataType, DatabaseDescription, GatewayService, InputColumn,
    PartitionDescription, RequestContext, TableDescription, TableRef, validate_data_type,
};
use crate::error::GatewayError;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Creates one database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabaseRequest {
    pub name: String,
    pub comment: Option<String>,
    pub custom_properties: HashMap<String, String>,
}

/// One column supplied by create or alter table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub comment: Option<String>,
}

/// Bucket distribution supplied by create table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDistributionDefinition {
    pub bucket_count: i32,
    pub bucket_keys: Vec<String>,
}

/// Creates one table using only user-owned metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableRequest {
    pub table: TableRef,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: Vec<String>,
    pub partitioned_by: Vec<String>,
    pub distribution: Option<TableDistributionDefinition>,
    pub configs: HashMap<String, String>,
    pub custom_properties: HashMap<String, String>,
    pub comment: Option<String>,
}

/// One supported table alteration. The containing vector preserves request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableChange {
    AddColumn(ColumnDefinition),
    SetConfig { key: String, value: String },
    ResetConfig { key: String },
}

/// Applies one ordered group of table alterations in a single native request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableRequest {
    pub table: TableRef,
    pub changes: Vec<TableChange>,
}

/// Ordered partition key and value supplied by a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSpecEntry {
    pub key: String,
    pub value: String,
}

/// Creates or identifies one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMutationRequest {
    pub table: TableRef,
    pub spec: Vec<PartitionSpecEntry>,
}

/// Validates one database creation before native dispatch.
pub fn validate_create_database(request: &CreateDatabaseRequest) -> Result<(), GatewayError> {
    validate_identifier("database name", &request.name)?;
    validate_properties("custom property", &request.custom_properties)
}

/// Validates a complete table creation before native dispatch.
pub fn validate_create_table(request: &CreateTableRequest) -> Result<(), GatewayError> {
    validate_identifier("database name", &request.table.database)?;
    validate_identifier("table name", &request.table.table)?;
    validate_table_schema(
        request
            .columns
            .iter()
            .map(|column| InputColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
            })
            .collect(),
        request.primary_key.clone(),
        request.partitioned_by.clone(),
    )?;
    for column in &request.columns {
        validate_creatable_data_type(&column.data_type)?;
    }

    if let Some(distribution) = &request.distribution {
        if distribution.bucket_count <= 0 {
            return Err(GatewayError::invalid_argument(
                "distribution bucket_count must be positive",
            ));
        }
        validate_existing_keys("bucket key", &distribution.bucket_keys, &request.columns)?;
    }
    validate_properties("table config", &request.configs)?;
    validate_properties("custom property", &request.custom_properties)
}

/// Validates every alteration before one native request is sent.
pub fn validate_alter_table(
    request: &AlterTableRequest,
    current: &TableDescription,
) -> Result<(), GatewayError> {
    if request.changes.is_empty() {
        return Err(GatewayError::invalid_argument(
            "changes must contain at least one alteration",
        ));
    }
    let mut column_names: HashSet<String> = current
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    for change in &request.changes {
        match change {
            TableChange::AddColumn(column) => {
                validate_identifier("column name", &column.name)?;
                validate_data_type(&column.data_type)?;
                validate_creatable_data_type(&column.data_type)?;
                if !column.data_type.nullable() {
                    return Err(GatewayError::invalid_argument(format!(
                        "added column `{}` must be nullable",
                        column.name
                    )));
                }
                if !column_names.insert(column.name.clone()) {
                    return Err(GatewayError::invalid_argument(format!(
                        "column `{}` already exists",
                        column.name
                    )));
                }
            }
            TableChange::SetConfig { key, value } => {
                validate_property("table config", key, value)?;
            }
            TableChange::ResetConfig { key } => {
                validate_property_key("table config", key)?;
            }
        }
    }
    Ok(())
}

/// Validates an exact, ordered partition specification against canonical table metadata.
pub fn validate_partition_spec(
    request: &PartitionMutationRequest,
    current: &TableDescription,
) -> Result<(), GatewayError> {
    if current.partition_keys.is_empty() {
        return Err(GatewayError::invalid_argument(format!(
            "table `{}` is not partitioned",
            request.table
        )));
    }
    if request.spec.len() != current.partition_keys.len() {
        return Err(GatewayError::invalid_argument(format!(
            "partition spec must contain exactly {} entries",
            current.partition_keys.len()
        )));
    }
    let mut seen = HashSet::with_capacity(request.spec.len());
    for (index, entry) in request.spec.iter().enumerate() {
        validate_identifier("partition key", &entry.key)?;
        if !seen.insert(entry.key.as_str()) {
            return Err(GatewayError::invalid_argument(format!(
                "duplicate partition key `{}`",
                entry.key
            )));
        }
        let expected = &current.partition_keys[index];
        if entry.key != *expected {
            return Err(GatewayError::invalid_argument(format!(
                "partition key at index {index} must be `{expected}`"
            )));
        }
    }
    Ok(())
}

/// Highest TIME precision the millisecond-of-day Fluss row representation can store.
const MAX_CREATABLE_TIME_PRECISION: u32 = 3;

/// Rejects declared types whose values the gateway write path could never decode.
///
/// Fluss stores TIME as milliseconds of day, so a declared precision above 3 promises
/// sub-millisecond values that every later write would reject. This check applies only to columns
/// created through the gateway. Existing tables keep their declared types and are never validated
/// through this path.
fn validate_creatable_data_type(data_type: &DataType) -> Result<(), GatewayError> {
    // Callers run validate_data_type or validate_table_schema first, which bounds type nesting.
    match data_type {
        DataType::Time { precision, .. } if *precision > MAX_CREATABLE_TIME_PRECISION => {
            Err(GatewayError::invalid_argument(format!(
                "TIME precision {precision} is not representable; gateway-created TIME columns \
                 support precision 0 to {MAX_CREATABLE_TIME_PRECISION}"
            )))
        }
        DataType::Array { element, .. } => validate_creatable_data_type(element),
        DataType::Map { key, value, .. } => {
            validate_creatable_data_type(key)?;
            validate_creatable_data_type(value)
        }
        DataType::Row { fields, .. } => fields
            .iter()
            .try_for_each(|field| validate_creatable_data_type(&field.data_type)),
        _ => Ok(()),
    }
}

fn validate_existing_keys(
    kind: &str,
    keys: &[String],
    columns: &[ColumnDefinition],
) -> Result<(), GatewayError> {
    let columns: HashSet<&str> = columns.iter().map(|column| column.name.as_str()).collect();
    let mut seen = HashSet::with_capacity(keys.len());
    for key in keys {
        if !seen.insert(key.as_str()) {
            return Err(GatewayError::invalid_argument(format!(
                "duplicate {kind} column `{key}`"
            )));
        }
        if !columns.contains(key.as_str()) {
            return Err(GatewayError::invalid_argument(format!(
                "{kind} column `{key}` does not exist"
            )));
        }
    }
    Ok(())
}

fn validate_properties(
    kind: &str,
    properties: &HashMap<String, String>,
) -> Result<(), GatewayError> {
    for (key, value) in properties {
        validate_property(kind, key, value)?;
    }
    Ok(())
}

fn validate_property(kind: &str, key: &str, value: &str) -> Result<(), GatewayError> {
    validate_property_key(kind, key)?;
    if value.chars().any(char::is_control) {
        return Err(GatewayError::invalid_argument(format!(
            "{kind} value for `{key}` must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_property_key(kind: &str, key: &str) -> Result<(), GatewayError> {
    if key.trim().is_empty() || key.chars().any(char::is_control) {
        return Err(GatewayError::invalid_argument(format!(
            "{kind} key must be non-empty and contain no control characters"
        )));
    }
    Ok(())
}

fn validate_identifier(kind: &str, value: &str) -> Result<(), GatewayError> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(GatewayError::invalid_argument(format!(
            "{kind} must be non-empty and contain no control characters"
        )));
    }
    Ok(())
}

/// Builds the `table/partition-name` resource name used in error details.
fn partition_resource_name(table: &TableRef, spec: &[PartitionSpecEntry]) -> String {
    let partition_name = spec
        .iter()
        .map(|entry| entry.value.as_str())
        .collect::<Vec<_>>()
        .join("$");
    format!("{table}/{partition_name}")
}

/// Catalog reads, catalog mutations, and cluster health.
///
/// One of several inherent `impl GatewayService` blocks; see [`crate::application::service`].
impl GatewayService {
    /// Lists every database name of the request's cluster, unsorted and unpaginated.
    pub async fn list_databases(
        &self,
        context: &RequestContext,
    ) -> Result<Vec<String>, GatewayError> {
        let backend = self.backend(context).await?;
        self.execute(context, backend.list_databases()).await
    }

    /// Describes one database.
    pub async fn describe_database(
        &self,
        context: &RequestContext,
        database: &str,
    ) -> Result<DatabaseDescription, GatewayError> {
        let backend = self.backend(context).await?;
        self.execute(context, backend.describe_database(database))
            .await
            .map_err(|error| resource_error(error, "database", database))
    }

    /// Creates one database and reads its canonical metadata back before returning.
    pub async fn create_database(
        &self,
        context: &RequestContext,
        request: CreateDatabaseRequest,
    ) -> Result<DatabaseDescription, GatewayError> {
        validate_create_database(&request)?;
        let backend = self.backend(context).await?;
        self.execute(context, backend.create_database(&request))
            .await
            .map_err(|error| resource_error(error, "database", &request.name))?;
        self.execute(context, backend.describe_database(&request.name))
            .await
            .map_err(|error| resource_error(error, "database", &request.name))
    }

    /// Drops one empty database and invalidates its table-cache prefix.
    pub async fn drop_database(
        &self,
        context: &RequestContext,
        database: &str,
    ) -> Result<(), GatewayError> {
        let backend = self.backend(context).await?;
        self.execute(context, backend.drop_database(database))
            .await
            .map_err(|error| resource_error(error, "database", database))?;
        self.cache(context)?.invalidate_database(database).await;
        Ok(())
    }

    /// Lists every table name of one database, unsorted and unpaginated.
    pub async fn list_tables(
        &self,
        context: &RequestContext,
        database: &str,
    ) -> Result<Vec<String>, GatewayError> {
        let backend = self.backend(context).await?;
        self.execute(context, backend.list_tables(database))
            .await
            .map_err(|error| resource_error(error, "database", database))
    }

    /// Describes one table straight from the backend, bypassing the write-path metadata cache.
    pub async fn describe_table(
        &self,
        context: &RequestContext,
        table: &TableRef,
    ) -> Result<Arc<TableDescription>, GatewayError> {
        let backend = self.backend(context).await?;
        self.execute(context, backend.describe_table(table))
            .await
            .map_err(|error| resource_error(error, "table", table.to_string()))
    }

    /// Creates one table, invalidates stale metadata, and returns a canonical describe result.
    pub async fn create_table(
        &self,
        context: &RequestContext,
        request: CreateTableRequest,
    ) -> Result<Arc<TableDescription>, GatewayError> {
        validate_create_table(&request)?;
        let backend = self.backend(context).await?;
        self.execute(context, backend.create_table(&request))
            .await
            .map_err(|error| resource_error(error, "table", request.table.to_string()))?;
        let cache = self.cache(context)?;
        cache.invalidate_table(&request.table).await;
        let description = self
            .execute(context, backend.describe_table(&request.table))
            .await
            .map_err(|error| resource_error(error, "table", request.table.to_string()))?;
        self.execute(context, cache_table(&cache, &request.table, description))
            .await
    }

    /// Validates all changes, sends one native alteration, and returns canonical metadata.
    pub async fn alter_table(
        &self,
        context: &RequestContext,
        request: AlterTableRequest,
    ) -> Result<Arc<TableDescription>, GatewayError> {
        let backend = self.backend(context).await?;
        let cache = self.cache(context)?;
        let current = self
            .execute(context, load_table(&cache, &backend, &request.table))
            .await
            .map_err(|error| resource_error(error, "table", request.table.to_string()))?;
        validate_alter_table(&request, &current)
            .map_err(|error| resource_error(error, "table", request.table.to_string()))?;
        self.execute(context, backend.alter_table(&request))
            .await
            .map_err(|error| resource_error(error, "table", request.table.to_string()))?;
        cache.invalidate_table(&request.table).await;
        let description = self
            .execute(context, backend.describe_table(&request.table))
            .await
            .map_err(|error| resource_error(error, "table", request.table.to_string()))?;
        self.execute(context, cache_table(&cache, &request.table, description))
            .await
    }

    /// Drops one table and invalidates any cached metadata for it.
    pub async fn drop_table(
        &self,
        context: &RequestContext,
        table: &TableRef,
    ) -> Result<(), GatewayError> {
        let backend = self.backend(context).await?;
        self.execute(context, backend.drop_table(table))
            .await
            .map_err(|error| resource_error(error, "table", table.to_string()))?;
        self.cache(context)?.invalidate_table(table).await;
        Ok(())
    }

    /// Lists every partition of a partitioned table, unsorted and unpaginated.
    pub async fn list_partitions(
        &self,
        context: &RequestContext,
        table: &TableRef,
    ) -> Result<Vec<PartitionDescription>, GatewayError> {
        let backend = self.backend(context).await?;
        self.execute(context, backend.list_partitions(table))
            .await
            .map_err(|error| resource_error(error, "table", table.to_string()))
    }

    /// Creates an exactly validated partition and reads its canonical metadata back.
    pub async fn create_partition(
        &self,
        context: &RequestContext,
        request: PartitionMutationRequest,
    ) -> Result<PartitionDescription, GatewayError> {
        let backend = self.backend(context).await?;
        let cache = self.cache(context)?;
        let table_name = request.table.to_string();
        let partition_name = partition_resource_name(&request.table, &request.spec);
        let current = self
            .execute(context, load_table(&cache, &backend, &request.table))
            .await
            .map_err(|error| resource_error(error, "table", &table_name))?;
        validate_partition_spec(&request, &current)?;
        self.execute(context, backend.create_partition(&request))
            .await
            .map_err(|error| resource_error(error, "partition", &partition_name))?;
        cache.invalidate_partition(&request.table).await;
        let partitions = self
            .execute(context, backend.list_partitions(&request.table))
            .await
            .map_err(|error| resource_error(error, "table", &table_name))?;
        let expected: Vec<(String, String)> = request
            .spec
            .iter()
            .map(|entry| (entry.key.clone(), entry.value.clone()))
            .collect();
        partitions
            .into_iter()
            .find(|partition| partition.spec == expected)
            .ok_or_else(|| {
                GatewayError::internal("created partition was absent from canonical read-back")
            })
    }

    /// Drops one partition selected by canonical partition name.
    pub async fn drop_partition(
        &self,
        context: &RequestContext,
        table: &TableRef,
        partition_name: &str,
    ) -> Result<(), GatewayError> {
        let backend = self.backend(context).await?;
        let cache = self.cache(context)?;
        let table_name = table.to_string();
        let resource_name = format!("{table}/{partition_name}");
        let current = self
            .execute(context, load_table(&cache, &backend, table))
            .await
            .map_err(|error| resource_error(error, "table", &table_name))?;
        let partition = self
            .execute(context, backend.list_partitions(table))
            .await
            .map_err(|error| resource_error(error, "table", &table_name))?
            .into_iter()
            .find(|partition| partition.partition_name == partition_name)
            .ok_or_else(|| {
                GatewayError::not_found(format!("partition `{partition_name}` does not exist"))
                    .with_resource("partition", Some(resource_name.clone()))
            })?;
        let request = PartitionMutationRequest {
            table: table.clone(),
            spec: partition
                .spec
                .into_iter()
                .map(|(key, value)| PartitionSpecEntry { key, value })
                .collect(),
        };
        validate_partition_spec(&request, &current)?;
        self.execute(context, backend.drop_partition(&request))
            .await
            .map_err(|error| resource_error(error, "partition", &resource_name))?;
        cache.invalidate_partition(table).await;
        Ok(())
    }

    /// Reports Fluss cluster health through the request's cluster backend.
    pub async fn cluster_health(
        &self,
        context: &RequestContext,
    ) -> Result<ClusterHealthReport, GatewayError> {
        let backend = self.backend(context).await?;
        self.execute(context, backend.cluster_health()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{ColumnDescription, TableCapabilities, TableKind};
    use arrow::datatypes::Schema;
    use std::sync::Arc;

    fn string_type(nullable: bool) -> DataType {
        DataType::String { nullable }
    }

    fn table() -> TableDescription {
        TableDescription {
            table: TableRef::new("db", "orders"),
            table_id: 1,
            schema_id: 1,
            kind: TableKind::PrimaryKey,
            columns: vec![
                ColumnDescription {
                    name: "region".to_string(),
                    data_type: string_type(false),
                    comment: None,
                },
                ColumnDescription {
                    name: "id".to_string(),
                    data_type: DataType::BigInt { nullable: false },
                    comment: None,
                },
            ],
            primary_keys: vec!["region".to_string(), "id".to_string()],
            physical_primary_keys: vec!["id".to_string()],
            bucket_keys: vec!["id".to_string()],
            partition_keys: vec!["region".to_string()],
            auto_increment_columns: Vec::new(),
            num_buckets: 1,
            log_format: None,
            kv_format: Some("COMPACTED".to_string()),
            comment: None,
            properties: HashMap::new(),
            custom_properties: HashMap::new(),
            created_time: 0,
            modified_time: 0,
            capabilities: TableCapabilities {
                exact_lookup_supported: true,
                prefix_lookup_supported: true,
            },
            arrow_schema: Arc::new(Schema::empty()),
        }
    }

    #[test]
    fn rejects_non_nullable_added_column_before_dispatch() {
        let request = AlterTableRequest {
            table: TableRef::new("db", "orders"),
            changes: vec![TableChange::AddColumn(ColumnDefinition {
                name: "status".to_string(),
                data_type: string_type(false),
                comment: None,
            })],
        };
        assert_eq!(
            validate_alter_table(&request, &table())
                .unwrap_err()
                .message(),
            "added column `status` must be nullable"
        );
    }

    #[test]
    fn rejects_duplicate_and_out_of_order_partition_keys() {
        let duplicate = PartitionMutationRequest {
            table: TableRef::new("db", "orders"),
            spec: vec![
                PartitionSpecEntry {
                    key: "region".to_string(),
                    value: "eu".to_string(),
                },
                PartitionSpecEntry {
                    key: "region".to_string(),
                    value: "us".to_string(),
                },
            ],
        };
        assert!(validate_partition_spec(&duplicate, &table()).is_err());

        let wrong = PartitionMutationRequest {
            table: TableRef::new("db", "orders"),
            spec: vec![PartitionSpecEntry {
                key: "country".to_string(),
                value: "eu".to_string(),
            }],
        };
        assert!(validate_partition_spec(&wrong, &table()).is_err());
    }

    #[test]
    fn rejects_sub_millisecond_time_precision_for_gateway_created_columns() {
        fn create_request(data_type: DataType) -> CreateTableRequest {
            CreateTableRequest {
                table: TableRef::new("db", "clock"),
                columns: vec![ColumnDefinition {
                    name: "at".to_string(),
                    data_type,
                    comment: None,
                }],
                primary_key: Vec::new(),
                partitioned_by: Vec::new(),
                distribution: None,
                configs: HashMap::new(),
                custom_properties: HashMap::new(),
                comment: None,
            }
        }

        let error = validate_create_table(&create_request(DataType::Time {
            nullable: true,
            precision: 6,
        }))
        .unwrap_err();
        assert!(error.message().contains("precision 0 to 3"));

        validate_create_table(&create_request(DataType::Time {
            nullable: true,
            precision: 3,
        }))
        .unwrap();

        let nested = DataType::Array {
            nullable: true,
            element: Box::new(DataType::Time {
                nullable: true,
                precision: 9,
            }),
        };
        let error = validate_create_table(&create_request(nested)).unwrap_err();
        assert!(error.message().contains("precision 0 to 3"));

        let alter = AlterTableRequest {
            table: TableRef::new("db", "orders"),
            changes: vec![TableChange::AddColumn(ColumnDefinition {
                name: "at".to_string(),
                data_type: DataType::Time {
                    nullable: true,
                    precision: 6,
                },
                comment: None,
            })],
        };
        let error = validate_alter_table(&alter, &table()).unwrap_err();
        assert!(error.message().contains("precision 0 to 3"));
    }

    #[test]
    fn validates_create_table_keys_and_distribution() {
        let request = CreateTableRequest {
            table: TableRef::new("db", "orders"),
            columns: vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::BigInt { nullable: false },
                comment: None,
            }],
            primary_key: vec!["id".to_string()],
            partitioned_by: Vec::new(),
            distribution: Some(TableDistributionDefinition {
                bucket_count: 3,
                bucket_keys: vec!["id".to_string()],
            }),
            configs: HashMap::new(),
            custom_properties: HashMap::new(),
            comment: None,
        };
        validate_create_table(&request).unwrap();
    }
}
