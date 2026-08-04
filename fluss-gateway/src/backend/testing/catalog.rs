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

//! In-memory catalog behind [`super::TestBackend`].
//!
//! The fixture is deterministic: one database `fluss` containing the primary-key table `users`, the partitioned
//! primary-key table `orders`, and the log table `events`.

use crate::application::DataType;
use crate::application::ddl::{
    AlterTableRequest, CreateDatabaseRequest, CreateTableRequest, PartitionMutationRequest,
    TableChange,
};
use crate::backend::model::{
    ColumnDescription, DatabaseDescription, PartitionDescription, TableCapabilities,
    TableDescription, TableKind, TableRef,
};
use crate::backend::testing::TestBackend;
use crate::error::GatewayError;
use arrow::datatypes::{DataType as ArrowType, Field, Schema, SchemaRef};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Fixed creation timestamp so fixture responses are byte-for-byte reproducible.
const FIXTURE_TIME_MS: i64 = 1_700_000_000_000;

/// Deterministic catalog contents.
pub struct Catalog {
    databases: BTreeMap<String, DatabaseDescription>,
    tables: BTreeMap<TableRef, Arc<TableDescription>>,
    partitions: BTreeMap<TableRef, Vec<PartitionDescription>>,
    next_table_id: i64,
    next_partition_id: i64,
}

impl Catalog {
    /// Builds the standard fixture: one database with three tables and one partitioned table's partitions.
    pub fn fixture() -> Self {
        let mut catalog = Self {
            databases: BTreeMap::new(),
            tables: BTreeMap::new(),
            partitions: BTreeMap::new(),
            next_table_id: 1,
            next_partition_id: 1,
        };
        catalog.insert_database("fluss", Some("fixture database"));

        catalog.insert_table(
            TableRef::new("fluss", "users"),
            TableKind::PrimaryKey,
            vec![
                ("id", DataType::Int { nullable: false }),
                ("name", DataType::String { nullable: true }),
            ],
            vec!["id"],
            Vec::new(),
            vec!["id"],
        );
        catalog.insert_table(
            TableRef::new("fluss", "orders"),
            TableKind::PrimaryKey,
            vec![
                ("region", DataType::String { nullable: false }),
                ("id", DataType::BigInt { nullable: false }),
                ("total", DataType::Double { nullable: true }),
            ],
            vec!["region", "id"],
            vec!["region"],
            vec!["id"],
        );
        catalog.insert_table(
            TableRef::new("fluss", "events"),
            TableKind::Log,
            vec![
                ("ts", DataType::BigInt { nullable: false }),
                ("message", DataType::String { nullable: true }),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        let orders = TableRef::new("fluss", "orders");
        for region in ["eu", "us"] {
            catalog.insert_partition(&orders, vec![("region".to_string(), region.to_string())]);
        }
        catalog
    }

    fn insert_database(&mut self, name: &str, comment: Option<&str>) {
        self.databases.insert(
            name.to_string(),
            DatabaseDescription {
                name: name.to_string(),
                comment: comment.map(str::to_string),
                custom_properties: HashMap::new(),
                created_time: FIXTURE_TIME_MS,
                modified_time: FIXTURE_TIME_MS,
            },
        );
    }

    fn insert_table(
        &mut self,
        table: TableRef,
        kind: TableKind,
        columns: Vec<(&str, DataType)>,
        primary_keys: Vec<&str>,
        partition_keys: Vec<&str>,
        bucket_keys: Vec<&str>,
    ) {
        let columns: Vec<ColumnDescription> = columns
            .into_iter()
            .map(|(name, data_type)| ColumnDescription {
                name: name.to_string(),
                data_type,
                comment: None,
            })
            .collect();
        let primary_keys: Vec<String> = primary_keys.into_iter().map(str::to_string).collect();
        let partition_keys: Vec<String> = partition_keys.into_iter().map(str::to_string).collect();
        let bucket_keys: Vec<String> = bucket_keys.into_iter().map(str::to_string).collect();
        let physical_primary_keys: Vec<String> = primary_keys
            .iter()
            .filter(|key| !partition_keys.contains(key))
            .cloned()
            .collect();
        let table_id = self.next_table_id;
        self.next_table_id += 1;
        let arrow_schema = arrow_schema(&columns);
        let description = TableDescription {
            table: table.clone(),
            table_id,
            schema_id: 1,
            kind,
            columns,
            primary_keys,
            physical_primary_keys,
            bucket_keys,
            partition_keys,
            auto_increment_columns: Vec::new(),
            num_buckets: 3,
            log_format: matches!(kind, TableKind::Log).then(|| "ARROW".to_string()),
            kv_format: matches!(kind, TableKind::PrimaryKey).then(|| "COMPACTED".to_string()),
            comment: None,
            properties: HashMap::new(),
            custom_properties: HashMap::new(),
            created_time: FIXTURE_TIME_MS,
            modified_time: FIXTURE_TIME_MS,
            capabilities: capabilities(kind),
            arrow_schema,
        };
        self.tables.insert(table, Arc::new(description));
    }

    fn insert_partition(&mut self, table: &TableRef, spec: Vec<(String, String)>) {
        let partition_id = self.next_partition_id;
        self.next_partition_id += 1;
        let partition_name = spec
            .iter()
            .map(|(_, value)| value.as_str())
            .collect::<Vec<_>>()
            .join("$");
        self.partitions
            .entry(table.clone())
            .or_default()
            .push(PartitionDescription {
                partition_id,
                partition_name,
                spec,
            });
    }

    /// Advances the schema ID of one table, simulating concurrent DDL.
    pub fn bump_schema_id(&mut self, table: &TableRef) {
        if let Some(existing) = self.tables.get(table) {
            let mut updated = (**existing).clone();
            updated.schema_id += 1;
            self.tables.insert(table.clone(), Arc::new(updated));
        }
    }

    /// The current description of one table, if it exists.
    pub fn table(&self, table: &TableRef) -> Option<Arc<TableDescription>> {
        self.tables.get(table).cloned()
    }
}

/// Derives fixture capabilities the same way the native backend derives them from table metadata.
fn capabilities(kind: TableKind) -> TableCapabilities {
    let primary_key = matches!(kind, TableKind::PrimaryKey);
    TableCapabilities {
        exact_lookup_supported: primary_key,
        prefix_lookup_supported: primary_key,
    }
}

/// Maps the fixture column types onto an Arrow schema. Only the types the fixture uses are covered.
fn arrow_schema(columns: &[ColumnDescription]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|column| {
            let arrow_type = match &column.data_type {
                DataType::Boolean { .. } => ArrowType::Boolean,
                DataType::TinyInt { .. } => ArrowType::Int8,
                DataType::SmallInt { .. } => ArrowType::Int16,
                DataType::Int { .. } | DataType::Date { .. } => ArrowType::Int32,
                DataType::BigInt { .. } => ArrowType::Int64,
                DataType::Float { .. } => ArrowType::Float32,
                DataType::Double { .. } => ArrowType::Float64,
                DataType::Bytes { .. } | DataType::Binary { .. } => ArrowType::Binary,
                _ => ArrowType::Utf8,
            };
            Field::new(&column.name, arrow_type, column.data_type.nullable())
        })
        .collect();
    Arc::new(Schema::new(fields))
}

fn missing_database(database: &str) -> GatewayError {
    GatewayError::not_found(format!("database `{database}` does not exist"))
}

fn missing_table(table: &TableRef) -> GatewayError {
    GatewayError::not_found(format!("table `{table}` does not exist"))
}

pub(crate) fn list_databases(backend: &TestBackend) -> Result<Vec<String>, GatewayError> {
    Ok(backend
        .state
        .lock()
        .catalog
        .databases
        .keys()
        .cloned()
        .collect())
}

pub(crate) fn describe_database(
    backend: &TestBackend,
    database: &str,
) -> Result<DatabaseDescription, GatewayError> {
    backend
        .state
        .lock()
        .catalog
        .databases
        .get(database)
        .cloned()
        .ok_or_else(|| missing_database(database))
}

pub(crate) fn create_database(
    backend: &TestBackend,
    request: &CreateDatabaseRequest,
) -> Result<(), GatewayError> {
    let mut state = backend.state.lock();
    if state.catalog.databases.contains_key(&request.name) {
        return Err(GatewayError::already_exists(format!(
            "database `{}` already exists",
            request.name
        )));
    }
    state.catalog.databases.insert(
        request.name.clone(),
        DatabaseDescription {
            name: request.name.clone(),
            comment: request.comment.clone(),
            custom_properties: request.custom_properties.clone(),
            created_time: FIXTURE_TIME_MS,
            modified_time: FIXTURE_TIME_MS,
        },
    );
    Ok(())
}

pub(crate) fn drop_database(backend: &TestBackend, database: &str) -> Result<(), GatewayError> {
    let mut state = backend.state.lock();
    if !state.catalog.databases.contains_key(database) {
        return Err(missing_database(database));
    }
    if state
        .catalog
        .tables
        .keys()
        .any(|table| table.database == database)
    {
        return Err(GatewayError::failed_precondition(format!(
            "database `{database}` is not empty"
        )));
    }
    state.catalog.databases.remove(database);
    Ok(())
}

pub(crate) fn list_tables(
    backend: &TestBackend,
    database: &str,
) -> Result<Vec<String>, GatewayError> {
    let state = backend.state.lock();
    if !state.catalog.databases.contains_key(database) {
        return Err(missing_database(database));
    }
    Ok(state
        .catalog
        .tables
        .keys()
        .filter(|table| table.database == database)
        .map(|table| table.table.clone())
        .collect())
}

pub(crate) fn describe_table(
    backend: &TestBackend,
    table: &TableRef,
) -> Result<Arc<TableDescription>, GatewayError> {
    backend
        .state
        .lock()
        .catalog
        .table(table)
        .ok_or_else(|| missing_table(table))
}

pub(crate) fn create_table(
    backend: &TestBackend,
    request: &CreateTableRequest,
) -> Result<(), GatewayError> {
    let mut state = backend.state.lock();
    if !state
        .catalog
        .databases
        .contains_key(&request.table.database)
    {
        return Err(missing_database(&request.table.database));
    }
    if state.catalog.tables.contains_key(&request.table) {
        return Err(GatewayError::already_exists(format!(
            "table `{}` already exists",
            request.table
        )));
    }
    let kind = if request.primary_key.is_empty() {
        TableKind::Log
    } else {
        TableKind::PrimaryKey
    };
    let columns: Vec<(&str, DataType)> = request
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column.data_type.clone()))
        .collect();
    let bucket_keys: Vec<&str> = request
        .distribution
        .as_ref()
        .map(|distribution| {
            distribution
                .bucket_keys
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    state.catalog.insert_table(
        request.table.clone(),
        kind,
        columns,
        request.primary_key.iter().map(String::as_str).collect(),
        request.partitioned_by.iter().map(String::as_str).collect(),
        bucket_keys,
    );
    Ok(())
}

pub(crate) fn alter_table(
    backend: &TestBackend,
    request: &AlterTableRequest,
) -> Result<(), GatewayError> {
    let mut state = backend.state.lock();
    let existing = state
        .catalog
        .table(&request.table)
        .ok_or_else(|| missing_table(&request.table))?;
    let mut updated = (*existing).clone();
    for change in &request.changes {
        match change {
            TableChange::AddColumn(column) => {
                if updated
                    .columns
                    .iter()
                    .any(|existing| existing.name == column.name)
                {
                    return Err(GatewayError::already_exists(format!(
                        "column `{}` already exists",
                        column.name
                    )));
                }
                updated.columns.push(ColumnDescription {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    comment: column.comment.clone(),
                });
            }
            TableChange::SetConfig { key, value } => {
                updated.properties.insert(key.clone(), value.clone());
            }
            TableChange::ResetConfig { key } => {
                updated.properties.remove(key);
            }
        }
    }
    updated.schema_id += 1;
    updated.arrow_schema = arrow_schema(&updated.columns);
    state
        .catalog
        .tables
        .insert(request.table.clone(), Arc::new(updated));
    Ok(())
}

pub(crate) fn drop_table(backend: &TestBackend, table: &TableRef) -> Result<(), GatewayError> {
    let mut state = backend.state.lock();
    if state.catalog.tables.remove(table).is_none() {
        return Err(missing_table(table));
    }
    state.catalog.partitions.remove(table);
    Ok(())
}

pub(crate) fn list_partitions(
    backend: &TestBackend,
    table: &TableRef,
) -> Result<Vec<PartitionDescription>, GatewayError> {
    let state = backend.state.lock();
    let description = state
        .catalog
        .table(table)
        .ok_or_else(|| missing_table(table))?;
    if !description.is_partitioned() {
        return Err(GatewayError::invalid_argument(format!(
            "table `{table}` is not partitioned"
        )));
    }
    Ok(state
        .catalog
        .partitions
        .get(table)
        .cloned()
        .unwrap_or_default())
}

pub(crate) fn create_partition(
    backend: &TestBackend,
    request: &PartitionMutationRequest,
) -> Result<(), GatewayError> {
    let mut state = backend.state.lock();
    if state.catalog.table(&request.table).is_none() {
        return Err(missing_table(&request.table));
    }
    let spec: Vec<(String, String)> = request
        .spec
        .iter()
        .map(|entry| (entry.key.clone(), entry.value.clone()))
        .collect();
    if state
        .catalog
        .partitions
        .get(&request.table)
        .is_some_and(|partitions| partitions.iter().any(|partition| partition.spec == spec))
    {
        return Err(GatewayError::already_exists("the partition already exists"));
    }
    state.catalog.insert_partition(&request.table, spec);
    Ok(())
}

pub(crate) fn drop_partition(
    backend: &TestBackend,
    request: &PartitionMutationRequest,
) -> Result<(), GatewayError> {
    let mut state = backend.state.lock();
    let spec: Vec<(String, String)> = request
        .spec
        .iter()
        .map(|entry| (entry.key.clone(), entry.value.clone()))
        .collect();
    let partitions = state
        .catalog
        .partitions
        .get_mut(&request.table)
        .ok_or_else(|| GatewayError::not_found("the requested partition does not exist"))?;
    let before = partitions.len();
    partitions.retain(|partition| partition.spec != spec);
    if partitions.len() == before {
        return Err(GatewayError::not_found(
            "the requested partition does not exist",
        ));
    }
    Ok(())
}
