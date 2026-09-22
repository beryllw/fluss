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

//! Fixed catalog fixtures, recorded mutations, and operation-scoped failures for protocol tests.

use crate::backend::context::RequestContext;
use crate::backend::types::ClusterId;
use crate::backend::{
    FlussBackend, LookupOutcome, LookupOutcomeKind, LookupRequest, PrefixLookupOutcome,
    PrefixLookupOutcomeKind, PrefixLookupRequest, RowWriteError, WriteRequest, WriteResult,
    unknown_cluster,
};
use crate::error::{GatewayError, GatewayResult, Resource};
use async_trait::async_trait;
use fluss::metadata::{
    AlterTableChanges, DataType, PartitionInfo, PartitionSpec, Schema, TableDescriptor, TableInfo,
    TablePath,
};
use fluss::record::RowAppendRecordBatchBuilder;
use fluss::row::{Date, Datum, Decimal, GenericRow, Time, TimestampLtz, TimestampNtz};
use std::collections::{BTreeMap, HashMap, btree_map::Entry};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

const FIXTURE_TIME: i64 = 1_700_000_000_000;

struct FakeTable {
    info: TableInfo,
    partitions: Vec<PartitionInfo>,
}

#[derive(Default)]
struct FakeState {
    databases: BTreeMap<String, BTreeMap<String, FakeTable>>,
    cached_tables: HashMap<TablePath, TableInfo>,
    calls: Vec<FakeCall>,
    failures: HashMap<Operation, GatewayError>,
    writes: Vec<Option<Vec<String>>>,
    write_failures: Vec<(usize, GatewayError)>,
    lookup_delay: Option<Duration>,
    prefix_lookup_delay: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    ListDatabases,
    CreateDatabase,
    DropDatabase,
    ListTables,
    DescribeTable,
    CreateTable,
    AlterTable,
    DropTable,
    ListPartitions,
    CreatePartition,
    DropPartition,
    Write,
    Lookup,
    PrefixLookup,
}

#[derive(Debug, Clone)]
pub enum FakeCall {
    CreateDatabase(String),
    DropDatabase(String),
    CreateTable(TablePath, TableDescriptor),
    AlterTable(TablePath, AlterTableChanges),
    DropTable(TablePath),
    CreatePartition(TablePath, PartitionSpec),
    DropPartition(TablePath, PartitionSpec),
}

pub struct FakeFlussBackend {
    clusters: Vec<ClusterId>,
    state: Mutex<FakeState>,
}

impl Default for FakeFlussBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeFlussBackend {
    pub fn new() -> Self {
        Self::with_catalog(&[])
    }

    pub fn with_catalog(databases: &[(&str, &[&str])]) -> Self {
        let backend = Self {
            clusters: vec![cluster_id("default")],
            state: Mutex::default(),
        };
        for (database, tables) in databases {
            backend.define_database(database);
            for table in *tables {
                backend.define_table(fixture_table(TablePath::new(*database, *table)));
            }
        }
        backend
    }

    pub fn with_clusters(ids: &[&str]) -> Self {
        let mut clusters: Vec<ClusterId> = ids.iter().map(|id| cluster_id(id)).collect();
        clusters.sort();
        Self {
            clusters,
            ..Self::new()
        }
    }

    pub fn define_database(&self, name: &str) {
        self.state().databases.entry(name.to_string()).or_default();
    }

    pub fn define_table(&self, info: TableInfo) {
        let mut state = self.state();
        let tables = state
            .databases
            .entry(info.table_path.database().to_string())
            .or_default();
        match tables.entry(info.table_path.table().to_string()) {
            Entry::Occupied(mut entry) => entry.get_mut().info = info,
            Entry::Vacant(entry) => {
                entry.insert(FakeTable {
                    info,
                    partitions: Vec::new(),
                });
            }
        }
    }

    pub fn with_table(self, info: TableInfo) -> Self {
        self.define_table(info);
        self
    }

    pub fn cache_table(&self, info: TableInfo) {
        self.state()
            .cached_tables
            .insert(info.table_path.clone(), info);
    }

    pub fn fail_rows(&self, failures: Vec<(usize, GatewayError)>) {
        self.state().write_failures = failures;
    }

    pub fn writes(&self) -> Vec<Option<Vec<String>>> {
        self.state().writes.clone()
    }

    pub fn delay_lookups(&self, point: Option<Duration>, prefix: Option<Duration>) {
        let mut state = self.state();
        state.lookup_delay = point;
        state.prefix_lookup_delay = prefix;
    }

    pub fn define_partition(&self, table: &TablePath, info: PartitionInfo) {
        let mut state = self.state();
        let entry = state
            .databases
            .get_mut(table.database())
            .and_then(|tables| tables.get_mut(table.table()))
            .expect("the fixture table is defined");
        entry.partitions.push(info);
    }

    pub fn fail_once(&self, operation: Operation, error: GatewayError) {
        self.state().failures.insert(operation, error);
    }

    pub fn calls(&self) -> Vec<FakeCall> {
        self.state().calls.clone()
    }

    fn call<T>(
        &self,
        ctx: &RequestContext,
        operation: Operation,
        mutation: Option<FakeCall>,
        answer: impl FnOnce(&FakeState) -> GatewayResult<T>,
    ) -> GatewayResult<T> {
        if !self.has_cluster(ctx.cluster_id().as_str()) {
            return Err(unknown_cluster(ctx.cluster_id().as_str()));
        }
        let mut state = self.state();
        if let Some(call) = mutation {
            state.calls.push(call);
        }
        if let Some(error) = state.failures.remove(&operation) {
            return Err(error);
        }
        answer(&state)
    }

    fn state(&self) -> MutexGuard<'_, FakeState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn cluster_id(id: &str) -> ClusterId {
    ClusterId::try_from(id).expect("valid fixture cluster ID")
}

fn fixture_table(table: TablePath) -> TableInfo {
    let schema = Schema::builder()
        .column("id", DataType::BigInt(fluss::metadata::BigIntType::new()))
        .build()
        .expect("the fixture schema is valid");
    let descriptor = TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(1), Vec::new())
        .build()
        .expect("the fixture descriptor is valid");
    TableInfo::of(table, 1, 1, descriptor, FIXTURE_TIME, FIXTURE_TIME)
}

fn write_table_info(
    table: &str,
    schema_id: i32,
    columns: Vec<(&str, DataType)>,
    primary_key: Option<&[&str]>,
) -> TableInfo {
    let mut schema = Schema::builder();
    for (name, data_type) in columns {
        schema = schema.column(name, data_type);
    }
    if let Some(keys) = primary_key {
        schema = schema
            .primary_key(keys.iter().copied())
            .expect("valid fixture primary key");
    }
    let descriptor = TableDescriptor::builder()
        .schema(schema.build().expect("valid fixture schema"))
        .distributed_by(Some(3), Vec::new())
        .build()
        .expect("valid fixture table");
    TableInfo::of(
        TablePath::new("fluss", table),
        1,
        schema_id,
        descriptor,
        0,
        0,
    )
}

pub(crate) fn users_table_info(schema_id: i32) -> TableInfo {
    write_table_info(
        "users",
        schema_id,
        vec![
            (
                "id",
                DataType::Int(fluss::metadata::IntType::with_nullable(false)),
            ),
            (
                "name",
                DataType::String(fluss::metadata::StringType::with_nullable(true)),
            ),
        ],
        Some(&["id"]),
    )
}

pub(crate) fn log_table_info(schema_id: i32) -> TableInfo {
    write_table_info(
        "applog",
        schema_id,
        vec![
            (
                "ts",
                DataType::BigInt(fluss::metadata::BigIntType::with_nullable(false)),
            ),
            (
                "message",
                DataType::String(fluss::metadata::StringType::with_nullable(true)),
            ),
        ],
        None,
    )
}

pub(crate) fn prefix_table_info(schema_id: i32) -> TableInfo {
    let schema = Schema::builder()
        .column(
            "user_id",
            DataType::Int(fluss::metadata::IntType::with_nullable(false)),
        )
        .column(
            "item_id",
            DataType::Int(fluss::metadata::IntType::with_nullable(false)),
        )
        .column(
            "note",
            DataType::String(fluss::metadata::StringType::with_nullable(true)),
        )
        .primary_key(["user_id", "item_id"])
        .unwrap()
        .build()
        .expect("valid prefix fixture schema");
    let descriptor = TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(3), vec!["user_id".to_string()])
        .build()
        .expect("valid prefix fixture table");
    TableInfo::of(
        TablePath::new("fluss", "items"),
        1,
        schema_id,
        descriptor,
        0,
        0,
    )
}

fn database_of<'state>(
    state: &'state FakeState,
    database: &str,
) -> GatewayResult<&'state BTreeMap<String, FakeTable>> {
    state.databases.get(database).ok_or_else(|| {
        GatewayError::not_found(format!("database `{database}` does not exist"))
            .with_resource(Resource::Database)
    })
}

fn table_of<'state>(
    state: &'state FakeState,
    table: &TablePath,
) -> GatewayResult<&'state FakeTable> {
    database_of(state, table.database())?
        .get(table.table())
        .ok_or_else(|| {
            GatewayError::not_found(format!("table `{table}` does not exist"))
                .with_resource(Resource::Table)
        })
}

#[async_trait]
impl FlussBackend for FakeFlussBackend {
    fn clusters(&self) -> Vec<ClusterId> {
        self.clusters.clone()
    }

    fn has_cluster(&self, id: &str) -> bool {
        self.clusters.iter().any(|cluster| cluster.as_str() == id)
    }

    async fn list_databases(&self, ctx: &RequestContext) -> GatewayResult<Vec<String>> {
        self.call(ctx, Operation::ListDatabases, None, |state| {
            Ok(state.databases.keys().cloned().collect())
        })
    }

    async fn create_database(&self, ctx: &RequestContext, database: &str) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::CreateDatabase,
            Some(FakeCall::CreateDatabase(database.to_string())),
            |_| Ok(()),
        )
    }

    async fn drop_database(&self, ctx: &RequestContext, database: &str) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::DropDatabase,
            Some(FakeCall::DropDatabase(database.to_string())),
            |_| Ok(()),
        )
    }

    async fn list_tables(
        &self,
        ctx: &RequestContext,
        database: &str,
    ) -> GatewayResult<Vec<String>> {
        self.call(ctx, Operation::ListTables, None, |state| {
            Ok(database_of(state, database)?.keys().cloned().collect())
        })
    }

    async fn table_info(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<TableInfo> {
        self.call(ctx, Operation::DescribeTable, None, |state| {
            if let Some(info) = state.cached_tables.get(table) {
                return Ok(info.clone());
            }
            Ok(table_of(state, table)?.info.clone())
        })
    }

    async fn describe_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<TableInfo> {
        self.call(ctx, Operation::DescribeTable, None, |state| {
            Ok(table_of(state, table)?.info.clone())
        })
    }

    async fn create_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        descriptor: &TableDescriptor,
    ) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::CreateTable,
            Some(FakeCall::CreateTable(table.clone(), descriptor.clone())),
            |_| Ok(()),
        )
    }

    async fn alter_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        changes: AlterTableChanges,
    ) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::AlterTable,
            Some(FakeCall::AlterTable(table.clone(), changes)),
            |_| Ok(()),
        )
    }

    async fn drop_table(&self, ctx: &RequestContext, table: &TablePath) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::DropTable,
            Some(FakeCall::DropTable(table.clone())),
            |_| Ok(()),
        )
    }

    async fn list_partitions(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<Vec<PartitionInfo>> {
        self.call(ctx, Operation::ListPartitions, None, |state| {
            Ok(table_of(state, table)?.partitions.clone())
        })
    }

    async fn create_partition(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        spec: &PartitionSpec,
    ) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::CreatePartition,
            Some(FakeCall::CreatePartition(table.clone(), spec.clone())),
            |_| Ok(()),
        )
    }

    async fn drop_partition(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        spec: &PartitionSpec,
    ) -> GatewayResult<()> {
        self.call(
            ctx,
            Operation::DropPartition,
            Some(FakeCall::DropPartition(table.clone(), spec.clone())),
            |_| Ok(()),
        )
    }

    async fn write(
        &self,
        ctx: &RequestContext,
        request: WriteRequest,
    ) -> GatewayResult<WriteResult> {
        let row_count = request.rows().len() as u64;
        self.state()
            .writes
            .push(request.partial_update_columns().map(<[String]>::to_vec));
        self.call(ctx, Operation::Write, None, move |state| {
            let failures = state
                .write_failures
                .iter()
                .map(|(index, error)| RowWriteError {
                    index: *index,
                    error: error.clone(),
                })
                .collect();
            Ok(WriteResult {
                row_count,
                failures,
            })
        })
    }

    async fn lookup(
        &self,
        ctx: &RequestContext,
        request: LookupRequest,
    ) -> GatewayResult<Vec<LookupOutcome>> {
        let delay = self.state().lookup_delay;
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        let (table, keys) = request.into_parts();
        self.call(ctx, Operation::Lookup, None, |state| {
            let table = &table_of(state, &table.table_path)?.info;
            keys.iter()
                .enumerate()
                .map(|(input_index, key)| {
                    let kind = match key_number(key) {
                        500 => LookupOutcomeKind::Error(GatewayError::unavailable(
                            "the tablet server holding this key is unavailable",
                        )),
                        value if value >= 100 => LookupOutcomeKind::NotFound,
                        _ => LookupOutcomeKind::Found(synthesise(
                            table,
                            table.get_primary_keys(),
                            key,
                            1,
                        )?),
                    };
                    Ok(LookupOutcome { input_index, kind })
                })
                .collect()
        })
    }

    async fn prefix_lookup(
        &self,
        ctx: &RequestContext,
        request: PrefixLookupRequest,
    ) -> GatewayResult<Vec<PrefixLookupOutcome>> {
        let delay = self.state().prefix_lookup_delay;
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        let (table, prefix_columns, prefixes, max_rows_per_prefix) = request.into_parts();
        self.call(ctx, Operation::PrefixLookup, None, |state| {
            let table = &table_of(state, &table.table_path)?.info;
            prefixes
                .iter()
                .enumerate()
                .map(|(input_index, prefix)| {
                    let kind = match key_number(prefix) {
                        500 => PrefixLookupOutcomeKind::Error(GatewayError::unavailable(
                            "the tablet server holding this prefix is unavailable",
                        )),
                        value => {
                            let rows = value.clamp(0, 250) as usize;
                            let truncated = rows > max_rows_per_prefix;
                            PrefixLookupOutcomeKind::Rows {
                                batch: synthesise(
                                    table,
                                    &prefix_columns,
                                    prefix,
                                    rows.min(max_rows_per_prefix),
                                )?,
                                truncated,
                            }
                        }
                    };
                    Ok(PrefixLookupOutcome { input_index, kind })
                })
                .collect()
        })
    }
}

fn key_number(key: &GenericRow<'_>) -> i64 {
    match key.values.last() {
        Some(Datum::Int8(value)) => i64::from(*value),
        Some(Datum::Int16(value)) => i64::from(*value),
        Some(Datum::Int32(value)) => i64::from(*value),
        Some(Datum::Int64(value)) => *value,
        _ => 1,
    }
}

fn synthesise(
    table: &TableInfo,
    key_columns: &[String],
    key: &GenericRow<'_>,
    rows: usize,
) -> GatewayResult<arrow::array::RecordBatch> {
    let mut builder = RowAppendRecordBatchBuilder::new(table.row_type()).map_err(|error| {
        GatewayError::internal(format!("failed to build the fake lookup schema: {error}"))
    })?;
    for row_index in 0..rows {
        let mut row = GenericRow::new(table.row_type().fields().len());
        for (column_index, field) in table.row_type().fields().iter().enumerate() {
            let value = key_columns
                .iter()
                .position(|name| name == field.name())
                .and_then(|key_index| key.values.get(key_index))
                .cloned()
                .unwrap_or_else(|| fixture_value(field.data_type(), row_index));
            row.set_field(column_index, value);
        }
        builder.append(&row).map_err(|error| {
            GatewayError::internal(format!("failed to append a fake lookup row: {error}"))
        })?;
    }
    builder
        .build_arrow_record_batch()
        .map(Arc::unwrap_or_clone)
        .map_err(|error| {
            GatewayError::internal(format!("failed to finish a fake lookup batch: {error}"))
        })
}

fn fixture_value(data_type: &DataType, row: usize) -> Datum<'static> {
    if data_type.is_nullable() {
        return Datum::Null;
    }
    match data_type {
        DataType::Boolean(_) => Datum::Bool(row.is_multiple_of(2)),
        DataType::TinyInt(_) => Datum::Int8(row as i8),
        DataType::SmallInt(_) => Datum::Int16(row as i16),
        DataType::Int(_) => Datum::Int32(row as i32),
        DataType::BigInt(_) => Datum::Int64(row as i64),
        DataType::Float(_) => Datum::from(row as f32 + 0.5),
        DataType::Double(_) => Datum::from(row as f64 + 0.5),
        DataType::Char(_) | DataType::String(_) => Datum::from(format!("value-{row}")),
        DataType::Bytes(_) | DataType::Binary(_) => Datum::from(vec![row as u8]),
        DataType::Decimal(decimal) => Decimal::from_unscaled_bytes(
            &(row as i128).to_be_bytes(),
            decimal.precision(),
            decimal.scale(),
        )
        .map(Datum::Decimal)
        .unwrap_or(Datum::Null),
        DataType::Date(_) => Datum::Date(Date::new(row as i32)),
        DataType::Time(_) => Datum::Time(Time::new(row as i32)),
        DataType::Timestamp(_) => TimestampNtz::from_millis_nanos(row as i64, 0)
            .map(Datum::TimestampNtz)
            .unwrap_or(Datum::Null),
        DataType::TimestampLTz(_) => TimestampLtz::from_millis_nanos(row as i64, 0)
            .map(Datum::TimestampLtz)
            .unwrap_or(Datum::Null),
        DataType::Array(_) | DataType::Map(_) | DataType::Row(_) => Datum::Null,
    }
}
