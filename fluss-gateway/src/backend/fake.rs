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

//! Recorded backend calls and fixed catalog responses for protocol tests.

use crate::backend::context::RequestContext;
use crate::backend::types::ClusterId;
use crate::backend::{FlussBackend, unknown_cluster};
use crate::error::{GatewayError, GatewayResult, Resource};
use async_trait::async_trait;
use fluss::metadata::{
    AlterTableChanges, DataType, PartitionInfo, PartitionSpec, ResolvedPartitionSpec, Schema,
    TableDescriptor, TableInfo, TablePath,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

const FIXTURE_TIME: i64 = 1_700_000_000_000;

struct FakeTable {
    info: TableInfo,
    partitions: BTreeMap<String, PartitionInfo>,
}

struct FakeState {
    databases: BTreeMap<String, BTreeMap<String, FakeTable>>,
    calls: Vec<FakeCall>,
    next_failure: Option<GatewayError>,
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
            state: Mutex::new(FakeState {
                databases: BTreeMap::new(),
                calls: Vec::new(),
                next_failure: None,
            }),
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
        self.state()
            .databases
            .insert(name.to_string(), BTreeMap::new());
    }

    pub fn define_table(&self, info: TableInfo) {
        let mut state = self.state();
        state
            .databases
            .entry(info.table_path.database().to_string())
            .or_default()
            .insert(
                info.table_path.table().to_string(),
                FakeTable {
                    info,
                    partitions: BTreeMap::new(),
                },
            );
    }

    pub fn define_partition(&self, table: &TablePath, name: &str) {
        let mut state = self.state();
        let entry = state
            .databases
            .get_mut(table.database())
            .and_then(|tables| tables.get_mut(table.table()))
            .expect("the fixture table is defined");
        let values = name.split('$').map(str::to_string).collect();
        let resolved = ResolvedPartitionSpec::new(Arc::clone(&entry.info.partition_keys), values)
            .expect("the fixture partition name matches");
        entry
            .partitions
            .insert(name.to_string(), PartitionInfo::new(1, resolved));
    }

    pub fn fail_next(&self, error: GatewayError) {
        self.state().next_failure = Some(error);
    }

    pub fn calls(&self) -> Vec<FakeCall> {
        self.state().calls.clone()
    }

    fn read<T>(
        &self,
        ctx: &RequestContext,
        answer: impl FnOnce(&FakeState) -> GatewayResult<T>,
    ) -> GatewayResult<T> {
        self.check_cluster(ctx)?;
        answer(&self.state())
    }

    fn record<T>(
        &self,
        ctx: &RequestContext,
        call: FakeCall,
        answer: impl FnOnce(&FakeState) -> GatewayResult<T>,
    ) -> GatewayResult<T> {
        self.check_cluster(ctx)?;
        let mut state = self.state();
        if let Some(error) = state.next_failure.take() {
            return Err(error);
        }
        let result = answer(&state)?;
        state.calls.push(call);
        Ok(result)
    }

    fn check_cluster(&self, ctx: &RequestContext) -> GatewayResult<()> {
        if self.has_cluster(ctx.cluster_id().as_str()) {
            Ok(())
        } else {
            Err(unknown_cluster(ctx.cluster_id().as_str()))
        }
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
    table_info(table, descriptor)
}

fn table_info(table: TablePath, descriptor: TableDescriptor) -> TableInfo {
    let descriptor = if descriptor.table_distribution().is_some() {
        descriptor
    } else {
        descriptor.with_bucket_count(1)
    };
    TableInfo::of(table, 1, 1, descriptor, FIXTURE_TIME, FIXTURE_TIME)
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
        self.read(ctx, |state| Ok(state.databases.keys().cloned().collect()))
    }

    async fn create_database(&self, ctx: &RequestContext, database: &str) -> GatewayResult<()> {
        self.record(ctx, FakeCall::CreateDatabase(database.to_string()), |_| {
            Ok(())
        })
    }

    async fn drop_database(&self, ctx: &RequestContext, database: &str) -> GatewayResult<()> {
        self.record(
            ctx,
            FakeCall::DropDatabase(database.to_string()),
            |_| Ok(()),
        )
    }

    async fn list_tables(
        &self,
        ctx: &RequestContext,
        database: &str,
    ) -> GatewayResult<Vec<String>> {
        self.read(ctx, |state| {
            Ok(database_of(state, database)?.keys().cloned().collect())
        })
    }

    async fn describe_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<TableInfo> {
        self.read(ctx, |state| Ok(table_of(state, table)?.info.clone()))
    }

    async fn create_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        descriptor: &TableDescriptor,
    ) -> GatewayResult<TableInfo> {
        self.record(
            ctx,
            FakeCall::CreateTable(table.clone(), descriptor.clone()),
            |_| Ok(table_info(table.clone(), descriptor.clone())),
        )
    }

    async fn alter_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        changes: AlterTableChanges,
    ) -> GatewayResult<TableInfo> {
        self.record(ctx, FakeCall::AlterTable(table.clone(), changes), |state| {
            Ok(table_of(state, table)?.info.clone())
        })
    }

    async fn drop_table(&self, ctx: &RequestContext, table: &TablePath) -> GatewayResult<()> {
        self.record(ctx, FakeCall::DropTable(table.clone()), |_| Ok(()))
    }

    async fn list_partitions(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<Vec<PartitionInfo>> {
        self.read(ctx, |state| {
            Ok(table_of(state, table)?
                .partitions
                .values()
                .cloned()
                .collect())
        })
    }

    async fn create_partition(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        spec: &PartitionSpec,
    ) -> GatewayResult<()> {
        self.record(
            ctx,
            FakeCall::CreatePartition(table.clone(), spec.clone()),
            |_| Ok(()),
        )
    }

    async fn drop_partition(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        spec: &PartitionSpec,
    ) -> GatewayResult<()> {
        self.record(
            ctx,
            FakeCall::DropPartition(table.clone(), spec.clone()),
            |_| Ok(()),
        )
    }
}
