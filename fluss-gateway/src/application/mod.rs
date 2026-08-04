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

//! Protocol-neutral gateway application boundary.
//!
//! Protocol adapters translate their wire types into the models in this module and call
//! [`GatewayService`]. No HTTP, Axum, JSON, or OpenAPI type belongs in this layer.
//!
//! [`GatewayService`] is defined in [`service`] and extended by one inherent `impl` block per domain, starting
//! with [`ddl`] for catalog reads and mutations.

pub mod context;
pub mod ddl;
pub mod input;
pub mod input_decode;
pub mod metadata_cache;
pub mod service;
pub mod types;

pub use context::{CancellationSignal, RequestContext};
pub use ddl::{
    AlterTableRequest, ColumnDefinition, CreateDatabaseRequest, CreateTableRequest,
    PartitionMutationRequest, PartitionSpecEntry, TableChange, TableDistributionDefinition,
};
pub use input::InputValue;
pub use input_decode::{
    DecodedRow, InputColumn, RowDecodeError, SchemaDecoder, ValidatedTableSchema,
    validate_data_type, validate_table_schema,
};
pub use metadata_cache::{
    DEFAULT_METADATA_CACHE_MAX_ENTRIES, DEFAULT_METADATA_CACHE_TTL, TableMetadataCache,
};
pub use service::GatewayService;
pub use types::{ClusterId, DataType, RowField};

// The backend models are the shared vocabulary of both layers. Re-exporting them here lets protocol adapters
// depend on the application boundary alone.
pub use crate::backend::model::{
    ClusterHealthReport, ClusterStatus, ColumnDescription, DatabaseDescription, KeyValue,
    LookupKey, LookupOutcome, LookupOutcomeKind, PartitionDescription, PrefixLookupOutcome,
    PrefixLookupRequest, PrefixOutcomeKind, TableCapabilities, TableDescription, TableKind,
    TableRef, WriteCompletion, WriteEntryResult, WriteFailure, WriteResult,
};
