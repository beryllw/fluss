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

//! Gateway application boundary: the orchestration behind the REST adapter.
//!
//! The REST layer translates its wire types into the models in this module and calls
//! [`GatewayService`]. No HTTP, Axum, JSON, or OpenAPI type belongs in this layer — the split
//! keeps request orchestration (deadlines, decoding, caching, identity resolution) testable
//! without a listener. It is not a multi-protocol framework: the gateway serves exactly the
//! FIP-49 REST API, and any future protocol is an evolution to design when it is needed.
//!
//! [`GatewayService`] is defined in [`service`] and extended by one inherent `impl` block per domain: [`ddl`]
//! for catalog reads and mutations, [`mod@write`] for the records endpoint, and [`lookup`] for the two lookup
//! endpoints.

pub mod ddl;
pub mod input;
pub mod input_decode;
pub mod lookup;
pub mod pagination;
pub mod service;
pub mod write;

pub use crate::backend::context::{CancellationSignal, RequestContext};
pub use crate::backend::metadata_cache::{
    DEFAULT_METADATA_CACHE_MAX_ENTRIES, DEFAULT_METADATA_CACHE_TTL, TableMetadataCache,
};
pub use crate::backend::types::{ClusterId, DataType, RowField};
pub use ddl::{
    AlterTableRequest, ColumnDefinition, CreateDatabaseRequest, CreateTableRequest,
    PartitionMutationRequest, PartitionSpecEntry, TableChange, TableDistributionDefinition,
};
pub use input::InputValue;
pub use input_decode::{
    DecodedRow, InputColumn, RowDecodeError, SchemaDecoder, ValidatedTableSchema,
    validate_data_type, validate_table_schema,
};
pub use pagination::{PAGE_TOKEN_VERSION, PageScope, decode_page_token, encode_page_token};
pub use service::GatewayService;
pub use write::{WriteEntry, WriteOperation, WriteRequest};

// The backend models are the shared vocabulary of both layers. Re-exporting them here lets the REST adapter
// depend on the application boundary alone.
pub use crate::backend::model::{
    ClusterHealthReport, ClusterStatus, ColumnDescription, DatabaseDescription, KeyValue,
    LookupKey, LookupOutcome, LookupOutcomeKind, PartitionDescription, PrefixLookupOutcome,
    PrefixLookupRequest, PrefixOutcomeKind, TableCapabilities, TableDescription, TableKind,
    TableRef, WriteCompletion, WriteEntryResult, WriteFailure, WriteResult,
};
