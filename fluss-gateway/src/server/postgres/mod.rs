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

//! P4 — PostgreSQL wire frontend (transport / handler / adapter / compat).
//!
//! Read-only SQL in Phase 1. The four submodules follow the design's layering
//! (`design/sql-path.md` §P4.1):
//! - [`transport`]: TCP listener / accept / per-connection task (cleartext only);
//! - [`handler`]: the pgwire protocol state machine, bridging to `Instance`;
//! - [`adapter`]: the wire <-> gateway boundary (startup mapping, Arrow->PG
//!   encoding, domain-error->PG mapping, the out-of-band cancel registry);
//! - [`compat`]: BI/IDE statement classification (SET/SHOW/txn/write/probe).
//!
//! `Instance` carries zero pgwire dependency: the frontend depends only on the
//! [`GatewayInstance`](crate::instance::GatewayInstance) trait, the auth seam,
//! and the neutral domain types/errors.

pub mod adapter;
pub mod compat;
pub mod handler;
pub mod transport;

pub use transport::PgServer;
