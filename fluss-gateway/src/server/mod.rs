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

//! P4/P5 — protocol frontends.
//!
//! Transport + adaptation only; all execution goes through `Instance`. Phase 1
//! ships `postgres` (read-only SQL) and `rest` (direct write). Future protocols
//! (mysql / flightsql / grpc) are NOT created until their phase.
//! Design: `design/sql-path.md` P4, `design/direct-path.md` P5, infra §P8.2.

pub mod postgres;
pub mod rest;

pub use postgres::PgServer;
pub use rest::RestServer;
