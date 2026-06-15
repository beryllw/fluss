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

//! P6 — BackendFacade + metadata read API.
//!
//! The direct-path backend: orchestrates direct read/write intents onto Fluss
//! and exposes read-only metadata (list_databases / list_tables / get_table_info
//! with a TTL cache). The SQL path does NOT go through here — it goes through
//! fluss-datafusion; the two paths only converge at the connection layer.
//! Design: `design/infra.md` §P6.2–P6.4.

// TODO(P6): define the BackendFacade trait + metadata read surface, plus a stub
// implementation behind a gateway-defined Fluss access contract (no fluss-rs dep).
