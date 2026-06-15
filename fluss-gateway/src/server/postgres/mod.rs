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
//! Read-only SQL in Phase 1. Owns the prepared-statement wire lifecycle and any
//! protocol-local caching; translates wire auth into a neutral `Credential` and
//! maps domain errors to PG error codes at the boundary. No global shared-session
//! model. Design: `design/sql-path.md` P4.

// TODO(P4): implement the pgwire transport, startup/auth handshake, simple/
// extended query handlers, and Arrow->PG result encoding (via arrow-pg).
