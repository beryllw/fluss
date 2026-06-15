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

//! P3 — SQL execution orchestration.
//!
//! Drives per-session `SessionContext` construction/rebuild and statement
//! execution, integrating with fluss-datafusion through a narrow, gateway-defined
//! contract (no direct fluss-datafusion dependency in Phase 1).
//! Design: `design/sql-path.md` P3 and `DESIGN.md` §3.3 (integration model).

// TODO(P3): define the SQL gateway service surface and the SessionContext build/
// rebuild flow (new ctx -> register fluss catalog -> apply SqlEnvironmentProvider).
