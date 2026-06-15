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

//! P5 — Direct path request models + service.
//!
//! Direct read/write intents (KvUpsert / KvDelete / LogAppend; reads deferred
//! past Phase 1) executed via `BackendFacade`. Phase 1 writes are at-least-once
//! with only request-scoped timeout/cancel — no user-visible Operation. Does NOT
//! flow through the SQL execution chain. Design: `design/direct-path.md` P5.

// TODO(P5): define DirectWriteRequest (and deferred read) models and the direct
// service, including RequestExecutionContext carrying the principal.
