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

//! P6 — FlussConnectionProvider.
//!
//! `resolve(cluster, principal) -> shared FlussConnection`. Phase 1 returns a
//! shared proxy-account connection for all principals (no doAs), but keeps the
//! `principal` argument so the call site does not change when per-user creds
//! land later. Design: `design/infra.md` §P6.5.

// TODO(P6): define the FlussConnectionProvider trait and the shared proxy-account
// Phase 1 implementation (backed by a gateway-defined connection contract trait,
// NOT a direct fluss-rs dependency).
