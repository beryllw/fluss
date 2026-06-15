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

//! P2 — GatewaySession / SessionVars / SessionManager + Operation /
//! OperationManager.
//!
//! Session is connection-scoped state; Operation is query-scoped state. Only the
//! SQL path exposes user-visible Operations. `SessionVars` is the single source
//! of truth for mutable session state; live mutation must be idempotent.
//! Design: `design/core-session.md` P2 and `DESIGN.md` §2.

pub mod manager;
pub mod operation;
// The `session` submodule holds the `GatewaySession` object itself; the repeated
// name is intentional and not module inception in the problematic sense.
#[allow(clippy::module_inception)]
pub mod session;
pub mod vars;

pub use manager::{SessionManager, SessionManagerConfig};
pub use operation::{Operation, OperationManager};
pub use session::{effective_timeout, GatewaySession, SessionContextBuilder};
pub use vars::apply_session_mutation;
