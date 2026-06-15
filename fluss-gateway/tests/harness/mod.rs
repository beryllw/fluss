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

//! T1-T4 — shared integration test harness.
//!
//! Assembles `Instance` + protocol frontends for harness-based integration tests
//! (protocol behavior / equivalence / timeout-cancel / write semantics).
//! Prefer this over ad hoc server bootstrapping per CLAUDE.md.
//! Design: `design/infra.md` §P8.5.

// TODO(T1-T4): provide a GatewayHarness builder that spins up an Instance and the
// PG / REST frontends for tests to drive.
