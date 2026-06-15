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

//! P5 — REST frontend (axum routes / handlers).
//!
//! The only write path in Phase 1, with at-least-once semantics; also serves
//! read-only metadata endpoints. Parses `Authorization: Basic` into a neutral
//! `Principal`. Multi-cluster evolution uses path prefixes, not headers.
//! Design: `design/direct-path.md` P5.

// TODO(P5): define the axum router, direct-write handlers, metadata read
// endpoints, and Basic-auth -> Principal extraction.
