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

//! Internal implementation crate of the Fluss Gateway.
//!
//! The gateway is a stateless REST front end for Apache Fluss (FIP-49): it
//! keeps no session, cursor, or replay state, so any instance can serve any
//! request behind a plain load balancer. This change only reserves the crate
//! layout; the runtime modules arrive with the Gateway foundation change.

#[cfg(test)]
mod tests {
    // A single smoke test so the CI build-and-test gate proves the test
    // harness is wired up from day one; real suites arrive with the runtime.
    #[test]
    fn crate_layout_is_wired() {
        assert_eq!(env!("CARGO_PKG_NAME"), "fluss-gateway");
    }
}
