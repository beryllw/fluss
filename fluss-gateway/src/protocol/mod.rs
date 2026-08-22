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

//! The gateway's REST API surface.
//!
//! The adapter is blind to how the backend reaches Fluss: it resolves a cluster, validates the request,
//! calls a [`crate::backend::FlussBackend`] method, and maps the returned error to a status. The test
//! below holds that boundary in place.

pub mod rest;

#[cfg(test)]
mod tests {
    /// The protocol layer names no part of the connection layer.
    ///
    /// Checked against the sources because the leak this prevents is one that compiles: reaching for a
    /// pool, a key, or an identity mode here would move connection policy into the adapter, which is
    /// exactly what the backend boundary exists to prevent.
    #[test]
    fn the_protocol_layer_names_no_connection_layer_symbol() {
        const FORBIDDEN: &[&str] = &[
            "FlussConnection",
            "ConnectionPool",
            "ConnectionKey",
            "IdentityPolicy",
            "IdentityMode",
            "connection.max",
            "connection.idle-timeout",
        ];
        let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/protocol");
        // This file spells the forbidden names out to declare the rule; it holds nothing else.
        let declaring_file = directory.join("mod.rs");
        let mut checked = 0;
        let mut pending = vec![directory];
        while let Some(path) = pending.pop() {
            for entry in std::fs::read_dir(&path).expect("the protocol sources are readable") {
                let path = entry.expect("a directory entry").path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                if path == declaring_file {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("a readable source file");
                for symbol in FORBIDDEN {
                    assert!(
                        !source.contains(symbol),
                        "{} names {symbol}",
                        path.display()
                    );
                }
                checked += 1;
            }
        }
        assert!(
            checked >= 6,
            "only {checked} protocol source files were read"
        );
    }
}
