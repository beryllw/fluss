<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Apache Fluss Gateway

A stateless REST gateway for Apache Fluss. It exposes REST APIs for writing to
Fluss tables and performing DDL operations, while keeping no session, cursor,
or replay state: any instance can serve any request behind a plain load
balancer.

The gateway is an executable, not a library on crates.io, and it is its own
Cargo workspace so its dependencies never touch the `fluss-rust` workspace's
lock file or its generated dependency inventories.

## Status

The gateway currently serves the process runtime and REST metadata discovery:
configuration loading and validation (`gateway.yaml` with flat dotted keys), the
HTTP listener with its request-id, body-size and deadline middleware,
`GET /health`, `GET /ready`, `GET /v1/openapi.json`, `GET /v1/clusters`,
`GET /v1/clusters/{cluster}/databases`,
`GET /v1/clusters/{cluster}/databases/{database}/tables`, the Prometheus
listener, and the backend runtime that owns one shared service connection per
configured cluster. A connection is opened lazily on the first request that
needs it, shared by every request to that cluster, released after the configured
`connection.idle-timeout`, and drained during shutdown. Concurrent cold requests
serialize behind one connection attempt. Cancelling that request cancels its
attempt and lets the next waiter retry; bootstrap timeout and retry behavior remain
owned by `fluss-rust`.
Connections use Fluss's default plaintext protocol unless
`connection.security.protocol: sasl` selects the configured service account. A
broken transport is left to the native client, which reconnects the affected
server on its own.
`connection.identity-mode: user` is refused at startup until Fluss supports
act-as. The describe-table, partition, DDL, write, and lookup APIs, and client
authentication, land in follow-up pull requests.

## Prerequisites

- Rust toolchain managed by [rustup](https://rustup.rs); the workspace pins the
  channel in `rust-toolchain.toml` (stable with `rustfmt` and `clippy`)
- The declared minimum supported Rust version is 1.88, enforced by the
  `gateway-msrv` CI job
- [`just`](https://github.com/casey/just) for the recipes below

## Build and test

Run everything from this directory, or use the `just` recipes:

```bash
just build        # cargo build --all-targets
just test         # cargo test --all-targets
just test-e2e     # real Gateway + Dockerized Fluss cluster
just fmt-check    # cargo fmt --all -- --check
just clippy       # cargo clippy --all-targets -- -D warnings
just doc          # RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
just licenses     # cargo deny check licenses
```

The E2E test requires Docker. It builds and invokes the `fluss-test-cluster`
helper from the `fluss-rust` workspace, creates catalog objects in a real Fluss
cluster, then verifies the cluster, database, and table REST APIs through both
its plaintext and SASL endpoints. Set `FLUSS_IMAGE` and `FLUSS_VERSION` to
select the Fluss image; CI builds and uses `fluss:dev` from the same source
revision.

Plaintext is the default and must not carry service credentials. To use
SASL/PLAIN, configure all three options explicitly:

```yaml
gateway.cluster.default.connection.security.protocol: sasl
gateway.cluster.default.connection.service.account: gateway_svc
gateway.cluster.default.connection.service.secret: change-me
gateway.cluster.default.connection.idle-timeout: 10m
```

The MSRV can be verified locally with `cargo +1.88.0 check --all-targets`.

## CI

Gateway changes are gated by a dedicated workflow, `.github/workflows/gateway-ci.yml`
(build and tests on Linux/macOS, MSRV 1.88, license headers, dependency
licenses via `cargo-deny`, formatting, clippy, rustdoc). Gateway-only changes
are excluded from the Java CI, mirroring `fluss-rust`, and the gateway workflow
never builds the `fluss-rust` workspace.

## License enforcement

Like `fluss-rust`, the gateway carries its own license enforcement: source
headers are checked by `skywalking-eyes` (`.licenserc.yaml`) and dependency
licenses by `cargo-deny` (`deny.toml`). Both run in CI.
