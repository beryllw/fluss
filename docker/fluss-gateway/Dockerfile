#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# ---------------------------------------------------------------------------
# fluss-gateway image. Multi-stage: compile the Linux binary in a Rust
# builder, then ship a slim runtime.
#
# The gateway depends on the in-tree fluss-rust workspace by path and the
# fluss-rs build script reads the canonical proto from fluss-rpc, so the
# build context is the repository root (see the root .dockerignore):
#
#   docker build -t apache/fluss-gateway -f docker/fluss-gateway/Dockerfile .
# ---------------------------------------------------------------------------

# --- builder ---
FROM rust:1.93-bookworm AS builder

# protoc is required by the fluss-rs build script. TLS uses rustls/ring, so no
# openssl is needed.
RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends protobuf-compiler ca-certificates pkg-config; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build
# The path dependency chain: fluss-gateway -> ../fluss-rust/crates/fluss,
# whose build script reads ../../../fluss-rpc/src/main/proto/FlussApi.proto.
COPY fluss-rust ./fluss-rust
COPY fluss-rpc/src/main/proto ./fluss-rpc/src/main/proto
COPY fluss-gateway ./fluss-gateway
RUN cargo build --release --manifest-path fluss-gateway/Cargo.toml --bin fluss-gateway

# --- runtime ---
FROM debian:bookworm-slim

RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    groupadd --system --gid=9999 fluss; \
    useradd --system --home-dir /opt/fluss-gateway --create-home --uid=9999 --gid=fluss fluss

COPY --from=builder /build/fluss-gateway/target/release/fluss-gateway /usr/local/bin/fluss-gateway

# Container defaults: listen on all interfaces instead of the loopback the
# bare binary defaults to. Everything else is configured through a mounted
# gateway.yaml (--config) or FLUSS_GATEWAY__* overrides. Deliberately no
# bootstrap-servers ENV here: environment overrides beat the configuration
# file, so baking one in would silently shadow a mounted gateway.yaml (the
# binary's built-in default is 127.0.0.1:9123 already).
ENV FLUSS_GATEWAY__SERVER_REST__BIND_ADDRESS=0.0.0.0:8080 \
    FLUSS_GATEWAY__SERVER_METRICS__BIND_ADDRESS=0.0.0.0:9095 \
    RUST_LOG=info

USER fluss
# 8080 = REST (data plane + metadata); 9095 = internal Prometheus exporter.
EXPOSE 8080 9095

ENTRYPOINT ["fluss-gateway"]
