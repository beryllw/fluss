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

# Apache Fluss (incubating) is an effort undergoing incubation at The Apache
# Software Foundation (ASF), sponsored by the Apache Incubator PMC.
#
# Incubation is required of all newly accepted projects until a further review
# indicates that the infrastructure, communications, and decision making process
# have stabilized in a manner consistent with other successful ASF projects.
#
# While incubation status is not necessarily a reflection of the completeness
# or stability of the code, it does indicate that the project has yet to be
# fully endorsed by the ASF.

# ---------------------------------------------------------------------------
# fluss-gateway image (demo). Multi-stage: compile the Linux binary in a Rust
# builder, then ship a slim runtime. Build context is the gateway crate dir:
#   docker build -t apache/fluss-gateway -f docker/fluss-gateway/Dockerfile fluss-gateway
# ---------------------------------------------------------------------------

# --- builder ---
FROM rust:1.93-bookworm AS builder

# protoc is required by the fluss-rs build script; git is needed to fetch the
# pinned fluss-rust dependency. TLS uses rustls/ring, so no openssl is needed.
# Use the Aliyun Debian mirror for faster apt downloads (bookworm DEB822 format,
# with the legacy sources.list handled too for portability).
RUN set -ex; \
    sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com|g' \
        /etc/apt/sources.list.d/debian.sources /etc/apt/sources.list 2>/dev/null || true; \
    apt-get update; \
    apt-get install -y --no-install-recommends protobuf-compiler git ca-certificates pkg-config; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build
# Manifests first, then sources (kept simple; no cargo-chef layer caching).
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin fluss-gateway

# --- runtime ---
FROM debian:bookworm-slim

RUN set -ex; \
    sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com|g' \
        /etc/apt/sources.list.d/debian.sources /etc/apt/sources.list 2>/dev/null || true; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    groupadd --system --gid=9999 fluss; \
    useradd --system --home-dir /opt/fluss-gateway --create-home --uid=9999 --gid=fluss fluss

COPY --from=builder /build/target/release/fluss-gateway /usr/local/bin/fluss-gateway

# Defaults suit a container talking to a Fluss cluster on the same host/network.
# Override at `docker run` time with -e.
ENV FLUSS_BOOTSTRAP_SERVERS=127.0.0.1:9123 \
    GATEWAY_PG_LISTEN=0.0.0.0:5432 \
    GATEWAY_REST_LISTEN=0.0.0.0:8080 \
    FLUSS_CLUSTER=default \
    RUST_LOG=info

USER fluss
# 5432 = PostgreSQL wire (read-only SQL); 8080 = REST (direct write + metadata).
EXPOSE 5432 8080

ENTRYPOINT ["fluss-gateway"]
