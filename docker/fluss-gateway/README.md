<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# apache/fluss-gateway

The FIP-49 REST gateway as a container image: a stateless HTTP entry point for
writing into, looking up from, and managing Fluss tables.

## Build

The gateway depends on the in-tree `fluss-rust` workspace by path, so the
build context is the repository root (the root `.dockerignore` keeps the
context small):

```bash
# from the repository root
docker build -t apache/fluss-gateway -f docker/fluss-gateway/Dockerfile .
```

## Quickstart

`docker-compose.yml` starts ZooKeeper, a CoordinatorServer, one TabletServer,
and the gateway:

```bash
docker compose -f docker/fluss-gateway/docker-compose.yml up -d
curl http://localhost:8080/health/live
```

Create a table and write through the gateway:

```bash
curl -X POST http://localhost:8080/v1/clusters/default/databases/fluss/tables \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"users","columns":[{"name":"id","data_type":{"type":"INT","nullable":false}},{"name":"name","data_type":{"type":"STRING","nullable":true}}],"primary_key":{"columns":["id"]}}'

curl -X POST http://localhost:8080/v1/clusters/default/databases/fluss/tables/users/records \
  -H 'Content-Type: application/json' \
  -d '{"entries":[{"id":"w1","upsert":{"id":1,"name":"alice"}}]}'

curl -X POST http://localhost:8080/v1/clusters/default/databases/fluss/tables/users/records/lookup \
  -H 'Content-Type: application/json' \
  -d '{"keys":[{"id":1}]}'
```

## Configuration

Everything is configured through a mounted `gateway.yaml` (flat dotted keys,
FIP-49) and/or `FLUSS_GATEWAY__*` environment overrides. The image defaults
listen on all interfaces:

| Variable | Default | Meaning |
| --- | --- | --- |
| `FLUSS_GATEWAY__SERVER_REST__BIND_ADDRESS` | `0.0.0.0:8080` | REST listener |
| `FLUSS_GATEWAY__SERVER_METRICS__BIND_ADDRESS` | `0.0.0.0:9095` | Prometheus exporter |
| `FLUSS_GATEWAY__CLUSTERS__DEFAULT__BOOTSTRAP_SERVERS` | `127.0.0.1:9123` | Fluss bootstrap servers |
| `RUST_LOG` | `info` | Log level |

To pass a full configuration file:

```bash
docker run -v $(pwd)/gateway.yaml:/etc/fluss-gateway/gateway.yaml:ro \
  -p 8080:8080 apache/fluss-gateway --config /etc/fluss-gateway/gateway.yaml
```

## Authentication

The compose file runs in `trust` mode (no credentials, suitable for local
demos only). `gateway.security.example.yaml` shows `password` mode with a
plaintext and a bcrypt (`htpasswd -B` compatible) user entry; it is verified
by the gateway's own configuration tests, so it cannot drift.

```bash
curl -u alice:secret123 http://localhost:8080/v1/clusters/default/databases
```

Per-user identity propagation to Fluss (`connection.identity-mode: user`,
SASL act-as) additionally requires a Fluss cluster with SASL/PLAIN
impersonation support — servers built from this branch include it; released
`apache/fluss` images may not yet.
