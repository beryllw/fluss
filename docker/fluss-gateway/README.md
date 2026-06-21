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

# apache/fluss-gateway (demo image)

A runnable Docker image for `fluss-gateway`. The container starts the gateway and
serves two frontends against a Fluss cluster:

- **PostgreSQL wire protocol** on `5432` — read-only SQL
- **REST API** on `8080` — direct write + metadata

This is a **demo image** intended for local testing. Real authentication and a
config file are not wired yet (the default authenticator trusts the username and
does not verify the password); a standardized/production build is planned later.

## Build

The build context is the gateway crate directory (`fluss-gateway/`); the binary
is compiled inside the image (multi-stage), so no host Rust toolchain is needed:

```bash
# from the repository root
docker build -t apache/fluss-gateway -f docker/fluss-gateway/Dockerfile fluss-gateway
```

The builder fetches the pinned `fluss-rust` git dependency, so the build needs
network access. The first build is slow (it compiles arrow/datafusion/fluss-rs).

## Run

The gateway needs a reachable Fluss cluster. Point it at your cluster's bootstrap
server and publish the two ports:

```bash
docker run --rm \
  -e FLUSS_BOOTSTRAP_SERVERS=host.docker.internal:9123 \
  -p 5432:5432 -p 8080:8080 \
  apache/fluss-gateway
```

### Configuration (environment variables)

| Variable                  | Default            | Meaning                              |
|---------------------------|--------------------|--------------------------------------|
| `FLUSS_BOOTSTRAP_SERVERS` | `127.0.0.1:9123`   | Fluss cluster bootstrap server(s)    |
| `GATEWAY_PG_LISTEN`       | `0.0.0.0:5432`     | PostgreSQL bind address              |
| `GATEWAY_REST_LISTEN`     | `0.0.0.0:8080`     | REST bind address                    |
| `FLUSS_CLUSTER`           | `default`          | Logical cluster id                   |
| `GATEWAY_CONFIG`          | unset              | YAML config file path                |
| `GATEWAY_USERS`           | unset              | `user:secret,...` auth override      |
| `GATEWAY_LAKE_S3_ACCESS_KEY` | unset          | lake (Paimon) S3 access key (server omits it) |
| `GATEWAY_LAKE_S3_SECRET_KEY` | unset          | lake (Paimon) S3 secret key (server omits it) |
| `GATEWAY_LAKE_S3_ENDPOINT` | unset            | lake S3 endpoint override (optional) |
| `GATEWAY_LAKE_S3_REGION`  | unset              | lake S3 region override (optional)   |
| `GATEWAY_LAKE_S3_PATH_STYLE_ACCESS` | unset    | `true` for MinIO/RustFS/path-style   |
| `GATEWAY_LAKE_STORAGE_OPTIONS` | unset         | generic `key=value,...` lake options (e.g. `oss.*`) |
| `RUST_LOG`                | `info`             | Tracing filter                       |

The gateway retries the cluster connection a few times at startup, so it can be
launched slightly before the cluster is ready.

## Connection recovery

If the shared Fluss connection dies at runtime (its RPC I/O task stops), the
gateway detects it on the next query and automatically rebuilds + hot-swaps a
fresh connection — no restart needed. Rebuilds are **bounded** (at most a few
attempts with exponential backoff), so a persistently unreachable cluster fails
fast with a clear error instead of looping forever. Read/metadata queries retry
once after a successful rebuild; writes are not auto-retried (at-least-once), so
the client should resubmit.

## Authentication

When **no users are configured**, the gateway stays in Phase-1 trust mode for
compatibility: the username is accepted as-is and the password is ignored.
When users are configured, both PostgreSQL and REST require a matching
username/password.

### YAML config file

Point `GATEWAY_CONFIG` at a YAML file:

```yaml
auth:
  users:
    - username: alice
      password: secret123
    - username: bob
      password: "sha256:8ed3f6ad685b959ead7022518e1af76cd816f8e8ec7ccdda1ed4018e8f2223f8"
```

Supported password formats:

- plaintext: `secret123`
- `sha256:<hex>` — the client still sends the **plaintext** password; the gateway
  hashes it and compares the digest in constant time

### Env override

`GATEWAY_USERS` is a fast override/append path for local runs and Compose:

```bash
export GATEWAY_USERS='alice:secret123,bob:sha256:8ed3f6ad685b959ead7022518e1af76cd816f8e8ec7ccdda1ed4018e8f2223f8'
```

The gateway loads users from the YAML file first, then overlays `GATEWAY_USERS`
by username. Secrets may contain `:` (only the first `:` splits user/secret).

### Example run

```bash
docker run --rm \
  -e FLUSS_BOOTSTRAP_SERVERS=host.docker.internal:9123 \
  -e GATEWAY_CONFIG=/etc/fluss/auth.yaml \
  -v "$PWD/auth.yaml:/etc/fluss/auth.yaml:ro" \
  -p 5432:5432 -p 8080:8080 \
  apache/fluss-gateway
```

### Client examples

```bash
# PostgreSQL
PGPASSWORD=secret123 psql "host=127.0.0.1 port=5432 user=alice dbname=fluss sslmode=disable" -c "SELECT 1"

# REST
curl -u alice:secret123 http://127.0.0.1:8080/v1/clusters/default/databases
```

## Quick verification

PostgreSQL (read-only SQL):

```bash
PGPASSWORD=ignored psql "host=127.0.0.1 port=5432 user=alice dbname=fluss sslmode=disable" -c "SELECT 1"
```

Inspect metadata over PostgreSQL — `information_schema`, `pg_catalog`, and psql
backslash commands all work:

```bash
PSQL='psql "host=127.0.0.1 port=5432 user=alice password=ignored dbname=fluss sslmode=disable"'

# list tables (standard SQL)
eval $PSQL -c "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema');"
# list columns of a table
eval $PSQL -c "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='<table>';"
# psql shortcuts
eval $PSQL -c "\dt"
eval $PSQL -c "\d <table>"
```

Read data:

```bash
# KV: full primary-key point lookup
eval $PSQL -c "SELECT * FROM <kv_table> WHERE id = 1;"
# KV: prefix lookup (bucket key is a strict prefix of the PK)
eval $PSQL -c "SELECT * FROM <kv_table> WHERE c1 = 10;"
# KV: bounded scan
eval $PSQL -c "SELECT * FROM <kv_table> LIMIT 10;"
# KV: full snapshot scan (no key predicate, no LIMIT)
eval $PSQL -c "SELECT * FROM <kv_table>;"
# Log: bounded scan / full snapshot
eval $PSQL -c "SELECT * FROM <log_table> LIMIT 10;"
```

Point lookup, prefix lookup, bounded `LIMIT` scan, and full snapshot scan are all
supported. Note: `count(*)` (and other empty-column-projection queries) currently
error on the KV path ("Column indices cannot be empty") — select at least one
column.

Troubleshooting note: if multiple PostgreSQL clients appear to "hang" while
running KV point/prefix lookups, the likely cause is lookup-path serialization,
not TCP accept or auth. All sessions share one `FlussConnection`, and its KV
lookup client batches/queues concurrent lookups behind a background sender, so
fresh concurrent clients can show staircase latency on the first KV queries.

REST (metadata + DDL + write):

```bash
# list databases
curl -u alice:ignored http://127.0.0.1:8080/v1/clusters/default/databases

# create a table (POST to the tables collection; name in body)
curl -u alice:ignored -H 'Content-Type: application/json' \
  -X POST http://127.0.0.1:8080/v1/clusters/default/databases/fluss/tables \
  -d '{
        "table_name": "gw_kv",
        "columns": [
          {"name": "id",   "type": "INT",    "nullable": false},
          {"name": "name", "type": "STRING"}
        ],
        "primary_key": ["id"],
        "distribution": {"bucket_keys": ["id"], "bucket_count": 1}
      }'

# write rows into an existing KV/Log table `t` in database `db`
curl -u alice:ignored -H 'Content-Type: application/json' \
  -X POST http://127.0.0.1:8080/v1/clusters/default/databases/db/tables/t/records \
  -d '[{"id":1,"name":"alice"}]'

# drop a table
curl -u alice:ignored -X DELETE \
  http://127.0.0.1:8080/v1/clusters/default/databases/fluss/tables/gw_kv
```

Create returns `201` (or `200` with `"validate_only": true` to dry-run), `409`
if the table already exists. See `fluss-gateway/design/direct-path.md` for the
full request schema (column types, `configs`, `validate_only`).

### JSON write value encodings

All Fluss column types are writable over the JSON record body. Most map to the
obvious JSON value; the non-numeric types use a string encoding:

| Column type            | JSON value                                            |
|------------------------|-------------------------------------------------------|
| `BOOLEAN`              | `true` / `false`                                      |
| `TINYINT`…`BIGINT`     | number (`7`)                                          |
| `FLOAT` / `DOUBLE`     | number (`1.5`)                                        |
| `DECIMAL(p,s)`         | number or string (`3.14`)                             |
| `CHAR(n)` / `STRING`   | string (`"hello"`)                                    |
| `DATE`                 | string `"YYYY-MM-DD"` (`"2024-03-15"`)                |
| `TIME(p)`              | string `"HH:MM:SS"` (`"12:34:56"`)                    |
| `TIMESTAMP(p)`         | string `"YYYY-MM-DDTHH:MM:SS.fff"`                    |
| `BINARY(n)` / `BYTES`  | **hex** string (`"00ff10"`) — not base64             |

Use the Arrow IPC stream body (`Content-Type: application/vnd.apache.arrow.stream`)
to write pre-typed columns directly without the JSON string encodings.

## Local test stack

A local test stack can be launched with:

- `docker/fluss-gateway/docker-compose.yml`

It starts:
- a Fluss cluster (`zookeeper`, `coordinator-server`, `tablet-server`)
- the `apache/fluss-gateway` container
- a minimal Flink stack (`jobmanager`, `taskmanager`, `sql-client`) so you can
  create tables before testing REST write + PG read through the gateway

Example:

```bash
cd docker/fluss-gateway
docker compose up -d
```

Then enter the SQL client to create a test table:

```bash
docker compose run sql-client
```

## Notes

- Use `host.docker.internal` (Docker Desktop) / `host.containers.internal`
  (Podman) to reach a Fluss cluster running on the host. On a shared Docker
  network, use the coordinator's service name instead.
- Deferred for later rounds: real PG/REST authentication, a config file, and a
  standardized multi-arch production build.
