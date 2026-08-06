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

The FIP-49 REST gateway: a stateless HTTP entry point for writing into,
looking up from, and managing Fluss tables. This guide is a hands-on tour —
every request below was executed against a real cluster, and the responses
are pasted from those runs.

**Contents:** [Quickstart](#quickstart) · [Create tables](#story-1-create-your-tables) ·
[Write data](#story-2-write-data) · [Read data](#story-3-read-it-back) ·
[Evolve and clean up](#story-4-evolve-and-clean-up) ·
[Authentication](#story-5-lock-the-door) · [Identity to Fluss](#story-6-carry-identity-into-fluss) ·
[Backpressure](#when-the-cluster-pushes-back) · [Errors & metrics](#errors-and-observability) ·
[Configuration](#configuration) · [E2E test suite](#running-the-end-to-end-suite)

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
and the gateway in `trust` mode (no credentials — local demos only):

```bash
docker compose -f docker/fluss-gateway/docker-compose.yml up -d

curl http://localhost:8080/health/live
# {"status":"live"}

curl http://localhost:8080/v1/clusters
# {"clusters":[{"id":"default","state":"available"}]}
```

`/health/live` says the process is up; `/health/ready` says it is ready to
serve; cluster discovery shows whether the backend connection to Fluss is
established. Everything below runs against this stack.

> Prefer running the gateway from source? Point it at any reachable cluster:
> `cd fluss-gateway && cargo run -- --config gateway.yaml` with
> `gateway.cluster.default.bootstrap.servers` set accordingly. The same
> requests apply unchanged.

## Story 1: create your tables

Create a database (the compose cluster ships with a default `fluss`
database):

```bash
curl -X POST http://localhost:8080/v1/clusters/default/databases \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo"}'
# {"name":"demo","custom_properties":{},"created_time":...}  → 201
```

Dry-run a table definition first — `validate_only` runs the whole
gateway-side validation chain (body shape, type vocabulary, primary-key
rules) without creating anything:

```bash
curl -X POST http://localhost:8080/v1/clusters/default/databases/demo/tables \
  -H 'Content-Type: application/json' \
  -d '{
    "validate_only": true,
    "table_name": "orders",
    "columns": [
      {"name": "region",   "data_type": {"type": "STRING", "nullable": false}},
      {"name": "order_id", "data_type": {"type": "BIGINT", "nullable": false}},
      {"name": "amount",   "data_type": {"type": "DOUBLE", "nullable": true}}
    ],
    "primary_key": {"columns": ["region", "order_id"]},
    "distribution": {"bucket_count": 1, "bucket_keys": ["region"]}
  }'
# {"validate_only":true,"database":"demo","table":"orders","column_count":3,
#  "primary_key":["region","order_id"]}  → 200
```

Drop `"validate_only": true` from the same body to create it for real. The
gateway answers `201` with a `Location` header and the full description —
note the derived capabilities:

```bash
# → 201, location: /v1/clusters/default/databases/demo/tables/orders
# {"database":"demo","table_name":"orders", ..., "kind":"PRIMARY_KEY",
#  "capabilities":{"exact_lookup_supported":true,"prefix_lookup_supported":true}, ...}
```

A table without a primary key is a **log table** — an append-only stream:

```bash
curl -X POST http://localhost:8080/v1/clusters/default/databases/demo/tables \
  -H 'Content-Type: application/json' \
  -d '{
    "table_name": "events",
    "columns": [
      {"name": "ts",      "data_type": {"type": "BIGINT", "nullable": false}},
      {"name": "message", "data_type": {"type": "STRING", "nullable": true}}
    ]
  }'
# → 201, "kind":"LOG"
```

Inspect and list (listing uses stateless keyset pagination —
`?max_results=` plus the returned `next_page_token`):

```bash
curl http://localhost:8080/v1/clusters/default/databases/demo/tables/orders
curl "http://localhost:8080/v1/clusters/default/databases/demo/tables?max_results=100"
# {"tables":["events","orders"]}
```

## Story 2: write data

One request carries a batch of entries; each entry is exactly one of
`upsert`, `delete`, or `append`, plus an `id` the caller chooses — every
per-entry outcome echoes it back.

**Primary-key tables** take `upsert` and `delete`, mixed freely:

```bash
curl -X POST http://localhost:8080/v1/clusters/default/databases/demo/tables/orders/records \
  -H 'Content-Type: application/json' \
  -d '{"entries":[
    {"id":"w1","upsert":{"region":"eu","order_id":1,"amount":9.5}},
    {"id":"w2","upsert":{"region":"us","order_id":2,"amount":20}},
    {"id":"w3","delete":{"region":"eu","order_id":999}}
  ]}'
# {"row_count":3,"success_count":3,"error_count":0,
#  "successes":[{"id":"w1"},{"id":"w2"},{"id":"w3"}],"failures":[]}  → 200
```

**Log tables** take `append`:

```bash
curl -X POST http://localhost:8080/v1/clusters/default/databases/demo/tables/events/records \
  -H 'Content-Type: application/json' \
  -d '{"entries":[{"id":"e1","append":{"ts":"1700000000000","message":"hello"}}]}'
# {"row_count":1,"success_count":1,"error_count":0,"successes":[{"id":"e1"}],"failures":[]}
```

**Partial update** targets a column subset on a primary-key table; untargeted
columns keep their current values:

```bash
curl -X POST http://localhost:8080/v1/clusters/default/databases/demo/tables/orders/records \
  -H 'Content-Type: application/json' \
  -d '{"partial_update_columns":["region","order_id","amount"],
       "entries":[{"id":"p1","upsert":{"region":"eu","order_id":1,"amount":42.0}}]}'
```

**Partial success is the contract.** The request-level status stays `200` as
long as the request itself was valid; individual entries report their own
verdicts inside `failures[]` (see [backpressure](#when-the-cluster-pushes-back)
for the retriable case). Requests that are invalid as a whole are rejected
up front — the table schema is the source of truth, checked before any row
is submitted:

```bash
curl -X POST http://localhost:8080/v1/clusters/default/databases/demo/tables/orders/records \
  -H 'Content-Type: application/json' \
  -d '{"entries":[{"id":"bad","upsert":{"region":"eu","order_id":3,"amountt":1}}]}'
# {"error":{"code":"invalid_argument","message":"entry `bad`: unknown column `amountt`",
#  "request_id":"...","retryable":false}}  → 400
```

## Story 3: read it back

**Point lookup** by full primary key — misses are `found: false`, not errors:

```bash
curl -X POST http://localhost:8080/v1/clusters/default/databases/demo/tables/orders/records/lookup \
  -H 'Content-Type: application/json' \
  -d '{"keys":[{"region":"eu","order_id":1},{"region":"eu","order_id":404}]}'
# {"schema_id":1,"results":[
#   {"input_index":0,"found":true,"row":{"amount":9.5,"order_id":"1","region":"eu"}},
#   {"input_index":1,"found":false,"row":null}]}
```

**Prefix lookup** scans by a key prefix. `prefix_columns` must cover the
table's partition keys plus its bucket keys in declared order — here the
bucket key `region`:

```bash
curl -X POST http://localhost:8080/v1/clusters/default/databases/demo/tables/orders/records/prefix-lookup \
  -H 'Content-Type: application/json' \
  -d '{"prefix_columns":["region"],"prefixes":[{"region":"eu"}]}'
# {"schema_id":1,"max_rows_per_prefix":1000,"results":[
#   {"input_index":0,"row_count":1,"truncated":false,
#    "rows":[{"amount":9.5,"order_id":"1","region":"eu"}]}]}
```

Note the type mapping: `BIGINT` values travel as JSON **strings** (`"1"`) in
both directions, preserving full 64-bit precision past JavaScript's 2^53
limit. Smaller integers and doubles stay JSON numbers.

## Story 4: evolve and clean up

Alter is a `PATCH` carrying ordered changes; each change names its `kind`
(`add_column`, `set_config`, `reset_config`):

```bash
curl -X PATCH http://localhost:8080/v1/clusters/default/databases/demo/tables/orders \
  -H 'Content-Type: application/json' \
  -d '{"changes":[{"kind":"add_column","name":"note","data_type":{"type":"STRING","nullable":true}}]}'
# → 200 with the new description; schema_id increments
```

Which options are alterable is decided by the Fluss server, and its answer
travels back through the envelope (released 0.9.x servers reject altering
`table.log.ttl`, for example).

Deletions answer `204` with no body, and the resource is gone:

```bash
curl -X DELETE http://localhost:8080/v1/clusters/default/databases/demo/tables/events
# → 204
curl http://localhost:8080/v1/clusters/default/databases/demo/tables/events
# {"error":{"code":"not_found","message":"the requested table does not exist", ...,
#  "details":{"resource_kind":"table","resource_name":"demo.events"}}}  → 404
```

## Story 5: lock the door

The compose file runs in `trust` mode: no credentials, and a `curl -u
alice:anything` Basic name is taken at face value. Production runs
`password` mode. `gateway.security.example.yaml` shows the shape — a
plaintext entry and a bcrypt (`htpasswd -B` compatible) entry; the example
file is parsed by the gateway's own configuration tests, so it cannot drift:

```yaml
gateway.security.authentication: password
gateway.security.users: "alice:secret123,bob:bcrypt:$2y$05$ZuVe..."
```

Mount it and point the gateway at it (in compose: add
`command: ["--config", "/etc/fluss-gateway/gateway.yaml"]` plus the volume).
Then:

```bash
curl -i http://localhost:8080/v1/clusters/default/databases
# HTTP/1.1 401 Unauthorized
# www-authenticate: Basic realm="fluss-gateway"
# {"error":{"code":"unauthenticated","message":"authentication failed", ...}}

curl -u alice:secret123 http://localhost:8080/v1/clusters/default/databases
# {"databases":["fluss"]}  → 200

curl http://localhost:8080/health/live
# {"status":"live"}  → 200 — health probes stay open; everything else is guarded
```

Unknown user and wrong password produce byte-identical 401 envelopes, so the
API cannot be used to enumerate accounts.

## Story 6: carry identity into Fluss

Independently of how clients authenticate to the gateway, the gateway
authenticates to Fluss. Two modes per cluster:

**`service` (default):** one shared connection as a service account. Without
credentials it is a plaintext connection; with them it is SASL/PLAIN:

```yaml
gateway.cluster.default.connection.service.account: gateway_svc
gateway.cluster.default.connection.service.secret: s3cret
```

Fluss then sees every request as `gateway_svc`. A wrong secret keeps the
cluster `unavailable` — the gateway never silently falls back to plaintext.

**`user`:** every authenticated principal gets its own connection carrying
the SASL authorization id, so Fluss authorizes each call as the end user
(act-as):

```yaml
gateway.cluster.default.connection.identity-mode: user
gateway.cluster.default.connection.max: 100
gateway.cluster.default.connection.idle-timeout: 10m
```

Requirements, enforced at startup: `service.account`/`service.secret` must be
set, and client authentication must not be `trust` (an anonymous caller must
never become an act-as identity). The Fluss server must support SASL/PLAIN
impersonation and grant it via the JAAS option
`impersonate_<account>="alice,bob"` (or `"*"`) — servers built from this
branch include it; released `apache/fluss` images may not yet. A principal
outside the allowlist gets `403 unauthorized`; when the pool is at
`connection.max` capacity, new identities get `429 resource_exhausted` with
`Retry-After`. The full journey (alice and bob acting as themselves, carol
refused) runs in the e2e suite below.

## When the cluster pushes back

Under sustained KV write pressure the server first asks the client to slow
down (the gateway's client throttles per bucket, surfacing only as latency),
then hard-rejects. After the client's retry budget is spent, the rejection
reaches the caller as an entry-level verdict — never as a whole-request HTTP
status:

```json
{"row_count": 400, "success_count": 361, "error_count": 39,
 "failures": [{"id": "e17", "error_code": "storage_backpressure",
               "completion": "rejected", "retryable": true, "message": "..."}]}
```

`completion: "rejected"` means the row is provably not written: retry exactly
the failed entries once pressure drains. The e2e suite drives a
backpressure-tuned cluster through this contract.

## Errors and observability

Every error is one envelope, and every response carries an `x-request-id`
header (also inside error bodies) for correlation:

```json
{"error": {"code": "not_found", "message": "the requested table does not exist",
           "request_id": "c94d446b-...", "retryable": false,
           "details": {"resource_kind": "table", "resource_name": "demo.orders"}}}
```

Codes are lowercase snake_case (`invalid_argument`, `unauthenticated`,
`unauthorized`, `not_found`, `already_exists`, `failed_precondition`,
`limit_exceeded`, `resource_exhausted`, `timeout`, `unavailable`,
`storage_backpressure`, ...); `retryable` tells the client whether backing
off and retrying can help.

Prometheus metrics are served on a separate port (`:9095` in compose):

```bash
curl http://localhost:9095/metrics
```

Key families: `fluss_gateway_rest_requests_total` /
`rest_request_duration_seconds` (per route and status),
`fluss_gateway_backend_write_rows_total` / `backend_write_bytes_total`,
`fluss_gateway_backend_connected`, `fluss_gateway_connections_*` (per-user
act-as pool), and the client-side `fluss_client_writer_kv_backpressure_*`.

The machine-readable API contract is served at `/v1/openapi.json`, generated
from the mounted routes themselves (guarded by authentication like every
non-health route).

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

Unknown keys are rejected at startup with the exact key name, so a typo can
never silently fall back to a default.

## Running the end-to-end suite

The scenarios above (and more: SASL service accounts, act-as, backpressure)
are automated in `fluss-gateway/tests/e2e_cluster.rs` against dockerized
clusters:

```bash
cd fluss-gateway
cargo test --features integration_tests --test e2e_cluster
```

The act-as and backpressure journeys need a Fluss image built from this
branch (SASL/PLAIN impersonation and the backpressure protocol); they skip
unless it is named:

```bash
mvn package -DskipTests -T 1C          # from the repository root
mkdir -p docker/fluss/build-target
tar -xzf fluss-dist/target/fluss-*-bin.tgz -C docker/fluss/build-target --strip-components=1
docker build -t apache/fluss:fip49-poc docker/fluss

FLUSS_IMPERSONATION_IMAGE=apache/fluss FLUSS_IMPERSONATION_VERSION=fip49-poc \
  cargo test --features integration_tests --test e2e_cluster
```

Container names and host ports are fixed; leave a few seconds between
back-to-back runs so the previous run's teardown finishes.
