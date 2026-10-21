# FIP-0003: Fluss Gateway REST / Direct Path

> Status: Draft
>
> 中文版: [0003-rest-path.md](./0003-rest-path.md)
>
> This FIP defines the Phase 1 REST / direct path for `fluss-gateway`, including the request-scoped execution model, direct read/write and metadata APIs, OTLP over HTTP adaptation, and write-semantics boundaries.
>
> Overall architecture: [0001-gateway-framework.md](./0001-gateway-framework.md)
>
> Related SQL-path design: [0002-sql-path.md](./0002-sql-path.md)

|                    | TBD |
|--------------------|-----|
| Discussion thread  | TBD |
| Vote thread        | TBD |
| ISSUE              | TBD |
| Release            | TBD |

## Motivation

`fluss-gateway` Phase 1 needs a direct protocol path that is independent of the SQL session model.

The SQL path serves BI / IDE clients through a read-only PostgreSQL wire protocol, but it is not the right abstraction for all access patterns. Programmatic read/write traffic, service-to-service access, and telemetry ingest need a simpler request model:

- no SQL parser or planner in the request path;
- no connection-scoped session state;
- no user-visible operation registry for direct APIs;
- explicit request-scoped timeout and cancellation;
- a single write-semantics model for REST JSON, Arrow IPC, and OTLP over HTTP.

Without a dedicated direct path, the gateway would either force HTTP requests through SQL concepts that do not fit, or duplicate backend access logic in protocol adapters. This FIP defines a protocol-agnostic core interface for direct access while keeping REST and OTLP logic at the boundary.

## Public Interfaces

This proposal introduces or standardizes the following public contracts for the gateway REST path.

### 1. Resource layout

The REST path uses cluster-prefixed URLs rather than headers for future multi-cluster evolution:

```text
/v1/clusters/{cluster}/...
```

Phase 1 only supports `default`, but the public path shape is defined with the cluster prefix from day one.

### 2. Metadata endpoints

Representative metadata resources:

```text
GET /v1/clusters/{cluster}/databases
GET /v1/clusters/{cluster}/databases/{db}/tables
GET /v1/clusters/{cluster}/databases/{db}/tables/{table}
```

These endpoints expose metadata only and do not create SQL sessions.

### 3. Direct gateway API shape

At the protocol-agnostic core, the REST path depends on neutral `Instance` capabilities:

```rust
async fn read_direct(&self, req: DirectReadRequest) -> Result<DirectReadResult>;
async fn write_direct(&self, req: DirectWriteRequest) -> Result<DirectWriteResult>;
async fn list_databases(&self, scope: MetadataScope) -> Result<Vec<String>>;
async fn list_tables(&self, scope: MetadataScope) -> Result<Vec<String>>;
async fn get_table_info(&self, scope: MetadataScope, table: TableRef) -> Result<TableInfo>;
```

Return values stay domain-native or Arrow-native. HTTP status codes, JSON payloads, and Arrow IPC encoding remain adapter concerns.

### 4. Request-scoped execution context

The direct path is driven by a request-scoped context:

```rust
struct RequestExecutionContext {
    principal: Principal,
    cluster: ClusterName,
    deadline: Option<Instant>,
    cancellation_token: CancellationToken,
}
```

### 5. Direct write semantics

Phase 1 direct writes expose **at-least-once** semantics:

- `2xx` means the gateway received backend acknowledgement;
- timeout or disconnect does not imply the write was cancelled;
- after timeout or disconnect, the final write outcome may be unknown;
- callers that retry must tolerate duplicate effects.

The same semantics apply to REST writes and OTLP-over-HTTP writes after adaptation into canonical direct writes.

## Proposed Changes

### 1. Define a dedicated direct path

REST is the direct protocol path of `fluss-gateway` in Phase 1:

- no session;
- no `SessionManager`, `GatewaySession`, or `SessionVars`;
- no user-visible direct operation lifecycle;
- all requests carry `principal`, `cluster`, and request-scoped timeout/cancel context explicitly.

This keeps connection-scoped SQL state fully decoupled from request-scoped direct access.

### 2. Keep the core protocol-agnostic

The REST adapter handles transport, authentication handshakes, request/response encoding, and protocol-specific error mapping. The core `Instance` facade owns direct read/write orchestration and metadata access. Backend-facing logic is shared with other protocol adapters instead of being duplicated in HTTP handlers.

### 3. Split metadata, direct read, and direct write concerns

The REST path exposes three categories of capabilities:

- metadata resources;
- direct read resources;
- direct write resources.

Metadata requests use metadata access only. Direct read/write requests use the direct service only. They do not pass through the SQL parser or planner.

### 4. Model direct reads independently from SQL

Direct reads are request-scoped, table-oriented operations. The model may include:

- `Lookup`;
- `BatchLookup`;
- `PrefixScan`;
- `LogScan`.

The request explicitly carries direct parameters such as table identity, projection, limit, or offsets. If some direct read APIs are not opened in the first implementation, the gateway should mark them as not yet implemented rather than routing them through SQL semantics.

### 5. Model direct writes explicitly

Phase 1 direct writes cover at least:

- `KvUpsert`;
- `KvDelete`;
- `LogAppend`.

Writes target existing table schemas:

- no schema-on-write;
- no SQL DML rewrite;
- no user-facing operation status lookup.

This keeps Phase 1 aligned with direct backend capabilities and avoids introducing a false transactional model.

### 6. Standardize timeout, disconnect, and cancellation behavior

For direct reads:

- use request-scoped `deadline` and `CancellationToken`;
- on client disconnect, timeout, or internal cancellation, stop local work on a best-effort basis and propagate cancellation downstream when possible.

For direct writes:

- no user-level cancel API is exposed;
- the gateway may stop local work before backend dispatch;
- once a write has been dispatched, timeout or disconnect must not be interpreted as successful cancellation.

This avoids giving clients a misleading guarantee that cancellation is deterministic.

### 7. Treat OTLP over HTTP as a direct adapter

OTLP over HTTP shares the same HTTP listener as REST, but it is not a third execution path. It is a specialized direct adapter that:

- decodes OTLP protobuf at the HTTP boundary;
- maps logs / metrics / traces into canonical direct write models;
- writes into fixed per-signal tables through `LogAppend`;
- does not leak OTLP wire types into `instance/` or `backend/`.

This preserves a clean boundary between protocol adaptation and gateway-core domain models.

### 8. Preserve `principal` through the full direct path

Authentication happens at the HTTP boundary and produces a neutral `Principal`. The direct path stores it in `RequestExecutionContext` and threads it into metadata access, backend orchestration, and connection resolution. Even if Fluss does not consume `principal` yet, the gateway must preserve it to keep the path open for future auditing and per-principal routing.

## Compatibility, Deprecation, and Migration Plan

- This is an additive Phase 1 design for a new REST / direct path; it does not remove any existing public SQL interface.
- The path layout uses `/v1/clusters/{cluster}/...` from the beginning. Phase 1 implementations may only accept `default`, but no migration from header-based cluster routing will be needed later.
- Direct writes intentionally expose at-least-once semantics from day one. There is no stronger existing write contract to deprecate.
- The proposal does not require migration tools. Clients adopting the REST path must explicitly handle timeout/disconnect as potentially unknown write outcomes.
- OTLP over HTTP is defined as an adapter onto canonical direct writes rather than as a separate execution stack, reducing future migration cost when more direct protocols are added.

## Test Plan

The implementation should be validated mainly through integration tests and protocol-equivalence tests.

1. Metadata API tests
   - list databases, list tables, and get table info through REST;
   - verify the results match the same Fluss truth source used by the SQL path.

2. Direct write tests
   - `KvUpsert`, `KvDelete`, and `LogAppend` succeed against real backend tables;
   - verify at-least-once behavior under retry, timeout, and disconnect scenarios.

3. Direct read tests
   - validate request-scoped projection, limit, and table-oriented read semantics;
   - verify cancellation and deadline propagation on long-running reads.

4. OTLP over HTTP tests
   - send OTLP payloads through the shared HTTP listener;
   - verify they are converted into canonical direct writes and land in the correct per-signal tables.

5. Boundary tests
   - verify REST requests do not create SQL sessions or SQL operations;
   - verify OTLP wire types do not cross into `instance/` or `backend/`.

## Rejected Alternatives

### 1. Route REST writes through SQL DML

Rejected because direct writes are not SQL-shaped operations, and forcing them through SQL planning would add the wrong abstractions, unnecessary coupling to SQL session state, and avoidable overhead.

### 2. Reuse SQL session and operation lifecycle for REST

Rejected because direct APIs are request-scoped rather than connection-scoped. Reusing SQL session and operation semantics would complicate the model and create a misleading user contract.

### 3. Treat OTLP as a third execution stack

Rejected because OTLP only needs protocol adaptation at the boundary. A separate execution core would duplicate write-path logic and leak wire-specific types downward.

### 4. Use headers instead of path prefixes for cluster routing

Rejected because path prefixes are clearer, easier to reason about in HTTP APIs, and align better with the planned multi-cluster evolution.
