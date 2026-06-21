# fluss-gateway — AI Assistant Instructions

## Project Context

- This file is the default AI context for the `fluss-gateway` module.
- Implementation repo: `/Users/boyu/IdeaProjects/fluss-community`
- Active branch: `feature/fluss-gateway`
- Language: Rust
- Target module root: `/Users/boyu/IdeaProjects/fluss-community/fluss-gateway`
- Upstream dependency under parallel development: `fluss-datafusion` in `/Users/boyu/IdeaProjects/fluss-rust-community`
- Design workspace and source-of-truth materials live in `/Users/boyu/AiWorkSpace/fluss-gateway`

## Source of Truth

Read these first before making gateway changes:

1. `/Users/boyu/AiWorkSpace/fluss-gateway/docs/FLUSS-GATEWAY-DESIGN.md`
2. `/Users/boyu/AiWorkSpace/fluss-gateway/docs/FLUSS_DATAFUSION.md`
3. `/Users/boyu/AiWorkSpace/fluss-gateway/CLAUDE.gateway.md`
4. `/Users/boyu/IdeaProjects/fluss-community/.claude/worktrees/ancient-noodling-canyon/fluss-query-gateway`
5. `/Users/boyu/AiWorkSpace/fluss-gateway/refs`

If implementation ideas conflict, prefer `FLUSS-GATEWAY-DESIGN.md`. Treat the old `fluss-query-gateway` as a reference baseline, not the target architecture.

## Architecture Rules / 架构规则

- Keep the gateway core protocol-agnostic.
- `Instance` is the unified facade for session, SQL, operation, direct read/write, and metadata access.
- Protocol modules only handle transport, adaptation, encoding/decoding, auth handshakes, and protocol-specific compatibility behavior.
- Separate SQL protocols from direct protocols.
  - SQL protocols: PostgreSQL now, Flight SQL / MySQL later
  - Direct protocols: REST now (table-oriented read/write) and OTLP over HTTP (telemetry ingest), gRPC later
- Do not force direct APIs through SQL execution.
- Reuse useful PostgreSQL compatibility libraries, but do not reuse a global shared-session model.
- Session is connection-scoped state; Operation is query-scoped state.
- SQL environment differences must be installed through `SqlEnvironmentProvider`, not hardcoded into `Instance`.
- PostgreSQL is read-only in Phase 1.
- Writes go only through direct protocols in Phase 1 (REST and OTLP over HTTP); PG does not write.
- OTLP over HTTP is a direct protocol adapter on the same HTTP listener: it decodes OTLP protobuf at the boundary and adapts each signal into a canonical `LogAppend` direct write to a fixed per-signal table. OTLP wire types never enter `instance`/`backend`.
- Direct write semantics are at-least-once.
- Preserve `principal` through the internal call chain even if Fluss does not consume it yet.
- REST multi-cluster evolution should use path prefixes, not headers.

## Technology Stack / 技术栈

| Component | Technology | Notes |
|---|---|---|
| Language | Rust | Main implementation language |
| Async runtime | Tokio + async/await | Long-lived service runtime |
| SQL engine | DataFusion + Arrow | Query planning and execution |
| PostgreSQL protocol | pgwire + arrow-pg + datafusion-pg-catalog | PG compatibility path |
| HTTP framework | axum | REST direct read/write path |
| Fluss access | fluss-rust client via connection provider | Shared proxy-account access in Phase 1 |
| Observability | tracing | Request and service instrumentation |

## Code Conventions / 编码规范

- Keep shared internal results Arrow-native where practical.
- Map domain errors to protocol-specific errors only at the boundary layer.
- `SessionVars` is the single source of truth for mutable session state.
- Live session mutation must be idempotent.
- Mark the SQL context dirty and rebuild before the next query when live application is unsafe.
- Do not destroy a `SessionContext` that is still used by a running operation.
- Keep prepared-statement wire lifecycle and any protocol-local caching in the PostgreSQL adapter.
- Thread cancellation tokens through long-running read paths when possible.
- Avoid empty future-oriented abstractions in MVP.
- Add comments only for non-obvious constraints.

## Module Boundaries / 模块边界

```text
src/
  instance/              -> depends on: session/, sql/, direct/, backend/, connection/, cluster/
  server/postgres/       -> depends on: instance/, session/, sql/environment/
  server/rest/           -> depends on: instance/, direct/, backend/
  session/               -> depends on: shared domain types only; no protocol types
  sql/gateway_service.rs -> depends on: session/, sql/environment/, fluss-datafusion
  sql/environment/       -> depends on: session/, fluss-datafusion, PG compat libs
  direct/                -> depends on: backend/, connection/
  auth/                  -> depends on: session-facing auth models only
  cluster/               -> depends on: config and routing state
  connection/            -> depends on: fluss-rust client access
  backend/               -> depends on: Fluss access orchestration only
```

Important boundary:

- `catalog/`, `execution/`, and `types/` belong in `fluss-datafusion`, not in `fluss-gateway`.
- Gateway owns protocol adapters, session/operation lifecycle, auth, request models, timeout/cancel semantics, metadata presentation, and direct read/write APIs.
- Gateway may depend on `fluss-datafusion`; `fluss-datafusion` must not depend on gateway types.

## Phase 1 Scope / 当前范围

This workstream only delivers:

- PostgreSQL wire protocol for read-only SQL
- REST direct read/write APIs
- session and operation management for the SQL path
- protocol-specific compatibility needed for BI/IDE clients

This workstream does not implement in Phase 1:

- MySQL wire protocol
- gRPC landing
- Flight SQL landing
- real multi-cluster support
- per-user Fluss credentials or doAs
- schema-on-write
- PostgreSQL SQL write support
- full transaction semantics

## Reference Projects / 参考项目

| Area | Reference | Path |
|---|---|---|
| Overall baseline | old fluss-query-gateway design | `/Users/boyu/IdeaProjects/fluss-community/.claude/worktrees/ancient-noodling-canyon/fluss-query-gateway/DESIGN.md` |
| Runtime composition | gateway harness wiring | `/Users/boyu/IdeaProjects/fluss-community/.claude/worktrees/ancient-noodling-canyon/fluss-query-gateway/src/harness/gateway.rs` |
| Backend seam | Arrow-native backend trait | `/Users/boyu/IdeaProjects/fluss-community/.claude/worktrees/ancient-noodling-canyon/fluss-query-gateway/src/backend/traits.rs` |
| PostgreSQL compatibility | datafusion-postgres family | `/Users/boyu/AiWorkSpace/fluss-gateway/refs/datafusion-postgres` |
| REST API ergonomics | Kafka REST patterns | `/Users/boyu/AiWorkSpace/fluss-gateway/refs/kafka-rest-community` |

## Testing / 测试

Gateway changes should cover:

- unit tests for request models, session vars, operation state, and error mapping
- integration tests for PostgreSQL protocol behavior
- integration tests for REST direct read/write behavior
- protocol-equivalence tests when the same read behavior is exposed through multiple frontends
- timeout/cancel semantics tests for SQL operations and request-scoped direct reads
- explicit tests for at-least-once write semantics and disconnect/timeout behavior

Default verification baseline:

- run `cargo test` in the target repo or workspace
- add focused tests near the modified gateway module
- prefer harness-based integration tests over ad hoc server bootstrapping

## Parallel Development Contract / 并行开发约束

- Assume `fluss-datafusion` is developed independently in parallel.
- Keep the gateway side dependent on a narrow, explicit installer/API contract.
- If gateway work needs a new `fluss-datafusion` capability, define the capability as a crate-facing API or behavior contract first.
- Do not absorb DataFusion catalog/execution/type logic back into the gateway just to unblock short-term progress.

## Suggested Working Order

1. Freeze dependency versions across DataFusion / Arrow / pgwire compatibility pieces.
2. Define `Instance` request/response types and facade surface.
3. Implement session and operation core structures.
4. Implement `SqlEnvironmentRegistry` and `PgSqlEnvironmentProvider`.
5. Implement PostgreSQL transport / adapter / handler layering.
6. Implement REST direct read/write APIs.
7. Add future protocols only after the Phase 1 core is stable.

## Risk Areas

Be careful around:

- overlap between PostgreSQL adapter logic and PostgreSQL SQL environment logic
- `SessionVars` vs live `SessionContext` synchronization
- cooperative cancel semantics in SQL execution chains
- metadata drift between SQL and REST views
- resource contention between SQL sessions and direct-path traffic
- overengineering during MVP

## Working Style

Before implementing a gateway feature:

1. Read the relevant section in `FLUSS-GATEWAY-DESIGN.md`.
2. Read `FLUSS_DATAFUSION.md` if SQL/DataFusion is involved.
3. Inspect the old `fluss-query-gateway` implementation for reusable patterns.
4. Inspect the external refs before inventing protocol or compatibility logic.
5. Default to the simpler Phase 1 interpretation instead of designing for future protocols prematurely.
