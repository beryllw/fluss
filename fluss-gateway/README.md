# fluss-gateway

`fluss-gateway` is the gateway module for Fluss. It exposes a PostgreSQL-compatible SQL entry for reads and a REST entry for direct writes and metadata.

`fluss-gateway` 是 Fluss 的网关模块：对外提供一个 PostgreSQL 兼容的只读 SQL 入口，以及一个用于 direct write 和 metadata 的 REST 入口。

## What it is / 模块定位

The gateway surface is split into two paths:

- **PostgreSQL wire protocol** for read-only SQL access
- **REST API** for direct writes and read-only metadata

网关对外能力分成两条路径：

- **PostgreSQL 协议**：只读 SQL
- **REST API**：direct write + metadata 只读呈现

## Capabilities / 能力

### PostgreSQL / PostgreSQL 入口

- connect with PostgreSQL clients and drivers
- simple and extended query flows
- read-only `SELECT`
- session commands such as `SET`, `SHOW`, `BEGIN`, `COMMIT`, `ROLLBACK`, `DISCARD`
- out-of-band PostgreSQL `CancelRequest`

- 使用 PostgreSQL 客户端和驱动连接
- simple query 与 extended query 流程
- 只读 `SELECT`
- 会话命令：`SET`、`SHOW`、`BEGIN`、`COMMIT`、`ROLLBACK`、`DISCARD`
- 带外 PostgreSQL `CancelRequest`

### REST / REST 入口

- direct write for KV and Log tables
- read-only metadata endpoints
- JSON rows input
- Arrow IPC stream input

- KV / Log 表 direct write
- metadata 只读接口
- JSON 行格式写入
- Arrow IPC stream 写入

### MCP / MCP 入口

- read-only MCP (Model Context Protocol) server for AI agents (Streamable HTTP)
- four tools: `list_databases`, `list_tables`, `describe_table`, `query` (read-only SQL)
- `query` keeps rows in structured JSON and echoes only the submitted SQL in MCP text content
- enable with `GATEWAY_MCP_ENABLED=true`; endpoint `http://<host>:8000/mcp`
- agent onboarding guide: [`design/mcp-access.md`](./design/mcp-access.md)

- 面向 AI agent 的只读 MCP server(Streamable HTTP)
- 4 个工具：`list_databases`、`list_tables`、`describe_table`、`query`(只读 SQL)
- `query` 会继续用结构化 JSON 返回行结果，MCP 文本内容里只回显提交的 SQL
- 用 `GATEWAY_MCP_ENABLED=true` 开启；端点 `http://<host>:8000/mcp`
- agent 接入指南：[`design/mcp-access.md`](./design/mcp-access.md)

Implemented REST endpoints:

```text
POST /v1/clusters/{cluster}/databases/{db}/tables/{table}/records
POST /v1/clusters/{cluster}/databases/{db}/tables/{table}/records:delete
GET  /v1/clusters/{cluster}/databases
GET  /v1/clusters/{cluster}/databases/{db}/tables
GET  /v1/clusters/{cluster}/databases/{db}/tables/{table}
```

## Quick usage / 快速使用

### PostgreSQL example / PostgreSQL 连接示例

The PostgreSQL path uses a cleartext password handshake and the default authenticator trusts the username. The connection shape is:

PostgreSQL 路径使用 cleartext password 握手，默认鉴权器只信任 username。连接形态如下：

```text
host=127.0.0.1 port=<pg-port> user=alice password=ignored dbname=fluss
```

Example with `psql`:

```bash
PGPASSWORD=ignored psql "host=127.0.0.1 port=<pg-port> user=alice dbname=fluss sslmode=disable"
```

The PostgreSQL entry is read-only; writes go through REST.

PostgreSQL 入口是只读的，写入走 REST。

### REST write example / REST 写入示例

JSON rows:

```bash
curl -u alice:ignored \
  -H 'Content-Type: application/json' \
  -X POST \
  http://127.0.0.1:<rest-port>/v1/clusters/default/databases/db/tables/t/records \
  -d '[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]'
```

Delete by keys:

```bash
curl -u alice:ignored \
  -H 'Content-Type: application/json' \
  -X POST \
  http://127.0.0.1:<rest-port>/v1/clusters/default/databases/db/tables/t/records:delete \
  -d '[{"id":1,"name":"alice"}]'
```

Metadata:

```bash
curl -u alice:ignored \
  http://127.0.0.1:<rest-port>/v1/clusters/default/databases
```

### Quickstart / 快速上手

If you want the fastest end-to-end path for understanding how `fluss-gateway` is used by an AI agent, start with the MCP quickstart:

如果你想用一条最短路径理解 `fluss-gateway` 如何被 AI agent 使用，优先看 MCP quickstart：

- [`quickstart/README.md`](./quickstart/README.md) — tiering-enabled local cluster + MCP-only refund investigation / 带 tiering 的本地集群与退款排查 quickstart

It brings up a local Fluss + Gateway + Paimon environment, connects through MCP, and walks through a customer-support refund investigation story.

它会启动一个本地 Fluss + Gateway + Paimon 环境，通过 MCP 接入，并演示一次客服退款排查流程。

### Deploying against a Fluss cluster (with / without tiering) / 配合 Fluss 集群部署（带 / 不带 tiering）

For end-to-end operation against a Fluss cluster — environment configuration,
running with a plain real-time cluster, and running with lake (Paimon) **tiering**
for `lake + log` union read (plus the required `datalake.*` / `s3.region` config and
troubleshooting) — see the usage guide:

配合 Fluss 集群的完整使用（环境变量配置、连普通实时集群、以及开 lake/Paimon **tiering**
做 `lake + log` union read，含必需的 `datalake.*` / `s3.region` 配置与排错），见使用文档：

- [`design/usage-tiering.md`](./design/usage-tiering.md) — gateway + Fluss cluster, with / without tiering / 带与不带 tiering 的使用文档

## Scope and limits / 范围与限制

The gateway's behavior is bounded as follows:

- PostgreSQL access is **read-only**; REST is the only write path.
- The REST direct-read endpoints (`lookup`, `prefix-scan`, `log-scan`) are not implemented and return `501 Not Implemented`.
- `default` is the only cluster the gateway routes to; the `{cluster}` path segment is fixed to `default`.
- Authentication is trust-by-username: REST uses `Authorization: Basic` and PostgreSQL uses a username/password handshake; the default authenticator turns the username into `principal` and does not verify the password.
- The PostgreSQL path is cleartext in-process; TLS should be terminated by a fronting proxy.
- The gateway does not do schema-on-write; the target table's schema is authoritative.

网关行为边界如下：

- PostgreSQL 入口**只读**；REST 是唯一写入路径。
- REST direct-read 端点（`lookup`、`prefix-scan`、`log-scan`）未实现，返回 `501 Not Implemented`。
- 网关只路由 `default` 集群；`{cluster}` 路径段固定为 `default`。
- 认证为 trust-by-username：REST 用 `Authorization: Basic`，PostgreSQL 用 username/password 握手；默认鉴权器把 username 转成 `principal`，不校验 password。
- PostgreSQL 路径是进程内明文；TLS 应在前置代理层终止。
- 不做 schema-on-write，目标表 schema 为准。

## Behavior / 行为语义

### Write semantics / 写入语义

REST writes are **at-least-once**:

- a successful `2xx` means the backend acknowledged the write
- a timeout or disconnect does **not** imply rollback
- the result may be unknown after timeout/disconnect
- retries may duplicate data unless the caller provides its own idempotency strategy

REST 写入语义是 **at-least-once**：

- 返回 `2xx` 表示后端已经确认该写入
- timeout 或 disconnect **不代表**写入已回滚
- timeout / disconnect 之后，结果可能是 unknown
- 重试可能造成重复写入，除非调用方自己处理幂等

### Session model / 会话模型

- PostgreSQL connections are stateful and create gateway sessions.
- REST requests are stateless and do **not** create gateway sessions.

- PostgreSQL 连接是有状态的，会创建 gateway session。
- REST 请求是无状态的，**不会**创建 gateway session。

## Internal docs / 内部设计文档

User-facing usage lives in this `README.md`. For internal design and implementation details, see:

面向用户的说明以本 `README.md` 为准。内部设计与实现约束见：

- [`DESIGN.md`](./DESIGN.md) — design overview / 设计总览
- [`design/core-session.md`](./design/core-session.md) — core contract, session, operation
- [`design/sql-path.md`](./design/sql-path.md) — SQL environment and PostgreSQL path
- [`design/direct-path.md`](./design/direct-path.md) — direct path and REST API design
- [`design/infra.md`](./design/infra.md) — backend, connection, auth, module layout
- [`design/datafusion-contract.md`](./design/datafusion-contract.md) — gateway ↔ fluss-datafusion contract

## Source of behavior / 行为依据

This README is based on the module implementation and tests, especially:

本 README 依据模块实现和测试整理，重点参考：

- `src/server/rest/mod.rs`
- `src/auth/mod.rs`
- `tests/harness/mod.rs`
- `tests/rest_integration.rs`
- `tests/integration.rs`
- `tests/cluster_e2e.rs`
