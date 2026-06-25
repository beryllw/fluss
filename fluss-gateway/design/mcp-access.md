# MCP 接入指南：让 agent 通过 MCP 读取 Fluss

> 面向使用者/agent 开发者。介绍如何把 AI agent 通过 **MCP(Model Context Protocol)** 接到
> fluss-gateway,只读访问 Fluss 表(含 lake 表的 lake+log union read)。
> 模块定位见 [`../README.md`](../README.md);带/不带 tiering 的部署见 [`usage-tiering.md`](usage-tiering.md)。

---

## 1. 这是什么

gateway 内置一个 **只读 MCP server**,把 Fluss 的读能力暴露成 4 个 MCP 工具,让 agent 即插即用:
发现库表、查看表结构、跑只读 SQL。传输用 **MCP Streamable HTTP**,端点 `/mcp`。

- **只读**:只放行 `SELECT` / `WITH` / `EXPLAIN` / `SHOW` / `DESCRIBE`,拒绝写/DDL/多语句。
- 与 PostgreSQL 走**同一条 SQL 路径**(同一套 `fluss-datafusion` 规划执行),所以 PG 能读的
  (点查、前缀、`LIMIT` 扫描、lake+log union read、`$lake`/`$options` 后缀)agent 也能读。

## 2. 启用 + 连接信息

| 项 | 值 |
|---|---|
| 开关 | `GATEWAY_MCP_ENABLED=true`(默认 `false`) |
| 监听 | `GATEWAY_MCP_LISTEN`(默认 `0.0.0.0:8000`) |
| 端点 | `http://<host>:<port>/mcp` |
| 传输 | MCP Streamable HTTP |
| 认证 | HTTP 头 `Authorization: Basic <base64(user:password)>` |

认证语义同 REST:配了用户(`GATEWAY_USERS` 或 YAML)就校验用户名/密码;没配则 trust 模式
(用户名即 principal,密码忽略)。认证后的 principal 映射到 Fluss 账户,**能读什么由 Fluss 侧权限裁决**。

## 3. 工具清单

| 工具 | 入参 | 返回 | 说明 |
|---|---|---|---|
| `list_databases` | 无 | `{ "databases": [..] }` | 列出库 |
| `list_tables` | `{ "database": "<db>" }` | `{ "tables": [..] }` | 列出库下的表 |
| `describe_table` | `{ "database": "<db>", "table": "<t>" }` | `{ "database","table","columns":[{name,data_type,nullable}] }` | 看表结构 |
| `query` | `{ "sql": "<read-only SQL>", "max_rows": <int?> }` | `structuredContent = { "rows":[{..}], "row_count": <int>, "truncated": <bool> }`; `content` 保留结果 JSON 文本镜像,并额外回显提交的 SQL | 跑单条只读 SQL |

`query` 细节:
- **表名三段式**:`fluss.<database>.<table>`(catalog 固定 `fluss`)。例:`fluss.mydb.orders`。
- **只读白名单**:仅 `SELECT/WITH/EXPLAIN/SHOW/DESCRIBE`;单条语句(多语句会被拒)。
- **结果双通道**:`structuredContent` 仍是结构化 JSON 行结果; `content` 继续保留结果 JSON 文本镜像,并额外回显本次提交的 SQL,兼顾兼容性与可见性。
- **行数上界**:`max_rows` 默认 `1000`,硬上限 `10000`;超出则 `truncated=true`。
- **lake 表**:普通 `SELECT ... FROM fluss.db.t` 即得 lake+log union;`...t$lake` 只读 Paimon 快照,
  `...t$options` 看 datalake 选项(详见 [`usage-tiering.md`](usage-tiering.md))。

## 4. 接入方式

### 4.1 Claude Code(CLI)
```bash
# Basic 头里的 base64 = base64("alice:secret")
claude mcp add --transport http fluss http://127.0.0.1:8000/mcp \
  --header "Authorization: Basic $(printf 'alice:secret' | base64)"
```
之后在会话里就能调用 `list_databases` / `list_tables` / `describe_table` / `query`。

### 4.2 通用 MCP 客户端(JSON 配置)
支持 Streamable HTTP 远程 server 的客户端,配置形如(具体字段名以客户端为准):
```json
{
  "mcpServers": {
    "fluss": {
      "type": "http",
      "url": "http://127.0.0.1:8000/mcp",
      "headers": { "Authorization": "Basic YWxpY2U6c2VjcmV0" }
    }
  }
}
```
> `YWxpY2U6c2VjcmV0` = base64(`alice:secret`)。换成你的用户名/密码的 base64。

### 4.3 原始 JSON-RPC(curl,用于自测/排错)
Streamable HTTP:先 `initialize` 拿 `Mcp-Session-Id`,再带着它调用。响应是 `text/event-stream`,
取 `data:` 行即是 JSON-RPC 结果。
```bash
MCP=http://127.0.0.1:8000/mcp
AUTH="Authorization: Basic $(printf 'alice:secret' | base64)"
ACC='Accept: application/json, text/event-stream'
CT='Content-Type: application/json'

# 1) initialize —— 从响应头取 Mcp-Session-Id
SID=$(curl -sS -D - -o /dev/null -H "$AUTH" -H "$CT" -H "$ACC" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}' \
  "$MCP" | awk -F': ' 'tolower($1)=="mcp-session-id"{print $2}' | tr -d '\r')

# 2) 通知 initialized
curl -sS -o /dev/null -H "$AUTH" -H "$CT" -H "$ACC" -H "Mcp-Session-Id: $SID" \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}' "$MCP"

# 3) 列工具
curl -sS -H "$AUTH" -H "$CT" -H "$ACC" -H "Mcp-Session-Id: $SID" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' "$MCP" | sed -n 's/^data: //p'

# 4) 调 query
curl -sS -H "$AUTH" -H "$CT" -H "$ACC" -H "Mcp-Session-Id: $SID" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"query","arguments":{"sql":"SELECT id, name FROM fluss.mydb.orders WHERE id = 1"}}}' \
  "$MCP" | sed -n 's/^data: //p'
```

## 5. 安全与限制

- **只读**:写/DDL/多语句在工具入口即被拒(`invalid argument: only read-only ...`)。
- **结果有界**:`query` 受 `max_rows`(默认 1000 / 上限 10000)约束,避免无界扫描。
- **认证**:`Authorization: Basic`;认证失败返回 401,工具不会执行。
- **授权**:gateway 不做表级鉴权,principal→Fluss 账户后由 Fluss 裁决可读范围。
- **非 lake KV 表全表扫描**:无主键/前缀谓词且无 `LIMIT` 会被拒(同 PG 路径),加谓词或 `LIMIT`。

## 6. 排错

| 现象 | 原因 / 处理 |
|---|---|
| 连接即 401 | `Authorization: Basic` 缺失/错误;检查 base64 与 `GATEWAY_USERS` |
| `tools/call` 报 `only read-only ...` | SQL 不在只读白名单或是多语句 —— 改成单条 SELECT/WITH |
| `No field named key` 等 | 查错表面:`key/value` 在 `<t>$options`,数据列在基表;见 usage-tiering §6 |
| lake 表读报 `loading credential` | gateway 缺 lake S3 凭据,配 `GATEWAY_LAKE_S3_*`(见 [`usage-tiering.md`](usage-tiering.md) §4.1) |

## 7. 关联文档

- 模块总览 / PG / REST:[`../README.md`](../README.md)
- 带/不带 tiering 的部署与 lake union read:[`usage-tiering.md`](usage-tiering.md)
- 容器与 compose 运行:[`../../docker/fluss-gateway/README.md`](../../docker/fluss-gateway/README.md)
