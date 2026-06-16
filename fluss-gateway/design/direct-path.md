# Direct path 与 REST API

> fluss-gateway 模块设计。全局决策见 [`../DESIGN.md`](../DESIGN.md)。
> 覆盖：Direct path 与 REST API → `direct/` `server/rest/`。
> 前置：核心类型见 [`core-session.md`](core-session.md)；共享 backend/connection 见 [`infra.md`](infra.md)。
> 面向用户的 REST 路径、语义与限制请先看 [`../README.md`](../README.md)；本文件保留内部请求模型与边界设计。

direct path 是网关的写入路径，不经过 SQL/Session/Operation 机制——它是 request-scoped、无状态的。REST 对外暴露 **direct write + metadata 只读呈现**；direct read 端点未实现，返回 `501`。

### RequestExecutionContext —— request-scoped，不是 session

```rust
pub struct RequestExecutionContext {
    pub principal: Principal,        // 认证后身份，贯穿内部链路（Fluss 暂不消费）
    pub cluster: ClusterId,          // 来自 path 前缀，恒为 default
    pub request_id: RequestId,       // 链路追踪用
    pub deadline: Option<Instant>,   // 由 server 超时配置 + 请求 override 推出
    pub cancel: CancellationToken,   // 客户端断开 / deadline 到期时触发
}
```

- 每个 REST 请求构造一个，请求结束即 drop。**不进 SessionManager，不建 Operation，无 session vars。**
- principal/cluster 全部来自请求本身（auth + path），不依赖任何长生命周期会话状态。
- 与 SQL 路径的本质区别：SQL 有 user-visible Operation 可查询/可取消；direct path 只有“这次 HTTP 请求”这一个生命周期单位。

### direct write 模型（DirectWriteRequest）

| 写类型 | 语义 |
|---|---|
| `KvUpsert` | KV 表 upsert 一批行 |
| `KvDelete` | KV 表按主键删一批 |
| `LogAppend` | Log 表 append 一批行 |

- 写入 body 支持两种编码，按 `Content-Type` 协商，统一在边界解码成 Arrow-native：
  - `application/json`：JSON 行（curl/BI 友好，小批量）。
  - `application/vnd.apache.arrow.stream`：Arrow IPC stream（大批量 ingest）。
- schema 以目标表为准（不做 schema-on-write）。
- **不走 SQL**：direct write 直接打 backend 写能力。

### REST 资源路径 —— 固定 /v1/clusters/{cluster}/...

cluster 前缀固定，恒为 `default`，为未来多集群留路径演进（用 path 前缀，不用 header）：

```text
# read（端点存在但未实现，返回 501）
POST /v1/clusters/{cluster}/databases/{db}/tables/{table}/lookup
POST /v1/clusters/{cluster}/databases/{db}/tables/{table}/prefix-scan
POST /v1/clusters/{cluster}/databases/{db}/tables/{table}/log-scan

# write
POST /v1/clusters/{cluster}/databases/{db}/tables/{table}/records          # upsert / log append
POST /v1/clusters/{cluster}/databases/{db}/tables/{table}/records:delete    # kv delete

# metadata（只读呈现，与 SQL 视图同源，见 infra.md）
GET /v1/clusters/{cluster}/databases
GET /v1/clusters/{cluster}/databases/{db}/tables
GET /v1/clusters/{cluster}/databases/{db}/tables/{table}
```

- read 端点保留在路径表里以固定 URL 形态；当前返回 `501`。
- 资源命名与错误返回风格参考 Kafka REST 习惯，但写语义按 Fluss 表类型走。

### direct write 的 at-least-once 与错误返回边界

- 写语义 = **at-least-once**。成功 = backend 已 ack。
- **timeout / disconnect != 写入已取消**：中途超时/断开时结果 unknown，**不回滚**。响应只承诺“已知信息”，客户端必须按“可能已写入”处理（幂等/可重试由调用方负责，不提供事务回滚）。
- direct write **无 user-level cancel**：不给取消接口，避免“以为取消了其实写进去了”的假象。
- 错误映射（domain error → HTTP status，在 REST 边界做）：`InvalidArgument`→400、`Unauthenticated`→401、`Unauthorized`→403、`*NotFound`→404、`Unsupported`→501、`Timeout`→504、`Backend/Internal`→5xx。

### direct path 不进入 SessionManager

- 显式约束：REST 这条链路完全无状态，不创建/复用 `GatewaySession`，不登记 `OperationManager`。
- `(principal, cluster)` 每次从请求重新解析，绝不从某个会话“继承”。
- 共享的是 backend/connection/metadata（见 [`infra.md`](infra.md)），**不是 session**。
