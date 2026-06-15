# Direct path 与 REST API（P5）

> fluss-gateway 模块设计。全局决策与 P0 见 [`../DESIGN.md`](../DESIGN.md)，待做清单见 [`../TASKS.md`](../TASKS.md)（同 P 编号）。
> 覆盖：**P5 Direct path 与 REST API** → `direct/` `server/rest/`。
> 前置：核心类型见 [`core-session.md`](core-session.md)（P1）；共享 backend/connection 见 [`infra.md`](infra.md)（P6）。

direct path 是 Phase 1 **唯一的写入路径**，且不经过 SQL/Session/Operation 机制——它是 request-scoped、无状态的。

> **Phase 1 范围收敛（2026-06-15）**：REST 本期只实现 **direct write + metadata 只读呈现**；**direct read（Lookup / BatchLookup / PrefixScan / LogScan）整体后置到后续阶段**。下方 read 模型（§2、§4 read 端点、§5）作为后续阶段设计先行保留，本期不实现；read body 的 Arrow IPC 响应编码随之后置，本期 Arrow IPC 仅用于 write 输入。

### 1. RequestExecutionContext —— request-scoped，不是 session

```rust
pub struct RequestExecutionContext {
    pub principal: Principal,        // 认证后身份，贯穿内部链路（Phase 1 Fluss 不消费）
    pub cluster: ClusterId,          // 来自 path 前缀，Phase 1 恒为 default
    pub request_id: RequestId,       // 链路追踪用
    pub deadline: Option<Instant>,   // 由 server 超时配置 + 请求 override 推出
    pub cancel: CancellationToken,   // 客户端断开 / deadline 到期时触发
}
```

- 每个 REST 请求构造一个，请求结束即 drop。**不进 SessionManager，不建 Operation，无 session vars。**
- principal/cluster 全部来自请求本身（auth + path），不依赖任何长生命周期会话状态。
- 与 SQL 路径的本质区别：SQL 有 user-visible Operation 可查询/可取消；direct path 只有"这次 HTTP 请求"这一个生命周期单位。

### 2. direct read 模型（DirectReadRequest）—— 本期后置，下为后续阶段设计

> Phase 1 不实现，保留设计。收敛成四种，全部产出 Arrow-native 结果（`SendableRecordBatchStream` 或单批次），编码到 JSON/Arrow 由 REST 层在边界做：

| 读类型 | 语义 | Phase 1 |
|---|---|---|
| `Lookup` | 完整主键等值 point lookup | 必做 |
| `BatchLookup` | 多 key 批量点查 | 可后置（底层具备即做） |
| `PrefixScan` | 单列 string/binary 主键前缀扫描 | 仅当底层 client 支持 |
| `LogScan` | bounded log scan，**LIMIT required**，offset ascending，默认 earliest（与 P0 Log 语义一致） | 必做 |

- 不把 direct read 伪装成全表扫；不满足下推条件直接给清晰错误（与 fluss-datafusion 保守策略一致，但 direct path 自己判定，不走 DataFusion）。

### 3. direct write 模型（DirectWriteRequest）

| 写类型 | 语义 |
|---|---|
| `KvUpsert` | KV 表 upsert 一批行 |
| `KvDelete` | KV 表按主键删一批 |
| `LogAppend` | Log 表 append 一批行 |

- 写入 body 支持两种编码，按 `Content-Type` 协商，统一在边界解码成 Arrow-native：
  - `application/json`：JSON 行（curl/BI 友好，小批量）。
  - `application/vnd.apache.arrow.stream`：Arrow IPC stream（大批量 ingest）。
- schema 以目标表为准（Phase 1 不做 schema-on-write）。
- **不走 SQL**：direct write 直接打 backend 写能力。

### 4. REST 资源路径 —— 固定 /v1/clusters/{cluster}/...

cluster 前缀从第一天就固定，Phase 1 恒为 `default`，为未来多集群留路径演进（用 path 前缀，不用 header）：

```text
# read（本期后置，路径先占位，Phase 1 不实现）
POST /v1/clusters/{cluster}/databases/{db}/tables/{table}/lookup
POST /v1/clusters/{cluster}/databases/{db}/tables/{table}/prefix-scan
POST /v1/clusters/{cluster}/databases/{db}/tables/{table}/log-scan

# write
POST /v1/clusters/{cluster}/databases/{db}/tables/{table}/records          # upsert / log append
POST /v1/clusters/{cluster}/databases/{db}/tables/{table}/records:delete    # kv delete

# metadata（只读呈现，与 SQL 视图同源，见 P6）
GET /v1/clusters/{cluster}/databases
GET /v1/clusters/{cluster}/databases/{db}/tables
GET /v1/clusters/{cluster}/databases/{db}/tables/{table}
```

- 读用 POST + body（key/projection/limit 是结构化输入，不塞 query string）。
- 资源命名与错误返回风格参考 `refs/kafka-rest-community`，但写语义按 Fluss 表类型走。

### 5. direct read 的 timeout / cancel（best-effort）—— 随 read 一并后置

- read 是 request-scoped best-effort：客户端断开或 deadline 到期 → `cancel` token fire → 底层 stream 协作式尽快结束（cooperative stop）。
- 不保证立即停；不产生 user-visible Operation，所以没有"查询取消状态"——停了就是 HTTP 请求结束。

### 6. direct write 的 at-least-once 与错误返回边界

- 写语义 = **at-least-once**。成功 = backend 已 ack。
- **timeout / disconnect != 写入已取消**：中途超时/断开时结果 unknown，**不回滚**。响应只承诺"已知信息"，客户端必须按"可能已写入"处理（幂等/可重试由调用方负责，Phase 1 不提供事务回滚）。
- direct write **无 user-level cancel**：不给取消接口，避免"以为取消了其实写进去了"的假象。
- 错误映射（domain error → HTTP status，在 REST 边界做）：`InvalidArgument`→400、`Unauthenticated`→401、`Unauthorized`→403、`*NotFound`→404、`Unsupported`→501、`Timeout`→504、`Backend/Internal`→5xx。

### 7. direct path 不进入 SessionManager

- 显式约束：REST 这条链路完全无状态，不创建/复用 `GatewaySession`，不登记 `OperationManager`。
- `(principal, cluster)` 每次从请求重新解析，绝不从某个会话"继承"。
- 共享的是 backend/connection/metadata（见 P6），**不是 session**——避免后续有人为复用 SQL 连接池/缓存而把 direct path 塞回 session 模型。

**完成标准**：REST 接入层与 direct service 的内部请求模型稳定（新增一种 direct 读/写类型不需改 session/operation 模型）；一张完整的 REST 资源路径表 + domain→HTTP 错误映射表定稿，可直接作为 axum 路由与 handler 的实现依据。
