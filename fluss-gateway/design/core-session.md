# Gateway 核心契约与会话模型（P1 + P2）

> fluss-gateway 模块设计。全局决策与 P0 见 [`../DESIGN.md`](../DESIGN.md)，待做清单见 [`../TASKS.md`](../TASKS.md)（同 P 编号）。
> 覆盖：**P1 核心接口与请求模型** → `instance/` `types.rs` `error.rs`；**P2 Session 与 Operation** → `session/`。

---

## P1. 核心接口与请求模型

只冻结接口面与核心类型边界，不在此展开 Session 内部状态机或 PG/REST 细节。

### 1. GatewayInstance 是唯一的 core facade

Gateway core 对协议模块只暴露一套异步 trait；PostgreSQL、REST、未来 Flight SQL / gRPC 都依赖它，而不各自直接碰 session manager、backend 或 DataFusion。

```rust
#[async_trait]
pub trait GatewayInstance {
    // Session
    async fn open_session(&self, req: OpenSessionRequest) -> Result<SessionSnapshot>;
    async fn close_session(&self, session_id: SessionId) -> Result<()>;
    async fn alter_session(
        &self,
        session_id: SessionId,
        mutation: SessionMutation,
    ) -> Result<SessionSnapshot>;
    async fn get_session(&self, session_id: SessionId) -> Result<SessionSnapshot>;

    // SQL
    async fn describe_sql(&self, req: DescribeSqlRequest) -> Result<SqlDescription>;
    async fn execute_sql(&self, req: ExecuteSqlRequest) -> Result<SqlExecution>;

    // Operation
    async fn cancel_operation(&self, op_id: OperationId) -> Result<CancelResult>;
    async fn get_operation_status(&self, op_id: OperationId) -> Result<OperationStatusSnapshot>;

    // Direct path
    async fn read_direct(&self, req: DirectReadRequest) -> Result<DirectReadResult>;
    async fn write_direct(&self, req: DirectWriteRequest) -> Result<DirectWriteResult>;

    // Metadata
    async fn list_databases(&self, scope: MetadataScope) -> Result<Vec<String>>;
    async fn list_tables(&self, scope: MetadataScope) -> Result<Vec<String>>;
    async fn get_table_info(&self, scope: MetadataScope, table: TableRef) -> Result<TableInfo>;
}
```

core 只暴露能力、不暴露内部 service 组合；协议层据此做集成测试与替身注入；内部 service 重构时协议层不受影响。

### 2. 核心类型先冻结"类别"，再细化字段

P1 先明确必须存在的类型，字段细化留给后续小节：

- identity / routing：`SessionId`、`OperationId`、`Principal`、`ClusterId`、`SqlEnvironmentId`、`RequestId`
- protocol context：`ProtocolKind`、`ClientInfo`
- session domain：`OpenSessionRequest`、`SessionSnapshot`、`SessionVars`、`SessionMutation`
- SQL domain：`DescribeSqlRequest`、`SqlDescription`、`ExecuteSqlRequest`、`SqlExecutionOptions`、`SqlExecution`
- operation domain：`CancelResult`、`OperationStatusSnapshot`
- direct domain：`RequestExecutionContext`、`DirectReadRequest`、`DirectReadResult`、`DirectWriteRequest`、`DirectWriteResult`
- metadata domain：`MetadataScope`、`TableRef`、`TableInfo`
- error domain：统一 `GatewayError` / `GatewayResult<T>`

优先引入小而稳定的 typed wrapper（`SessionId`、`OperationId`、`ClusterId`、`Principal`、`TableRef { database, table }`），把 transport 层字符串解析与 core 领域对象分开。`ClusterId` 是类型名，其内部值语义上是 cluster 名称（Phase 1 恒为 `default`）。

### 3. metadata API 走显式 scope

`MetadataScope` 直接表达访问范围，不让 metadata API 隐式读某个 session 的内部状态：

```rust
struct MetadataScope {
    principal: Principal,
    cluster: ClusterId,
}
```

metadata 本质 cluster-scoped；REST 本就无 session；PG adapter 从 session snapshot 取 `(principal, cluster)` 再转调即可。若后续需要 session-sensitive 视角，再在不破坏接口前提下扩展。

### 4. SQL / direct 结果保持 Arrow-native

`execute_sql` 与 `read_direct` 返回值保持 Arrow-native，不提前变成协议格式：

- `SqlExecution::Query` 返回 `SchemaRef + SendableRecordBatchStream + OperationId`
- `SqlExecution::Command` 返回 `affected_rows + OperationId`
- `DirectReadResult` 返回 Arrow-native，不返回 JSON/HTTP body
- `DirectWriteResult` 返回领域写入结果摘要，不返回 HTTP status 语义

由各协议 adapter 自己做编码（PG row / JSON / Arrow IPC），未来 Flight SQL / gRPC 不必从别的协议格式反解。

### 5. SqlExecution::Command 先保留接口形状

PG 本期只读，但保留 Command 分支，避免后续扩展重改核心返回类型：

```rust
enum SqlExecution {
    Query { operation_id: OperationId, schema: SchemaRef, stream: SendableRecordBatchStream },
    Command { operation_id: OperationId, affected_rows: u64 },
}
```

保留形状 != 本期支持 SQL 写入。

### 6. direct path 请求显式携带 execution context

```rust
struct RequestExecutionContext {
    principal: Principal,
    cluster: ClusterId,
    request_id: RequestId,
    deadline: Option<Instant>,
    cancel: CancellationToken,
}
// 权威定义见 direct-path.md §1；此处与之保持一致。
```

让 direct path 不依赖 SessionManager、REST 与未来 gRPC 共享同一内部请求模型、cancel/timeout 在 core 层即可表达。

### 7. domain error 只在 core 表达业务语义

- core error 只表达领域语义，不含 PG error code / HTTP status / gRPC status。
- backend / datafusion / auth / validation 错误在 core 汇总为统一 gateway domain error。
- 协议层负责把 domain error 映射成各自协议错误格式。

至少覆盖：`InvalidArgument`、`Unauthenticated`、`Unauthorized`、`SessionNotFound`、`OperationNotFound`、`DatabaseNotFound`、`TableNotFound`、`Unsupported`、`Timeout`、`Cancelled`、`Backend`、`Internal`。

### 8. 不属于 core contract 的内容

以下属于协议 adapter 层，不进 `GatewayInstance`：PostgreSQL startup 参数细节、`SET/SHOW` rewrite 规则、prepared statement wire lifecycle、HTTP path/query/header、JSON schema / HTTP status、PG row / FlightData / protobuf message。

**完成标准**：PG / REST adapter 只依赖这套接口即可开发；P2/P4/P5 细化时不推翻 `GatewayInstance` 接口面；transport 细节不倒灌进 core。

---

## P2. Session 与 Operation 核心模型

SQL 路径最难返工的生命周期模型。关键是把 session、`SessionContext`、operation、cancel/timeout 的关系定清楚。

### 1. Session 是连接级对象，direct path 不进 SessionManager

- PostgreSQL：显式 open / close session。
- 未来 Flight SQL / MySQL：复用同一套 session / operation 模型。
- REST / future gRPC direct path：不创建 session，不进 SessionManager。

```rust
struct GatewaySession {
    id: SessionId,
    principal: Principal,
    cluster: ClusterId,
    sql_environment: Option<SqlEnvironmentId>,
    vars: Arc<RwLock<SessionVars>>,
    client_info: ClientInfo,
    operation_manager: OperationManager,
    sql_context: tokio::sync::RwLock<Option<Arc<SessionContext>>>,
    sql_context_generation: AtomicU64,
    sql_context_dirty: AtomicBool,
    created_at: SystemTime,
    last_access_at: AtomicU64,
}
```

session 存连接级状态与 SQL 执行上下文入口；operation 存单次查询级状态；direct path 只拿 `RequestExecutionContext`，不复用 session 结构。

### 2. OpenSessionRequest 只表达连接建立时确定的信息

```rust
struct OpenSessionRequest {
    principal: Principal,
    cluster: ClusterId,
    sql_environment: Option<SqlEnvironmentId>,
    initial_vars: SessionVars,
    client_info: ClientInfo,
}
```

- PostgreSQL：`sql_environment = Some("postgres")`；direct path 通常不调 `open_session`。
- `principal`、`cluster`、`sql_environment` 在 session 生命周期内只读；可变项都进 `SessionVars`。避免把"连接身份"和"会话变量"混在一起改。

### 3. SessionVars 是 mutable session state 的单一事实源

typed core vars + namespaced environment vars：

```rust
struct SessionVars {
    statement_timeout: Option<Duration>,
    timezone: Option<String>,
    current_catalog: Option<String>,
    current_schema: Option<String>,
    environment: BTreeMap<String, SessionVarValue>,
}
```

- 只有真正跨协议都讲得通的变量进顶层 typed fields；协议局部变量进 `environment`（如 `pg.search_path`、`pg.standard_conforming_strings`、`pg.application_name`、未来 `mysql.sql_mode`）。
- `SessionVars` 是持久状态；`SessionContext` 只是某一代运行时投影。core 不被协议变量表污染，新增 SQL 协议无需重构 session 结构。

### 4. Session mutation 必须先改 vars，再处理运行态影响

```rust
enum SessionMutation {
    SetStatementTimeout(Option<Duration>),
    SetTimezone(Option<String>),
    SetCurrentCatalog(Option<String>),
    SetCurrentSchema(Option<String>),
    SetEnvironmentVar { key: String, value: SessionVarValue },
    UnsetEnvironmentVar { key: String },
}

enum SessionMutationEffect {
    SessionOnly,
    ApplyToExistingContext,
    RebuildContextBeforeNextQuery,
}
```

处理顺序固定：① 更新 `SessionVars` → ② 计算 effect → ③ 按 effect 处理 live `SessionContext`。

分类：

- `statement_timeout` -> `SessionOnly`
- `timezone` -> `ApplyToExistingContext`
- `current_catalog` / `current_schema` / `pg.search_path` -> `RebuildContextBeforeNextQuery`
- `pg.application_name` / `pg.datestyle` / `pg.bytea_output` 等显示型 -> `SessionOnly`

约束：`apply_session_mutation` 必须幂等；不允许只改运行态不改 `SessionVars`；任何时候以 `SessionVars` 为真值源。

### 5. SessionContext 采用 lazy init + dirty rebuild

Phase 1 最重要的运行时约束：

- open session 时不强制立即创建 `SessionContext`。
- 第一次 `describe_sql` / `execute_sql` 时按需创建。
- `sql_context_dirty = true` 时，下一次 SQL 请求前基于最新 `SessionVars` 重建。
- 旧 context 若仍被运行中 operation 持有，允许它跑到结束。

```text
open_session
  -> create GatewaySession
  -> sql_context = None

first describe/execute
  -> build SessionContext
  -> install fluss-datafusion
  -> install SQL environment
  -> sql_context_dirty = false

alter_session (needs rebuild)
  -> update SessionVars
  -> sql_context_dirty = true

next describe/execute
  -> detect dirty
  -> build new SessionContext from latest SessionVars
  -> swap current session context pointer
  -> old running ops keep old Arc<SessionContext>
```

禁止：因 `SET search_path` 等立即销毁旧 context；为省事把所有 mutation 都变 rebuild；回到全局共享 `SessionContext`。

### 6. Session close 与运行中 operation 解耦

- 关闭后 session 不再接受新的 `describe_sql` / `execute_sql` / `alter_session`。
- 关闭时对 active operations 发 cancel request。
- manager 从 registry 移除该 session。
- 已拿到旧 `Arc<SessionContext>` 的 operation 在 cooperative cancel 下自行收尾。

即：close 不等于同步等所有 operation 退出，但 close 后不能再用该 session id 发新请求。

### 7. Operation 状态机保持最小清晰

```rust
struct Operation {
    id: OperationId,
    statement_summary: String,
    state: OperationState,
    created_at: SystemTime,
    started_at: Option<SystemTime>,
    finished_at: Option<SystemTime>,
    error: Option<String>,
    cancel_token: CancellationToken,
}

enum OperationState {
    Pending,
    Running,
    CancelRequested,
    Finished,
    Failed,
    Cancelled,
    TimedOut,
}
```

```text
Pending -> Running -> Finished
Pending -> Running -> Failed
Pending -> CancelRequested -> Cancelled
Running -> CancelRequested -> Cancelled
Running -> TimedOut
```

`CancelRequested` 是过渡态；`Cancelled` / `TimedOut` / `Failed` 互斥；进入最终态不回退。

### 8. tracked stream 驱动 operation 状态

`SqlExecution::Query` 返回的 stream 必须是 tracked stream：

- 第一次 poll：`Pending -> Running`
- 正常 EOF：`Running -> Finished`
- deadline 命中：`Running/CancelRequested -> TimedOut`
- cancel token 命中并按取消路径退出：`Running/CancelRequested -> Cancelled`
- 非取消类执行错误：`Running -> Failed`

取消/超时触发的错误不记为 `Failed`；`Failed` 只用于真正的执行失败。这样 operation 状态与真实数据流生命周期一致，而非只与"提交执行请求"同步。

### 9. timeout 优先级固定

```text
effective_timeout = min(statement_timeout, request_timeout_override)
```

- 两者都空 -> 无额外 gateway deadline
- 仅一个存在 -> 用该值
- 两者都存在 -> 取更小值
- `CancelRequest` / client disconnect 可在 deadline 前主动触发 cancel

### 10. CancelRequest 与 client disconnect 语义

统一成"发起 operation cancel request"：

- PG `CancelRequest` -> 按 operation id 或连接绑定找到目标 operation -> `CancelRequested`
- 客户端断开 -> 对该连接正在运行的 operation 发 cancel
- cancel 是 cooperative / best-effort，不承诺瞬时停机

`CancelResult` 至少能区分：找不到 operation / operation 已在最终态 / cancel request 已接受。

### 11. SessionManager 只做连接级治理

负责：open / close session、session registry、最大 session 数限制、idle timeout / reaper、基础查找与快照读取。

不负责：query concurrency scheduling、REST direct path throttle、backend 连接池策略、protocol-specific prepared statement cache。

idle reaper：只回收空闲 session；有 active operations 的不回收；`last_access_at` 至少在 `open_session`、`get_session`、`alter_session`、`describe_sql`、`execute_sql` 时更新。session 限流 != query 限流；SQL path 与 direct path 的资源配额后续分开设计。

**完成标准**：SQL 路径的会话/取消/超时/重建模型清晰、不依赖协议细节；P3 可在不重谈 session 生命周期下设计 environment provider；P4 可在不重谈 cancel/timeout 下接 PG adapter；direct path 不会被塞回 SessionManager / OperationManager。
