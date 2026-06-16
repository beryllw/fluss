# 共享地基：Backend / Connection / Auth / 模块骨架

> fluss-gateway 模块设计。全局决策见 [`../DESIGN.md`](../DESIGN.md)。
> 覆盖：Backend/Metadata/Connection → `backend/` `connection/` `cluster/`；Auth → `auth/`；模块骨架 → 目录落点。
> 面向用户的能力说明请先看 [`../README.md`](../README.md)；本文件只保留内部基础设施与模块边界设计。

---

## Backend / Metadata / Connection 边界

SQL 路径与 direct path 的**共享地基**——它们共享 connection / metadata / backend，但**不共享 session**（见 [`direct-path.md`](direct-path.md)）。目标是划清职责，避免出现重复元数据与重复连接逻辑。

### 三层职责

```text
ClusterRegistry          -> 有哪些 cluster、各自的连接配置（仅 default）
FlussConnectionProvider  -> resolve(cluster, principal) -> 共享 FlussConnection
BackendFacade            -> 基于 connection 编排 Fluss 访问（direct read/write + metadata）
```

依赖方向单向向下：`BackendFacade -> FlussConnectionProvider -> ClusterRegistry`。上层（SQL/direct）只认 `BackendFacade` 与 connection，不直接碰 `ClusterRegistry`。

### BackendFacade 职责边界

- 负责：把 gateway 的 direct 读写意图（`KvUpsert`/`KvDelete`/`LogAppend`，以及保留接口的读类型，见 [`direct-path.md`](direct-path.md)）编排到 fluss-rust client 真实能力上，产出 Arrow-native 结果。
- 不负责：SQL 规划/下推（那是 fluss-datafusion）、协议编码、session/operation 生命周期。
- 这是 **direct path 的后端**；SQL path 不经过 `BackendFacade`，它经过 fluss-datafusion（后者自己持有 connection）。两条路在 connection 层汇合，不在 backend 层汇合。

### MetadataService 并入 BackendFacade

- **不单独立服务**：把 `list_databases` / `list_tables` / `get_table_info` 作为 `BackendFacade` 的元数据读 API 暴露，内部带一个轻量 metadata cache（TTL）。
- 保留它作为一个**逻辑表面**（一组方法签名），未来要拆成独立 `MetadataService` 时不改调用方。
- 用途：REST metadata 端点 + PG 路径的 pg_catalog overlay 都从这里读，保证 **gateway 内部元数据源头单一**。

### 跨 crate 的元数据现实：两个 cache

- fluss-datafusion 按其设计**自带**内部 metadata cache（服务 SQL 规划），gateway 够不到也不该够到（并行开发契约）。
- gateway 的 `BackendFacade` metadata cache 服务 REST + pg_catalog overlay。
- 因此存在两个 cache，但**上游唯一真相是 Fluss cluster 本身**。立场：
  - 不强行跨 crate 边界共享一个 cache（会破坏并行开发契约）。
  - 对齐两边 TTL，把“SQL 视图与 REST 视图可能短暂漂移”作为**已知风险**写明。
  - 不做跨 cache 的失效广播。

### FlussConnectionProvider.resolve(cluster, principal)

```rust
async fn resolve(&self, cluster: &ClusterId, principal: &Principal)
    -> Result<Arc<FlussConnection>>;
```

- 返回**共享 proxy-account 连接**，对所有 principal 相同——不做 doAs / per-user 凭证。
- 签名仍带 `principal`：让上 per-user 凭证 / doAs 时**不改调用点**，且强制 principal 贯穿到连接解析这一层不丢失。
- 同一 cluster 的连接跨 session、跨请求复用；不每 session/每请求新建。

### SQL 与 direct 如何共享

- **共享**：`FlussConnection`（同 cluster 一份）、gateway metadata cache（在 `BackendFacade`）。
- **不共享**：`SessionContext`（per-session）、session vars、Operation 状态、fluss-datafusion 内部 cache（属 SQL 路径）。
- 落地形态：每个 cluster 一份共享 `FlussDatafusion`（用同一个 `FlussConnection` 构造，见 DESIGN.md 集成模型）+ 一个 `BackendFacade`（同连接）。SQL 路径走前者，direct 走后者。

### runtime 装配约束

- gateway core（`Instance` / `BackendFacade` / `SqlGatewayService`）保持 runtime-agnostic；runtime 的选择与线程预算属于装配层，不应泄漏进 core 接口面。
- 协议门面层（PG / REST）负责 accept、握手、编解码与取消监听；不要在协议 accept 任务里长期占用 DataFusion 计算或 Fluss I/O。
- direct write 是 direct path，运行时不应强制经过 SQL 执行链。
- Fluss 连接解析层保持统一入口：`FlussConnectionProvider.resolve(cluster, principal)`；不因 PostgreSQL / REST 两条链路分别长出不同的连接归属逻辑。

### 错误分层映射

```text
fluss-rust / fluss-datafusion error    (backend 原始错误)
        -> gateway domain error         (统一错误分类，统一收口)
        -> protocol boundary            (PG error code / HTTP status)
```

- 映射只发生在固定的两道边界：backend→domain 在 `BackendFacade`（及 SQL 服务）入口，domain→protocol 在协议适配层（PG adapter / REST handler）。
- 中间链路只传 domain error，不让 fluss-rust 原始错误类型泄漏到协议层，也不让协议错误码渗回 backend。

---

## Auth 与接入控制

把**协议特有的 auth 握手**与**协议无关的认证判定**分开：协议层负责握手并提取中立凭证，`auth/` 只面向中立的 `Credential`/`Principal`，不认 pgwire/HTTP 类型。

### 协议无关的 Authenticator trait

```rust
pub struct Principal { pub name: String }   // principal 即 username

pub enum Credential {
    Trust { username: String },                            // 信任声明，无 secret
    Password { username: String, password: SecretString }, // 明文校验（由 store 决定）
    // future: Token { .. } 等
}

#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthError>;
}
```

- `auth/` 只依赖 session-facing 中立模型；**协议层把 wire 凭证翻成 `Credential`**，再调 `authenticate`。
- `AuthError` 在边界映射到 domain 的 `Unauthenticated` / `Unauthorized`。
- gateway 全局配置**一个** `Authenticator` 实例，PG 与 REST 共用；两协议各自做握手、喂中立 `Credential`。

### TrustAuthenticator（默认）

- 接受任意 `Credential`，`Principal.name = 声称的 username`，不校验 secret。
- 无 username 时**拒**（`Unauthenticated`），强制每个连接/请求带身份，保证 principal 链路非空。
- 协议握手喂给它的凭证：PG = cleartext password 交换后取 username（password 丢弃）；REST = `Authorization: Basic` 取 username（password 丢弃）。两者都落成 `Principal { name = username }`。

### ConfigUserStoreAuthenticator（预留接口形状）

- 形状：从配置加载 `username -> password`（或 hash）用户表，`authenticate` 校验 `Password` 凭证。
- 只定义形状；它是 trust 之外的第二个可选实现，证明 trait 抽象够用、可替换。

### principal 与 username 1:1

- principal **就是** username，无别名、无 group/role、无映射表。
- 不引入授权（authorization）：认证通过即可访问。

### 认证结果如何进入下游 context

| 路径 | 握手 | 落点 |
|---|---|---|
| PostgreSQL（SQL） | PG startup + auth 消息交换 | `Principal` → `OpenSessionRequest.principal` → `GatewaySession`（连接级保存） |
| REST（direct） | 解析 `Authorization: Basic`（取 username 作 principal，password 忽略=trust） | `Principal` → `RequestExecutionContext.principal`（请求级，不入 session） |

- 两条路下游统一：principal 流到 `FlussConnectionProvider.resolve(cluster, principal)`，保存但不消费（共享 proxy 账号，无 doAs）。
- 即“认证结果差异在协议层，principal 之后协议无关”：SQL 存进 session，direct 存进 request context，再往下都是同一个 `Principal`。

---

## 模块骨架与目录落点

crate 根在 `fluss-gateway/`（Rust crate，独立于仓库的 Java 部分）。

### 目录树

```text
fluss-gateway/
  Cargo.toml
  src/
    lib.rs                  # 组装入口：Builder/harness 暴露
    error.rs                # gateway domain error 分类
    types.rs                # 共享中立类型 (ids / scope / arrow result)

    instance/               # GatewayInstance facade
      mod.rs

    auth/                   # Authenticator / Credential / Principal
      mod.rs

    cluster/                # ClusterRegistry（仅 default）
      mod.rs
    connection/             # FlussConnectionProvider
      mod.rs
    backend/                # BackendFacade + metadata 读 API
      mod.rs

    session/                # GatewaySession / SessionVars / SessionManager
      mod.rs                #   + Operation / OperationManager

    sql/
      gateway_service.rs    # SQL 执行编排，接 fluss-datafusion
      environment/          # SqlEnvironmentProvider / Registry
        mod.rs              #   + PgSqlEnvironmentProvider

    direct/                 # DirectReadRequest/WriteRequest + service
      mod.rs

    server/
      postgres/             # transport/handler/adapter/compat
        mod.rs
      rest/                 # axum routes/handlers
        mod.rs

  tests/
    harness/                # 起 Instance + 协议端的集成测试脚手架
    ...                     # 协议行为 / 等价性 / 超时取消 / 写语义测试
```

### 不创建的目录（避免空目录空抽象）

- `server/mysql/`、`server/flightsql/`、`server/grpc/`
- 多集群路由表、`doAs`/per-user 凭证模块
- schema-on-write、PG 写支持、事务模块

到需要时再建；现在连空 `mod.rs` 都不放，防止误导读者以为已有接缝。

### 明确不属于 gateway 的东西

- `catalog/` / `execution/` / `types/`（指 DataFusion 的 catalog/exec/类型桥接）**属于 fluss-datafusion**，不在本 crate 出现。
- 本 crate 的 `types.rs` 是 gateway 自己的中立域类型（id/scope/error/arrow result 包装），与 fluss-datafusion 的 `types/` 是两回事，命名上别混。
- gateway 依赖 fluss-datafusion，反向不成立（并行开发契约）。

### 测试与 harness 落点

- 集成测试统一进 `tests/`，共享 `tests/harness/`：在测试里组装 `GatewaySession`→`Instance`→协议端，按 CLAUDE.md「prefer harness-based integration tests over ad hoc server bootstrapping」。
- 单元测试就近放各模块（如 `session/` 内测状态机、`sql/environment/` 内测装配顺序），不堆进 `tests/`。
