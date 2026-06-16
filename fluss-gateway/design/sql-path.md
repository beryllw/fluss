# SQL 路径：环境装配与 PostgreSQL 协议

> fluss-gateway 模块设计。全局决策见 [`../DESIGN.md`](../DESIGN.md)。
> 覆盖：SQL 环境装配层 → `sql/environment/`；PostgreSQL 协议路径 → `server/postgres/`。
> 前置：session/operation 模型见 [`core-session.md`](core-session.md)；与 fluss-datafusion 的契约见 [`datafusion-contract.md`](datafusion-contract.md)。
> 面向用户的连接方式、限制与使用示例请看 [`../README.md`](../README.md)；本文件只讨论内部 SQL 装配与协议设计。

---

## SQL 环境装配层

把“协议差异”从 `Instance` 拆出去，让 `SessionContext` 的装配与 live mutation 成为一个**可按协议替换的插件点**。`Instance` 只知道“拿到一个装配好的 SQL 环境”，不知道里面是不是 PostgreSQL。

### SqlEnvironmentProvider trait —— 唯一的装配入口

```rust
#[async_trait]
pub trait SqlEnvironmentProvider: Send + Sync {
    /// 首次构建（或 rebuild）一个 SessionContext 时调用。
    /// 负责把该协议需要的 catalog / 兼容对象 / 初始 session vars 全部装好。
    async fn prepare_session_context(
        &self,
        session: &GatewaySession,
        ctx: &SessionContext,
    ) -> Result<()>;

    /// session vars 发生 ApplyToExistingContext 类变更时调用。
    /// 必须 idempotent；不安全的变更不在这里做，交给 rebuild。
    async fn apply_session_mutation(
        &self,
        session: &GatewaySession,
        ctx: &SessionContext,
        mutation: &SessionMutation,
    ) -> Result<()>;
}
```

- `prepare_session_context` 是**全量装配**：每次 lazy init / rebuild 都从干净的 `SessionContext` 走一遍，保证可重入。
- `apply_session_mutation` 只承接判定为 `ApplyToExistingContext` 的那一类（见 [`core-session.md`](core-session.md) 的 `SessionMutationEffect`）。`SessionOnly` 不进 provider；`RebuildContextBeforeNextQuery` 走 dirty + 下次 `prepare_session_context` 重建。
- provider **不持有** per-session 状态：读 `GatewaySession`（含 `SessionVars`）作为输入，把结果写进调用方给的 `ctx`。single source of truth 仍是 `SessionVars`。

### SqlEnvironmentRegistry —— 按协议选 provider

- `Instance` 持有一个 `SqlEnvironmentRegistry`，按 `SqlEnvironmentId`（如 `"postgres"`）注册/查找 provider。`SqlEnvironmentId` 标识 SQL 协议环境，与 `ProtocolKind` 1:1 对应。
- `OpenSessionRequest.sql_environment` 由协议层握手时填。SQL 路径首次执行时用它从 registry 取 provider。
- registry 是 shared/只读；provider 内部的重对象（共享 `FlussDatafusion`、pg_catalog 模板）由 provider 自己持有并跨 session 复用，不每 session 重建。

### 固定装配顺序（PgSqlEnvironmentProvider::prepare_session_context）

顺序是契约，写死并在测试里钉住：

```text
1. 创建干净的 SessionContext（datafusion 默认 + gateway 公共 SessionConfig）
2. fluss_datafusion.register_catalog(&ctx, "fluss", RegisterCatalogOptions)
      -> 装真实 Fluss catalog（仅 fluss-datafusion 负责）
3. 安装 datafusion-pg-catalog base objects（pg_catalog / information_schema 基础）
4. 安装 Fluss 专属 pg_catalog overlay（把 Fluss 元数据投射成 PG 系统视图）
5. apply 初始 session vars（timezone / search_path / current schema / application_name）
```

- 第 2 步只装 Fluss catalog，**绝不**在 fluss-datafusion 里碰 `pg_catalog`——pg 兼容是 gateway 的责任。
- 第 3、4 步边界：base objects 来自 `datafusion-pg-catalog`；overlay 是 gateway 把 Fluss 真实库表映射进 PG 视图的部分。两者都属 environment provider，不属 adapter。
- 第 5 步用 session 当前 `SessionVars` 快照，所以 rebuild 后状态自然恢复，无需 replay 历史 mutation。

### provider 与 PgProtocolAdapter 的责任边界

| 关注点 | 归属 |
|---|---|
| wire 解析 / startup handshake / auth | `PgProtocolAdapter` |
| query rewrite / probe query / 协议兼容改写 | `PgProtocolAdapter` |
| bind 参数解码、Arrow→PG rows 编码 | `PgProtocolAdapter` |
| prepared statement 的协议本地缓存与 wire 生命周期 | `PgProtocolAdapter` |
| `SessionContext` 装配（catalog / pg_catalog / overlay） | `PgSqlEnvironmentProvider` |
| session vars 的 live apply / rebuild 触发 | `PgSqlEnvironmentProvider` |
| `SET` / `SHOW` 的**语义落点**（改 `SessionVars`） | `Instance` + provider |
| `SET` / `SHOW` 的**wire 表现**（如何回包） | `PgProtocolAdapter` |

一句话边界：**adapter 处理“线上长什么样”，provider 处理“SessionContext 里装了什么”。** 任何“PG 特有但与 SessionContext 内容无关”的东西都不进 provider；任何“需要改 SessionContext 内容”的东西都不进 adapter。

---

## PostgreSQL 协议路径

第一条 SQL 协议接入路径，**只设计 wire 侧**。SessionContext 装配已交给 `PgSqlEnvironmentProvider`，这里不碰 catalog/pg_catalog 内容。

### server/postgres/ 分层

```text
server/postgres/
  transport.rs   -> pgwire TCP listener / accept / per-connection task（明文 only，TLS 交前置反代，留扩展点）
  handler.rs     -> pgwire Startup/SimpleQuery/ExtendedQuery handler，桥接到 Instance
  adapter.rs     -> PgProtocolAdapter：wire <-> gateway 模型的翻译、编码、cancel 映射
  compat.rs      -> query rewrite / probe query / SET/SHOW wire 表现 / system query 兼容
```

- `transport` 只管连接与字节流，不懂 SQL。
- `handler` 实现 pgwire 三个 handler trait，是协议状态机；把请求翻成对 `Instance` 的调用，不直接写兼容逻辑。
- `adapter` 是“线上长什么样”全在这里的边界。
- `compat` 收纳所有 BI/IDE 兼容的脏活，可被 `adapter` 调用。

### startup / auth handshake → OpenSessionRequest

| startup 来源 | 落点 |
|---|---|
| 认证后的用户身份 | `principal`（经 `TrustAuthenticator`，见 [`infra.md`](infra.md)） |
| 固定值 | `sql_environment = "postgres"` |
| `database` 参数 | 初始 current catalog/schema（`fluss` catalog + 该名作 database） |
| `application_name` | `SessionVars.application_name` |
| `client_encoding` | 固定 UTF-8，非 UTF-8 给明确错误 |
| `TimeZone` / `search_path`（若给） | 初始 `SessionVars` |
| cluster | 恒为 default |

- handshake 成功即 `Instance.open_session(OpenSessionRequest)`，拿到 `GatewaySession`。
- auth 方式走 **cleartext-then-trust**：PG 发 cleartext password 请求，`TrustAuthenticator` 接受任意密码、`principal = username`（保证 BI/IDE/psql 的密码框流程正常，并强制 principal 非空）。

### 兼容责任表（query rewrite / probe / SET / SHOW / system query）

BI/IDE（DBeaver、psql、Tableau 等）连上来先打一堆探活与系统查询。责任划分：

| 查询类别 | 由谁应答 |
|---|---|
| `pg_catalog` / `information_schema` 真实系统表查询 | `datafusion-pg-catalog` base + Fluss overlay（装好后），直接走 DataFusion |
| `SELECT version()` / `current_schema()` / `current_database()` 等标量探活 | **优先**由 pg_catalog 提供的函数应答；仅 catalog 答不了的才进 `compat` 拦截 |
| `SHOW <var>` | 从 `SessionVars` 读，`adapter` 编码成单行结果 |
| `SET <var> = ...` | 语义落点改 `SessionVars`（`Instance`+provider）；wire 上回 `SET` command-complete（`adapter`） |
| `BEGIN` / `COMMIT` / `ROLLBACK` | autocommit no-op：接受并回对应 command tag，不建真实事务（避免 BI 工具断流） |
| `DISCARD ALL` 等会话重置 | 触发 session vars 复位 + `RebuildContextBeforeNextQuery` |
| 写类 SQL（INSERT/UPDATE/DELETE/DDL） | 直接拒，返回 `Unsupported`（只读约束） |
| 其余普通 `SELECT` | passthrough 到 `Instance.execute_sql` |

原则：**能用真实 pg_catalog 应答就别 rewrite**；只有 DataFusion/真实 catalog 答不了的探活才进 `compat` 拦截，且拦截清单显式、可测、尽量小。

### extended query 协议与编码流程

- simple query（`Q`）与 extended（`Parse`/`Bind`/`Describe`/`Execute`/`Sync`）都要支持；BI 工具大量走 extended。
- **bind 参数解码**：PG wire 的 text/binary 参数 → DataFusion `ParamValues`/`ScalarValue`，复用 `arrow-pg` 的类型映射。
- **结果编码**：Arrow `RecordBatch` → PG `DataRow`；`RowDescription` 由 Arrow schema 推出，含 PG type OID 映射；支持 text/binary result format。统一用 Arrow-native 结果，编码只发生在 `adapter`。
- `Describe`（statement/portal）→ 走 `Instance.describe_sql` 拿 schema/param 类型，再翻成 `ParameterDescription` + `RowDescription`。

### prepared statement 生命周期与本地缓存边界

- `Parse` 建命名/匿名 prepared statement，`Bind` 建 portal——**这套 wire 生命周期与协议本地缓存只活在 `adapter` 内**，不进 `Instance` 的 Operation 模型。
- 每次 `Execute` 才映射成一次 `Instance.execute_sql` → 一个 user-visible Operation。
- prepared statement 缓存是 per-connection（per-session），随 session 关闭清理，不跨 session 共享。

### PG CancelRequest → Operation cancel

- PG cancel 是带外的：客户端用**另一条连接**发 `CancelRequest`，带 backend PID + secret key。
- `adapter`/`transport` 维护 `(PID, secret) -> session + 当前 running operation` 映射。
- 收到 `CancelRequest`：校验 secret，解析出当前 running operation，调 `Instance.cancel_operation`（best-effort / cooperative）。无运行中的 operation 则忽略。

### 只读约束下 Command 类型 SQL 的处理

- PostgreSQL **只读**。
- 沿用 `SqlExecution::Command` 形状：`SET`/`SHOW`/`BEGIN`/`COMMIT`/`ROLLBACK`/`DISCARD` 这类不产生结果集的语句走 command 分支，返回合适的 command tag，不进 Operation 结果流。
- 任何写/DDL 一律 `Unsupported`，错误信息明确指向“PostgreSQL 为只读，请用 REST 写入”。
