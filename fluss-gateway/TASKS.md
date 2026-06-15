# Fluss Gateway Phase 1 任务清单

> 本文档只跟踪 **做什么、做到哪了**：待做项、产出、实现顺序、可并行项、Backlog。
>
> **设计依据不在这里**。每项任务背后的目标、推荐定稿、设计重点与完成标准在 `DESIGN.md`（总览 + P0）与 `design/` 下的模块设计文档，按相同的 `P0-P8` / `D1-D4` 编号对应（文档映射见 `DESIGN.md` §4 模块索引）。T1-T4 是测试待做，没有独立设计小节，每条标注它验证的 P 编号。
>
> 用法：从下面挑一项 → 回对应设计文档读同编号小节 → 按其“当前推荐定稿”落地 → 勾掉。

---

## 1. 设计前置阻塞项（P0）

> 设计见 `DESIGN.md` → §3 P0。这些不先冻结，P1-P5 都会反复返工。

### P0. 版本线与跨仓契约冻结

待做：

- [ ] 产出一份依赖矩阵，明确 DataFusion / Arrow / pgwire / PG compat 相关库版本
- [ ] 产出一份 `fluss-datafusion` crate 契约，明确它的职责边界、公共 API 和非目标
- [ ] 产出一份 gateway <-> datafusion 集成流程说明，固定 shared installer + per-session install 模型
- [ ] 产出一份 Log 表 SQL 语义说明，固定 LIMIT、顺序、默认起点与非目标
- [ ] 产出一份 cancel / timeout 契约说明，区分 SQL path、direct read、direct write

产出：

- 一份稳定的依赖矩阵
- 一份稳定的 `fluss-datafusion` API 与职责契约
- 一份固定的 gateway/datafusion 集成流程说明
- 一份固定的 Log scan 语义说明
- 一份固定的 cancel / timeout 契约说明

---

## 2. Gateway 本体待做（P1-P8）

### P1. 核心接口与请求模型

> 设计见 `design/core-session.md` → P1。只冻结接口面与核心类型边界，不展开内部状态机。

- [ ] 固定 `GatewayInstance` 统一接口面
- [ ] 固定核心领域类型清单与命名
- [ ] 固定 `MetadataScope` 与 `TableRef` 的最小形态
- [ ] 固定 SQL / direct result 保持 Arrow-native 的约束
- [ ] 固定 `SqlExecution::Command` 的保留策略
- [ ] 固定 direct request 显式携带 `RequestExecutionContext` 的约束
- [ ] 固定 domain error 分层原则与最小错误类别集合
- [ ] 明确不属于 core contract 的协议细节清单

### P2. Session 与 Operation 核心模型

> 设计见 `design/core-session.md` → P2。SQL 路径最难返工的生命周期模型。

- [ ] 固定 `GatewaySession` 内部模型
- [ ] 固定 `OpenSessionRequest` / `SessionSnapshot` 的连接级语义
- [ ] 固定 `SessionVars` 作为单一事实源的原则
- [ ] 固定 `SessionMutation` / `SessionMutationEffect` 与推荐分类
- [ ] 固定 lazy init + `sql_context_dirty` + rebuild-before-next-query 模型
- [ ] 固定 session close 与运行中 operation 的解耦语义
- [ ] 固定 `Operation` / `OperationState` / `OperationStatusSnapshot` 的最小状态机
- [ ] 固定 tracked stream 驱动 operation 状态同步的约束
- [ ] 固定 statement timeout、request timeout、CancelRequest、client disconnect 的交互规则
- [ ] 固定 SessionManager 的职责边界、连接上限与 idle reaper 规则

### P3. SQL 环境装配层

> 设计见 `design/sql-path.md` → P3。把协议差异从 `Instance` 拆成可替换插件点。

- [ ] 定义 `SqlEnvironmentProvider` trait（`prepare_session_context` / `apply_session_mutation`）
- [ ] 定义 `SqlEnvironmentId` 与 `SqlEnvironmentRegistry`，挂到 `Instance`
- [ ] 在 `OpenSessionRequest` 增加 `sql_environment` 字段，约定由协议层填充
- [ ] 设计 `PgSqlEnvironmentProvider`，实现固定的 5 步装配顺序
- [ ] 钉死装配顺序的契约测试（catalog 先于 pg_catalog base，base 先于 overlay）
- [ ] 明确 session vars 分类表：哪些 `ApplyToExistingContext`、哪些 `RebuildContextBeforeNextQuery`、哪些 `SessionOnly`
- [ ] 约定 provider 内 shared 重对象（`FlussDatafusion` / pg_catalog 模板）的持有与复用方式
- [ ] 写清 provider 与 `PgProtocolAdapter` 的责任分工表，作为 P4 的输入

### P4. PostgreSQL 协议路径

> 设计见 `design/sql-path.md` → P4。第一条 SQL 协议接入路径，只设计 wire 侧。

- [ ] 拆分 `server/postgres/` 的 transport / handler / adapter / compat 结构
- [ ] 设计 startup/auth handshake（**cleartext-then-trust**）与 `OpenSessionRequest` 的映射表（含 `database`→catalog/schema、`sql_environment` 固定为 `"postgres"`）
- [ ] 固化 query rewrite / probe / `SET` / `SHOW` / system query 的兼容责任表与显式拦截清单
- [ ] 设计 bind 参数解码（PG wire → `ScalarValue`）与 Arrow→PG rows 编码（含 type OID 与 text/binary format）
- [ ] 设计 `Describe` → `Instance.describe_sql` 的 schema/param 映射
- [ ] 设计 prepared statement / portal 的协议本地生命周期与缓存，确认不泄漏进 Operation 模型
- [ ] 设计 PG `CancelRequest`（PID+secret）到 `Instance.cancel_operation` 的映射与映射表维护
- [ ] 明确 `BEGIN`/`COMMIT`/`ROLLBACK` 的 autocommit no-op 策略与写类 SQL 的 `Unsupported` 拒绝路径

### P5. Direct path 与 REST API

> 设计见 `design/direct-path.md` → P5。Phase 1 唯一写入路径，request-scoped、无状态。**本期范围收敛（2026-06-15）：REST 只实装 direct write + metadata 只读呈现；direct read（lookup/scan）整体后置，见 §7 Backlog。**

- [ ] 定义 `RequestExecutionContext`（principal / cluster / request_id / deadline / cancel）
- [ ] 定义 `DirectWriteRequest` 的 Arrow-native 输入形状（`DirectReadRequest` 随 read 后置，见 §7）
- [ ] 收敛 direct write 模型：`KvUpsert` / `KvDelete` / `LogAppend`
- [ ] 设计 write body 双编码：`application/json` 行 + `application/vnd.apache.arrow.stream`（Content-Type 协商，边界统一解码成 Arrow-native）
- [ ] 固化 REST 资源路径表，锁定 `/v1/clusters/{cluster}/...` 前缀；本期实装 write + metadata 端点，read 端点先占位不实现
- [ ] 设计 REST 写入的 at-least-once 语义、unknown 结果处理与 domain→HTTP 错误映射表（`Unsupported`→501）
- [ ] 在设计上钉死 direct path 不进 SessionManager / 不建 Operation

### P6. Backend / Metadata / Connection 边界

> 设计见 `design/infra.md` → P6。SQL 与 direct 的共享地基：共享 connection/metadata，不共享 session。

- [ ] 定义 `BackendFacade` 职责边界（direct 读写编排 + 元数据读 API，产出 Arrow-native）
- [ ] 确认 MVP 把 `MetadataService` 并入 `BackendFacade`，但保留独立逻辑表面
- [ ] 定义 `ClusterRegistry` 最小形态（仅 default + 连接配置）
- [ ] 定义 `FlussConnectionProvider.resolve(cluster, principal)`，Phase 1 返回共享 proxy 连接
- [ ] 明确 SQL 路径（经 `fluss-datafusion`）与 direct path（经 `BackendFacade`）在 connection 层汇合、在 backend 层不汇合
- [ ] 写明两个 metadata cache 的边界与 drift 风险、TTL 对齐策略（overlay 是否改从注册 catalog 派生：defer 到 fluss-datafusion 代码放出，见 `design/infra.md` §P6.4）
- [ ] 定义 backend→domain→protocol 的两道错误映射边界

### P7. Auth 与接入控制

> 设计见 `design/infra.md` → P7。留好鉴权接缝，不超出本期复杂度。

- [ ] 定义 `Principal` / `Credential` / `AuthError` 中立模型与 `Authenticator` trait
- [ ] 实现 `TrustAuthenticator`，约定无身份时拒绝；协议握手喂凭证：PG=cleartext password 取 username、REST=`Authorization: Basic` 取 username（password 均丢弃）
- [ ] 预留 `ConfigUserStoreAuthenticator` 的接口形状（不要求完整实现）
- [ ] 钉死 principal 与 username 1:1、本期不做 authorization
- [ ] 明确 PG 握手（cleartext-then-trust）→ `OpenSessionRequest.principal`、REST `Authorization: Basic` → `RequestExecutionContext.principal` 的两条注入路径
- [ ] 确认 principal 一直流到 `resolve(cluster, principal)` 不丢

### P8. 模块骨架与目录落点

> 设计见 `design/infra.md` → P8。代码落点一次设计好，避免反复迁移文件。

- [ ] 按设计的树创建 Phase 1 目录骨架（含 `lib.rs` / `error.rs` / `types.rs`）
- [ ] 在 `Cargo.toml` 固定 P0 冻结的依赖版本线（DataFusion/Arrow/pgwire/axum/fluss-rs/fluss-datafusion）
- [ ] 钉死“暂不创建”清单，评审时拦截提前建的未来协议空目录
- [ ] 明确 `catalog/`/`execution/`/`types/` 归 fluss-datafusion，本 crate `types.rs` 与之区分命名
- [ ] 建 `tests/harness/` 骨架，约定集成测试统一入口
- [ ] 在每个目录的 `mod.rs` 顶部用一行注释回指其设计出处（P 编号）

---

## 3. 与 `fluss-datafusion` 的并行待做项（D1-D4）

> 设计见 `design/datafusion-contract.md` → D1-D4。**不在本仓实现**（落在 `fluss-rust` 的 `fluss-datafusion` crate），但是 gateway SQL 路径的前置契约。

### D1. 共享 installer 契约（gateway 实际调用面）

- [ ] `FlussDatafusion::new(connection, options)` —— gateway 每 cluster 构造一个，**共享**复用
- [ ] `register_catalog(&ctx, "fluss", options)` —— 把**真实 Fluss catalog**装进 gateway 提供的 per-session `SessionContext`；**只装 Fluss catalog，绝不装 pg_catalog**
- [ ] 共享 metadata cache / provider descriptor / helper 全在 `FlussDatafusion` 内部，gateway 够不到——契约上确认这层不暴露

### D2. SQL 能力边界（gateway 依赖的下推语义）

- [ ] KV point lookup 下推：完整主键等值 → 单次 point lookup，**不经全表扫**
- [ ] Log bounded scan 下推：**LIMIT required**、offset ascending、默认 earliest（必须与 P0 锁定的 Log 语义一致）
- [ ] prefix scan：Phase 1 **可选**，仅当底层 client 支持单列 string/binary 主键前缀
- [ ] 非下推 SQL 的保守处理：KV 无完整主键 / Log 无 LIMIT → **返回清晰错误**，不伪装成全表扫
- [ ] **cancel 协作性**：下推 scan 的 `ExecutionPlan`/stream 接受协作取消（`CancellationToken` / `poll_next` 响应），gateway 停 poll 或 cancel/timeout 时能中止后端读取并释放资源（支撑 P2 cooperative cancel）

### D3. 类型与错误边界（跨 crate 的对接面）

- [ ] Arrow schema / row conversion 契约：Fluss schema → Arrow schema、Fluss row → `RecordBatch`，结果统一 Arrow-native
- [ ] `ScalarValue -> Fluss key` 转换边界：谓词/bind 值的基础 `ScalarValue` 安全转 Fluss key representation，转换失败给明确错误
- [ ] `FlussDatafusionError` 与 gateway domain error 的分层：crate 只产 DataFusion 集成层错误（不掺协议码），gateway 在 SQL 服务入口映射成 P1 domain error

### D4. 性能与缓存要求（gateway 体验依赖）

- [ ] 新建 SQL session 不触发全量 metadata 扫描：`register_catalog` 默认 lazy，不预热所有 db/table
- [ ] 相同 database/table 元数据跨 session 复用：依赖 `FlussDatafusion` 共享 cache
- [ ] per-session 只新建 `SessionContext`，不新建整套 metadata/cache

---

## 4. 测试与验证待做（T1-T4）

> 测试没有独立设计小节：每条任务末尾标注它验证的 P 编号，去对应模块设计文档（见 `DESIGN.md` §4 模块索引）读那一节。单元测就近放各模块，集成测走 `tests/harness/`（见 P8）。

### T1. 单元测试（就近放模块内）

- [ ] Session vars / mutation effect：`SessionOnly` / `ApplyToExistingContext` / `RebuildContextBeforeNextQuery` 三类判定正确（P2）
- [ ] operation state transition：Pending→Running→(CancelRequested)→Finished/Failed/Cancelled/TimedOut 合法迁移与非法迁移拒绝（P2）
- [ ] request model validation：`OpenSessionRequest` / `DirectWriteRequest` 的非法输入被拒（P1/P5）
- [ ] error mapping：domain error → PG error code、domain error → HTTP status 两张映射表（P4/P5/P6）
- [ ] SQL environment 装配顺序契约：catalog 先于 pg_catalog base、base 先于 overlay（P3）

### T2. 集成测试（harness）

- [ ] PostgreSQL 只读查询：psql/extended query 走通 connect→探活→`SELECT`（P4）
- [ ] PostgreSQL `CancelRequest`：带外 PID+secret 取消 running operation，secret 错误被拒（P4）
- [ ] ~~REST direct read：`lookup` / `log-scan` 返回 Arrow-native 结果~~（随 direct read 后置，见 §7）
- [ ] REST direct write：`KvUpsert` / `KvDelete` / `LogAppend` 成功路径（P5）
- [ ] timeout / disconnect：write 超时后结果 unknown、不回滚（P5）（read 的 cooperative stop 随 direct read 后置）

### T3. 一致性测试

- [ ] ~~同一读能力在 SQL / REST 暴露下结果一致（PG `SELECT` 与 REST `lookup` 行一致）~~（随 REST direct read 后置，见 §7）
- [ ] metadata 在 PG（pg_catalog 视图）与 REST（metadata 端点）中一致；并记录两 cache 的 drift 是已知风险（P6 第 4 点）

### T4. 语义测试（钉死 Phase 1 承诺）

- [ ] REST 写 at-least-once：成功=backend ack；中途失败可能已写入、不提供回滚（P5）
- [ ] SessionContext dirty/rebuild：不安全变更标 dirty，下次查询前用当前 `SessionVars` 全量重装且状态恢复（P2/P3）
- [ ] operation cancel 是 cooperative / best-effort：取消请求只尽快停，不保证立即、不做强制回滚（P2/P4）
- [ ] direct path 无 session：REST 请求不创建 `GatewaySession`、不登记 Operation（P5/P6）

---

## 5. 推荐实现顺序

建议按下面顺序推进，尽量减少返工：

1. **P0** 冻结版本线与跨仓契约
2. **P1** 定义 `Instance` 和所有核心 request/response model
3. **P2** 定义 Session / Operation 核心模型
4. **P3** 定义 SQL environment 装配层
5. **P6** 定义 backend / metadata / connection 边界
6. **P7** 定义 auth 接缝
7. **P8** 落定模块目录骨架
8. **P4** 落 PostgreSQL 协议路径
9. **P5** 落 REST direct path
10. **T1-T4** 补齐测试与语义验证

---

## 6. 可并行推进的部分

在不破坏主顺序的前提下，可考虑并行：

### 可以并行

- `P4 PostgreSQL` 与 `P5 REST`：在 `P1-P3` 稳定后可并行推进
- `P6 backend/metadata/connection` 与 `P7 auth`：契约层可并行
- `T1` 单元测试设计可随各模块同步落地

### 不建议并行过早推进

- 在 `P0` 之前写 PG 兼容逻辑
- 在 `P2` 未定前写 SessionContext rebuild 机制
- 在 `P6` 未定前分别给 PG/REST 各写一套 metadata/connection 逻辑

---

## 7. 暂缓项 Backlog

这些先明确记账，但不纳入本期主线：

- [ ] Flight SQL 协议接入
- [ ] MySQL 协议接入
- [ ] gRPC direct path
- [ ] 多集群真实实现
- [ ] doAs / per-user Fluss credentials
- [ ] REST 写入幂等 key
- [ ] REST direct read（`lookup` / `prefix-scan` / `log-scan` / `batch-lookup` 数据查询）：本期后置，路径已占位
- [ ] REST direct read 的 best-effort timeout/cancel（cancel token → cooperative stop）
- [ ] 读能力 SQL/REST 一致性测试（PG `SELECT` 与 REST `lookup` 行一致）：随 REST direct read 一并恢复
- [ ] PostgreSQL SQL 写入
- [ ] schema-on-write
- [ ] 更完整的 PG compatibility surface
