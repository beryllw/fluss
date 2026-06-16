# Fluss Gateway 设计总览

> 这是 **内部设计索引**，面向模块实现与评审。
> 如果你是模块使用者或联调方，请先看 [`README.md`](./README.md)。

本文件保留全局范围、架构决策、跨仓契约入口和设计索引。详细设计拆在 `design/` 下。

---

## 1. 范围

网关交付：

- PostgreSQL wire protocol：只读 SQL
- REST：direct write（KV upsert/delete、Log append）+ metadata 只读呈现
- SQL 路径的 Session / Operation 生命周期管理
- 面向 BI / IDE 的 PostgreSQL 兼容
- `principal` 在网关内部调用链全程保留

行为边界（与 [`README.md`](./README.md) 的“范围与限制”一致）：

- PostgreSQL 入口只读；REST 是唯一写入路径。
- REST direct-read 端点未实现，返回 `501`。
- 只路由 `default` 集群。
- 认证为 trust-by-username，不做 authorization。
- 不做 schema-on-write。
- REST 写语义 at-least-once；timeout / disconnect 后结果可能未知，不承诺回滚。

---

## 2. 架构决策

- **核心分层**：`Instance` 是统一 facade（session / sql / operation / direct / metadata）；协议模块只做 transport、编解码、握手、协议兼容。SQL 协议与 direct 协议分开建模，不把 direct path 套进 SQL 执行链。
- **Session / Operation**：Session 是连接级状态，Operation 是查询级状态；只有 SQL 路径暴露用户可见的 Operation。direct path 只用 request-scoped timeout / cancel，无用户级 cancel API。
- **PostgreSQL / REST 职责**：PG 只读；REST 是唯一写入入口，写语义 at-least-once；timeout / disconnect 后写入结果可能未知，不承诺回滚。
- **fluss-datafusion 边界**：它负责 `SQL -> Fluss` 的 DataFusion 集成；网关负责协议、session、operation、auth、metadata 展示、direct API。这些 SQL 内核能力不回流网关。
- **连接与身份**：Fluss 连接用共享 proxy 账号；`FlussConnectionProvider.resolve(cluster, principal)` 保留 principal；REST 多集群演进用路径前缀，不用 header。
- **runtime 归属**：网关 core 保持 runtime-agnostic；协议门面、DataFusion 执行、Fluss I/O 的 runtime 划分属于装配层与基础设施约束，不应泄漏进 core 接口面。

---

## 3. 版本线与跨仓契约

### 依赖版本线

- 网关与 fluss-datafusion 共用同一条 DataFusion / Arrow 主版本线。
- pgwire / arrow-pg / datafusion-pg-catalog 围绕这条线选型，网关不另起平行兼容层。

### 网关与 fluss-datafusion 的边界

- fluss-datafusion：catalog/schema/table provider、predicate pushdown、execution plan、Arrow 类型桥接、metadata cache。
- 网关：协议接入、session、operation、auth、cluster、connection provider、pg_catalog 兼容、direct read/write、timeout/cancel。
- 网关不回收 `catalog/` `execution/` `types/`；fluss-datafusion 不感知 protocol / principal / session vars / pg_catalog / REST 写入。

### fluss-datafusion 集成模型

- 每个 `(cluster, proxy connection)` 一个共享 `FlussDatafusion`。
- 每个 SQL session 独立 `SessionContext`。
- 首次执行或 rebuild：建新 `SessionContext` -> `register_catalog(&ctx, "fluss", ...)` -> `SqlEnvironmentProvider` 安装 pg_catalog 与 session vars。

### Log 表 SQL 语义

- 查询必须带 `LIMIT`，否则报错。
- 返回固定 offset ascending。
- SQL 层默认从 earliest available offset 读。
- 不暴露 offset 伪列、不支持基于 offset 的谓词下推。
- tail / read-latest 留给 direct path，不混入 SQL。

### cancel / timeout 传播边界

- **SQL path**：statement timeout + request timeout 收敛到 operation deadline；operation 持 `CancellationToken`；tracked stream 对齐真实状态；下游 cooperative cancel。
- **direct read**：仅 request-scoped deadline + token，对外 best-effort cancel。
- **direct write**：无用户可见 Operation、无用户级 cancel；timeout/disconnect 只表示网关停止等待，不代表后端写入取消；对外 at-least-once，结果可能未知。

---

## 4. 模块索引

| 设计文档 | 覆盖 | 对应代码模块 |
|---|---|---|
| 本文件 `DESIGN.md` | 范围 / 架构决策 / 跨仓契约 / 设计入口 | — |
| [`design/core-session.md`](design/core-session.md) | 核心接口与请求模型；Session 与 Operation | `instance/` `types.rs` `error.rs` `session/` |
| [`design/sql-path.md`](design/sql-path.md) | SQL 环境装配；PostgreSQL 协议 | `sql/environment/` `server/postgres/` |
| [`design/direct-path.md`](design/direct-path.md) | Direct path 与 REST API | `direct/` `server/rest/` |
| [`design/infra.md`](design/infra.md) | Backend/Metadata/Connection；Auth；模块骨架 | `backend/` `connection/` `cluster/` `auth/` |
| [`design/datafusion-contract.md`](design/datafusion-contract.md) | 与 fluss-datafusion 的契约 | 跨仓契约 |

---

## 5. 文档使用方式

- 用户或联调方：先看 [`README.md`](./README.md)
- 模块实现：从本文件确认范围与边界，再进入 `design/*.md`

一句话总结：网关的核心是 **core contract、session/operation 生命周期、SQL environment 装配、REST 写语义、与 fluss-datafusion 的边界**。
