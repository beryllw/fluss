# Fluss Gateway Phase 1 设计 · 总览

> 本目录是 fluss-gateway 的设计 source-of-truth。本文件只承载**全局决策与索引**：Phase 1 范围、锁定的架构决策、跨仓契约（P0）、模块索引、实现顺序。
> 各模块的详细设计拆在 `design/` 下，按 P 编号分组（见 §4）。可执行的待做清单与进度在 `TASKS.md`，与设计同编号对应。

---

## 1. Phase 1 范围

**必须交付：**

- PostgreSQL wire protocol：只读 SQL
- REST：direct write（KV upsert/delete、Log append）+ metadata 只读呈现
- SQL 路径的 Session / Operation 生命周期管理
- 面向 BI / IDE 的 PostgreSQL 兼容
- `principal` 在 Gateway 内部调用链全程保留

**明确不做：**

- MySQL / Flight SQL / gRPC 落地
- 多集群真实实现
- per-user Fluss credentials / doAs
- schema-on-write 自动建表
- PostgreSQL SQL 写入、完整事务语义
- direct write 幂等去重
- REST direct read（数据 lookup / scan 查询）：本期后置到后续阶段

---

## 2. 锁定的架构决策

实现时不再反复摇摆：

- **核心分层**：`Instance` 是统一 facade（session / sql / operation / direct / metadata）；协议模块只做 transport、编解码、握手、协议兼容。SQL 协议与 direct 协议分开建模，不把 direct path 套进 SQL 执行链。
- **Session / Operation**：Session 是连接级状态，Operation 是查询级状态；只有 SQL 路径暴露用户可见的 Operation。direct path 只用 request-scoped timeout / cancel，无用户级 cancel API。
- **PostgreSQL / REST 职责**：PG 本期只读；REST 是唯一写入入口，写语义 at-least-once；timeout / disconnect 后写入结果可能未知，不承诺回滚。
- **fluss-datafusion 边界**：它负责 `SQL -> Fluss` 的 DataFusion 集成（`catalog/` `execution/` `types/` 内核）；gateway 负责协议、session、operation、auth、metadata 展示、direct API。这些 SQL 内核能力不回流 gateway。详见 [`design/datafusion-contract.md`](design/datafusion-contract.md)。
- **连接与身份**：Fluss 连接本期用共享 proxy 账号；`FlussConnectionProvider.resolve(cluster, principal)` 保留 principal（即使本期不下推）；REST 多集群演进用路径前缀，不用 header。

> **起点**：仓库内尚无 fluss-gateway 代码；旧 `fluss-query-gateway` 仅作参考基线。其关键模型与新方案相反——旧方案全局共享 `SessionContext` 且把 `catalog/execution/types` 放 gateway，新方案要求 per-session `SessionContext` 且下沉这些能力到 fluss-datafusion。后续是先冻结契约、再按模块搭建，只选择性复用旧实现的协议兼容与 wiring 经验。

---

## 3. P0 · 版本线与跨仓契约冻结

P0 不先冻结，P1-P5 会反复返工。

### 1. 依赖版本线

- gateway 与 fluss-datafusion 共用同一条 DataFusion / Arrow 主版本线。
- pgwire / arrow-pg / datafusion-pg-catalog 围绕这条线选型，gateway 不另起平行兼容层。
- Phase 1 优先"版本一致 + 能工作"，不追新；依赖升级是独立工作项。

### 2. gateway 与 fluss-datafusion 的边界

- fluss-datafusion：catalog/schema/table provider、predicate pushdown、execution plan、Arrow 类型桥接、metadata cache。
- gateway：协议接入、session、operation、auth、cluster、connection provider、pg_catalog 兼容、direct read/write、timeout/cancel。
- gateway 不回收 `catalog/` `execution/` `types/`；fluss-datafusion 不感知 protocol / principal / session vars / pg_catalog / REST 写入。

### 3. fluss-datafusion 集成模型

- 每个 `(cluster, proxy connection)` 一个共享 `FlussDatafusion`。
- 每个 SQL session 独立 `SessionContext`。
- 首次执行或 rebuild：① 建新 `SessionContext` → ② `register_catalog(&ctx, "fluss", ...)` → ③ 由 `SqlEnvironmentProvider` 装 pg_catalog 与 session vars。
- 不复用旧 gateway 的全局共享 `SessionContext`。

### 4. Log 表 SQL 语义（最保守集合）

- 查询必须带 `LIMIT`，否则报错。
- 返回固定 offset ascending。
- SQL 层默认从 earliest available offset 读。
- 不暴露 offset 伪列、不支持基于 offset 的谓词下推。
- tail / read-latest 留给 direct path 或后续协议，不混入本期 SQL。

### 5. cancel / timeout 传播边界

- **SQL path**：statement_timeout + request timeout 收敛到 operation deadline；operation 持 `CancellationToken`；tracked stream 对齐真实状态；下游 cooperative cancel。
- **direct read**：仅 request-scoped deadline + token，对外 best-effort cancel。
- **direct write**：无用户可见 Operation、无用户级 cancel；timeout/disconnect 只表示 gateway 停止等待，不代表后端写入取消；对外 at-least-once，结果可能未知。

**完成标准**：P1-P5 实现时不再重谈 fluss-datafusion 职责；SQL session 初始化/rebuild 流程稳定；Log 表最小语义锁定；三条链路 cancel 语义互不污染。

---

## 4. 模块索引

详细设计按 P 编号分组在 `design/`，与 `TASKS.md` 同编号对应：

| 设计文档 | 覆盖 | 对应代码模块 |
|---|---|---|
| 本文件 `DESIGN.md` | 范围 / 锁定决策 / P0 / 实现顺序 | — |
| [`design/core-session.md`](design/core-session.md) | P1 核心接口与请求模型；P2 Session 与 Operation | `instance/` `types.rs` `error.rs` `session/` |
| [`design/sql-path.md`](design/sql-path.md) | P3 SQL 环境装配；P4 PostgreSQL 协议 | `sql/environment/` `server/postgres/` |
| [`design/direct-path.md`](design/direct-path.md) | P5 Direct path 与 REST API | `direct/` `server/rest/` |
| [`design/infra.md`](design/infra.md) | P6 Backend/Metadata/Connection；P7 Auth；P8 模块骨架 | `backend/` `connection/` `cluster/` `auth/` |
| [`design/datafusion-contract.md`](design/datafusion-contract.md) | D1-D4 与 fluss-datafusion 的契约 | （跨仓，不在本 crate 实现） |

---

## 5. 推荐实现顺序

1. **P0** 冻结版本线与跨仓契约（本文件 §3）
2. **P1** 定义 `Instance` 与核心 request/response model（core-session.md）
3. **P2** 定义 Session / Operation 核心模型（core-session.md）
4. **P3** 定义 SQL environment 装配层（sql-path.md）
5. **P6** 定义 backend / metadata / connection 边界（infra.md）
6. **P7** 定义 auth 接缝（infra.md）
7. **P8** 落定模块目录骨架（infra.md）
8. **P4** 落 PostgreSQL 协议路径（sql-path.md）
9. **P5** 落 REST direct path（direct-path.md）
10. **T1-T4** 补齐测试与语义验证（见 TASKS.md）

**可并行**：P4 与 P5 在 P1-P3 稳定后并行；P6 与 P7 契约层并行。**不要**在 P0 前写 PG 兼容逻辑、P2 前写 rebuild 机制、P6 前给 PG/REST 各写一套 metadata/connection。

---

## 6. 一句话总结

后续重点不是"先把 server 跑起来"，而是先固定 **core contract、session/operation 生命周期、SQL environment 装配、REST 写语义、与 fluss-datafusion 的边界**；前置设计清楚后，PostgreSQL 与 REST 实现可按模块逐项落地，不反复返工。
