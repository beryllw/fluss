# Fluss Gateway 设计总览

> 这是 `fluss-gateway` 的设计入口文档。
> 当前规范性设计以 `design/fip/` 下的 FIP 文档为准；本文件只保留范围摘要与索引。
> 如果你是模块使用者或联调方，请先看 [`README.md`](./README.md)。

---

## 1. 当前设计范围

Phase 1 的 `fluss-gateway` 设计分成三份规范性 FIP：

1. **总体框架 FIP**：定义协议无关 core、共享边界、模块职责与整体不变量。
2. **SQL 路径 FIP**：定义 PostgreSQL wire protocol、session / operation 生命周期、DataFusion 集成与 PG 兼容边界。
3. **REST / direct 路径 FIP**：定义 request-scoped direct read/write / metadata API、写语义与 OTLP over HTTP 适配。

统一约束：

- Gateway core 保持 protocol-agnostic。
- `Instance` 是统一 facade。
- SQL 协议与 direct 协议严格分层。
- PostgreSQL 在 Phase 1 只读。
- 写入只通过 direct path。
- `principal` 在内部调用链全程保留。
- `fluss-datafusion` 是独立 SQL 集成库，不反向依赖 gateway。

---

## 2. FIP 文档索引

| FIP | 主题 | 说明 |
|---|---|---|
| [`design/fip/0001-gateway-framework.md`](./design/fip/0001-gateway-framework.md) | Gateway Framework | 总体架构、共享语义、模块边界、身份与连接模型 |
| [`design/fip/0002-sql-path.md`](./design/fip/0002-sql-path.md) | SQL Path | PostgreSQL、session vars、query lifecycle、`pg_catalog`、`fluss-datafusion` 集成 |
| [`design/fip/0003-rest-path.md`](./design/fip/0003-rest-path.md) | REST / Direct Path | request-scoped direct API、metadata、at-least-once 写语义、OTLP over HTTP |

---

## 3. 相关设计文档

以下文档继续保留，作为实现细化或跨仓契约参考：

| 文档 | 用途 |
|---|---|
| [`design/datafusion-contract.md`](./design/datafusion-contract.md) | gateway ↔ `fluss-datafusion` 的跨仓契约 |
| [`design/core-session.md`](./design/core-session.md) | 旧的 session / operation 设计拆分稿，可作为实现细节参考 |
| [`design/sql-path.md`](./design/sql-path.md) | 旧的 SQL 路径拆分稿，可作为实现细节参考 |
| [`design/direct-path.md`](./design/direct-path.md) | 旧的 direct path 拆分稿，可作为实现细节参考 |
| [`design/infra.md`](./design/infra.md) | backend / connection / auth / 模块骨架细化说明 |

如 FIP 与旧拆分稿冲突，以 `design/fip/` 下的 FIP 为准。

---

## 4. 文档使用方式

- **看整体边界**：先读 `0001-gateway-framework.md`
- **看 PostgreSQL / SQL 设计**：再读 `0002-sql-path.md`
- **看 REST / direct / OTLP 设计**：再读 `0003-rest-path.md`
- **看跨仓 SQL 内核契约**：补充阅读 `design/datafusion-contract.md`

一句话总结：当前设计已经按 **总体框架 / SQL 路径 / REST 路径** 三层整理，后续评审与实现应以这三份 FIP 为主线。
