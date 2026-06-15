# 与 fluss-datafusion 的契约（D1 – D4）

> fluss-gateway 模块设计。全局决策与 P0 集成模型见 [`../DESIGN.md`](../DESIGN.md)，待做清单见 [`../TASKS.md`](../TASKS.md)（同 D 编号）。
> 这部分能力**不在本仓实现**（落在 `fluss-rust` 的 `fluss-datafusion` crate），但它们是 gateway SQL 路径的前置契约。

并行开发约束（CLAUDE.md）：gateway 依赖一个**窄而明确的 installer/API 契约**；若需要新能力，先把它定义成 crate-facing API / 行为契约，**不**把 DataFusion catalog/execution/type 逻辑吸收回 gateway。

gateway 只通过两个东西与该 crate 交互：**一个共享 `FlussDatafusion` 对象** + **`register_catalog` 安装动作**。其余（metadata cache、provider、predicate analyzer、类型桥接）都是 crate 内部实现，gateway 既不持有也不旁路。出处：`FLUSS_DATAFUSION.md`。

### D1. 共享 installer 契约（gateway 实际调用面）

- `FlussDatafusion::new(connection, options)`：gateway 每 cluster 构造一个，**共享**复用（见 DESIGN.md P0 集成模型）。
- `register_catalog(&ctx, "fluss", options)`：把**真实 Fluss catalog**装进 gateway 提供的 per-session `SessionContext`；**只装 Fluss catalog，绝不装 pg_catalog**（pg 兼容是 gateway P3 第 3/4 步的事）。
- 共享 metadata cache / provider descriptor / helper 全在 `FlussDatafusion` 内部，gateway 够不到——契约上确认这层不暴露。

### D2. SQL 能力边界（gateway 依赖的下推语义）

- KV point lookup 下推：完整主键等值 → 单次 point lookup，**不经全表扫**。
- Log bounded scan 下推：**LIMIT required**、offset ascending、默认 earliest（必须与 P0 锁定的 Log 语义一致）。
- prefix scan：Phase 1 **可选**，仅当底层 client 支持单列 string/binary 主键前缀。
- 非下推 SQL 的保守处理：KV 无完整主键 / Log 无 LIMIT → **返回清晰错误**，不伪装成全表扫（gateway 据此把错误映射到协议层）。
- **cancel 协作性（crate-facing 契约）**：下推的 KV lookup / Log scan 对应的 `ExecutionPlan` / stream 必须接受协作取消（`CancellationToken` 或在 `poll_next` 中响应取消信号），在执行过程中尽快协作退出并释放底层资源。gateway 的 tracked stream 停止 poll 或触发 cancel/timeout 时，依赖这点真正中止后端读取——否则 [`core-session.md`](core-session.md) P2 的 cooperative cancel / timeout 语义无法落地。

### D3. 类型与错误边界（跨 crate 的对接面）

- Arrow schema / row conversion 契约：Fluss schema → Arrow schema、Fluss row → `RecordBatch`，结果统一 Arrow-native（gateway 编码层依赖这点）。
- `ScalarValue -> Fluss key` 转换边界：谓词/bind 值的基础 `ScalarValue` 安全转 Fluss key representation，转换失败给明确错误。
- `FlussDatafusionError` 与 gateway domain error 的分层：crate 只产 DataFusion 集成层错误（不掺协议码），gateway 在 SQL 服务入口映射成 P1 domain error（呼应 P6 错误分层）。

### D4. 性能与缓存要求（gateway 体验依赖）

- 新建 SQL session 不触发全量 metadata 扫描：`register_catalog` 默认 lazy，不预热所有 db/table。
- 相同 database/table 元数据跨 session 复用：依赖 `FlussDatafusion` 共享 cache。
- per-session 只新建 `SessionContext`，不新建整套 metadata/cache（与 P3 rebuild 模型契合——rebuild 换 ctx，不换共享对象）。
