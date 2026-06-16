# fluss-gateway 三 runtime 架构设计（实现规格）

> 状态：设计（待实现 — 将由独立 agent 据此落地）
> 关联文档：`design/upstream-runtime-requirements.md`（上游 fluss-rust/fluss-datafusion 改动与接入说明）。
> 本文 **取代** 早期草稿 `design/gateway-runtime-architecture.md`，给出可实现的接口规格。
> **依赖前提**：上游 fluss-rust 在 `datafusion-v0.2.2` 已落地 R1（连接 actor 化）+ R2（可注入 I/O `Handle`）的核心修复，gateway 已通过 cluster e2e 证实原 SQL/runtime 死锁消失。本文保留为网关长期三 runtime 架构的实现规格；当前正确性已不再被上游连接模型阻塞。

---

## 0. 设计目标

1. **正确性**：消除"一条 fluss 连接被多 runtime 触碰"导致的死锁。手段 = 让 fluss 全部 socket I/O 落在单一长生命 runtime（`fluss_rt`），并由上游 R1 保证连接对调用方 runtime 无关。
2. **workload 隔离**：把"协议门面 I/O"、"DataFusion 计算（CPU-bound）"、"fluss 网络 I/O"分到三个独立 runtime，互不饿死。
3. **最小耦合**：网关对上游的唯一耦合是注入一个 `Handle` 到 fluss `Config`。

---

## 1. 三 runtime 拓扑与职责红线

```
+-------------------------------------------------------------+
|                       Gateway Process                       |
|                                                             |
|  protocol_rt          df_rt              fluss_rt           |
|  (IO-bound,           (CPU-bound,        (IO-bound,         |
|   必须秒回)            DataFusion)         所有 fluss socket) |
|                                                             |
|  axum accept          logical plan        connection actor  |
|  pgwire accept        physical exec       reader + writer   |
|  codec / 握手 / auth   join/sort/agg       metadata refresh  |
|  cancel 监听           stream poll         writer/lookup RPC |
+-------------------------------------------------------------+
```

**三条红线（设计骨架，实现必须遵守）：**

1. `protocol_rt` **绝不** 直接调 DataFusion、**绝不** 直接调 fluss client。只做 accept / 编解码 / 握手 / auth / 取消监听，然后把活 dispatch 出去、await 句柄。
2. `df_rt` 只做计算。它需要 fluss 数据时不在本 runtime 直接发 RPC——靠上游 R1，`request()` 是 runtime 无关的 channel 操作，inline `.await` 即可（socket I/O 实际发生在 `fluss_rt` 的连接 actor 里）。
3. `fluss_rt` 独占所有 fluss 连接的 socket I/O（每条连接的 reader 和 writer）。连接在此诞生（经注入 handle）。

> 注：red line 2 之所以允许 `df_rt` inline await fluss，是因为 R1 让 socket I/O 与调用方 runtime 解耦。若上游 R1 未就绪，则 red line 2 需改为"`df_rt` 把 fluss future spawn 到 `fluss_rt` 再 await"——但那是 §9 临时态，不是目标态。

---

## 2. `GatewayRuntimes`：runtime 的创建与持有

在 `src/runtime/mod.rs`（新增模块）定义：

```rust
pub struct GatewayRuntimes {
    /// 协议门面：axum / pgwire accept、编解码、握手。
    pub protocol: tokio::runtime::Runtime,
    /// DataFusion 规划 + 执行（CPU-bound）。
    pub df: tokio::runtime::Runtime,
    /// fluss 全部 socket I/O（连接 actor 在此）。注入给 fluss Config。
    pub fluss: tokio::runtime::Runtime,
}

pub struct RuntimeConfig {
    pub protocol_workers: usize,
    pub df_workers: usize,
    pub fluss_workers: usize,
}
```

**worker 预算规则（实现必须显式配置，不靠默认）：**
- 三者 worker 之和 **≈ 核数**，不超订（每个都给满核会导致 N×核数 线程抢同一批核）。
- 默认建议（按 `available_parallelism()` 推导后分配，给出下限保护）：
  - `df`：主力，约 60%~70% 核（下限 2）。
  - `fluss`：网络足够，约 2~3 个（下限 2）。
  - `protocol`：约 2 个（下限 2，保证 accept/cancel 响应性）。
- **容器陷阱**：`available_parallelism()` 在 Linux 上不看 cgroup CPU quota，会误报宿主核数。实现必须支持从配置/环境变量显式覆盖三个 worker 数。

每个 runtime 用 `Builder::new_multi_thread().worker_threads(n).enable_all().thread_name(...)` 构造，便于 `tracing`/线程栈区分。

**持有与生命周期**：`GatewayRuntimes` 由进程入口（`main` / harness）创建并持有到进程结束。`fluss` runtime 的 `Handle` 注入 fluss `Config`；`protocol` / `df` 的 `Handle` 传给装配层做 dispatch。

> 进程入口形态：`main` 用一个轻量 runtime（或直接 `protocol_rt.block_on(serve)`）启动；三个业务 runtime 显式建。**不要** 用顶层 `#[tokio::main]` 然后在其中再嵌套建 runtime 而不规划归属。

---

## 3. dispatch 接缝（谁 spawn 到谁）

### 3.1 SQL 读路径（PG SELECT）

```
pgwire accept [protocol_rt]
   |  解码 + auth，拿到 SQL 文本与 session
   |  df_rt.spawn( async { instance.execute_sql(...).await } )   <- 经并发闸
   v
plan + execute [df_rt]
   |  catalog 回调 / scan / lookup -> fluss client（inline await，R1 保证安全）
   v
socket I/O [fluss_rt]
   |  连接 actor 投递响应 -> 回到 df_rt
   v
RecordBatch 跨 runtime 回到 protocol_rt -> 编码为 PG DataRow 下发
```

### 3.2 REST 直写路径（无 DataFusion）

```
axum accept [protocol_rt]
   |  解码 + auth
   |  fluss_rt.spawn( async { backend.write_direct(...).await } ).await
   v
socket I/O [fluss_rt]  ->  200 回 protocol_rt
```

**direct 路径不进 `df_rt`**——直写是纯 fluss I/O，`protocol_rt` 直接 dispatch 到 `fluss_rt`。

### 3.3 dispatch 工具

封装一个跨 runtime dispatch 助手，统一背压与取消：

```rust
pub struct Dispatcher {
    df: Handle,
    fluss: Handle,
    sql_permits: Arc<Semaphore>,     // df_rt 并发上限（背压）
    write_permits: Arc<Semaphore>,   // fluss_rt 直写并发上限
}

impl Dispatcher {
    // 在 df_rt 上跑 SQL 操作，受 sql_permits 限并发，受 token 取消。
    pub async fn run_sql<F, T>(&self, token: CancellationToken, fut: F) -> GatewayResult<T>;
    // 在 fluss_rt 上跑直写操作。
    pub async fn run_write<F, T>(&self, fut: F) -> GatewayResult<T>;
}
```

- **背压**：`df_rt` 饱和时新 SQL 在 `Semaphore` 上排队，而非无限 `spawn`。
- **取消**：`CancellationToken`（runtime 无关）从 `protocol_rt`（PG cancel / 客户端断连）传入 `df_rt` 任务；任务协作检查 token，外层同时 `abort` JoinHandle 作为兜底。对齐 CLAUDE.md "thread cancellation tokens through long-running read paths"。
- **错误边界**：spawn 失败 / JoinHandle panic / 取消，统一映射为 `GatewayError`，不泄漏 join 细节。

---

## 4. 连接 provider（P6）改动

`src/connection/mod.rs::SharedProxyConnectionProvider`：

1. **注入 fluss I/O runtime**：`build_config` 时把 `fluss_rt` 的 `Handle` 写入 fluss `Config::io_runtime_handle`（上游 R2 提供的字段）。这样连接的 I/O actor 始终在 `fluss_rt` 诞生，**与 `resolve()` 在哪个 runtime 被调用无关**。

```rust
fn build_config(cfg: &ClusterConfig, fluss_io: &Handle) -> fluss::config::Config {
    fluss::config::Config {
        bootstrap_servers: cfg.bootstrap_servers.clone(),
        io_runtime_handle: Some(fluss_io.clone()),   // R2
        ..Default::default()
    }
}
```

2. **共享连接保持不变**：`resolve(cluster, principal) -> 按 cluster 缓存的共享 Arc<FlussConnection>`，SQL 与 direct 共享同一条。共享与否不是病根（病根是 runtime 归属，已由 R1+R2 根治），**不拆**。
3. provider 构造增加 `fluss_io: Handle` 依赖（由装配层从 `GatewayRuntimes.fluss.handle()` 传入）。

> 实现注意：即使不依赖 R2（缺省进程级 I/O runtime 也已正确），仍 **建议** 注入，以统一线程数 / 可观测 / workload 隔离。注入是网关对上游的唯一耦合点。

---

## 5. 装配层改动（Instance / harness）

`FlussDatafusion::new(connection, options)` 与 `FlussBackendFacade::new(connection)` **签名不变**——它们拿到的 `Arc<FlussConnection>` 已经是"I/O 落在 `fluss_rt`"的连接。装配层要变的是：

- 创建 `GatewayRuntimes` 并把 `fluss_rt.handle()` 传给 `SharedProxyConnectionProvider`。
- 把 `df_rt.handle()` / `fluss_rt.handle()` + `Semaphore` 组装成 `Dispatcher`，注入到协议 server（PG/REST handler）。
- 协议 handler 的"调 instance"处，全部改为经 `Dispatcher`（§3）跨 runtime dispatch，而非在 accept 任务里 inline 调用。

涉及文件（实现 checklist）：

| 文件 | 改动 |
|---|---|
| `src/runtime/mod.rs`（新增） | `GatewayRuntimes` / `RuntimeConfig` / `Dispatcher` |
| `src/connection/mod.rs` | provider 注入 `fluss_rt` handle 到 `Config`（§4） |
| `src/server/postgres/handler.rs` | `execute_select` 等改为经 `Dispatcher::run_sql`（df_rt + token + 背压） |
| `src/server/rest/*` | 写 handler 改为经 `Dispatcher::run_write`（fluss_rt） |
| `src/sql/gateway_service.rs` | `execute_sql` 保持在传入 runtime 上执行；确保 stream drain 也在 df_rt（不要把 drain 漏回 protocol_rt） |
| `src/backend/mod.rs` | `write_direct` 等保持 async；由 `Dispatcher` 决定落 `fluss_rt`（facade 自身不持 runtime） |
| `src/lib.rs` / harness / `main` | 创建 `GatewayRuntimes`、组装 `Dispatcher`、注入 provider |

> 关键：`FlussBackendFacade` / `FlussDatafusion` / `SqlGatewayService` 本身 **不持有 runtime handle**、不自己 spawn——runtime 归属由外层 `Dispatcher` / provider 决定。保持核心组件 runtime 无关（对齐 CLAUDE.md "gateway core protocol-agnostic"，这里延伸为 runtime-agnostic）。

---

## 6. 不变式与必须避免的反模式

**不变式**：fluss 连接的 socket 读写终生只被 `fluss_rt` 驱动（由 R1+R2 + §4 注入共同保证）。

**反模式（实现 review 时要查）：**
- ❌ 在 `protocol_rt` 的 accept 任务里 inline 调 `instance.execute_sql` / `backend.write_direct`（CPU 重活 / fluss I/O 占住门面 worker）。
- ❌ 在 `df_rt` 里 `fluss_rt.spawn(...).await` 之外又新建临时 runtime / 裸 `tokio::spawn` 跑 fluss。
- ❌ provider 忘记注入 `io_runtime_handle`，退回缺省进程级 runtime（虽仍正确，但丢隔离/可观测）。
- ❌ 把 stream drain 从 `df_rt` 漏回 `protocol_rt`（执行期算子会在错误 runtime 上 poll）。

---

## 7. 可观测性

- 三个 runtime 用区分的 `thread_name`，`tracing` span 标注所在 runtime，便于线程栈（`sample` / `tokio-console`）定位。
- 暴露每个 runtime 的活跃任务数、`Semaphore` 排队长度、跨 runtime dispatch 延迟（直写 / SQL 各一条）。
- 保留一个"连接清单"指标（按 cluster/server 的连接数），便于确认连接复用与 I/O 归属。

---

## 8. 测试与验收

当前保留一条最终的 cluster e2e 回归：
- `tests/cluster_e2e.rs::cluster_rest_kv_and_log_then_pg_selects`
  - 覆盖原始全量流：REST KV upsert -> REST Arrow append -> fluss client readback -> log offsets readable -> PG KV SELECT -> PG log SELECT。
  - 所有关键阶段都带有显式 timeout 与语义断言，既证明原 SQL/runtime 死锁已修，也守护 log append 与 mixed-flow 组合路径。

在 `datafusion-v0.2.2` 上，这条测试已转绿，说明：
- 原 SQL/runtime 死锁已修；
- log-only 与 mixed-flow 路径在当前 gateway 代码下也能走通；
- 后续若再出现不稳定挂起，可直接从这条最终 e2e 复现入口继续拆分定位。

此外，长期三 runtime 架构落地后仍应补充：
- **取消语义**：PG 客户端在长 SELECT 中途 cancel / 断连 → `df_rt` 任务在有限时间内停止、连接不被毒化、后续查询正常。
- **背压**：超过 `sql_permits` 的并发 SELECT 排队而非 OOM/线程爆炸。
- **隔离**：一个 CPU 重 SQL 不影响 REST 直写 / 新连接 accept 的延迟（protocol_rt 不被 df_rt 饿死）。
- 默认 `cargo test`（无 `integration_tests` feature）不受影响：协议管路仍由 fake-based 单测/集成测覆盖。

---

## 9. 临时态（仅 R1 落地前解阻，不进主线）

二选一，仅用于在上游合入前不阻塞网关其余开发：
- **预热**：服务接受流量前，在"会被 SQL 复用的 runtime"上对默认 cluster 触发一次轻量 SQL/建连。已在 `cluster_e2e.rs` 验证有效；局限：运行期新 server/分区在请求里首次建连会复发。
- **A1 本地 patch**：临时把 fluss-datafusion `block_on_with_runtime` 改用 `Handle::current()`（spawn 线程 block_on 调用方 runtime），把规划收进调用方 runtime。比预热稳，仍是临时。

> R1+R2 落地后：移除预热 / A1 patch，移除 §10 列出的诊断插桩，e2e 应天然转绿。

## 10. 收尾清理清单（R1 落地、e2e 转绿后）

- 移除临时诊断插桩，恢复运行时代码的最终交付形态。
- 移除 `tests/cluster_e2e.rs` 里的预热块与临时诊断测试，收敛为单一干净 e2e。
- `tests` 的 `[features] integration_tests` + `fluss-test-cluster` dev-dep **保留**（正式集群集成测试开关）。
- `OverlayCatalogProvider`（`collaborators.rs`）**保留**——与 runtime 问题无关，是 pg_catalog 装配所需，已验证正确。
- 更新 / 删除早期草稿 `design/gateway-runtime-architecture.md`、`design/fluss-connection-runtime-proposal.md`（已被本文与 `upstream-runtime-requirements.md` 取代）。

---

## 11. 实现顺序（给实现 agent）

1. 新增 `src/runtime/mod.rs`：`GatewayRuntimes` + `RuntimeConfig`（worker 预算 + 容器覆盖）+ `Dispatcher`（背压 + 取消 + 错误边界）。先写单测（worker 配置、semaphore 背压、取消传播）。
2. `connection/mod.rs`：provider 接收并注入 `fluss_rt` handle 到 `Config`（依赖上游 R2 字段；R2 未就绪时先留 TODO + 缺省 runtime）。
3. 装配层（lib/harness/main）：创建 `GatewayRuntimes`、组装 `Dispatcher`、串联 provider 与协议 server。
4. `server/postgres/handler.rs` + `server/rest/*`：accept 任务改为经 `Dispatcher` dispatch（SQL→df_rt、写→fluss_rt）。
5. e2e：跑最终 real-cluster 回归 `cluster_rest_kv_and_log_then_pg_selects`（覆盖 KV + Log + PG KV + PG Log）→ 转绿。
6. 收尾清理（§10）。
