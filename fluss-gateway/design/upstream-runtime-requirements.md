# 上游需求与设计：fluss-rust 连接 runtime 无关化 + fluss-datafusion 配合

> 状态：设计收敛 + 接入验证说明（`datafusion-v0.2.2` 已基本实现核心修复）
> 目标仓库：`beryllw/fluss-rust`（验证 tag `datafusion-v0.2.2`，commit `68eba7b813aae79536a9a1b72f97b669dd670386`）
> 关联文档：`design/gateway-runtime-design.md`（网关侧配合）
> 本文 **取代** 早期草稿 `design/fluss-connection-runtime-proposal.md`（路线 A 提案），把它收敛为可实现的需求 + 接口设计。
> 决策前提：调用方（fluss-gateway）采用 **三 runtime 架构**（协议门面 / DataFusion 执行 / fluss I/O），且 **允许修改上游 fluss-rust**。

---

## 1. 一句话问题

在 `datafusion-v0.2.0` 上，一条 fluss `FlussConnection` 一旦被 **两个 tokio runtime** 触碰（典型：SQL 规划经 fluss-datafusion 私有全局 runtime、执行/直写在应用 runtime），跨 runtime 复用会 **确定性死锁**：请求字节发出后，响应永远收不到。这让"把 fluss-rs 与 DataFusion 一起用、且应用还有别的 runtime（HTTP server 等）"——也就是一个真实查询网关的标准形态——无法工作。

在 `datafusion-v0.2.2` 上，这个核心问题已基本被上游修复：`server_connection.rs` 已改成完整 connection actor，`RpcClient` 支持注入 I/O runtime handle，并且 socket 建连与 I/O task 都落在同一个 handle 上。gateway 切依赖并重跑最终的 cluster e2e（覆盖 KV 写、Log append、PG KV 查询、PG Log 查询的完整链路）后通过，说明本文中的核心改造方向已被上游吸收，当前文档更多用于**说明为什么这个修复有效、网关该如何接入、以及后续哪些 fluss-datafusion 优化仍可选**。

## 2. 根因（精确版，逐行源码佐证）

### 2.1 连接是"半个 actor"——读在任务里，写在调用方

`crates/fluss/src/rpc/server_connection.rs`：

```
ServerConnectionInner<RW> {                              // :437
    stream_write: Arc<AsyncMutex<WriteHalf<RW>>>,        // 写半边，按发送加锁
    state: Arc<Mutex<ConnectionState::RequestMap>>,      // request_id -> oneshot，reader 与调用方共享
    join_handle: JoinHandle<()>,                         // reader 任务
}
```

- `new()`（:459）`tokio::io::split(stream)` 后用 **裸 `tokio::spawn`** 起 reader（:465），隐式捕获 **首次建连时的 `Handle::current()`**。reader 是该连接 **唯一** 的响应分发者：读 socket → 解析 `request_id` → 从 `state` map 取出 `oneshot` 投递响应。
- `request()`（:536）把 `(request_id -> tx)` 插入 `state`，然后 **在调用方任务内** `self.send_message(buf).await`（:576）写 socket，再 `rx.await`（:580）等响应。
- `send_message_inner()`（:627）锁 `stream_write` → `write_message + flush`，用 `CancellationSafeFuture` 包裹防止取消留半包。

> **关键：写（`send_message`）发生在 `request()` 的调用方任务上，不在 reader 任务里。** 于是 socket 的 **写半边被调用方 runtime 触碰、读半边绑在建连 runtime**。

### 2.2 连接按 server 缓存、跨路径共享

- `RpcClient.connections: RwLock<HashMap<String, ServerConnection>>`（:181），按 `server_id` 缓存（:209-234），注释明写区分 "coordinator vs tablet server"（:174）。
- `ServerConnection = Arc<ServerConnectionInner>`（:54），读/写/元数据共享同一个 `Arc<RpcClient>`。

### 2.3 叠加成死锁

被迫跨 runtime 的是 **coordinator/metadata 连接**：catalog 规划经 fluss-datafusion 的 `block_on_with_runtime`（私有全局 runtime B）发元数据 RPC，而写/执行在应用 runtime A。谁先建这条 coordinator 连接，reader 就钉在谁那；另一边复用时写半边落到错误 runtime → 响应永远不被投递 → `rx.await` 永挂。fluss-datafusion 的桥又用 `std::thread::spawn(...).join()` 等结果，把它升级成 **硬 OS 线程死锁**（加 worker 无效）。

### 2.4 证据

- **二分隔离**：唯一触发变量 = "写在另一个 runtime 的任务里首次建连"。
- **预热实验**：把首次建连搬到会被 SQL 复用的 runtime → 死锁消失（反证因果）。
- **线程栈（macOS `sample`）**：SQL 线程卡在 `block_on_with_runtime → std::thread::join`；其 spawn 的 OS 线程 parked 在 `Handle::block_on`；**全程无线程在 `read_message`/`send_message`** → reader 未投递、写已返回。

## 3. 目标不变式（要建立的契约）

> **一条连接的 socket 全部 I/O（读 + 写）只在一个固定的 I/O 任务里发生，该任务跑在一个稳定的长生命 runtime 上；调用方只用 runtime 无关原语（mpsc 投递 + oneshot 等待）与之交互。连接对调用方在哪个 runtime 完全无关。**

成熟的 Rust 异步客户端都这么做（tokio-postgres 的 `Connection` 任务、redis-rs `MultiplexedConnection` 的 driver 任务、IOx/GreptimeDB 的专用 I/O runtime）。fluss-rs 只实现了一半（reader 是任务，但写在调用方、reader 绑调用方 runtime），所以踩坑。

---

## 4. fluss-rust 改动（已在 `datafusion-v0.2.2` 基本落地）

集中在 `crates/fluss/src/rpc/server_connection.rs` + 一处 runtime 持有（`RpcClient` / `Config`）。

### R1 —— 写搬进 I/O 任务，连接成为完整 actor（`datafusion-v0.2.2` 已实现）

**目标结构：**

```rust
struct Outgoing {
    request_id: i32,
    bytes: Vec<u8>,
    resp_tx: oneshot::Sender<Result<Response, RpcError>>,
}

pub struct ServerConnectionInner<RW> {
    client_id: Arc<str>,
    request_id: AtomicI32,
    outgoing_tx: mpsc::UnboundedSender<Outgoing>,   // 调用方 -> I/O 任务
    poison: Arc<Mutex<Option<Arc<RpcError>>>>,      // 供 request() 快速失败
    api_versions: Mutex<Option<ServerApiVersions>>,
    io_handle: JoinHandle<()>,                      // 单一 I/O 任务（读 + 写）
}
```

**单一 I/O 任务**独占 `stream_read` 和 `stream_write`，`request_id -> resp_tx` 的 map **收进任务本地**（不再 `Arc<Mutex>`）：

```rust
io_runtime.spawn(async move {
    let mut pending: HashMap<i32, oneshot::Sender<_>> = HashMap::new();
    loop {
        tokio::select! {
            // 出站：调用方提交的请求
            maybe = outgoing_rx.recv() => match maybe {
                Some(Outgoing { request_id, bytes, resp_tx }) => {
                    pending.insert(request_id, resp_tx);
                    if let Err(e) = write_frame(&mut stream_write, &bytes).await {
                        poison_all(&mut pending, e); break;       // 写错误 -> 全部失败
                    }
                }
                None => break,   // 所有 sender drop（连接 Arc 全释放）-> 自然退出
            },
            // 入站：socket 读到响应
            res = stream_read.read_message(max) => match res {
                Ok(msg) => {
                    let hdr = parse_header(&msg);
                    if let Some(tx) = pending.remove(&hdr.request_id) {
                        let _ = tx.send(Ok(Response { header: hdr, data: cursor }));
                    }
                }
                Err(e) => { poison_all(&mut pending, e); break; }
            },
        }
    }
});
```

**`request()` 改为纯 runtime 无关原语：**

```rust
pub async fn request<R>(&self, msg: R) -> Result<R::ResponseBody, Error> {
    if let Some(e) = self.poisoned() { return Err(...); }          // 快速失败
    let api_version = self.resolve_api_version(R::API_KEY)?;
    let request_id = self.request_id.fetch_add(1, SeqCst) & 0x7FFFFFFF;
    let bytes = encode(header(request_id, api_version), msg)?;
    let (resp_tx, resp_rx) = oneshot::channel();
    self.outgoing_tx
        .send(Outgoing { request_id, bytes, resp_tx })
        .map_err(|_| /* I/O 任务已退出 -> Poisoned */)?;
    let response = resp_rx.await.map_err(|_| /* I/O 任务在响应前消失 */)?;
    decode(response)
}
```

**附带简化：**
- `CancellationSafeFuture`（:631-707）**移除**：写只在 I/O 任务里、不被调用方取消，天然不会留半包。
- `ConnectionState::RequestMap` 的 `Arc<Mutex<..>>` 收进 I/O 任务本地，省一把锁；`Poison` 改为 `poison` 字段广播。
- `send_message` / `send_message_inner` / `stream_write: Arc<AsyncMutex<..>>` 全部删除。

**原因**：消除"写在调用方 runtime"的泄漏，使连接对调用方 runtime 完全无关（对齐 redis-rs / tokio-postgres）。这是三 runtime 架构成立的 **前置条件**。

### R2 —— I/O 任务跑在可注入的专用 runtime

- `RpcClient` 持有一个 I/O runtime 的 `Handle`，连接 I/O 任务用 `io_handle.spawn(io_task)` 启动，**替代 :465 的裸 `tokio::spawn`**。
- 注入入口（二选一或都提供）：
  - `fluss::config::Config` 增加 `io_runtime_handle: Option<Handle>`；
  - 或 `RpcClient::with_io_handle(Handle)` 构造器。
- 缺省（`None`）：惰性建一个进程级 `OnceLock<Runtime>`（`enable_all`，独立线程，长生命）。**注意**：这与今天 fluss-datafusion 里那个全局 runtime 形似，但语义已根治——因为现在 **所有 socket I/O 都在它里面**，不再有"写在别处"的泄漏。

**原因**：让宿主（网关）能注入自己的 `fluss_rt`，统一线程数 / 可观测性 / workload 隔离；即使不注入，进程级缺省 runtime 也已保证正确。

### R3 —— 生命周期收敛

- I/O 任务在 `outgoing_rx` 所有 sender 关闭（连接 `Arc` 全 drop）或 socket 出错（poison）时退出。
- `Drop`（:644）的 `join_handle.abort()` 退化为 backstop（正常路径靠"drop sender 自然结束"），不再依赖 abort 一个绑在某 runtime 上的任务。
- 取消语义：调用方取消 `request()`（drop `resp_rx`）时，I/O 任务从 `pending` 里发现 `resp_tx` 已 closed 即清理，不影响其它在飞请求；已排队/已发出的写由 I/O 任务原子完成整帧（绝不留半包）——取代原 `CancellationSafeFuture` 的职责。

### 公共 API 兼容性

- `FlussConnection` / `RpcClient::get_connection` / `ServerConnection::request` 的 **对外签名与语义不变**；连接按 server 缓存复用、`Arc<RpcClient>` 共享 **不变**。
- 改的只是连接 **内部** I/O 执行模型。`writer_client` / `lookup_client` / metadata 全部受益、无需改动。
- 新增的只有 `Config::io_runtime_handle`（可选，向后兼容）。

### fluss-rust 验收标准

1. 单元/集成：构造连接的 runtime 与发起 `request()` 的 runtime **不同** 时，请求成功返回（新增针对性测试，复刻跨 runtime 场景）。
2. 取消：drop `request()` future 后，同连接后续请求不受影响、无半包、无泄漏（`pending` map 清理）。
3. 生命周期：连接 `Arc` 全 drop 后 I/O 任务在有限时间内退出（不靠 abort）。
4. 回归：现有 fluss-rs RPC/writer/lookup 测试全绿。
5. 端到端：作为下游验证，gateway 全量 e2e（并发 REST 写 + PG SELECT、三 runtime、共享连接）**去掉预热后转绿**（见 `design/gateway-runtime-design.md` §测试）。

---

## 5. fluss-datafusion 改动（R1 落地后：正确性零改动，仅性能优化）

### 现状

- `FlussDatafusionOptions` 是空结构体（`config.rs:21`）。
- catalog 同步回调（`schema_names`/`schema`/`table_names`/`table_exist`）经 `runtime::block_on_with_runtime`（`runtime.rs:47`）→ 私有全局 `OnceLock<Runtime>` + `std::thread::spawn(move || handle.block_on(fut)).join()`（`provider.rs:55,73`、`schema.rs:64,106`）。
- execution（`execution/{log_scan,lookup,stream}.rs`）是纯 `.await`，由调用方 runtime 驱动。

### 关键认知：R1 之后，桥不再是正确性问题

R1 让 `request()` 变成纯 channel 操作（runtime 无关）。于是：
- catalog 桥在私有全局 runtime B 上 `block_on(rpc)` —— RPC future 只做 mpsc.send + oneshot.await，**不碰 socket**；socket 永远由 fluss I/O runtime 的 actor 独占驱动 → **不死锁**。
- execution 在 DataFusion 执行 runtime 上 inline `.await` —— 同理安全。

> **结论：R1 落地后，fluss-datafusion 不需要任何正确性改动即可在多 runtime 下工作。** 下面两项是 **可选** 的性能/整洁优化，不阻塞。

### D1（可选，性能）—— 让桥不再 per-call spawn OS 线程

现状每次 catalog 回调都 `std::thread::spawn(...).join()`，规划期 schema/table 探查会反复付出建线程开销。

- 方案 a：`FlussDatafusionOptions` 增加 `fluss_runtime: Option<Handle>`，桥改为在该 handle 上 `block_on`（仍需独立线程避免"在 runtime 内 block_on 当前 runtime"的 panic，但可复用而非每次新建——用一个常驻 block-on 工作线程 + channel，或 `tokio::task::block_in_place` 视上下文）。
- 方案 b（更彻底，见 D2）：干脆去掉 sync 桥。

### D2（可选，整洁/性能）—— async-upfront catalog

参考 iceberg 集成的做法：在 catalog 注册期 **异步预取一次元数据快照**（schema/table 列表），把 `schema_names`/`schema`/`table_names`/`table_exist` 变成 **纯内存查询**，彻底移除 `block_on_with_runtime`。
- 收益：消除 sync 桥与 per-call 线程；规划期零阻塞 RPC。
- 代价：元数据快照的新鲜度策略（TTL / 失效刷新）。
- 与本问题正交：即使不做，R1 已保证正确性。

### fluss-datafusion 验收标准

- R1 落地后，`FlussDatafusion` 在"建连 runtime ≠ 规划 runtime ≠ 执行 runtime"下，catalog 规划 + 点查/扫描全部成功（由 gateway e2e 覆盖）。
- 若实施 D1/D2：catalog 回调不再 per-call `std::thread::spawn`；现有 datafusion 集成测试全绿。

---

## 6. 依赖关系与优先级

```
R1 (连接 actor 化)  ----+----> 三 runtime 架构正确性的地基（已在 v0.2.2 验证）
R2 (注入 I/O handle)    |       网关 workload 隔离/可观测（已在 v0.2.2 验证）
R3 (生命周期收敛)  ------+       随 R1，一并由上游吸收

D1 / D2 (fluss-datafusion) ---> 仅性能/整洁，当前非阻塞（可后置）
```

- **已证实**：R1 + R2 的核心修复思路已经在 `datafusion-v0.2.2` 生效，gateway 端到端验证通过。
- **可后置**：D1 / D2（fluss-datafusion 性能优化，例如减少 per-call `thread::spawn().join()`）。
- **网关侧唯一上游耦合**：经 `Config::io_runtime_handle` 注入一个 `Handle`（见网关文档）；当前 gateway 尚未落三 runtime 装配，但这已不再阻塞正确性。

## 7. 验证计划

1. 基线：在 `datafusion-v0.2.0` 上，gateway e2e 可稳定复现 SQL/runtime 死锁。
2. 切到 `datafusion-v0.2.2` 后，重跑最终的 cluster e2e：
   - `cluster_rest_kv_and_log_then_pg_selects`（覆盖 KV 写、Log append、PG KV 查询、PG Log 查询的完整链路）
3. 该测试转绿后，确认：
   - 原 SQL/runtime 死锁已修；
   - log-only 与 mixed-flow 路径在当前代码下也可走通。
4. 后续若再出现不稳定挂起，再从这条最终 e2e 出发按阶段拆分定位是否需要继续修改上游 writer / metadata 路径。

## 8. 风险 / 注意

- **mpsc 背压**：出站用 unbounded 或合理 bounded；确认 writer 的 PutKv 批处理路径也走同一条连接 actor（不另开旁路）。
- **poison 广播**：socket 出错时，`pending` 里所有在飞请求都要收到 `Poisoned`，且后续 `request()` 快速失败（保留现有 `ConnectionState::poison` 的语义，迁移到任务本地 + `poison` 字段）。
- **启动顺序**：注入的 I/O runtime 必须在第一次建连前就绪；缺省 `OnceLock` 惰性初始化即可。
- **api_versions 握手**：握手 RPC 同样要走新的 `outgoing_tx` 路径（握手发生在连接可用之前，注意 I/O 任务启动与握手的时序）。
