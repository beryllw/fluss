<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to you under the Apache License, Version 2.0.
-->

# 升级到 fluss-rust datafusion-v0.3.1 + 连接自动恢复

## 概要
gateway 升级到上游 `beryllw/fluss-rust` `datafusion-v0.3.1`（rev `aa19a58`），并实现“Fluss 连接挂掉 → 自动关闭旧连接 + 新建 + 热替换、有界、不无限重试”。

升级过程中发现并由 v0.3.1 解决的两个上游问题：
1. **git 依赖不自洽**：`fluss-datafusion` 经 `fluss-lake` 用 path 依赖外部 `paimon`，git 消费无法解析。v0.3.1（`da2061c`）把 `fluss-lake` 改为可选特性 `lake`（默认关闭）并 cfg-gate 相关源码 → 默认 git 依赖自洽。
2. **全量/Log 扫描打死共享连接且不自愈**：v0.3.1（`aa19a58`）自愈被 stop 的连接并对 scanner 元数据重试做有界化 → 共享连接不再永久死亡。

## 依赖矩阵
| 依赖 | 版本 |
|---|---|
| fluss / fluss-datafusion / fluss-test-cluster | git rev `aa19a58`（tag datafusion-v0.3.1） |
| arrow / arrow-schema | 58 |
| datafusion | 53 |
| pgwire | 0.40 |
| arrow-pg | 0.14 |
| datafusion-pg-catalog | 0.17 |
| arc-swap | 1（连接热替换） |

- gateway 业务代码对上游 API 零改即兼容（从 df52/arrow57 line 抬到 df53/arrow58）。
- `fluss-datafusion` 的 `lake`（Paimon）特性默认关闭 → git 依赖自洽、可移植、可提交。

## 连接自动恢复（gateway 侧）
`src/connection/mod.rs::ConnectionManager`：
- 持有当前 `Arc<FlussConnection>`（`ArcSwap`）+ 重建用 `Config` + `on_swap` 钩子（调 `FlussDatafusion::swap_connection`）。
- `current()`：SQL 与 direct 两路按操作取最新连接（backend 不再按值定死连接）。
- `is_connection_dead(&str)`：匹配 `connection I/O task has stopped` / `connection closed before response` / `poisoned`。
- `recover()`：**单飞 + 有界**。新建连接最多 3 次、指数退避（1s/2s/4s）；成功则 `swap_connection` + `ArcSwap` 换入 + 后台 close 旧连接；2s 冷却窗口仅合并瞬时惊群。失败到上限返回明确错误。
- 触发（`GatewayInstanceImpl`）：读/SQL 路径失败若 `is_connection_dead` → `recover()` → 重试一次；写/DDL → 仅 `recover()`（为下次成功，避免 at-least-once 双写）。
- **绝不无限重试**：每次 `recover()` ≤3 次；每个失败查询至多 1 次重试。

这层与上游 v0.3.1 的自愈互补：多数瞬时断连由上游 RPC 层透明重连，gateway 的重建是兜底。

## 实测结论
- **依赖自洽**：纯 git rev（无本地 path、无 paimon）即可 `cargo build`。
- **大表全扫不再打死连接**：此前在 v0.2.6/v0.3.0 上能稳定打死共享连接并永久不可用的“并发全扫 + 其它查询”压力场景，在 v0.3.1 上**全部查询成功、无需重启**；压力期连接偶有 stopped，但上游自愈 + gateway 重建使查询持续可用。
- **有界**：连接重建受 3 次上限约束，无无限刷。
- 服务端重启：底层 RPC 透明重连，gateway 无需介入。

## 验证
- `cargo test` → 157 单测 + 9 PG 集成 + 17 REST 全绿。
- `cargo test --features integration_tests --test cluster_e2e -- --test-threads=1` → 绿（含 recovery 接线）。
- 本机 host-main：并发全扫压力 → 所有查询成功（含 `count(id)` / 点查 / `\dt`）；日志可见有界的 `rebuilding dead → rebuilt and swapped in` 兜底。
