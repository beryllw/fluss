# 需求(上游 fluss-datafusion / fluss-lake):lake 读取需要一个生产级的存储凭据注入入口

> 状态:**已在上游 `datafusion-v0.5.0` 落地**(`FlussDatafusionOptions.lake_storage_options`),gateway 已通过
> `GATEWAY_LAKE_S3_*` / `GATEWAY_LAKE_STORAGE_OPTIONS` 配置接入(见 [`usage-tiering.md`](usage-tiering.md) §4.1)。
> 本文件保留为问题记录与契约说明。
> 影响版本:`fluss-rust` `datafusion-v0.4.1`(rev `cb11ac6`)出现;`datafusion-v0.5.0`(rev `3cf1603`)修复。
> 相关:[`datafusion-contract.md`](datafusion-contract.md)、[`usage-tiering.md`](usage-tiering.md)。

---

## 1. 一句话

开启 lake 后,gateway 在**生产**里读 lake(Paimon on S3)表会失败,因为 **Fluss 服务端不把 S3 凭据(`s3.access-key`/`s3.secret-key`)下发到表的 lake catalog 属性里**,而 `fluss-datafusion` / `fluss-lake` **没有任何生产途径让调用方(gateway)补这份凭据** —— 唯一的注入点是 `#[cfg(feature = "integration_tests")]` 的测试桩。需要上游提供一个生产级的「lake 存储选项」注入 API。

## 2. 背景 / 触发场景

- gateway 通过 `fluss-datafusion` 读 lake 表,做 lake(Paimon 快照)+ log(Fluss 日志尾)的 union read。
- lake 仓库在 S3 兼容存储(实测用 RustFS;真实环境是 S3 / OSS / MinIO)。
- tiering 作业(Flink)把数据下沉到 Paimon;gateway 只负责**读**。
- Flink tiering 作业是自带凭据的(CLI 传 `--datalake.paimon.s3.access.key` 等),所以它能写 Paimon;**gateway 读 Paimon 时却没有凭据来源**。

## 3. 现象(按配置完善程度演进出两个错误)

**(a)** 当 datalake 配置缺 region 时:
```
ERROR: External error: paimon error: ... IO operation failed on underlying storage:
ConfigInvalid (permanent) at Builder::build => region is missing.
Please find it by S3::detect_region() or set them in env.
  service: s3
```

**(b)** 补齐 `datalake.paimon.s3.region` 后,进一步暴露凭据缺失:
```
ERROR: External error: paimon error: Paimon hitting unexpected error
Failed to check existence of 's3://fluss/paimon/fluss.db/orders_live/schema/schema-0':
Unexpected (temporary) at stat => loading credential to sign http request
  called: reqsign::LoadCredential
  service: s3
  path: paimon/fluss.db/orders_live/schema/schema-0
```
即:OpenDAL 拿到了 endpoint + region,但**没有 access-key/secret-key**,无法对 S3 请求签名。

## 4. 根因(已逐层定位)

### 4.1 服务端剥离了凭据
对一张 lake 表,gateway 经 `$options`(等价于 `TableConfig::get_lake_catalog_properties()`)拿到的属性是:

```
datalake.enabled                      = true
datalake.format                       = paimon
datalake.paimon.metastore             = filesystem
datalake.paimon.warehouse             = s3://fluss/paimon
datalake.paimon.s3.endpoint           = http://rustfs:9000
datalake.paimon.s3.region             = us-east-1
datalake.paimon.s3.path-style-access  = true
# 注意:没有 datalake.paimon.s3.access-key,也没有 datalake.paimon.s3.secret-key
```

集群侧 `FLUSS_PROPERTIES` 明明配了 `datalake.paimon.s3.access-key` / `secret-key`,但服务端**不把这两个带 secret 的 key 下发到表属性**(合理——不应把 secret 通过元数据发给任意客户端)。endpoint / region / path-style / warehouse / metastore 都在,唯独凭据被剥掉。

### 4.2 fluss-datafusion / fluss-lake 没有生产注入口
lake catalog 配置**完全**来自服务端下发的表属性,逐条透传给 Paimon:

- `crates/integrations/datafusion/src/backend/real.rs:76`
  `let lake_catalog_properties = config.get_lake_catalog_properties().ok().flatten();`
  —— 唯一来源,无补充。
- `crates/integrations/datafusion/src/config.rs:21,24`
  `pub struct FlussDatafusionOptions {}` / `pub struct RegisterCatalogOptions {}`
  —— **空结构体,没有任何字段能传 lake 存储选项**。
- `crates/lake/src/config.rs:48` `LakeCatalogConfig::from_catalog_properties(props)`
  —— 把 props 逐条 `options.set(...)` 塞进 `paimon::Options`,只校验 `warehouse` 存在;凭据缺了不会补。
- `crates/lake/src/config.rs:52-57` 唯一的注入点:
  ```rust
  #[cfg(feature = "integration_tests")]
  let props = &{
      let mut props = props.clone();
      crate::test_overrides::apply_s3_endpoint_override(&mut props);
      props
  };
  ```
  —— **测试专用**。生产构建不编译,凭据无从注入。
- `crates/lake/src/test_overrides.rs`(测试桩 `set_test_lake_s3_endpoint_override` + `apply_s3_endpoint_override`)做的正是「补 `s3.endpoint`/`s3.access-key`/`s3.secret-key`/`s3.path-style-access`,并 `or_insert` `s3.region`」——这恰好是生产里缺的能力,但只在 `integration_tests` 下存在。

### 4.3 为什么之前的 e2e 没暴露
gateway 仓库里那条 `cluster_lake_log_union_read_via_mcp`(testcontainers)能过,**正是因为它调了 `set_test_lake_s3_endpoint_override` 注入了凭据**。这个测试桩把生产缺口掩盖了。换到 compose + 生产 gateway 镜像(无 `integration_tests`)后,缺口立刻暴露。

### 4.4 结论
**生产环境下 gateway 无法读取任何 lake 表**,与 datalake 配置是否齐全无关(region 补了也不行)——因为服务端按设计不下发 S3 凭据,而 gateway 没有别的途径提供凭据。

## 5. 复现

环境:`docker/fluss-gateway/docker-compose.tiering.yml`(Fluss + RustFS(S3) + Flink tiering + 生产 gateway 镜像 `localhost/apache/fluss-gateway:latest`,无 `integration_tests`)。

1. `docker compose -f docker-compose.tiering.yml -p fgtier up -d`
2. 经 gateway REST 建 lake KV 表(`table.datalake.enabled=true`,`freshness=30s`)。
3. 提交 Flink tiering 作业(`flink run fluss-flink-tiering-*.jar ...`)。
4. 经 gateway REST 持续写入 `fluss.fluss.orders_live`。
5. psql 到 gateway PG:
   ```sql
   -- ✅ 正常
   SELECT key, value FROM fluss.fluss.orders_live$options;
   -- ❌ loading credential to sign http request
   SELECT id, name FROM fluss.fluss.orders_live;
   SELECT id      FROM fluss.fluss.orders_live$lake;
   ```
完整步骤见 gateway 仓库 `docker/fluss-gateway/`(compose + 操作说明)。

## 6. 影响

- lake/tiering 模式下,gateway 的 SQL 读路径(PostgreSQL + MCP `query`)对 lake 表**完全不可用**(连 log 半边也读不到——union 要先打开 Paimon catalog,打开即失败)。
- 非 lake 表不受影响。
- 阻塞 gateway 的「湖流一体」对外读取能力。

## 7. 需求 / 建议方案

### 7.1 能力需求
`fluss-datafusion` 暴露一个**生产级**入口,让调用方(gateway)提供一组「lake 存储选项」(至少包含 S3 凭据),在打开 Paimon catalog 前 merge 进服务端下发的 lake catalog 属性里。等价于把 `apply_s3_endpoint_override` 的行为从测试桩提升为正式 API。

### 7.2 建议 API(供参考,具体以上游为准)
给 `FlussDatafusionOptions`(优先)或 `RegisterCatalogOptions` 加一个通用的存储选项 map:

```rust
pub struct FlussDatafusionOptions {
    /// 附加到每张 lake 表 catalog 属性上的存储选项(prefix-stripped 形态,
    /// 如 `s3.access-key` / `s3.secret-key`)。用于补服务端按安全策略不下发的
    /// 凭据。调用方从自身安全配置提供,不经服务端。
    pub lake_storage_options: std::collections::HashMap<String, String>,
}
```

merge 点二选一:
- **(推荐)** 在 `backend/real.rs:76` 拿到 `lake_catalog_properties` 后,叠加 `options.lake_storage_options`,使下游 union/pk/append 全部继承;`fluss-lake` 的 `from_catalog_properties` 保持「逐条透传」不变。
- 或在 `fluss-lake::config::from_catalog_properties` 增加一个 `extra: &HashMap` 参数,在那个唯一 chokepoint merge(与 `apply_s3_endpoint_override` 同位置)。

### 7.3 语义细节
- **Key 命名**:服务端下发的是 prefix-stripped 后的 `s3.*`(如 `s3.endpoint`、`s3.region`)。调用方补的凭据应同样用 `s3.access-key` / `s3.secret-key`(测试桩 `apply_s3_endpoint_override` 用的就是这两个,且被验证可用)。
- **优先级**:建议「调用方提供的选项覆盖服务端同名 key」(caller wins),既能补凭据,也能在需要时覆盖 endpoint/region(例如容器内 endpoint → host endpoint,正是测试桩做的)。
- **通用性**:做成通用 `HashMap` 而非 S3 专用,这样 `oss.*` 等其它对象存储的同类缺口一并覆盖。
- **安全**:凭据来自 gateway 自身安全配置(env / 配置文件),不打日志、不回传服务端;与 Flink tiering 作业自带凭据是同一思路。

### 7.4 gateway 侧配套(上游 API 落地后,gateway 自己做)
- 从 env/config 读 lake S3 凭据(如 `GATEWAY_LAKE_S3_ACCESS_KEY` / `GATEWAY_LAKE_S3_SECRET_KEY`,可选 endpoint/region 覆盖),组装成 `FlussDatafusionOptions.lake_storage_options` 传入 `FlussDatafusion::new`。
- 文档化:gateway 持有 lake 对象存储凭据是部署前提。

## 8. 验收标准

- 在**不开 `integration_tests`** 的前提下,调用方通过新 API 传入 `s3.access-key`/`s3.secret-key` 后:
  - `SELECT ... FROM <lake_table>`(union read)成功返回 lake + log 合并结果;
  - `SELECT ... FROM <lake_table>$lake` 成功返回 Paimon 快照;
  - 不再出现 `region is missing` / `loading credential` 错误。
- 复现用的 compose + psql 流程(§5)端到端通过。
- 不传该选项时行为不变(向后兼容)。

## 9. 关联文件(fluss-rust @ datafusion-v0.4.1, rev cb11ac6)

- `crates/integrations/datafusion/src/config.rs:21,24` — `FlussDatafusionOptions{}` / `RegisterCatalogOptions{}`(空,需加字段)
- `crates/integrations/datafusion/src/backend/real.rs:76` — lake catalog props 唯一来源(建议 merge 点)
- `crates/integrations/datafusion/src/backend/mod.rs:106` — `lake_catalog_properties` 字段
- `crates/lake/src/config.rs:48,52-57,60-64` — `from_catalog_properties`(透传)+ 测试专用注入块
- `crates/lake/src/test_overrides.rs:98` — `set_test_lake_s3_endpoint_override` + `apply_s3_endpoint_override`(要提升为生产能力的参考实现)
- `crates/lake/src/catalog.rs:41-43` — `open_catalog` → `CatalogFactory::create(options)`
