# 使用文档：Gateway 配合 Fluss 集群（带 / 不带 tiering）

> 面向运维与使用者的操作文档。覆盖两种部署形态：
>
> 1. **不带 tiering** —— gateway 直连一个普通 Fluss 集群，只读实时 log / KV 表。
> 2. **带 tiering** —— 表开启 lake（Paimon），由外部 tiering 作业下沉历史数据到 Paimon；
>    gateway 做 **lake + log 的 union read**（湖快照 + Fluss log 尾合并）。
>
> 模块定位与协议能力见 [`../README.md`](../README.md)；内部设计见 [`../DESIGN.md`](../DESIGN.md)。

---

## 1. 总览

```
                 +-------------------------------------+
   PG client --->| PostgreSQL :5432   (read-only SQL)  |
   REST client ->| REST       :8080   (write + meta)   |
   AI agent ---->| MCP        :8000   (read-only tools)|
                 +------------------+------------------+
                                    |
                                    v
                          +-------------------+
                          |  GatewayInstance  |  (protocol-agnostic)
                          +---------+---------+
                                    |
                  SQL path          |          direct path
            (fluss-datafusion)      |        (BackendFacade)
                                    v
                          +-------------------+
                          |  Fluss cluster    |
                          +---------+---------+
                                    |  (only when lake enabled)
                                    v
                          +-------------------+
                          | Paimon warehouse  |  (S3 / OSS / fs)
                          +-------------------+
```

要点：
- **读**走 SQL 路径（PG / MCP `query`），由 `fluss-datafusion` 规划执行。
- **写**只走 REST direct path（at-least-once）；PG 只读。
- **lake union read 是 SQL 路径的能力**：表开了 lake 就自动合并「Paimon 快照 + log 尾」，
  gateway 不需要额外开关（lake 已编译进发布版,见 §4.1）。
- tiering 作业（把 Fluss 数据下沉到 Paimon）是 **Fluss 侧的外部作业**，不在 gateway 内；
  gateway 只负责**读**这份 union。

---

## 2. 通用配置（两种形态都适用）

gateway 二进制全部走环境变量配置：

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `FLUSS_BOOTSTRAP_SERVERS` | `127.0.0.1:9123` | Fluss 集群 bootstrap 地址 |
| `FLUSS_CLUSTER` | `default` | 逻辑 cluster id（本期仅 `default`） |
| `GATEWAY_PG_LISTEN` | `0.0.0.0:5432` | PostgreSQL 监听地址 |
| `GATEWAY_REST_LISTEN` | `0.0.0.0:8080` | REST 监听地址 |
| `GATEWAY_MCP_ENABLED` | `false` | 是否启用 MCP 前端 |
| `GATEWAY_MCP_LISTEN` | `0.0.0.0:8000` | MCP 监听地址（启用时） |
| `GATEWAY_CONFIG` | 无 | 鉴权用户 YAML 文件路径（可选） |
| `GATEWAY_USERS` | 无 | `user:secret,...` 形式的鉴权覆盖（可选） |
| `RUST_LOG` | `info` | tracing 日志级别 |

认证：
- 配了用户（YAML 或 `GATEWAY_USERS`）→ PG / REST / MCP 都校验用户名密码。
- 没配用户 → **trust 模式**：用户名即 principal，不校验密码（仅适合可信内网 / 本地）。
- REST、MCP 用 `Authorization: Basic`；PG 用 username/password 握手。

启动（容器或本地二进制）：

```bash
FLUSS_BOOTSTRAP_SERVERS=fluss-coordinator:9123 \
GATEWAY_MCP_ENABLED=true \
GATEWAY_USERS='alice:s3cret' \
RUST_LOG=info \
  fluss-gateway
```

---

## 3. 不带 tiering：直连普通 Fluss 集群

适用：集群没有配置 datalake，或表没有开 lake。gateway 只读 Fluss 的实时 log / KV 表。

### 3.1 前提
- Fluss 集群可达（`FLUSS_BOOTSTRAP_SERVERS`）。
- 表已经存在（gateway 不做 schema-on-write）。

### 3.2 启动
按 §2 启动即可，无需任何 datalake / S3 配置。

### 3.3 读（PG）
PG 入口只读；表名形态为 `fluss.<database>.<table>`（catalog.database.table）。

```bash
PGPASSWORD=s3cret psql "host=127.0.0.1 port=5432 user=alice dbname=fluss sslmode=disable"
```

```sql
-- KV 表：主键点查 / 前缀（bucket key）/ 有界 LIMIT 扫描
SELECT id, name FROM fluss.mydb.orders WHERE id = 42;
SELECT id, name FROM fluss.mydb.orders LIMIT 100;

-- Log 表：有界 LIMIT 扫描
SELECT * FROM fluss.mydb.events LIMIT 100;
```

> 注意（非 lake 表的约束）：KV 表**没有主键/bucket-key 谓词且没有 LIMIT 的全表扫描会被拒绝**
> （避免无界扫描）。要么带谓词、要么带 `LIMIT`。lake 表不受此限（见 §4）。

### 3.4 写（REST）
```bash
curl -u alice:s3cret -H 'Content-Type: application/json' -X POST \
  http://127.0.0.1:8080/v1/clusters/default/databases/mydb/tables/orders/records \
  -d '[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]'
```

### 3.5 读（MCP，给 agent）
启用 `GATEWAY_MCP_ENABLED=true` 后，MCP 端点在 `http://<host>:8000/mcp`，提供 4 个只读工具：
`list_databases` / `list_tables` / `describe_table` / `query`。`query` 跑单条只读 SQL，
与上面 PG 的 SELECT 等价（同一条 SQL 路径）。

---

## 4. 带 tiering：lake（Paimon）+ log 的 union read

适用：表开启 `table.datalake.enabled=true`，外部 **tiering 作业**把历史数据下沉到 Paimon。
读取时 gateway 自动合并：
- **湖侧**：已 tiered 到 Paimon 的历史快照；
- **log 尾**：还没被 tiering 下沉、仍在 Fluss log 里的最新增量。

对 KV（主键）表，union 是 **PK 合并**（log 尾的 update/delete/insert 覆盖湖快照）；
对 append 表，是**拼接**。

### 4.1 gateway 侧：需要提供 lake 的 S3 凭据
发布版已经编译了 lake 能力（`fluss-datafusion` 的 `lake` feature 默认开）。但 **Fluss 服务端按安全
策略不会把 S3 凭据（`s3.access-key` / `s3.secret-key`）下发到表属性**里——endpoint/region/path-style
会下发,凭据不会。所以 gateway 必须从自身配置提供这份凭据,否则读 lake 表会报
`loading credential to sign http request`(见 §6)。

通过环境变量提供(datafusion-v0.5.0+,gateway 转发给 `FlussDatafusionOptions.lake_storage_options`):

| 环境变量 | 映射到的 lake 选项 | 说明 |
|---|---|---|
| `GATEWAY_LAKE_S3_ACCESS_KEY` | `s3.access-key` | **必填**(服务端不下发) |
| `GATEWAY_LAKE_S3_SECRET_KEY` | `s3.secret-key` | **必填**(服务端不下发) |
| `GATEWAY_LAKE_S3_ENDPOINT` | `s3.endpoint` | 可选,覆盖服务端下发值 |
| `GATEWAY_LAKE_S3_REGION` | `s3.region` | 可选,覆盖服务端下发值 |
| `GATEWAY_LAKE_S3_PATH_STYLE_ACCESS` | `s3.path-style-access` | MinIO/RustFS 等 path-style 存储设 `true` |
| `GATEWAY_LAKE_STORAGE_OPTIONS` | 任意 `key=value,...` | 通用透传(如 `oss.*`);离散 `GATEWAY_LAKE_S3_*` 优先 |

这些选项以 **caller-wins** 合并进每张 lake 表的服务端属性,在打开 Paimon catalog 前生效;不会出现在
表的 `$options` 视图里。凭据由 gateway 自身安全配置持有,不经服务端——和 Flink tiering 作业自带
`--datalake.paimon.s3.access.key` 是同一思路。

> `integration_tests` feature 只用于测试期的 S3 endpoint 覆盖桩,**不要**在生产构建里开。

### 4.2 Fluss 侧：datalake 配置（关键）
lake 表的非密 S3 配置（endpoint/region/warehouse 等）来自服务端表属性,gateway 原样透传给
Paimon / OpenDAL;**凭据(access-key/secret-key)服务端不下发,由 gateway 按 §4.1 提供**。
集群级 datalake 配置（coordinator + 每个 tablet server 都要有）至少需要：

```properties
datalake.enabled                      = true
datalake.format                       = paimon
datalake.paimon.metastore             = filesystem
datalake.paimon.warehouse             = s3://<bucket>/paimon
datalake.paimon.s3.endpoint           = <s3-endpoint>
datalake.paimon.s3.region             = <region>        # 必填，见 §6 排错
datalake.paimon.s3.access-key         = <ak>
datalake.paimon.s3.secret-key         = <sk>
datalake.paimon.s3.path.style.access  = true            # MinIO / RustFS / path-style 存储需要
```

建表时开启 lake（示例为 Flink/Fluss DDL 语义，按你的建表入口调整）：

```sql
-- 表属性
'table.datalake.enabled'   = 'true'
'table.datalake.freshness' = '30s'   -- 期望的 lake 新鲜度
```

> ⚠️ **`datalake.paimon.s3.region` 经常被漏配**。OpenDAL 的 S3 客户端做请求签名时强制要 region，
> 缺了会在第一次读 lake 表时报 `region is missing`（见 §6）。

### 4.3 tiering 作业
tiering 是 **Fluss 侧的外部作业**（Flink tiering job，按 Fluss lakehouse 文档部署），
负责把 Fluss 数据持续写进 Paimon warehouse。gateway **不**启动也**不**管理它。

- tiering **在跑**：湖快照随 freshness 推进，历史进 Paimon、最新留 log，union read 返回全量。
- tiering **没跑 / 刚建表**：湖快照可能为空或落后，union read 退化为「只读到 log 尾里现有的数据」
  —— 不会报错，但读不到尚未下沉的历史（取决于 log 保留）。

### 4.4 读 union（PG / MCP）
普通 `SELECT` 即得到 union：

```sql
-- 完整 union：湖快照 + log 尾（KV 表做 PK 合并）
SELECT id, name FROM fluss.mydb.orders_history;
```

lake 表支持**无谓词全表扫描**（湖快照提供主体数据，不受 §3.3 的非 lake KV 全扫限制）。

诊断用的两个虚拟后缀（DataFusion 方言里 `$` 是合法标识符字符，无需转义）：

```sql
-- 只读已 tiered 的 Paimon 快照（不含 log 尾），用于确认湖侧内容
SELECT id FROM fluss.mydb.orders_history$lake;

-- 查看该表的 datalake 选项（确认确实开了 lake、格式是 paimon）
SELECT key, value FROM fluss.mydb.orders_history$options
 WHERE key IN ('datalake.enabled', 'datalake.format');
```

MCP 同理 —— `query` 工具传同样的 SQL 即可，agent 拿到的就是 union 结果。

### 4.5 一眼判断 union 是否生效
- `SELECT ... FROM t` 的行数 ≥ `SELECT ... FROM t$lake` 的行数（union ⊇ 湖快照）。
- `t$lake` 只含已 tiered 的历史；新写但未 tiered 的行只出现在 `SELECT ... FROM t`，不出现在 `t$lake`。
- `t$options` 里 `datalake.enabled = true`、`datalake.format = paimon`。

---

## 5. 验证 checklist

不带 tiering：
1. `GET /v1/clusters/default/databases` 能列出库 → 集群连通。
2. PG `SELECT ... WHERE pk=...` / `... LIMIT n` 能返回行。

带 tiering：
1. `SELECT key,value FROM <t>$options` 显示 `datalake.enabled=true`。
2. `SELECT ... FROM <t>$lake` 能读到湖快照（tiering 跑过之后非空）。
3. 写入新行（REST）后，`SELECT ... FROM <t>` 立刻能读到（log 尾），`<t>$lake` 暂时读不到（未 tiered）。
4. 等 tiering 推进后，新行也出现在 `<t>$lake`。

---

## 6. 排错

### `region is missing`（读 lake 表时）
```
ERROR: External error: paimon error: ... IO operation failed on underlying storage:
ConfigInvalid (permanent) at Builder::build => region is missing.
Please find it by S3::detect_region() or set them in env.   service: s3
```
**原因**：该 lake 表的 `table.datalake.paimon.s3.*` 属性里缺 `s3.region`。配置完全来自
Fluss 服务端,gateway 原样透传;OpenDAL 的 S3 签名强制要 region。

**修复**：要么在 Fluss 集群 datalake 配置补上 `datalake.paimon.s3.region`（见 §4.2）让它传播到表属性,
要么直接在 gateway 设 `GATEWAY_LAKE_S3_REGION`（见 §4.1，gateway 提供的优先）。
- 已存在的表:若 region 走服务端,看你的 Fluss 实现是「读时合并集群配置」还是「建表时固化」——若固化,
  可能要重建表 / alter 表属性;走 gateway `GATEWAY_LAKE_S3_REGION` 则对所有表立即生效,最省事。
- **在 gateway 上设 `AWS_REGION` 环境变量无效**:用 `GATEWAY_LAKE_S3_REGION`(它会进
  `lake_storage_options`),`AWS_REGION` 不会被 Paimon/OpenDAL 读取。

### `loading credential to sign http request`（读 lake 表时)
**原因**:Fluss 服务端按安全策略**不把 `s3.access-key` / `s3.secret-key` 下发到表属性**,所以 Paimon
的 OpenDAL 有 endpoint+region 却没有凭据签名。
**修复**:在 gateway 配 `GATEWAY_LAKE_S3_ACCESS_KEY` / `GATEWAY_LAKE_S3_SECRET_KEY`(见 §4.1)。
datafusion-v0.5.0+ 的 `FlussDatafusionOptions.lake_storage_options` 把它们合并进 lake catalog 属性后即可读。
（该缺口的上游需求记录见 [`lake-s3-credentials-requirement.md`](lake-s3-credentials-requirement.md),已在 v0.5.0 落地。）

### lake 表全表扫描很慢 / 想只看湖侧
用 `<t>$lake` 单独读 Paimon 快照,排除 log 尾,便于定位是湖侧数据问题还是 log 侧。

### 非 lake KV 表报「全表扫描被拒绝」
非 lake 的 KV 表全扫无界,被设计性拒绝。加主键/bucket-key 谓词或 `LIMIT`;或给表开 lake。

### `501 Not Implemented`（REST lookup / scan）
REST direct-read（`lookup` / `prefix-scan` / `log-scan`）未实现,读走 PG / MCP SQL 路径。

---

## 7. 与文档的关系

- 协议能力 / REST 端点 / 写语义：[`../README.md`](../README.md)
- SQL 环境与 PG 路径：[`sql-path.md`](sql-path.md)
- direct path / REST 设计：[`direct-path.md`](direct-path.md)
- gateway ↔ fluss-datafusion 契约（含 lake union read 的边界）：[`datafusion-contract.md`](datafusion-contract.md)
