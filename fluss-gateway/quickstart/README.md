# fluss-gateway Quickstart: MCP Refund Investigation

This quickstart shows the shortest path to understand how to use `fluss-gateway` through **MCP**.

You will bring up a local Fluss cluster with **lake tiering enabled**, connect an MCP client to `fluss-gateway`, and walk through a customer-support investigation that feels like a realtime-context story:

- orders and support cases keep a readable **current state**,
- refund events keep arriving as a **live flow**,
- history is tiered into lake storage for `$lake` inspection,
- support uses an agent to answer why one refund still has not arrived.

The main order this story eventually lands on is still:

- `ORD-20260625-1001`

But the quickstart no longer starts from a fixed point lookup. It starts from what support already knows about the user, for example:

- customer id: `CUS-1001`
- the user says they bought **PeakStore Air Fryer Pro**
- the order was cancelled successfully yesterday
- the refund still has not arrived

For protocol details and deeper operational notes, see:

- [`../design/mcp-access.md`](../design/mcp-access.md)
- [`../design/usage-tiering.md`](../design/usage-tiering.md)

---

## 1. What you will learn

By the end of this quickstart, you will know how to:

1. start a tiering-enabled local Fluss + Gateway environment,
2. connect an MCP client to `fluss-gateway`,
3. inspect a **live refund event stream**,
4. confirm the **current state** of an order and support case,
5. compare the normal history view with the `$lake` history view.

Expected conclusion for `ORD-20260625-1001`:

- the order is already `CANCELLED`,
- the refund is still `PROCESSING`,
- the live event feed keeps surfacing provider-pending / retry / escalation signals,
- support should explain that the refund is still being processed and has not completed yet.

---

## 2. Prerequisites

You need:

- Docker / Docker Compose (or a compatible Podman compose setup)
- Claude Code or another MCP client that supports **Streamable HTTP**
- the local Paimon S3 plugin jar referenced by [`./.env`](./.env)

The current quickstart environment uses:

- Fluss server image: `apache/fluss:0.9.1-incubating`
- Flink image: `apache/fluss-quickstart-flink:1.20-0.9.1-incubating-rc1`
- Gateway image: `apache/fluss-gateway:latest`
- RustFS as the local S3-compatible object store

---

## 3. Start the quickstart cluster

From the `fluss-gateway` module root:

```bash
bash quickstart/scripts/bootstrap.sh
```

Check container state:

```bash
cd quickstart && docker compose ps
```

Expected services:

- `zookeeper`
- `rustfs`
- `rustfs-init`
- `coordinator-server`
- `tablet-server`
- `gateway`
- `jobmanager`
- `taskmanager`
- `sql-client`

Useful endpoints:

- Flink UI: http://localhost:8083/
- Gateway MCP: `http://127.0.0.1:8000/mcp`
- Gateway REST: `127.0.0.1:8080`
- Gateway PostgreSQL: `127.0.0.1:5432`
- RustFS S3 endpoint: `127.0.0.1:9000`
- RustFS console: http://localhost:9001/

---

## 4. Load the refund quickstart data and start the live stream

Create the tables, seed the stable context rows, and start the realtime refund signal job:

```bash
bash quickstart/scripts/run-demo-flow.sh
```

This prepares database `refund_demo` with the same five tables as before:

- `customer_profiles`
- `refund_orders`
- `support_cases`
- `refund_events`
- `refund_audit_history`

But their roles are now clearer:

- `refund_events` is the **live entry point**: new refund-related signals keep arriving here.
- `refund_orders` is the **current state** for each order.
- `support_cases` is the **current support context**.
- `refund_audit_history` is the **historical trail** and lake-enabled view.

After the script finishes, wait about **30-60 seconds** so the live refund stream has time to produce fresh rows.

---

## 5. Start lake tiering

After the seed data and live stream are running, start the Fluss tiering job:

```bash
bash quickstart/scripts/run-tiering-job.sh
```

Expected result:

- a new Flink job appears in the Flink UI,
- `refund_audit_history$lake` becomes queryable after tiering catches up.

Wait about **30 seconds** before querying `refund_audit_history$lake`.

---

## 6. Connect MCP to fluss-gateway

### Option A: Claude Code

`fluss-gateway` MCP is exposed at:

- `http://127.0.0.1:8000/mcp`

The quickstart cluster runs in trust mode, so the username becomes the principal and the password is ignored. A simple local setup command is:

```bash
claude mcp add --transport http fluss http://127.0.0.1:8000/mcp \
  --header "Authorization: Basic $(printf 'alice:ignored' | base64)"
```

### Option B: Generic MCP client

```json
{
  "mcpServers": {
    "fluss": {
      "type": "http",
      "url": "http://127.0.0.1:8000/mcp",
      "headers": {
        "Authorization": "Basic YWxpY2U6aWdub3JlZA=="
      }
    }
  }
}
```

More details:

- [`../design/mcp-access.md`](../design/mcp-access.md)

---

## 7. Investigate the refund case

Once MCP is connected, the agent will have four tools:

- `list_databases`
- `list_tables`
- `describe_table`
- `query`

This quickstart is most useful if you let the agent start from **the clues the user already gave to support**, use those clues to find the right order, and then drill into **what is happening now** and **why the refund is still delayed**.

### Step 1: discover the database

Ask the agent:

> List the databases exposed by fluss-gateway and identify the refund investigation database.

Expected answer:

- the database `refund_demo` is available.

### Step 2: inspect available tables

Ask the agent:

> List the tables in `refund_demo` and tell me which ones show live signals, current state, and history.

Expected answer should identify at least:

- `refund_events` as the live event stream
- `refund_orders` as the current order state
- `support_cases` as the support context
- `refund_audit_history` as the historical / lake-backed trail
- `customer_profiles` as customer context

### Step 3: first use the customer and item clues to find the order

Ask the agent:

> 用户给客服的线索是：customer id 是 `CUS-1001`，买的是 `PeakStore Air Fryer Pro`，昨天已经取消订单，但退款还没到账。先根据这些线索帮我把最可能对应的订单找出来。

Equivalent SQL shape:

```sql
SELECT order_id, customer_id, item_name, item_category, order_status, refund_status, cancelled_at, updated_at
FROM fluss.refund_demo.refund_orders
WHERE customer_id = 'CUS-1001'
LIMIT 20;
```

Expected observation:

- the agent should narrow the candidate set to `ORD-20260625-1001`
- `item_name` should match `PeakStore Air Fryer Pro`
- the order should already be cancelled while the refund is still processing

### Step 4: confirm the current order state

Ask the agent:

> 确认一下是不是这笔单：它现在到底是不是已经取消了？退款状态是不是还在 processing？

Equivalent SQL shape:

```sql
SELECT order_id, customer_id, item_name, order_status, refund_status, refund_amount, cancelled_at, updated_at
FROM fluss.refund_demo.refund_orders
WHERE order_id = 'ORD-20260625-1001';
```

Expected conclusion:

- `order_status = CANCELLED`
- `refund_status = PROCESSING`

### Step 5: inspect the latest refund timeline for that order

Ask the agent:

> Show the most recent refund events for `ORD-20260625-1001` and explain whether the refund is still stuck before completion.

Equivalent SQL shape:

```sql
SELECT event_at, event_type, event_summary, operator_note
FROM fluss.refund_demo.refund_events
WHERE order_id = 'ORD-20260625-1001'
ORDER BY event_at DESC
LIMIT 20;
```

Expected conclusion:

- `REFUND_PENDING_PROVIDER` should appear repeatedly
- `REFUND_PROVIDER_RETRY` or escalation-style events should appear
- there should still be **no stable live signal** that replaces the order's current `PROCESSING` state with `COMPLETED`

### Step 6: inspect the support case

Ask the agent:

> Show the current support case for `ORD-20260625-1001` and summarize what support currently knows.

Equivalent SQL shape:

```sql
SELECT order_id, case_id, issue_type, case_status, opened_at, last_customer_message
FROM fluss.refund_demo.support_cases
WHERE order_id = 'ORD-20260625-1001';
```

Expected conclusion:

- the case should remain `OPEN` or become `ESCALATED`
- the latest message should still reflect refund delay or escalation context

### Step 7: inspect customer context if needed

Ask the agent:

> Show the customer profile for the customer who owns `ORD-20260625-1001`.

Equivalent SQL shape:

```sql
SELECT customer_id, customer_name, customer_tier, contact_email, region_name
FROM fluss.refund_demo.customer_profiles
WHERE customer_id = 'CUS-1001';
```

### Step 8: inspect the historical lake view

Ask the agent:

> Compare the current history table with the lake-only history view for `ORD-20260625-1001`.

Equivalent SQL shapes:

```sql
SELECT order_id, audit_at, audit_step, status_summary, actor
FROM fluss.refund_demo.refund_audit_history
WHERE order_id = 'ORD-20260625-1001'
LIMIT 20;
```

```sql
SELECT order_id, audit_at, audit_step, status_summary, actor
FROM fluss.refund_demo.refund_audit_history$lake
WHERE order_id = 'ORD-20260625-1001'
LIMIT 20;
```

Expected observation:

- `refund_audit_history` keeps the full gateway-visible history trail
- `refund_audit_history$lake` shows the tiered lake-side history after the tiering job has had time to run

### Step 9: ask for the customer-support conclusion

Ask the agent:

> Based on the live refund events, current order state, support case, and historical trail, write a short explanation that customer support can send to the user.

Expected answer should be close to:

> Your order has already been cancelled successfully. The refund request has been created, but the payment provider is still reporting the refund as pending, and our system is still retrying / escalating that settlement path. That is why the refund has not completed yet.

### Step 10: a simple customer-support flow

In this quickstart, the role is simple: **you are a customer-support agent, and you use an MCP-connected AI agent to investigate the refund**.

下面这 5 句就够做一次完整演示：

#### 1. 先认表

> 现在 fluss 集群下有哪些表，这些表之间的关系是什么？

#### 2. 根据用户线索定位订单

> 用户说他买的是 `PeakStore Air Fryer Pro`，后来取消了订单，但退款一直没到账。客服这边知道 customer id 是 `CUS-1001`。先帮我找到对应的订单。

#### 3. 确认最新状态

> 帮我确认一下这笔订单最新的状态，退款是不是还在处理中。

#### 4. 看最近 10 条退款事件

> 看一下这笔订单最近 10 条退款事件，告诉我它现在的最新状态。

#### 5. 看客服 case，并生成回复

> 这笔订单对应的客服 case 现在是什么状态？帮我整理一段回复用户的话。

如果你想再补一层历史确认，可以继续追问：

> 再结合 `refund_audit_history` 和 `refund_audit_history$lake` 看一下，这笔退款历史上到底卡在哪一步。

---

## 8. What this quickstart demonstrates

This flow shows the Phase 1 role of `fluss-gateway` more clearly:

- **MCP** gives an agent a small, safe, read-only surface.
- The agent can start from **live events** instead of only fixed point lookups.
- The same small table set can expose **current state + live flow + historical lake view**.
- The gateway stays read-only on this path; it does not turn MCP into a write surface.

---

## 9. Verify and troubleshoot

### Basic verification

You should be able to confirm all of the following:

1. the MCP server is reachable at `http://127.0.0.1:8000/mcp`
2. the MCP tool list contains exactly:
   - `list_databases`
   - `list_tables`
   - `describe_table`
   - `query`
3. `refund_demo` exists
4. `refund_events` keeps receiving fresh rows after the quickstart stream starts
5. `refund_orders` returns `ORD-20260625-1001` with `CANCELLED` + `PROCESSING`
6. `support_cases` for `ORD-20260625-1001` stays `OPEN` or becomes `ESCALATED`
7. `refund_audit_history$lake` becomes readable after tiering catches up

### If MCP connection fails

See:

- [`../design/mcp-access.md`](../design/mcp-access.md)

### If lake reads fail

Typical issues include:

- missing `region`
- missing lake credentials
- querying `$lake` before tiering has had time to run

See:

- [`../design/usage-tiering.md`](../design/usage-tiering.md)

---

## 10. Clean up

```bash
bash quickstart/scripts/bootstrap.sh clean
```

This removes the quickstart containers and volumes.
