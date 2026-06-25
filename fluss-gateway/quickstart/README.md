# fluss-gateway Quickstart: MCP Refund Investigation

This quickstart shows the shortest path to understand how to use `fluss-gateway` through **MCP**.

You will bring up a local Fluss cluster with **lake tiering enabled**, connect an MCP client to `fluss-gateway`, and walk through one customer-support investigation:

- the order has already been cancelled,
- the customer still has not received the refund,
- support uses an agent to inspect Fluss through `fluss-gateway` and explain why.

This quickstart is intentionally narrow:

- it focuses on the **MCP read path** only,
- it uses one fixed refund story instead of a broad benchmark-style dataset,
- it shows how tiering helps you inspect **current view + historical lake view**.

For protocol details and deeper operational notes, see:

- [`../design/mcp-access.md`](../design/mcp-access.md)
- [`../design/usage-tiering.md`](../design/usage-tiering.md)

---

## 1. What you will learn

By the end of this quickstart, you will know how to:

1. start a tiering-enabled local Fluss + Gateway environment,
2. connect an MCP client to `fluss-gateway`,
3. discover available databases and tables,
4. inspect a refund case step by step,
5. compare the normal table view with the `$lake` history view.

The target investigation order is:

- `ORD-20260625-1001`

Expected conclusion:

- the order is already `CANCELLED`,
- a `REFUND_REQUESTED` event exists,
- there is still no `REFUND_COMPLETED` event,
- support should explain that the refund is still being processed.

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

## 4. Load the refund quickstart data

Create the tables and seed the fixed refund scenario:

```bash
bash quickstart/scripts/run-demo-flow.sh
```

This loads one small, deterministic dataset into database `refund_demo`, including:

- `customer_profiles`
- `refund_orders`
- `support_cases`
- `refund_events`
- `refund_audit_history`

The quickstart focuses on one order:

- `ORD-20260625-1001`

---

## 5. Start lake tiering

After the seed data is ready, start the Fluss tiering job:

```bash
bash quickstart/scripts/run-tiering-job.sh
```

Expected result:

- a new Flink job appears in the Flink UI,
- the lake-enabled history table can later be inspected through `$lake`.

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

The rest of the quickstart is just using those four tools well.

### Step 1: discover the database

Ask the agent:

> List the databases exposed by fluss-gateway and identify the refund investigation database.

Expected answer:

- the database `refund_demo` is available.

### Step 2: inspect available tables

Ask the agent:

> List the tables in `refund_demo` and tell me which ones are relevant for refund investigation.

Expected answer should identify at least:

- `refund_orders`
- `refund_events`
- `support_cases`
- `customer_profiles`
- `refund_audit_history`

### Step 3: inspect the order state

Ask the agent:

> Check order `ORD-20260625-1001`. Has it already been cancelled, and what is the current refund status?

Equivalent SQL shape:

```sql
SELECT order_id, customer_id, order_status, refund_status, refund_amount, cancelled_at
FROM fluss.refund_demo.refund_orders
WHERE order_id = 'ORD-20260625-1001';
```

Expected conclusion:

- `order_status = CANCELLED`
- `refund_status = PROCESSING`

### Step 4: inspect the refund timeline

Ask the agent:

> Show the refund timeline for `ORD-20260625-1001` and tell me whether a refund completion event exists.

Equivalent SQL shape:

```sql
SELECT event_at, event_type, event_summary, operator_note
FROM fluss.refund_demo.refund_events
WHERE order_id = 'ORD-20260625-1001'
ORDER BY event_at DESC
LIMIT 20;
```

Expected conclusion:

- `ORDER_CANCELLED` exists
- `REFUND_REQUESTED` exists
- `REFUND_PENDING_PROVIDER` exists
- **no** `REFUND_COMPLETED` event exists

### Step 5: inspect the support case

Ask the agent:

> Show the support case for `ORD-20260625-1001` and summarize what the customer is asking.

Equivalent SQL shape:

```sql
SELECT order_id, case_id, issue_type, case_status, opened_at, last_customer_message
FROM fluss.refund_demo.support_cases
WHERE order_id = 'ORD-20260625-1001';
```

Expected conclusion:

- the case is open,
- the customer is asking why the refund has not arrived yet.

### Step 6: inspect customer context if needed

Ask the agent:

> Show the customer profile for the customer who owns `ORD-20260625-1001`.

Equivalent SQL shape:

```sql
SELECT customer_id, customer_name, customer_tier, contact_email, region_name
FROM fluss.refund_demo.customer_profiles
WHERE customer_id = 'CUS-1001';
```

### Step 7: inspect the historical lake view

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

- `refund_audit_history` is the normal table view exposed by the gateway,
- `refund_audit_history$lake` shows the tiered lake-side history after the tiering job has had time to run.

### Step 8: ask for the customer-support conclusion

Ask the agent:

> Based on the order state, refund events, and support case, write a short explanation that customer support can send to the user.

Expected answer should be close to:

> Your order has already been cancelled successfully. The refund request has also been created, but we still have not received a refund-completed confirmation from the payment provider, so the refund is currently still processing.

---

## 8. What this quickstart demonstrates

This flow shows the Phase 1 role of `fluss-gateway` clearly:

- **MCP** gives an agent a small, safe, read-only surface.
- The agent can inspect Fluss data without direct cluster-specific tooling.
- A lake-enabled table can be read through the normal view and the `$lake` view.
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
4. `refund_orders` returns `ORD-20260625-1001` with `CANCELLED` + `PROCESSING`
5. `refund_events` contains `REFUND_REQUESTED` but not `REFUND_COMPLETED` for that order
6. `refund_audit_history$lake` becomes readable after tiering catches up

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
