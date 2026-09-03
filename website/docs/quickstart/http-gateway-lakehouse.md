---
title: Ingesting HTTP Events into a Real-Time Lakehouse
sidebar_position: 3
---

# Ingest HTTP Events into a Real-Time Lakehouse

This guide will help you ingest JSON events through Fluss Gateway, query them
immediately from Fluss, and continuously tier the same table into Apache Paimon
for historical analysis.

The example uses telemetry events from a fleet application. The application
performs one HTTP write, while Fluss provides immediate query visibility in the
hot tier and continuously writes the data to the lake through the reusable
Lakehouse Tiering Service.

In this quickstart, you will:

- start a local Fluss cluster, Fluss Gateway, Flink, Paimon, and RustFS;
- create a lake-enabled Fluss table through the Gateway REST API;
- send schema-aware batches of JSON events through HTTP;
- query newly written rows immediately through Flink SQL and Union Read;
- verify that the events are continuously tiered to Paimon.

:::caution Preview

Fluss Gateway is a preview feature in Fluss 1.0. This quickstart uses its
unauthenticated `trust` mode and is intended only for local development. See
[Fluss Gateway security](../gateway/index.md#security) before planning a
production deployment.

:::

## Environment Setup

### Prerequisites

Before proceeding with this guide, ensure that the following tools are
installed on your machine:

- [Docker](https://docs.docker.com/engine/install/)
- [Docker Compose](https://docs.docker.com/compose/install/linux/)
- `curl`

:::note
We encourage you to use a recent version of Docker and
[Compose v2](https://docs.docker.com/compose/releases/migrate/).
:::

### Starting required components

We will use `docker compose` to spin up the required components for this
tutorial.

1. Create a working directory for this guide.

```shell
mkdir fluss-http-lakehouse
cd fluss-http-lakehouse
```

2. Create a `lib` directory and download the Paimon S3 plugin required by the
   Fluss servers.

```shell
mkdir lib
curl -fL -o "lib/paimon-s3-$PAIMON_VERSION$.jar" \
  "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/$PAIMON_VERSION$/paimon-s3-$PAIMON_VERSION$.jar"
```

3. Create a `docker-compose.yml` file with the following content:

```yaml
services:
  rustfs:
    image: rustfs/rustfs:1.0.0-alpha.83
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      - RUSTFS_ACCESS_KEY=rustfsadmin
      - RUSTFS_SECRET_KEY=rustfsadmin
      - RUSTFS_CONSOLE_ENABLE=true
    volumes:
      - rustfs-data:/data
    command: /data

  rustfs-init:
    image: minio/mc
    depends_on:
      - rustfs
    entrypoint: >
      /bin/sh -c "
      until mc alias set rustfs http://rustfs:9000 rustfsadmin rustfsadmin; do
        echo 'Waiting for RustFS...';
        sleep 1;
      done;
      mc mb --ignore-existing rustfs/fluss;
      "

  zookeeper:
    image: zookeeper:3.9.2
    restart: always

  coordinator-server:
    image: apache/fluss:$FLUSS_DOCKER_VERSION$
    command: coordinatorServer
    depends_on:
      zookeeper:
        condition: service_started
      rustfs-init:
        condition: service_completed_successfully
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://coordinator-server:9123
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.region: us-east-1
        s3.path-style-access: true
        s3.assumed.role.arn: arn:aws:iam::000000000000:role/rustfsadmin
        s3.assumed.role.sts.endpoint: http://rustfs:9000
        datalake.enabled: true
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: s3://fluss/paimon
        datalake.paimon.s3.endpoint: http://rustfs:9000
        datalake.paimon.s3.access-key: rustfsadmin
        datalake.paimon.s3.secret-key: rustfsadmin
        datalake.paimon.s3.path.style.access: true
    volumes:
      - ./lib/paimon-s3-$PAIMON_VERSION$.jar:/opt/fluss/plugins/paimon/paimon-s3-$PAIMON_VERSION$.jar

  tablet-server:
    image: apache/fluss:$FLUSS_DOCKER_VERSION$
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://tablet-server:9123
        data.dir: /tmp/fluss/data
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.region: us-east-1
        s3.path-style-access: true
        s3.assumed.role.arn: arn:aws:iam::000000000000:role/rustfsadmin
        s3.assumed.role.sts.endpoint: http://rustfs:9000
        datalake.enabled: true
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: s3://fluss/paimon
        datalake.paimon.s3.endpoint: http://rustfs:9000
        datalake.paimon.s3.access-key: rustfsadmin
        datalake.paimon.s3.secret-key: rustfsadmin
        datalake.paimon.s3.path.style.access: true
    volumes:
      - ./lib/paimon-s3-$PAIMON_VERSION$.jar:/opt/fluss/plugins/paimon/paimon-s3-$PAIMON_VERSION$.jar

  gateway:
    image: apache/fluss-gateway:$FLUSS_DOCKER_VERSION$
    depends_on:
      - coordinator-server
    ports:
      - "8080:8080"
    environment:
      - FLUSS_GATEWAY__CLUSTER__DEFAULT__BOOTSTRAP__SERVERS=coordinator-server:9123

  jobmanager:
    image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
    ports:
      - "8083:8081"
    entrypoint: ["/opt/flink/init_paimon.sh"]
    command: ["jobmanager"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager

  taskmanager:
    image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
    depends_on:
      - jobmanager
    entrypoint: ["/opt/flink/init_paimon.sh"]
    command: ["taskmanager"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.task.off-heap.size: 128m

  sql-client:
    image: apache/fluss-quickstart-flink:$FLUSS_QUICKSTART_FLINK_DOCKER_VERSION$
    depends_on:
      - jobmanager
    entrypoint: ["/opt/flink/init_paimon.sh"]
    command: ["/opt/sql-client/sql-client"]
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        rest.address: jobmanager

volumes:
  rustfs-data:
```

The Docker Compose environment consists of the following containers:

- **Fluss Cluster:** a Fluss `CoordinatorServer`, a Fluss `TabletServer`, and a
  `ZooKeeper` server.
- **Fluss Gateway:** a stateless REST service that validates metadata and
  schema-aware record write requests before writing to Fluss.
- **Flink Cluster:** a Flink `JobManager`, a Flink `TaskManager`, and a Flink
  SQL client container. The Lakehouse Tiering Service also runs as a Flink job.
- **RustFS:** an S3-compatible object store used by Fluss remote storage and the
  Paimon warehouse.

:::tip
[RustFS](https://github.com/rustfs/rustfs) is used as an S3 replacement in this
quickstart. For a production setup, configure a supported cloud filesystem.
See [Filesystems](../maintenance/tiered-storage/filesystems/overview.md) for
more information.
:::

4. Start all containers.

```shell
docker compose up -d
docker compose ps
```

:::note
The `sql-client` service may exit after `docker compose up -d` because no
interactive terminal is attached. This is expected. You will start a new SQL
Client container with `docker compose run --rm sql-client` later in this guide.
:::

5. Wait until the Gateway can reach Fluss.

```shell
until curl -fsS \
  http://localhost:8080/v1/clusters/default/databases >/dev/null; do
  sleep 2
done
```

The Gateway listens on `http://localhost:8080`. The Flink Web UI is available
at [http://localhost:8083](http://localhost:8083), and the RustFS console is
available at [http://localhost:9001](http://localhost:9001) with
`rustfsadmin/rustfsadmin`.

:::note
All the following commands involving `docker compose` should be executed in the
working directory that contains the `docker-compose.yml` file.
:::

Congratulations, you are all set!

## Start the Lakehouse Tiering Service

Submit the reusable Lakehouse Tiering Service as a detached Flink job:

```shell
docker compose exec jobmanager \
  /opt/flink/bin/flink run -d \
  /opt/flink/opt/fluss-flink-tiering-$FLUSS_VERSION$.jar \
  --fluss.bootstrap.servers coordinator-server:9123 \
  --datalake.format paimon \
  --datalake.paimon.metastore filesystem \
  --datalake.paimon.warehouse s3://fluss/paimon \
  --datalake.paimon.s3.endpoint http://rustfs:9000 \
  --datalake.paimon.s3.access.key rustfsadmin \
  --datalake.paimon.s3.secret.key rustfsadmin \
  --datalake.paimon.s3.path.style.access true
```

The Flink Web UI should show one running tiering job.

## Create a Lake-Enabled Fluss Table

### Set Gateway variables

Set reusable shell variables:

```shell
GATEWAY_URL=http://localhost:8080
CLUSTER=default
DATABASE=fleet
TABLE=device_events
```

### Create a database

Create the database:

```shell
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases" \
  -d "{\"database\":\"$DATABASE\"}"
```

### Create a table

Create an append-only telemetry table. The two table options enable Paimon
tiering and request a target lake freshness of 30 seconds:

```shell
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables" \
  -d '{
    "table_name": "device_events",
    "columns": [
      {"name": "event_id", "data_type": {"type": "STRING"}, "nullable": false},
      {"name": "device_id", "data_type": {"type": "STRING"}, "nullable": false},
      {"name": "site", "data_type": {"type": "STRING"}, "nullable": false},
      {"name": "temperature_c", "data_type": {"type": "DOUBLE"}, "nullable": false},
      {
        "name": "event_time",
        "data_type": {
          "type": "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
          "precision": 3
        },
        "nullable": false
      }
    ],
    "distribution": {"bucket_count": 1, "bucket_keys": []},
    "configs": {
      "table.datalake.enabled": "true",
      "table.datalake.freshness": "30s"
    }
  }'
```

No SQL DDL or Fluss client code was required by the application.

## Ingest Events Through the Gateway

Send a schema-aware batch of JSON rows:

```shell
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/$TABLE/records" \
  -d '{
    "entries": [
      {
        "id": "request-row-1",
        "append": {
          "event_id": "evt-1001",
          "device_id": "truck-7",
          "site": "singapore",
          "temperature_c": 37.4,
          "event_time": "2026-09-02T07:00:00Z"
        }
      },
      {
        "id": "request-row-2",
        "append": {
          "event_id": "evt-1002",
          "device_id": "truck-8",
          "site": "singapore",
          "temperature_c": 38.1,
          "event_time": "2026-09-02T07:00:01Z"
        }
      },
      {
        "id": "request-row-3",
        "append": {
          "event_id": "evt-1003",
          "device_id": "van-3",
          "site": "jakarta",
          "temperature_c": 36.8,
          "event_time": "2026-09-02T07:00:02Z"
        }
      }
    ]
  }'
```

A successful response contains one outcome for each entry:

```json
{
  "row_count": 3,
  "success_count": 3,
  "error_count": 0,
  "successes": [
    {"id": "request-row-1"},
    {"id": "request-row-2"},
    {"id": "request-row-3"}
  ],
  "failures": []
}
```

Always check `error_count` and `failures`; an HTTP 200 response can contain
per-row failures.

## Query Events Immediately

Open the Flink SQL client:

```shell
docker compose run --rm sql-client
```

Create and select the Fluss catalog:

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123',
    'paimon.s3.access-key' = 'rustfsadmin',
    'paimon.s3.secret-key' = 'rustfsadmin'
);
```

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
USE fleet;
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';
```

Query the logical table immediately after the HTTP request:

```sql title="Flink SQL"
SELECT event_id, device_id, site, temperature_c, event_time
FROM device_events
ORDER BY event_time;
```

The result includes the three events even if Paimon has not committed its first
snapshot. Union Read gets the newest rows from Fluss and combines them with any
data that has already been tiered.

```text
+----------+-----------+-----------+---------------+-------------------------+
| event_id | device_id |      site | temperature_c |              event_time |
+----------+-----------+-----------+---------------+-------------------------+
| evt-1001 |   truck-7 | singapore |          37.4 | 2026-09-02 07:00:00.000 |
| evt-1002 |   truck-8 | singapore |          38.1 | 2026-09-02 07:00:01.000 |
| evt-1003 |     van-3 |   jakarta |          36.8 | 2026-09-02 07:00:02.000 |
+----------+-----------+-----------+---------------+-------------------------+
```

## Verify Data in Paimon

After approximately 30 seconds, inspect the Paimon snapshots through the
`$lake` system table:

```sql title="Flink SQL"
SELECT snapshot_id, total_record_count
FROM device_events$lake$snapshots
ORDER BY snapshot_id;
```

Then query only the lake data:

```sql title="Flink SQL"
SELECT COUNT(*) AS lake_rows
FROM device_events$lake;
```

The count becomes `3` after the tiering cycle commits the rows. You can also
open the RustFS console and browse the `fluss/paimon` warehouse.

The two query forms serve different purposes:

| Query | Data read | Expected freshness |
| --- | --- | --- |
| `SELECT ... FROM device_events` | Fluss hot data plus the latest Paimon snapshot | Sub-second for newly written rows |
| `SELECT ... FROM device_events$lake` | Paimon only | The configured tiering freshness |

## Keep the Stream Live

From another terminal, append another event:

```shell
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/json' \
  "$GATEWAY_URL/v1/clusters/$CLUSTER/databases/$DATABASE/tables/$TABLE/records" \
  -d '{
    "entries": [
      {
        "id": "request-row-4",
        "append": {
          "event_id": "evt-1004",
          "device_id": "truck-7",
          "site": "singapore",
          "temperature_c": 39.2,
          "event_time": "2026-09-02T07:00:03Z"
        }
      }
    ]
  }'
```

Run the Union Read query again. The new event is immediately queryable from
Fluss; the tiering service adds it to a later Paimon snapshot without any
change to the application.

## What This Quickstart Demonstrates

For this user story, the application does not need to create or operate:

- a Kafka topic and producer;
- a Flink ingestion job;
- a Paimon writer;
- separate schemas for the streaming and lake tables.

The reusable tiering service still runs as a Flink job, but it is infrastructure
for all lake-enabled Fluss tables rather than application-specific ingestion
code.

## Production Considerations

- The Gateway preview does not authenticate HTTP callers or terminate TLS.
  Protect it with an authenticated ingress and network controls.
- Gateway writes are at least once from the caller's perspective. Retrying a
  timed-out append can create duplicates. See
  [Check write results](../gateway/index.md#check-write-results).
- The Gateway does not expose record-read APIs in Fluss 1.0. Query records
  through Flink, Spark, or another supported engine.
- `table.datalake.freshness` controls Paimon materialization, not immediate
  query visibility. Immediate visibility comes from Fluss and Union Read.
- A shorter lake freshness creates snapshots more frequently; choose it based
  on the trade-off between lake latency and file efficiency.

## Clean up

Stop and delete the local environment when finished:

```shell
docker compose down -v
```
