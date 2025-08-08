/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.lakehouse;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.tiering.committer.FlussTableLakeSnapshotCommitter;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;

/** A base class for testing with Fluss cluster with enable datalake prepared. */
public class LakehouseTestBase extends AbstractTestBase {

    protected static FlussTableLakeSnapshotCommitter lakeSnapshotCommitter;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new Configuration()
                                    // set snapshot interval to 1s for testing purposes
                                    .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
                                    // not to clean snapshots for test purpose
                                    .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE)
                                    .set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON))
                    .setNumOfTabletServers(3)
                    .build();

    protected static final int DEFAULT_BUCKET_NUM = 3;

    protected static final Schema DEFAULT_PK_TABLE_SCHEMA =
            Schema.newBuilder()
                    .primaryKey("id")
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .build();

    protected static final TableDescriptor DEFAULT_PK_TABLE_WITH_LAKE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_PK_TABLE_SCHEMA)
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .property("table.datalake.enabled", "true")
                    .build();

    protected static final Schema DEFAULT_PARTITION_PK_TABLE_SCHEMA =
            Schema.newBuilder()
                    .primaryKey("id", "dt")
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("dt", DataTypes.STRING())
                    .build();

    protected static final TableDescriptor DEFAULT_PARTITION_PK_TABLE_WITH_LAKE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_PARTITION_PK_TABLE_SCHEMA)
                    .partitionedBy("dt")
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .property("table.datalake.enabled", "true")
                    .build();

    protected static final Schema DEFAULT_LOG_TABLE_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .build();

    protected static final Schema DEFAULT_PARTITION_LOG_TABLE_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("dt", DataTypes.STRING())
                    .build();

    protected static final TableDescriptor DEFAULT_LOG_WITH_LAKE_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_LOG_TABLE_SCHEMA)
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .property("table.datalake.enabled", "true")
                    .build();

    protected static final TableDescriptor DEFAULT_PARTITION_LOG_WITH_LAKE_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_PARTITION_LOG_TABLE_SCHEMA)
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .partitionedBy("dt")
                    .property("table.datalake.enabled", "true")
                    .build();

    protected static final TableDescriptor DEFAULT_PK_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_PK_TABLE_SCHEMA)
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .build();

    protected static final String DEFAULT_DB = "test-flink-db";

    protected static final TablePath DEFAULT_TABLE_PATH =
            TablePath.of(DEFAULT_DB, "test-flink-table");

    protected static final TablePath DEFAULT_PARTITION_TABLE_PATH =
            TablePath.of(DEFAULT_DB, "test-flink-table-partition");

    protected static Connection conn;
    protected static Admin admin;

    protected static Configuration clientConf;
    protected static String bootstrapServers;

    protected long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    protected void waitUntilSnapshot(long tableId, long snapshotId) {
        for (int i = 0; i < DEFAULT_BUCKET_NUM; i++) {
            TableBucket tableBucket = new TableBucket(tableId, i);
            FLUSS_CLUSTER_EXTENSION.waitUntilSnapshotFinished(tableBucket, snapshotId);
        }
    }

    @BeforeAll
    protected static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
        lakeSnapshotCommitter =
                new FlussTableLakeSnapshotCommitter(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        lakeSnapshotCommitter.open();
    }

    @BeforeEach
    void beforeEach() throws Exception {
        admin.createDatabase(DEFAULT_DB, DatabaseDescriptor.EMPTY, true).get();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (lakeSnapshotCommitter != null) {
            lakeSnapshotCommitter.close();
            lakeSnapshotCommitter = null;
        }

        if (admin != null) {
            admin.close();
            admin = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }
    }
}
