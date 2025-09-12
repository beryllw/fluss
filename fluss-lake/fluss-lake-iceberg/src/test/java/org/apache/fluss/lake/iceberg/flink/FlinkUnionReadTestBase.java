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

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;

/** Base class for iceberg union read test. */
class FlinkUnionReadTestBase extends FlinkIcebergTieringTestBase {
    protected static final String DEFAULT_DB = "fluss";

    protected static final String CATALOG_NAME = "test_iceberg_lake";
    protected static final int DEFAULT_BUCKET_NUM = 1;
    StreamTableEnvironment batchTEnv;

    @BeforeAll
    protected static void beforeAll() {
        FlinkIcebergTieringTestBase.beforeAll();
    }

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        // create table environment
        batchTEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inBatchMode());
        // crate catalog using sql
        batchTEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        batchTEnv.executeSql("use catalog " + CATALOG_NAME);
        batchTEnv.executeSql("use " + DEFAULT_DB);
    }
}
