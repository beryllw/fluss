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

package org.apache.fluss.flink.catalog;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;
import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test enable lake for {@link FlinkCatalog}. */
public class LakeEnabledFlinkCatalogTest {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(1)
                    .build();

    private static final String CATALOG_NAME = "test-catalog";
    private static final String DEFAULT_DB = "default";
    static Catalog catalog;
    private final ObjectPath tableInDefaultDb = new ObjectPath(DEFAULT_DB, "t1");

    @BeforeAll
    static void beforeAll() {
        // set fluss conf
        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        catalog =
                new FlinkCatalog(
                        CATALOG_NAME,
                        DEFAULT_DB,
                        String.join(",", flussConf.get(BOOTSTRAP_SERVERS)),
                        Thread.currentThread().getContextClassLoader(),
                        Collections.emptyMap());
        catalog.open();
    }

    @AfterAll
    static void afterAll() {
        if (catalog != null) {
            catalog.close();
        }
    }

    @BeforeEach
    void beforeEach() throws Exception {
        // First check if database exists, and drop it if it does
        if (catalog.databaseExists(DEFAULT_DB)) {
            catalog.dropDatabase(DEFAULT_DB, true, true);
        }
        try {
            catalog.createDatabase(
                    DEFAULT_DB, new CatalogDatabaseImpl(Collections.emptyMap(), null), true);
        } catch (CatalogException e) {
            // the auto partitioned manager may create the db zk node
            // in an another thread, so if exception is NodeExistsException, just ignore
            if (!ExceptionUtils.findThrowableWithMessage(e, "KeeperException$NodeExistsException")
                    .isPresent()) {
                throw e;
            }
        }
    }

    private static Configuration initConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        configuration.set(ConfigOptions.DEFAULT_BUCKET_NUMBER, 3);
        return configuration;
    }

    private CatalogTable newCatalogTable(Map<String, String> options) {
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("first", DataTypes.STRING().notNull()),
                                Column.physical("second", DataTypes.INT()),
                                Column.physical("third", DataTypes.STRING().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey(
                                "PK_first_third", Arrays.asList("first", "third")));
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        options);
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @Test
    void testCreateLakeTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(TABLE_DATALAKE_ENABLED.key(), "true");
        options.put(TABLE_DATALAKE_FORMAT.key(), DataLakeFormat.PAIMON.name());
        assertThatThrownBy(() -> catalog.getTable(tableInDefaultDb))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage(
                        String.format(
                                "Table (or view) %s does not exist in Catalog %s.",
                                tableInDefaultDb, CATALOG_NAME));
        CatalogTable table = this.newCatalogTable(options);
        catalog.createTable(this.tableInDefaultDb, table, false);
        assertThat(catalog.tableExists(this.tableInDefaultDb)).isTrue();
        // drop fluss table
        catalog.dropTable(this.tableInDefaultDb, false);
        // create the table again, should throw exception with ignore if exist = false
        assertThatThrownBy(() -> catalog.createTable(this.tableInDefaultDb, table, false))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessage(
                        String.format(
                                "Table (or view) %s already exists in Catalog %s.",
                                this.tableInDefaultDb, "lakeCatalog"));
    }
}
