/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.source.enumerator;

import com.alibaba.fluss.client.admin.OffsetSpec;
import com.alibaba.fluss.client.metadata.LakeSnapshot;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.FlinkConnectorOptions;
import com.alibaba.fluss.flink.lakehouse.LakehouseTestBase;
import com.alibaba.fluss.flink.lakehouse.TestingLakeSource;
import com.alibaba.fluss.flink.lakehouse.split.LakeSnapshotAndFlussLogSplit;
import com.alibaba.fluss.flink.lakehouse.split.LakeSnapshotSplit;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.split.LogSplit;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.flink.tiering.committer.FlussTableLakeSnapshot;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link FlinkSourceEnumerator} with lake source. */
class FlinkLakeSourceEnumeratorTest extends LakehouseTestBase {

    private static Configuration flussConf;
    private static final long DEFAULT_SCAN_PARTITION_DISCOVERY_INTERVAL_MS = 10000L;

    @BeforeAll
    protected static void beforeAll() {
        LakehouseTestBase.beforeAll();
        flussConf = new Configuration(clientConf);
        flussConf.setString(
                FlinkConnectorOptions.SCAN_PARTITION_DISCOVERY_INTERVAL.key(),
                Duration.ofSeconds(10).toString());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPkTableLakeSnapshotDataWithLakeSource(boolean isPartitioned) throws Throwable {
        TablePath tablePath = isPartitioned ? DEFAULT_PARTITION_TABLE_PATH : DEFAULT_TABLE_PATH;
        TableDescriptor tableDescriptor =
                isPartitioned
                        ? DEFAULT_PARTITION_PK_TABLE_WITH_LAKE_DESCRIPTOR
                        : DEFAULT_PK_TABLE_WITH_LAKE_DESCRIPTOR;
        long tableId = createTable(tablePath, tableDescriptor);
        Map<TableBucket, Long> logEndOffsets = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            logEndOffsets.put(
                    isPartitioned ? new TableBucket(tableId, 0L, i) : new TableBucket(tableId, i),
                    1L);
        }

        putRows(tablePath, 10, isPartitioned);

        FlussTableLakeSnapshot flussTableLakeSnapshot =
                new FlussTableLakeSnapshot(tableId, 0, logEndOffsets);
        lakeSnapshotCommitter.commit(flussTableLakeSnapshot);

        int numSubtasks = 3;
        // test get snapshot split & log split and the assignment
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            FlinkSourceEnumerator enumerator =
                    new FlinkSourceEnumerator(
                            tablePath,
                            flussConf,
                            true,
                            false,
                            context,
                            OffsetsInitializer.full(),
                            DEFAULT_SCAN_PARTITION_DISCOVERY_INTERVAL_MS,
                            false,
                            Collections.emptyList(),
                            new TestingLakeSource(
                                    () ->
                                            Arrays.asList(
                                                    new TestingLakeSource.TestingLakeSplit(
                                                            0,
                                                            isPartitioned
                                                                    ? Arrays.asList("20250801")
                                                                    : Collections.emptyList()),
                                                    new TestingLakeSource.TestingLakeSplit(
                                                            1,
                                                            isPartitioned
                                                                    ? Arrays.asList("20250801")
                                                                    : Collections.emptyList()),
                                                    new TestingLakeSource.TestingLakeSplit(
                                                            2,
                                                            isPartitioned
                                                                    ? Arrays.asList("20250801")
                                                                    : Collections.emptyList()))));

            enumerator.start();

            // register all read
            for (int i = 0; i < 3; i++) {
                registerReader(context, enumerator, i);
            }

            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // make enumerate to get splits and assign
            context.runNextOneTimeCallable();

            Map<Integer, List<SourceSplitBase>> expectedAssignment = new HashMap<>();
            LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
            Map<TableBucket, Long> tableBucketsOffset = lakeSnapshot.getTableBucketsOffset();
            Map<Integer, Long> latestBuckets =
                    isPartitioned
                            ? admin.listOffsets(
                                            tablePath,
                                            "20250801",
                                            Arrays.asList(0, 1, 2),
                                            new OffsetSpec.LatestSpec())
                                    .all()
                                    .get()
                            : admin.listOffsets(
                                            tablePath,
                                            Arrays.asList(0, 1, 2),
                                            new OffsetSpec.LatestSpec())
                                    .all()
                                    .get();
            for (int i = 0; i < numSubtasks; i++) {
                // one split for one subtask

                LakeSnapshotAndFlussLogSplit split;
                if (isPartitioned) {
                    TableBucket bucket = new TableBucket(tableId, 0L, i);
                    split =
                            new LakeSnapshotAndFlussLogSplit(
                                    bucket,
                                    "20250801",
                                    new TestingLakeSource.TestingLakeSplit(
                                            i, Arrays.asList("20250801")),
                                    tableBucketsOffset.get(bucket),
                                    latestBuckets.get(bucket.getBucket()));
                } else {
                    TableBucket bucket = new TableBucket(tableId, i);
                    split =
                            new LakeSnapshotAndFlussLogSplit(
                                    bucket,
                                    new TestingLakeSource.TestingLakeSplit(
                                            i, Collections.emptyList()),
                                    tableBucketsOffset.get(bucket),
                                    latestBuckets.get(bucket.getBucket()));
                }
                expectedAssignment.put(i, Collections.singletonList(split));
            }

            Map<Integer, List<SourceSplitBase>> actualAssignment =
                    getLastReadersAssignments(context);
            assertThat(actualAssignment).isEqualTo(expectedAssignment);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNonPkTableLakeSnapshotDataWithLakeSource(boolean isPartitioned) throws Throwable {
        TablePath tablePath = isPartitioned ? DEFAULT_PARTITION_TABLE_PATH : DEFAULT_TABLE_PATH;
        TableDescriptor tableDescriptor =
                isPartitioned
                        ? DEFAULT_PARTITION_LOG_WITH_LAKE_TABLE_DESCRIPTOR
                        : DEFAULT_LOG_WITH_LAKE_TABLE_DESCRIPTOR;
        long tableId = createTable(tablePath, tableDescriptor);

        Map<TableBucket, Long> logEndOffsets = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            logEndOffsets.put(
                    isPartitioned ? new TableBucket(tableId, 0L, i) : new TableBucket(tableId, i),
                    1L);
        }

        putLogRows(tablePath, 10, isPartitioned);

        FlussTableLakeSnapshot flussTableLakeSnapshot =
                new FlussTableLakeSnapshot(tableId, 0, logEndOffsets);
        lakeSnapshotCommitter.commit(flussTableLakeSnapshot);

        int numSubtasks = 3;
        // test get snapshot split & log split and the assignment
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            FlinkSourceEnumerator enumerator =
                    new FlinkSourceEnumerator(
                            tablePath,
                            flussConf,
                            true,
                            false,
                            context,
                            OffsetsInitializer.full(),
                            DEFAULT_SCAN_PARTITION_DISCOVERY_INTERVAL_MS,
                            false,
                            Collections.emptyList(),
                            new TestingLakeSource(
                                    () ->
                                            Arrays.asList(
                                                    new TestingLakeSource.TestingLakeSplit(
                                                            0,
                                                            isPartitioned
                                                                    ? Arrays.asList("20250801")
                                                                    : Collections.emptyList()),
                                                    new TestingLakeSource.TestingLakeSplit(
                                                            1,
                                                            isPartitioned
                                                                    ? Arrays.asList("20250801")
                                                                    : Collections.emptyList()),
                                                    new TestingLakeSource.TestingLakeSplit(
                                                            2,
                                                            isPartitioned
                                                                    ? Arrays.asList("20250801")
                                                                    : Collections.emptyList()))));

            enumerator.start();

            // register all read
            for (int i = 0; i < 3; i++) {
                registerReader(context, enumerator, i);
            }

            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // make enumerate to get splits and assign
            context.runNextOneTimeCallable();

            Map<Integer, List<SourceSplitBase>> expectedAssignment = new HashMap<>();
            Map<Integer, Long> latestBuckets =
                    isPartitioned
                            ? admin.listOffsets(
                                            tablePath,
                                            "20250801",
                                            Arrays.asList(0, 1, 2),
                                            new OffsetSpec.LatestSpec())
                                    .all()
                                    .get()
                            : admin.listOffsets(
                                            tablePath,
                                            Arrays.asList(0, 1, 2),
                                            new OffsetSpec.LatestSpec())
                                    .all()
                                    .get();
            for (int i = 0; i < numSubtasks; i++) {
                // one split for one subtask

                TableBucket bucket =
                        isPartitioned
                                ? new TableBucket(tableId, 0L, i)
                                : new TableBucket(tableId, i);
                expectedAssignment.put(
                        i,
                        Arrays.asList(
                                new LakeSnapshotSplit(
                                        bucket,
                                        isPartitioned ? "20250801" : null,
                                        new TestingLakeSource.TestingLakeSplit(
                                                i,
                                                isPartitioned
                                                        ? Arrays.asList("20250801")
                                                        : Collections.emptyList())),
                                new LogSplit(
                                        bucket,
                                        isPartitioned ? "20250801" : null,
                                        1,
                                        latestBuckets.get(bucket.getBucket()))));
            }

            Map<Integer, List<SourceSplitBase>> actualAssignment =
                    getLastReadersAssignments(context);
            assertThat(actualAssignment).isEqualTo(expectedAssignment);
        }
    }

    private void putRows(TablePath tablePath, int rowsNum, boolean isPartitioned) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = 0; i < rowsNum; i++) {
                if (isPartitioned) {
                    InternalRow row = row(i, "v" + i, "20250801");
                    upsertWriter.upsert(row);
                } else {
                    InternalRow row = row(i, "v" + i);
                    upsertWriter.upsert(row);
                }
            }
            upsertWriter.flush();
        }
    }

    private void putLogRows(TablePath tablePath, int rowsNum, boolean isPartitioned)
            throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 0; i < rowsNum; i++) {
                if (isPartitioned) {
                    GenericRow row = row(i, "v" + i, "20250801");
                    appendWriter.append(row);
                } else {
                    GenericRow row = row(i, "v" + i);
                    appendWriter.append(row);
                }
            }
            appendWriter.flush();
        }
    }

    // ---------------------
    private void registerReader(
            MockSplitEnumeratorContext<SourceSplitBase> context,
            FlinkSourceEnumerator enumerator,
            int readerId) {
        context.registerReader(new ReaderInfo(readerId, "location " + readerId));
        enumerator.addReader(readerId);
    }

    private Map<Integer, List<SourceSplitBase>> getLastReadersAssignments(
            MockSplitEnumeratorContext<SourceSplitBase> context) {
        List<SplitsAssignment<SourceSplitBase>> splitsAssignments =
                context.getSplitsAssignmentSequence();
        // get the last one
        SplitsAssignment<SourceSplitBase> splitAssignment =
                splitsAssignments.get(splitsAssignments.size() - 1);
        return splitAssignment.assignment();
    }
}
