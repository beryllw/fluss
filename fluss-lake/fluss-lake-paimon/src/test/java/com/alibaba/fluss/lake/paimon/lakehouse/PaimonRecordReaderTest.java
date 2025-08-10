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

package com.alibaba.fluss.lake.paimon.lakehouse;

import com.alibaba.fluss.lake.paimon.flink.PaimonLakeHouseTestBase;
import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.lake.source.RecordReader;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.*;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.flink.types.Row;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test case for {@link PaimonRecordReader}. */
public class PaimonRecordReaderTest extends PaimonLakeHouseTestBase {
    @BeforeAll
    protected static void beforeAll() {
        PaimonLakeHouseTestBase.beforeAll();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReadLogTable(boolean isPartitioned) throws Exception {
        // first of all, create table and prepare data
        String tableName = "logTable_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        List<InternalRow> writtenRows = new ArrayList<>();
        prepareLogTable(tablePath, isPartitioned, DEFAULT_BUCKET_NUM, writtenRows);

        LakeSource<PaimonSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        Table table = getTable(tablePath);
        Snapshot snapshot = table.latestSnapshot().get();
        List<PaimonSplit> paimonSplits = lakeSource.createPlanner(snapshot::id).plan();

        List<Split> splits = ((FileStoreTable) table).newScan().plan().splits();
        assertThat(splits).hasSize(paimonSplits.size());
        assertThat(splits)
                .isEqualTo(
                        paimonSplits.stream()
                                .map(PaimonSplit::dataSplit)
                                .collect(Collectors.toList()));

        List<Row> actual = new ArrayList<>();

        InternalRow.FieldGetter[] fieldGetters =
                InternalRow.createFieldGetters(getFlussRowType(isPartitioned));
        for (PaimonSplit paimonSplit : paimonSplits) {
            RecordReader recordReader = lakeSource.createRecordReader(() -> paimonSplit);
            CloseableIterator<LogRecord> iterator = recordReader.read();
            while (iterator.hasNext()) {
                InternalRow row = iterator.next().getRow();
                Row flinkRow = new Row(fieldGetters.length);
                for (int i = 0; i < fieldGetters.length; i++) {
                    flinkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
                }
                actual.add(flinkRow);
            }
            iterator.close();
        }
        List<Row> expectRows = new ArrayList<>();
        for (InternalRow row : writtenRows) {
            Row flinkRow = new Row(fieldGetters.length);
            for (int i = 0; i < fieldGetters.length; i++) {
                flinkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
            }
            expectRows.add(flinkRow);
        }

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectRows);
    }

    private RowType getFlussRowType(boolean isPartitioned) {
        return isPartitioned
                ? RowType.of(
                        new BooleanType(),
                        new TinyIntType(),
                        new SmallIntType(),
                        new IntType(),
                        new BigIntType(),
                        new FloatType(),
                        new DoubleType(),
                        new StringType(),
                        new DecimalType(5, 2),
                        new DecimalType(20, 0),
                        new LocalZonedTimestampType(3),
                        new LocalZonedTimestampType(6),
                        new TimestampType(3),
                        new TimestampType(6),
                        new BinaryType(4),
                        new StringType())
                : RowType.of(
                        new BooleanType(),
                        new TinyIntType(),
                        new SmallIntType(),
                        new IntType(),
                        new BigIntType(),
                        new FloatType(),
                        new DoubleType(),
                        new StringType(),
                        new DecimalType(5, 2),
                        new DecimalType(20, 0),
                        new LocalZonedTimestampType(3),
                        new LocalZonedTimestampType(6),
                        new TimestampType(3),
                        new TimestampType(6),
                        new BinaryType(4));
    }

    private void prepareLogTable(
            TablePath tablePath, boolean isPartitioned, int bucketNum, List<InternalRow> rows)
            throws Exception {
        createFullTypeLogTable(tablePath, isPartitioned, bucketNum);
        rows.addAll(writeFullTypeRows(tablePath, 10, isPartitioned ? "test" : null));
    }

    private void createFullTypeLogTable(TablePath tablePath, boolean isPartitioned, int bucketNum)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("f_boolean", DataTypes.BOOLEAN())
                        .column("f_byte", DataTypes.TINYINT())
                        .column("f_short", DataTypes.SMALLINT())
                        .column("f_int", DataTypes.INT())
                        .column("f_long", DataTypes.BIGINT())
                        .column("f_float", DataTypes.FLOAT())
                        .column("f_double", DataTypes.DOUBLE())
                        .column("f_string", DataTypes.STRING())
                        .column("f_decimal1", DataTypes.DECIMAL(5, 2))
                        .column("f_decimal2", DataTypes.DECIMAL(20, 0))
                        .column("f_timestamp_ltz1", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                        .column("f_timestamp_ltz2", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6))
                        .column("f_timestamp_ntz1", DataTypes.TIMESTAMP(3))
                        .column("f_timestamp_ntz2", DataTypes.TIMESTAMP(6))
                        .column("f_binary", DataTypes.BINARY(4));

        if (isPartitioned) {
            schemaBuilder.column("p", DataTypes.STRING());
            schemaBuilder.partitionKeys("p");
            schemaBuilder.option(CoreOptions.BUCKET.key(), String.valueOf(bucketNum));
            schemaBuilder.option(CoreOptions.BUCKET_KEY.key(), "f_long");
        }
        schemaBuilder
                .column("__bucket", DataTypes.INT())
                .column("__offset", DataTypes.BIGINT())
                .column("__timestamp", DataTypes.TIMESTAMP(6));
        createTable(tablePath, schemaBuilder.build());
    }

    private List<InternalRow> writeFullTypeRows(
            TablePath tablePath, int rowCount, @Nullable String partition) throws Exception {
        List<org.apache.paimon.data.InternalRow> rows = new ArrayList<>();
        List<InternalRow> flussRows = new ArrayList<>();
        Table table = getTable(tablePath);

        for (int i = 0; i < rowCount; i++) {
            if (partition == null) {
                com.alibaba.fluss.row.GenericRow row =
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                i + 400L,
                                500.1f,
                                600.0d,
                                com.alibaba.fluss.row.BinaryString.fromString(
                                        "another_string_" + i),
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                0,
                                (long) i,
                                TimestampNtz.fromMillis(System.currentTimeMillis()));
                rows.add(new FlussRowAsPaimonRow(row, table.rowType()));
                flussRows.add(row);
            } else {
                com.alibaba.fluss.row.GenericRow row =
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                i + 400L,
                                500.1f,
                                600.0d,
                                com.alibaba.fluss.row.BinaryString.fromString(
                                        "another_string_" + i),
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                com.alibaba.fluss.row.BinaryString.fromString(partition),
                                0,
                                (long) i,
                                TimestampNtz.fromMillis(System.currentTimeMillis()));
                rows.add(new FlussRowAsPaimonRow(row, table.rowType()));
                flussRows.add(row);
            }
        }
        writeRecord(tablePath, rows);
        return flussRows;
    }
}
