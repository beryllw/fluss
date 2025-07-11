/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.source1;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lake.source1.LakeSource;
import com.alibaba.fluss.lake.source1.Planner;
import com.alibaba.fluss.lake.source1.SortedRecordReader;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.predicate.Predicate;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.ProjectedRow;
import com.alibaba.fluss.utils.CloseableIterator;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toChangeType;
import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** */
public class PaimonLakeSource implements LakeSource<PaimonSplit> {

    private final Configuration paimonConfig;
    private final TablePath tablePath;

    private @Nullable int[][] project;
    private @Nullable org.apache.paimon.predicate.Predicate predicate;

    public PaimonLakeSource(Configuration paimonConfig, TablePath tablePath) {
        this.paimonConfig = paimonConfig;
        this.tablePath = tablePath;
    }

    @Override
    public void withProject(int[][] project) {
        this.project = project;
    }

    @Override
    public void witLimit(int limit) {
        throw new UnsupportedOperationException("PaimonLakeSource does not support limit");
    }

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        return null;
    }

    @Override
    public Planner<PaimonSplit> createPlanner(PlannerContext plannerContext) {
        return new PaimonSplitPanner(
                paimonConfig, tablePath, predicate, plannerContext.snapshotId());
    }

    @Override
    public com.alibaba.fluss.lake.source1.RecordReader createRecordReader(
            ReaderContext<PaimonSplit> context) throws IOException {
        try {
            try (Catalog catalog = getCatalog()) {
                FileStoreTable fileStoreTable = getTable(catalog, tablePath);
                ReadBuilder readBuilder = fileStoreTable.newReadBuilder();
                if (project != null) {
                    readBuilder = project(readBuilder, project);
                }

                if (predicate != null) {
                    readBuilder.withFilter(predicate);
                }

                TableRead tableRead = readBuilder.newRead();
                RowType rowType = readBuilder.readType();

                RecordReader<InternalRow> recordReader =
                        tableRead.createReader(context.lakeSplit().dataSplit());
                if (fileStoreTable.primaryKeys().isEmpty()) {
                    return () ->
                            new PaimonRowAsFlussRecordIterator(
                                    recordReader.toCloseableIterator(), rowType);
                } else {
                    KeyValueFileStore keyValueFileStore =
                            (KeyValueFileStore) fileStoreTable.store();
                    return new SortedRecordReader() {

                        @Override
                        public CloseableIterator<LogRecord> read() {
                            return new PaimonRowAsFlussRecordIterator(
                                    recordReader.toCloseableIterator(), rowType);
                        }

                        @Override
                        public Comparator<com.alibaba.fluss.row.InternalRow> order() {
                            return toFlussRowComparator(
                                    fileStoreTable.rowType(), keyValueFileStore.newKeyComparator());
                        }
                    };
                }
            }
        } catch (Exception e) {
            throw new IOException("Fail to create record reader.", e);
        }
    }

    @Override
    public SimpleVersionedSerializer<PaimonSplit> getSplitSerializer() {
        return new PaimonSplitSerializer();
    }

    private Catalog getCatalog() {
        return CatalogFactory.createCatalog(
                CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
    }

    private FileStoreTable getTable(Catalog catalog, TablePath tablePath) throws Exception {
        return (FileStoreTable) catalog.getTable(toPaimon(tablePath));
    }

    private ReadBuilder project(ReadBuilder readBuilder, int[][] projects) {
        int[] paimonProject = new int[projects.length];
        for (int i = 0; i < projects.length; i++) {
            paimonProject[i] = projects[i][0];
        }
        return readBuilder.withProjection(paimonProject);
    }

    private Comparator<com.alibaba.fluss.row.InternalRow> toFlussRowComparator(
            RowType rowType, Comparator<InternalRow> paimonRowcomparator) {
        return (row1, row2) ->
                paimonRowcomparator.compare(
                        new FlussRowAsPaimonRow(row1, rowType),
                        new FlussRowAsPaimonRow(row2, rowType));
    }

    private static class PaimonRowAsFlussRecordIterator implements CloseableIterator<LogRecord> {

        private final org.apache.paimon.utils.CloseableIterator<InternalRow> paimonRowIterator;

        private final ProjectedRow flussRow;

        private final int logOffsetColIndex;
        private final int timestampColIndex;

        public PaimonRowAsFlussRecordIterator(
                org.apache.paimon.utils.CloseableIterator<InternalRow> paimonRowIterator,
                RowType paimonRowType) {
            this.paimonRowIterator = paimonRowIterator;
            this.logOffsetColIndex = paimonRowType.getFieldIndex(OFFSET_COLUMN_NAME);
            this.timestampColIndex = paimonRowType.getFieldIndex(TIMESTAMP_COLUMN_NAME);

            int[] project = IntStream.range(0, paimonRowType.getFieldCount() - 3).toArray();
            flussRow = ProjectedRow.from(project);
        }

        @Override
        public void close() {
            try {
                paimonRowIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return paimonRowIterator.hasNext();
        }

        @Override
        public LogRecord next() {
            InternalRow paimonRow = paimonRowIterator.next();
            ChangeType changeType = toChangeType(paimonRow.getRowKind());
            long offset = paimonRow.getLong(logOffsetColIndex);
            long timestamp = paimonRow.getTimestamp(timestampColIndex, 6).getMillisecond();

            return new GenericRecord(
                    offset,
                    timestamp,
                    changeType,
                    flussRow.replaceRow(new PaimonRowAsFlussRow(paimonRow)));
        }
    }
}
