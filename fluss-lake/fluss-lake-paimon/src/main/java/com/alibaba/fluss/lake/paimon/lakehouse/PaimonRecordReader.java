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

import com.alibaba.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import com.alibaba.fluss.lake.source.RecordReader;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.ProjectedRow;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.stream.IntStream;

import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toChangeType;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** Record reader for paimon table. */
public class PaimonRecordReader implements RecordReader {

    protected PaimonRowAsFlussRecordIterator iterator;
    protected @Nullable int[][] project;
    protected @Nullable Predicate predicate;
    protected RowType paimonRowType;

    public PaimonRecordReader(
            FileStoreTable fileStoreTable,
            PaimonSplit split,
            @Nullable int[][] project,
            @Nullable Predicate predicate)
            throws IOException {

        ReadBuilder readBuilder = fileStoreTable.newReadBuilder();
        if (project != null) {
            readBuilder = applyProject(readBuilder, project);
        }

        if (predicate != null) {
            readBuilder.withFilter(predicate);
        }

        TableRead tableRead = readBuilder.newRead();
        RowType paimonRowType = readBuilder.readType();

        org.apache.paimon.reader.RecordReader<InternalRow> recordReader =
                tableRead.createReader(split.dataSplit());
        iterator =
                new PaimonRecordReader.PaimonRowAsFlussRecordIterator(
                        recordReader.toCloseableIterator(), paimonRowType);
    }

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        return iterator;
    }

    private ReadBuilder applyProject(ReadBuilder readBuilder, int[][] projects) {
        int[] paimonProject = new int[projects.length];
        for (int i = 0; i < projects.length; i++) {
            paimonProject[i] = projects[i][0];
        }
        return readBuilder.withProjection(paimonProject);
    }

    /** Iterator for paimon row as fluss record. */
    public static class PaimonRowAsFlussRecordIterator implements CloseableIterator<LogRecord> {

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
