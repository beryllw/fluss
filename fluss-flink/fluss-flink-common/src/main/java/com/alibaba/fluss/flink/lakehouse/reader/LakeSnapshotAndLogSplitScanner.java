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

package com.alibaba.fluss.flink.lakehouse.reader;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.batch.BatchScanner;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.flink.lakehouse.split.LakeSnapshotAndFlussLogSplit;
import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.lake.source.LakeSplit;
import com.alibaba.fluss.lake.source.RecordReader;
import com.alibaba.fluss.lake.source.SortedRecordReader;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

/** A scanner to merge the lakehouse's snapshot and change log. */
public class LakeSnapshotAndLogSplitScanner implements BatchScanner {

    private final LakeSnapshotAndFlussLogSplit lakeSnapshotSplitAndFlussLogSplit;
    private Comparator<InternalRow> rowComparator;
    private CloseableIterator<LogRecord> lakeRecords;
    private final LakeSource<LakeSplit> lakeSource;

    private final int[] pkIndexes;

    // the sorted logs in memory, mapping from key -> value
    private SortedMap<InternalRow, KeyValueRow> logRows;

    private final LogScanner logScanner;
    private final long stoppingOffset;
    private boolean logScanFinished;

    private SortMergeReader currentSortMergeReader;

    public LakeSnapshotAndLogSplitScanner(
            Table table,
            LakeSource<LakeSplit> lakeSource,
            LakeSnapshotAndFlussLogSplit lakeSnapshotAndFlussLogSplit) {
        this.pkIndexes = table.getTableInfo().getSchema().getPrimaryKeyIndexes();
        this.lakeSnapshotSplitAndFlussLogSplit = lakeSnapshotAndFlussLogSplit;
        this.lakeSource = lakeSource;

        this.logScanner = table.newScan().createLogScanner();

        TableBucket tableBucket = lakeSnapshotAndFlussLogSplit.getTableBucket();
        if (tableBucket.getPartitionId() != null) {
            this.logScanner.subscribe(
                    tableBucket.getPartitionId(),
                    tableBucket.getBucket(),
                    lakeSnapshotAndFlussLogSplit.getStartingOffset());
        } else {
            this.logScanner.subscribe(
                    tableBucket.getBucket(), lakeSnapshotAndFlussLogSplit.getStartingOffset());
        }

        this.stoppingOffset =
                lakeSnapshotAndFlussLogSplit
                        .getStoppingOffset()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "StoppingOffset is null for split: "
                                                        + lakeSnapshotAndFlussLogSplit));

        this.logScanFinished = lakeSnapshotAndFlussLogSplit.getStartingOffset() >= stoppingOffset;
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (logScanFinished) {
            if (lakeRecords == null) {
                RecordReader recordReader =
                        lakeSource.createRecordReader(
                                (LakeSource.ReaderContext<LakeSplit>)
                                        lakeSnapshotSplitAndFlussLogSplit::getLakeSplit);

                lakeRecords = recordReader.read();
            }
            if (currentSortMergeReader == null) {
                currentSortMergeReader =
                        new SortMergeReader(
                                pkIndexes,
                                lakeRecords,
                                rowComparator,
                                CloseableIterator.wrap(
                                        logRows == null
                                                ? Collections.emptyIterator()
                                                : logRows.values().iterator()));
            }
            return currentSortMergeReader.readBatch();
        } else {
            if (lakeRecords == null) {
                RecordReader recordReader =
                        lakeSource.createRecordReader(
                                (LakeSource.ReaderContext<LakeSplit>)
                                        lakeSnapshotSplitAndFlussLogSplit::getLakeSplit);
                if (recordReader instanceof SortedRecordReader) {
                    rowComparator = ((SortedRecordReader) recordReader).order();
                    lakeRecords = recordReader.read();
                } else {
                    throw new UnsupportedOperationException(
                            "lake records must instance of sorted view.");
                }
                logRows = new TreeMap<>(rowComparator);
            }
            pollLogRecords(timeout);
            return CloseableIterator.wrap(Collections.emptyIterator());
        }
    }

    private void pollLogRecords(Duration timeout) {
        ScanRecords scanRecords = logScanner.poll(timeout);
        for (ScanRecord scanRecord : scanRecords) {
            boolean isDelete =
                    scanRecord.getChangeType() == ChangeType.DELETE
                            || scanRecord.getChangeType() == ChangeType.UPDATE_BEFORE;
            KeyValueRow keyValueRow = new KeyValueRow(pkIndexes, scanRecord.getRow(), isDelete);
            InternalRow keyRow = keyValueRow.keyRow();
            // upsert the key value row
            logRows.put(keyRow, keyValueRow);
            if (scanRecord.logOffset() >= stoppingOffset - 1) {
                // has reached to the end
                logScanFinished = true;
                break;
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (logScanner != null) {
                logScanner.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close resources", e);
        }
    }
}
