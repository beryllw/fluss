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

package com.alibaba.fluss.flink.lake;

import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.lake.source1.LakeSplit;
import com.alibaba.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/** A split mixing Lake snapshot and Fluss log. */
public class LakeSnapshotAndFlussLogSplit extends SourceSplitBase {

    public static final byte LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND = -2;

    // may be null when no snapshot data for the bucket
    @Nullable private final List<LakeSplit> lakeSnapshotSplits;

    /** The records to skip when reading the splits. */
    private int fileOffset = -1;

    private long recordOffset = 0;

    private final long startingOffset;
    private final long stoppingOffset;

    public LakeSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable List<LakeSplit> snapshotSplit,
            long startingOffset,
            long stoppingOffset) {
        this(tableBucket, null, snapshotSplit, startingOffset, stoppingOffset, 0);
    }

    public LakeSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable List<LakeSplit> snapshotSplit,
            long startingOffset,
            long stoppingOffset) {
        this(tableBucket, partitionName, snapshotSplit, startingOffset, stoppingOffset, 0);
    }

    public LakeSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable List<LakeSplit> snapshotSplit,
            long startingOffset,
            long stoppingOffset,
            long recordsToSkip) {
        super(tableBucket, partitionName);
        this.lakeSnapshotSplits = snapshotSplit;
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
        this.recordOffset = recordsToSkip;
    }

    public LakeSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable List<LakeSplit> snapshotSplit,
            long startingOffset,
            long stoppingOffset,
            int fileOffset,
            long recordsToSkip) {
        super(tableBucket, partitionName);
        this.lakeSnapshotSplits = snapshotSplit;
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
        this.fileOffset = fileOffset;
        this.recordOffset = recordsToSkip;
    }

    public LakeSnapshotAndFlussLogSplit updateWithRecordsToSkip(long recordsToSkip) {
        return new LakeSnapshotAndFlussLogSplit(
                getTableBucket(),
                getPartitionName(),
                lakeSnapshotSplits,
                startingOffset,
                stoppingOffset,
                recordsToSkip);
    }

    public LakeSnapshotAndFlussLogSplit updateWithRecordsToSkip(
            int fileOffset, long recordsToSkip) {
        return new LakeSnapshotAndFlussLogSplit(
                getTableBucket(),
                getPartitionName(),
                lakeSnapshotSplits,
                startingOffset,
                stoppingOffset,
                fileOffset,
                recordsToSkip);
    }

    public long getRecordsToSkip() {
        return recordOffset;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public Optional<Long> getStoppingOffset() {
        return stoppingOffset >= 0 ? Optional.of(stoppingOffset) : Optional.empty();
    }

    @Override
    public boolean isLakeSplit() {
        return true;
    }

    protected byte splitKind() {
        return LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND;
    }

    @Override
    public String splitId() {
        return toSplitId("lake-hybrid-snapshot-log-", tableBucket);
    }

    public List<LakeSplit> getLakeSplits() {
        return lakeSnapshotSplits;
    }
}
