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

package com.alibaba.fluss.flink.lakehouse.split;

import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.lake.source.LakeSplit;
import com.alibaba.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/** A split mixing Lake snapshot and Fluss log. */
public class LakeSnapshotAndFlussLogSplit extends SourceSplitBase {

    public static final byte LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND = -2;

    // may be null when no snapshot data for the bucket
    @Nullable private final LakeSplit lakeSnapshotSplit;

    /** The records to skip when reading the splits. */
    private long recordOffset = 0;
    // TODO: Support skip read file by record fileOffset

    private final long startingOffset;
    private final long stoppingOffset;

    public LakeSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable LakeSplit snapshotSplit,
            long startingOffset,
            long stoppingOffset) {
        this(tableBucket, null, snapshotSplit, startingOffset, stoppingOffset, 0);
    }

    public LakeSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable LakeSplit snapshotSplit,
            long startingOffset,
            long stoppingOffset) {
        this(tableBucket, partitionName, snapshotSplit, startingOffset, stoppingOffset, 0);
    }

    public LakeSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable LakeSplit snapshotSplit,
            long startingOffset,
            long stoppingOffset,
            long recordsToSkip) {
        super(tableBucket, partitionName);
        this.lakeSnapshotSplit = snapshotSplit;
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
        this.recordOffset = recordsToSkip;
    }

    public LakeSnapshotAndFlussLogSplit updateWithRecordsToSkip(long recordsToSkip) {
        return new LakeSnapshotAndFlussLogSplit(
                getTableBucket(),
                getPartitionName(),
                lakeSnapshotSplit,
                startingOffset,
                stoppingOffset,
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

    public LakeSplit getLakeSplit() {
        return lakeSnapshotSplit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LakeSnapshotAndFlussLogSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LakeSnapshotAndFlussLogSplit that = (LakeSnapshotAndFlussLogSplit) o;
        return Objects.equals(lakeSnapshotSplit, that.lakeSnapshotSplit)
                && Objects.equals(recordOffset, that.recordOffset);
    }

    @Override
    public String toString() {
        return "LakeSnapshotAndFlussLogSplit{"
                + "lakeSnapshotSplit="
                + lakeSnapshotSplit
                + ", recordOffset="
                + recordOffset
                + ", startingOffset="
                + startingOffset
                + ", stoppingOffset="
                + stoppingOffset
                + ", tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + '}';
    }
}
