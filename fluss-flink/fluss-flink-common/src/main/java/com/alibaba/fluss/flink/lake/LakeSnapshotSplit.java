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

/** . */
public class LakeSnapshotSplit extends SourceSplitBase {

    public static final byte LAKE_SNAPSHOT_SPLIT_KIND = -1;

    private final LakeSplit lakeSplit;

    private final long recordsToSplit;

    public LakeSnapshotSplit(
            TableBucket tableBucket, @Nullable String partitionName, LakeSplit lakeSplit) {
        this(tableBucket, partitionName, lakeSplit, 0);
    }

    public LakeSnapshotSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            LakeSplit lakeSplit,
            long recordsToSplit) {
        super(tableBucket, partitionName);
        this.lakeSplit = lakeSplit;
        this.recordsToSplit = recordsToSplit;
    }

    public LakeSplit getLakeSplit() {
        return lakeSplit;
    }

    public long getRecordsToSplit() {
        return recordsToSplit;
    }

    @Override
    public String splitId() {
        return toSplitId(
                "lake-snapshot-",
                new TableBucket(
                        tableBucket.getTableId(),
                        tableBucket.getPartitionId(),
                        lakeSplit.bucket()));
    }

    @Override
    public boolean isLakeSplit() {
        return true;
    }

    @Override
    public byte splitKind() {
        return LAKE_SNAPSHOT_SPLIT_KIND;
    }
}
