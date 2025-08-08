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

package com.alibaba.fluss.flink.lakehouse;

import com.alibaba.fluss.flink.lakehouse.paimon.split.PaimonSnapshotAndFlussLogSplit;
import com.alibaba.fluss.flink.lakehouse.paimon.split.PaimonSnapshotAndFlussLogSplitState;
import com.alibaba.fluss.flink.lakehouse.paimon.split.PaimonSnapshotSplit;
import com.alibaba.fluss.flink.lakehouse.paimon.split.PaimonSnapshotSplitState;
import com.alibaba.fluss.flink.lakehouse.split.LakeSnapshotAndFlussLogSplit;
import com.alibaba.fluss.flink.lakehouse.split.LakeSnapshotAndFlussLogSplitState;
import com.alibaba.fluss.flink.lakehouse.split.LakeSnapshotSplit;
import com.alibaba.fluss.flink.lakehouse.split.LakeSnapshotSplitState;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.flink.source.split.SourceSplitState;

/** The state initializer for lake split. */
public class LakeSplitStateInitializer {

    public static SourceSplitState initializedState(SourceSplitBase split) {
        if (split instanceof PaimonSnapshotSplit) {
            return new PaimonSnapshotSplitState((PaimonSnapshotSplit) split);
        } else if (split instanceof PaimonSnapshotAndFlussLogSplit) {
            return new PaimonSnapshotAndFlussLogSplitState((PaimonSnapshotAndFlussLogSplit) split);
        } else if (split instanceof LakeSnapshotSplit) {
            return new LakeSnapshotSplitState((LakeSnapshotSplit) split);
        } else if (split instanceof LakeSnapshotAndFlussLogSplit) {
            return new LakeSnapshotAndFlussLogSplitState((LakeSnapshotAndFlussLogSplit) split);
        } else {
            throw new UnsupportedOperationException("Unsupported split type: " + split);
        }
    }
}
