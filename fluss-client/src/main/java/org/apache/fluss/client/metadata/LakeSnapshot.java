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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.lake.committer.PartitionMarkDoneState;
import org.apache.fluss.lake.committer.TieringStateEntry;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A class representing the lake snapshot information of a table. It contains:
 * <li>The snapshot id and the log offset for each bucket.
 * <li>The keyed tiering-state entries, exposed raw via {@link #getTieringState(String)} or parsed
 *     via typed accessors like {@link #getPartitionMarkDoneState()}. Empty when talking to an old
 *     coordinator that does not report them.
 *
 * @since 0.3
 */
@PublicEvolving
public class LakeSnapshot {

    private final long snapshotId;

    // the specific log offset of the snapshot
    private final Map<TableBucket, Long> tableBucketsOffset;

    // the keyed tiering-state entries; payloads parsed lazily by the typed accessors.
    private final List<TieringStateEntry> tieringStates;

    public LakeSnapshot(long snapshotId, Map<TableBucket, Long> tableBucketsOffset) {
        this(snapshotId, tableBucketsOffset, Collections.emptyList());
    }

    public LakeSnapshot(
            long snapshotId,
            Map<TableBucket, Long> tableBucketsOffset,
            List<TieringStateEntry> tieringStates) {
        this.snapshotId = snapshotId;
        this.tableBucketsOffset = tableBucketsOffset;
        this.tieringStates = tieringStates;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public Map<TableBucket, Long> getTableBucketsOffset() {
        return Collections.unmodifiableMap(tableBucketsOffset);
    }

    /** Returns the raw tiering-state entry for the given key, or {@code null} if absent. */
    @Nullable
    public TieringStateEntry getTieringState(String stateKey) {
        for (TieringStateEntry entry : tieringStates) {
            if (entry.getStateKey().equals(stateKey)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * Parses and returns the partition mark-done state, or {@code null} if absent.
     *
     * @throws IllegalArgumentException if the entry has an unsupported (newer) version or a corrupt
     *     payload; the raw entry stays available via {@link #getTieringState(String)}
     */
    @Nullable
    public PartitionMarkDoneState getPartitionMarkDoneState() {
        TieringStateEntry entry = getTieringState(PartitionMarkDoneState.STATE_KEY);
        return entry == null ? null : PartitionMarkDoneState.fromStateEntry(entry);
    }

    @Override
    public String toString() {
        return "LakeSnapshot{"
                + "snapshotId="
                + snapshotId
                + ", tableBucketsOffset="
                + tableBucketsOffset
                + ", tieringStates="
                + tieringStates
                + '}';
    }
}
