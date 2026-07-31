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

package org.apache.fluss.lake.committer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.PartitionMarkDoneStateJsonSerde;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The partition mark-done state, persisted as the {@link TieringStateEntry} keyed by {@link
 * #STATE_KEY} in the lake offsets file. Each tracked partition records its last update time and
 * done time ({@link #NOT_DONE} until first marked done); a partition absent from the map has never
 * been tracked. Entries of dropped partitions are removed by omitting them from the next payload.
 *
 * <p>The payload schema version lives in the entry envelope. Evolve the payload by bumping {@link
 * #CURRENT_VERSION}; {@link #fromStateEntry} rejects a higher version, and the caller must pass the
 * entry through unchanged so a newer build's state is never dropped.
 *
 * @since 0.9
 */
@PublicEvolving
public class PartitionMarkDoneState {

    /** The tiering-state key of the partition mark-done state. */
    public static final String STATE_KEY = "fluss.partition-mark-done";

    public static final int CURRENT_VERSION = 1;

    /** Sentinel done time of a partition that has not been marked done yet. */
    public static final long NOT_DONE = -1L;

    private final Map<Long, PartitionState> partitionStates;

    public PartitionMarkDoneState(Map<Long, PartitionState> partitionStates) {
        this.partitionStates =
                partitionStates == null ? Collections.emptyMap() : new HashMap<>(partitionStates);
    }

    public Map<Long, PartitionState> getPartitionStates() {
        return Collections.unmodifiableMap(partitionStates);
    }

    /** Wraps this state into its {@link TieringStateEntry} at {@link #CURRENT_VERSION}. */
    public TieringStateEntry toStateEntry() {
        return new TieringStateEntry(
                STATE_KEY,
                CURRENT_VERSION,
                JsonSerdeUtils.writeValueAsBytes(this, PartitionMarkDoneStateJsonSerde.INSTANCE));
    }

    /**
     * Parses the state from its {@link TieringStateEntry}.
     *
     * @throws IllegalArgumentException if the entry has a different key, an unsupported (newer)
     *     version, or a corrupt payload
     */
    public static PartitionMarkDoneState fromStateEntry(TieringStateEntry entry) {
        checkArgument(
                STATE_KEY.equals(entry.getStateKey()),
                "Expected state key %s but got %s.",
                STATE_KEY,
                entry.getStateKey());
        if (entry.getStateVersion() > CURRENT_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported partition mark-done state version "
                            + entry.getStateVersion()
                            + " > "
                            + CURRENT_VERSION
                            + "; pass the entry through unchanged.");
        }
        return JsonSerdeUtils.readValue(
                entry.getPayload(), PartitionMarkDoneStateJsonSerde.INSTANCE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionMarkDoneState that = (PartitionMarkDoneState) o;
        return Objects.equals(partitionStates, that.partitionStates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionStates);
    }

    @Override
    public String toString() {
        return "PartitionMarkDoneState{" + "partitionStates=" + partitionStates + '}';
    }

    /** The mark-done state of a single partition. */
    public static class PartitionState {

        private final long updateTime;
        private final long doneTime;

        public PartitionState(long updateTime, long doneTime) {
            checkArgument(
                    updateTime >= 0, "updateTime must be non-negative but got %s.", updateTime);
            checkArgument(
                    doneTime == NOT_DONE || doneTime > 0,
                    "doneTime must be %s (not done) or positive but got %s.",
                    NOT_DONE,
                    doneTime);
            this.updateTime = updateTime;
            this.doneTime = doneTime;
        }

        public long getUpdateTime() {
            return updateTime;
        }

        public long getDoneTime() {
            return doneTime;
        }

        public boolean isDone() {
            return doneTime != NOT_DONE;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionState that = (PartitionState) o;
            return updateTime == that.updateTime && doneTime == that.doneTime;
        }

        @Override
        public int hashCode() {
            return Objects.hash(updateTime, doneTime);
        }

        @Override
        public String toString() {
            return "PartitionState{" + "updateTime=" + updateTime + ", doneTime=" + doneTime + '}';
        }
    }
}
