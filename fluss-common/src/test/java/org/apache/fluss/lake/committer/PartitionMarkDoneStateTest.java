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

import org.apache.fluss.lake.committer.PartitionMarkDoneState.PartitionState;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.lake.committer.PartitionMarkDoneState.NOT_DONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PartitionMarkDoneState} and its {@link TieringStateEntry} conversion. */
class PartitionMarkDoneStateTest {

    @Test
    void testStateEntryRoundTrip() {
        Map<Long, PartitionState> partitionStates = new HashMap<>();
        partitionStates.put(5L, new PartitionState(1704153550000L, NOT_DONE));
        partitionStates.put(7L, new PartitionState(1704153560000L, 1704153570000L));
        PartitionMarkDoneState state = new PartitionMarkDoneState(partitionStates);

        TieringStateEntry entry = state.toStateEntry();
        assertThat(entry.getStateKey()).isEqualTo(PartitionMarkDoneState.STATE_KEY);
        assertThat(entry.getStateVersion()).isEqualTo(PartitionMarkDoneState.CURRENT_VERSION);
        assertThat(PartitionMarkDoneState.fromStateEntry(entry)).isEqualTo(state);

        // empty state round-trips too (null map normalizes to empty).
        PartitionMarkDoneState empty = new PartitionMarkDoneState(null);
        assertThat(PartitionMarkDoneState.fromStateEntry(empty.toStateEntry())).isEqualTo(empty);
    }

    @Test
    void testPartitionStateValidation() {
        assertThat(new PartitionState(1000L, NOT_DONE).isDone()).isFalse();
        assertThat(new PartitionState(1000L, 2000L).isDone()).isTrue();
        assertThatThrownBy(() -> new PartitionState(-5L, NOT_DONE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("updateTime");
        // doneTime 0 and negatives other than the NOT_DONE sentinel are invalid.
        assertThatThrownBy(() -> new PartitionState(1000L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("doneTime");
        assertThatThrownBy(() -> new PartitionState(1000L, -2L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("doneTime");
    }

    @Test
    void testFromStateEntryRejectsWrongKey() {
        TieringStateEntry entry =
                new TieringStateEntry("other-key", 1, "{}".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> PartitionMarkDoneState.fromStateEntry(entry))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected state key");
    }

    @Test
    void testFromStateEntryRejectsNewerVersion() {
        // a newer build's payload cannot be interpreted; the entry must be passed through.
        TieringStateEntry entry =
                new TieringStateEntry(
                        PartitionMarkDoneState.STATE_KEY,
                        PartitionMarkDoneState.CURRENT_VERSION + 1,
                        "{}".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> PartitionMarkDoneState.fromStateEntry(entry))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pass the entry through unchanged");
    }

    @Test
    void testFromStateEntryRejectsCorruptPayload() {
        assertThatThrownBy(() -> PartitionMarkDoneState.fromStateEntry(markDoneEntry("[1,2]")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expected a JSON object");
        assertThatThrownBy(
                        () ->
                                PartitionMarkDoneState.fromStateEntry(
                                        markDoneEntry("{\"partitions\":{\"abc\":{}}}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid partitionId");
        // record missing done_time
        assertThatThrownBy(
                        () ->
                                PartitionMarkDoneState.fromStateEntry(
                                        markDoneEntry(
                                                "{\"partitions\":{\"5\":{\"update_time\":1}}}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("done_time");
        // invalid done_time value, rejected by the PartitionState constructor with context
        assertThatThrownBy(
                        () ->
                                PartitionMarkDoneState.fromStateEntry(
                                        markDoneEntry(
                                                "{\"partitions\":{\"5\":{\"update_time\":1,\"done_time\":0}}}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition 5");
    }

    @Test
    void testPayloadTolerantToUnknownFields() {
        // unknown fields (added by a compatible newer build) are ignored at both levels.
        PartitionMarkDoneState state =
                PartitionMarkDoneState.fromStateEntry(
                        markDoneEntry(
                                "{\"partitions\":{\"5\":{\"update_time\":1000,\"done_time\":-1,"
                                        + "\"max_timestamp\":9}},\"future_field\":123}"));
        assertThat(state.getPartitionStates())
                .containsOnlyKeys(5L)
                .containsEntry(5L, new PartitionState(1000L, NOT_DONE));
    }

    @Test
    void testEntryEnvelopeValidation() {
        byte[] payload = "{}".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> new TieringStateEntry("", 1, payload))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TieringStateEntry("key", 0, payload))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TieringStateEntry("key", 1, null))
                .isInstanceOf(NullPointerException.class);
    }

    private static TieringStateEntry markDoneEntry(String payloadJson) {
        return new TieringStateEntry(
                PartitionMarkDoneState.STATE_KEY,
                PartitionMarkDoneState.CURRENT_VERSION,
                payloadJson.getBytes(StandardCharsets.UTF_8));
    }
}
