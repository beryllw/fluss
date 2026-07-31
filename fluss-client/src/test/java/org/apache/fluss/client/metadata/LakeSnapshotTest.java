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

import org.apache.fluss.lake.committer.PartitionMarkDoneState;
import org.apache.fluss.lake.committer.PartitionMarkDoneState.PartitionState;
import org.apache.fluss.lake.committer.TieringStateEntry;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LakeSnapshot}, in particular the keyed tiering-state accessors. */
class LakeSnapshotTest {

    @Test
    void testKeyedStateAccess() {
        // absent -> null (raw and typed).
        LakeSnapshot absent = new LakeSnapshot(1L, Collections.emptyMap());
        assertThat(absent.getTieringState(PartitionMarkDoneState.STATE_KEY)).isNull();
        assertThat(absent.getPartitionMarkDoneState()).isNull();

        // present -> raw entry exposed, typed accessor parses lazily; unrelated keys untouched.
        PartitionMarkDoneState state =
                new PartitionMarkDoneState(
                        Collections.singletonMap(
                                5L, new PartitionState(1000L, PartitionMarkDoneState.NOT_DONE)));
        TieringStateEntry otherEntry =
                new TieringStateEntry("other-key", 3, "{}".getBytes(StandardCharsets.UTF_8));
        LakeSnapshot present =
                new LakeSnapshot(
                        1L,
                        Collections.emptyMap(),
                        Arrays.asList(otherEntry, state.toStateEntry()));
        assertThat(present.getPartitionMarkDoneState()).isEqualTo(state);
        assertThat(present.getTieringState("other-key")).isEqualTo(otherEntry);
        assertThat(present.getTieringState("unknown-key")).isNull();
    }

    @Test
    void testNewerVersionExposesRawEntryForPassthrough() {
        TieringStateEntry newerEntry =
                new TieringStateEntry(
                        PartitionMarkDoneState.STATE_KEY,
                        PartitionMarkDoneState.CURRENT_VERSION + 1,
                        "{}".getBytes(StandardCharsets.UTF_8));
        LakeSnapshot snapshot =
                new LakeSnapshot(1L, Collections.emptyMap(), Collections.singletonList(newerEntry));
        // unreadable here: parsing fails so the caller passes the raw entry through unchanged.
        assertThatThrownBy(snapshot::getPartitionMarkDoneState)
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(snapshot.getTieringState(PartitionMarkDoneState.STATE_KEY))
                .isEqualTo(newerEntry);
    }

    @Test
    void testCorruptStateDoesNotBlockBucketOffsets() {
        TableBucket bucket = new TableBucket(1L, 0);
        Map<TableBucket, Long> offsets = Collections.singletonMap(bucket, 100L);
        TieringStateEntry corruptEntry =
                new TieringStateEntry(
                        PartitionMarkDoneState.STATE_KEY,
                        1,
                        "{not-json".getBytes(StandardCharsets.UTF_8));
        LakeSnapshot snapshot =
                new LakeSnapshot(1L, offsets, Collections.singletonList(corruptEntry));

        // bucket offsets remain accessible even though the state payload is corrupt.
        assertThat(snapshot.getTableBucketsOffset()).containsEntry(bucket, 100L);
        // only the typed accessor surfaces the parse failure.
        assertThatThrownBy(snapshot::getPartitionMarkDoneState)
                .isInstanceOf(RuntimeException.class);
    }
}
