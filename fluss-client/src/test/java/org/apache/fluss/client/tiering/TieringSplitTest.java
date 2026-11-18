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

package org.apache.fluss.client.tiering;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.utils.InstantiationUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests Java serialization and copying of engine-agnostic tiering splits. */
class TieringSplitTest {

    @Test
    void testArchivedSegmentCutsWithinThePendingRange() {
        RemoteLogManifest manifest =
                manifest(
                        segment(0, 10, 64),
                        segment(10, 20, 64),
                        segment(20, 30, 64),
                        segment(30, 40, 64),
                        segment(40, 50, 64),
                        segment(50, 60, 64));
        assertThat(TieringSplitGenerator.segmentCuts(manifest, 0, 60, 128, 4))
                .containsExactly(20L, 40L);
        assertThat(TieringSplitGenerator.segmentCuts(manifest, 5, 55, 128, 2)).containsExactly(30L);
        assertThat(TieringSplitGenerator.segmentCuts(manifest, 21, 29, 1, 4)).isEmpty();
        assertThat(TieringSplitGenerator.segmentCuts(manifest, 0, 60, 1, 1)).isEmpty();
    }

    @Test
    void testOverlappingAndIncompleteManifests() {
        // The second physical segment replaces [20, 30) in the first one.
        RemoteLogManifest overlap = manifest(segment(0, 30, 90), segment(20, 40, 60));
        assertThat(TieringSplitGenerator.segmentCuts(overlap, 0, 40, 100, 4)).containsExactly(20L);
        assertThat(TieringSplitGenerator.segmentCuts(overlap, 0, 40, 128, 4)).isEmpty();
        RemoteLogManifest gap = manifest(segment(0, 10, 64), segment(20, 30, 64));
        assertThat(TieringSplitGenerator.segmentCuts(gap, 0, 30, 1, 4)).isEmpty();
    }

    private static RemoteLogManifest manifest(RemoteLogSegment... segments) {
        return new RemoteLogManifest(
                segments[0].physicalTablePath(),
                segments[0].tableBucket(),
                Arrays.asList(segments));
    }

    private static RemoteLogSegment segment(long start, long end, int bytes) {
        return RemoteLogSegment.Builder.builder()
                .physicalTablePath(PhysicalTablePath.of(TablePath.of("db", "table"), null))
                .tableBucket(new TableBucket(1L, 0))
                .remoteLogSegmentId(UUID.randomUUID())
                .remoteLogStartOffset(start)
                .remoteLogEndOffset(end)
                .segmentSizeInBytes(bytes)
                .build();
    }

    @Test
    void testRangeCommitStopsAtFirstGap() {
        TablePath path = TablePath.of("db", "log");
        TableBucket bucket = new TableBucket(7, 0);
        TieringLogSplit first = TieringLogSplit.range(path, bucket, null, 100, 200, 100);
        TieringLogSplit second = TieringLogSplit.range(path, bucket, null, 200, 300, 100);
        TableBucketWriteResult<String> partial =
                new TableBucketWriteResult<>(path, bucket, null, "a", 175, 1, 2, 1, first);
        TableBucketWriteResult<String> complete =
                new TableBucketWriteResult<>(path, bucket, null, "b", 300, 2, 2, 1, second);
        List<String> aborted = new ArrayList<>();
        assertThat(
                        TieringCommitter.selectContiguousRanges(
                                Arrays.asList(complete, partial), aborted))
                .containsExactly(partial);
        assertThat(aborted).containsExactly("b");
        aborted.clear();
        TableBucketWriteResult<String> emptyWal =
                new TableBucketWriteResult<>(path, bucket, null, null, 200, -1, 2, 1, first);
        assertThat(
                        TieringCommitter.selectContiguousRanges(
                                Arrays.asList(complete, emptyWal), aborted))
                .containsExactly(emptyWal, complete);
        assertThat(aborted).isEmpty();
        TableBucketWriteResult<String> skipped =
                new TableBucketWriteResult<>(path, bucket, null, null, -1, -1, 2, 1, first);
        assertThat(
                        TieringCommitter.selectContiguousRanges(
                                Arrays.asList(skipped, complete), aborted))
                .isEmpty();
        assertThat(aborted).containsExactly("b");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testJavaSerializationAndCopyPreserveMetadata(boolean snapshotSplit) throws Exception {
        TablePath tablePath = TablePath.of("db", "table");
        TableBucket tableBucket = new TableBucket(1L, 2L, 3);
        TieringSplit split =
                snapshotSplit
                        ? new TieringSnapshotSplit(
                                tablePath, tableBucket, "p1", 4L, 5L, 6, 1, 1000L)
                        : new TieringLogSplit(tablePath, tableBucket, "p1", 4L, 5L, 6, 1, 1000L);
        split.skipCurrentRound();

        TieringSplit restored = InstantiationUtils.clone(split);
        assertThat(restored).isEqualTo(split).isNotSameAs(split);
        assertThat(restored.splitId()).isEqualTo(split.splitId());
        assertThat(restored.shouldSkipCurrentRound()).isTrue();
        assertThat(restored.getSplitIndex()).isEqualTo(1);
        assertThat(restored.getTieringRoundTimestamp()).isEqualTo(1000L);

        TieringSplit copy = restored.copy(restored.getNumberOfSplits());
        assertThat(copy).isEqualTo(restored).isNotSameAs(restored);
        TieringSplit copyWithMetadata = restored.copy(10, 2, 2000L);
        assertThat(copyWithMetadata.getNumberOfSplits()).isEqualTo(10);
        assertThat(copyWithMetadata.getSplitIndex()).isEqualTo(2);
        assertThat(copyWithMetadata.getTieringRoundTimestamp()).isEqualTo(2000L);
        assertThat(copyWithMetadata.shouldSkipCurrentRound()).isTrue();
        assertThat(copyWithMetadata.copy(6, 1, 1000L)).isEqualTo(restored);
    }
}
