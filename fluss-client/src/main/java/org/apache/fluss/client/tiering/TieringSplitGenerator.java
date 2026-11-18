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

import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.BucketOffsetsRetrieverImpl;
import org.apache.fluss.client.initializer.OffsetsInitializer.BucketOffsetsRetriever;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.metadata.RemoteLogManifestInfo;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_TIERING_MAX_SPLITS_PER_BUCKET;
import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_TIERING_TARGET_SPLIT_SIZE;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/** A generator for lake splits. */
public class TieringSplitGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(TieringSplitGenerator.class);

    private final Admin flussAdmin;

    @Nullable private final FlussConnection connection;
    private final Predicate<TableInfo> supportsParallelLogSplits;

    public TieringSplitGenerator(Admin flussAdmin) {
        this.flussAdmin = flussAdmin;
        this.connection = null;
        this.supportsParallelLogSplits = table -> false;
    }

    /** Enables manifest-based split planning for lake writers that support parallel ranges. */
    public TieringSplitGenerator(
            FlussConnection connection, Predicate<TableInfo> supportsParallelLogSplits) {
        this.flussAdmin = connection.getAdmin();
        this.connection = connection;
        this.supportsParallelLogSplits = supportsParallelLogSplits;
    }

    public List<TieringSplit> generateTableSplits(TableInfo tableInfo) throws Exception {
        List<PartitionInfo> partitionInfos =
                tableInfo.isPartitioned()
                        ? flussAdmin.listPartitionInfos(tableInfo.getTablePath(), true).get()
                        : Collections.emptyList();
        return generateTableSplits(tableInfo, partitionInfos);
    }

    /**
     * Generates splits using the supplied partition metadata, including historical partitions.
     * Callers can reuse this metadata to initialize writers for the same tiering round. For
     * non-partitioned tables, pass an empty list.
     */
    public List<TieringSplit> generateTableSplits(
            TableInfo tableInfo, List<PartitionInfo> partitionInfos) throws Exception {
        TablePath tablePath = tableInfo.getTablePath();
        final BucketOffsetsRetriever bucketOffsetsRetriever =
                new BucketOffsetsRetrieverImpl(flussAdmin, tablePath);

        // Get table lake snapshot info of the given table.
        LakeSnapshot lakeSnapshotInfo;
        try {
            lakeSnapshotInfo = flussAdmin.getLatestLakeSnapshot(tableInfo.getTablePath()).get();
            LOG.info(
                    "Last committed lake table snapshot: snapshotId={}, numBucketOffsets={}",
                    lakeSnapshotInfo.getSnapshotId(),
                    lakeSnapshotInfo.getTableBucketsOffset().size());
            LOG.debug("Last committed lake table snapshot detail: {}", lakeSnapshotInfo);
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (t instanceof LakeTableSnapshotNotExistException) {
                lakeSnapshotInfo = null;
            } else {
                throw new FlussRuntimeException(
                        String.format(
                                "Failed to get table snapshot for table %s",
                                tableInfo.getTablePath()),
                        ExceptionUtils.stripCompletionException(e));
            }
        }
        // partitioned table
        if (tableInfo.isPartitioned()) {
            Map<Long, String> partitionNameById =
                    partitionInfos.stream()
                            .collect(
                                    Collectors.toMap(
                                            PartitionInfo::getPartitionId,
                                            PartitionInfo::getPartitionName));
            Map<Long, Integer> bucketCountById =
                    partitionInfos.stream()
                            .collect(
                                    Collectors.toMap(
                                            PartitionInfo::getPartitionId,
                                            PartitionInfo::getBucketCount));

            return generatePartitionTableSplit(
                    tableInfo,
                    partitionNameById,
                    bucketCountById,
                    bucketOffsetsRetriever,
                    lakeSnapshotInfo);
        } else {
            // non-partitioned table
            return generateNonPartitionedTableSplit(
                    tableInfo, bucketOffsetsRetriever, lakeSnapshotInfo);
        }
    }

    /** Generates all splits for partitioned table. */
    private List<TieringSplit> generatePartitionTableSplit(
            TableInfo tableInfo,
            Map<Long, String> partitionNameById,
            Map<Long, Integer> bucketCountById,
            BucketOffsetsRetriever bucketOffsetsRetriever,
            @Nullable LakeSnapshot lakeSnapshotInfo)
            throws InterruptedException {
        List<TieringSplit> splits = new ArrayList<>();
        for (Map.Entry<Long, String> partitionNameByIdEntry : partitionNameById.entrySet()) {
            long partitionId = partitionNameByIdEntry.getKey();
            String partitionName = partitionNameByIdEntry.getValue();
            int partitionBucketCount = bucketCountById.get(partitionId);
            Map<Integer, Long> latestBucketsOffset =
                    bucketOffsetsRetriever.latestOffsets(
                            partitionName,
                            IntStream.range(0, partitionBucketCount)
                                    .boxed()
                                    .collect(Collectors.toList()));
            splits.addAll(
                    generateTableSplit(
                            tableInfo,
                            partitionId,
                            partitionName,
                            partitionBucketCount,
                            lakeSnapshotInfo,
                            latestBucketsOffset));
        }
        return splits;
    }

    /** Generates all splits for Non-partitioned table. */
    private List<TieringSplit> generateNonPartitionedTableSplit(
            TableInfo tableInfo,
            BucketOffsetsRetriever bucketOffsetsRetriever,
            @Nullable LakeSnapshot lakeSnapshotInfo)
            throws InterruptedException {
        Map<Integer, Long> latestBucketsOffset =
                bucketOffsetsRetriever.latestOffsets(
                        null,
                        IntStream.range(0, tableInfo.getNumBuckets())
                                .boxed()
                                .collect(Collectors.toList()));
        return generateTableSplit(
                tableInfo,
                null,
                null,
                tableInfo.getNumBuckets(),
                lakeSnapshotInfo,
                latestBucketsOffset);
    }

    private List<TieringSplit> generateTableSplit(
            TableInfo tableInfo,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            int numBuckets,
            @Nullable LakeSnapshot lakeSnapshotInfo,
            Map<Integer, Long> latestBucketsOffset)
            throws InterruptedException {
        List<TieringSplit> splits = new ArrayList<>();

        if (tableInfo.hasPrimaryKey()) {
            // it's primary key table
            // Historical partitions have no local KV snapshots and only tier retained WAL.
            boolean historicalPartition = HISTORICAL_PARTITION_VALUE.equals(partitionName);
            KvSnapshots latestKvSnapshots =
                    new KvSnapshots(
                            tableInfo.getTableId(),
                            partitionId,
                            Collections.emptyMap(),
                            Collections.emptyMap());
            boolean kvSnapshotsFetched = false;
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long lastCommittedBucketOffset =
                        lakeSnapshotInfo != null
                                ? lakeSnapshotInfo.getTableBucketsOffset().get(tableBucket)
                                : null;
                long latestBucketOffset = latestBucketsOffset.get(bucket);
                boolean needsKvSnapshot =
                        !historicalPartition
                                && latestBucketOffset > 0
                                && lastCommittedBucketOffset == null;
                if (needsKvSnapshot && !kvSnapshotsFetched) {
                    latestKvSnapshots =
                            fetchLatestKvSnapshots(tableInfo.getTablePath(), partitionName);
                    checkState(latestKvSnapshots != null);
                    kvSnapshotsFetched = true;
                }

                generateSplitForPrimaryKeyTableBucket(
                                tableInfo.getTablePath(),
                                tableBucket,
                                partitionName,
                                latestKvSnapshots,
                                lastCommittedBucketOffset,
                                latestBucketOffset)
                        .ifPresent(splits::add);
            }

        } else {
            // it's log table
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long lastCommittedOffset =
                        lakeSnapshotInfo != null
                                ? lakeSnapshotInfo.getTableBucketsOffset().get(tableBucket)
                                : null;
                long latestBucketOffset = latestBucketsOffset.get(bucket);
                generateSplitForLogTableBucket(
                                tableInfo.getTablePath(),
                                tableBucket,
                                partitionName,
                                lastCommittedOffset,
                                latestBucketOffset)
                        .ifPresent(splits::add);
            }
            int maxSplits =
                    tableInfo.getProperties().get(TABLE_DATALAKE_TIERING_MAX_SPLITS_PER_BUCKET);
            checkArgument(maxSplits > 0, "Tiering max splits per bucket must be positive.");
            if (connection != null
                    && maxSplits > 1
                    && !HISTORICAL_PARTITION_VALUE.equals(partitionName)
                    && splits.stream()
                            .map(TieringSplit::asTieringLogSplit)
                            .anyMatch(
                                    split ->
                                            split.getStartingOffset() >= 0
                                                    && split.getStoppingOffset()
                                                                    - split.getStartingOffset()
                                                            > 1)
                    && supportsParallelLogSplits.test(tableInfo)) {
                long targetBytes =
                        tableInfo
                                .getProperties()
                                .get(TABLE_DATALAKE_TIERING_TARGET_SPLIT_SIZE)
                                .getBytes();
                checkArgument(targetBytes > 0, "Tiering target split size must be positive.");
                return splitArchivedRanges(
                        tableInfo.getTableId(),
                        partitionId,
                        splits,
                        targetBytes,
                        Math.min(64, maxSplits));
            }
        }

        return splits;
    }

    private List<TieringSplit> splitArchivedRanges(
            long tableId,
            @Nullable Long partitionId,
            List<TieringSplit> splits,
            long targetBytes,
            int maxSplits)
            throws InterruptedException {
        Map<TableBucket, RemoteLogManifestInfo> manifests;
        try {
            manifests =
                    flussAdmin.listRemoteLogManifests(tableId, partitionId).get().stream()
                            .collect(
                                    Collectors.toMap(
                                            RemoteLogManifestInfo::getTableBucket,
                                            Function.identity()));
        } catch (ExecutionException e) {
            LOG.warn(
                    "Unable to list remote manifests for table {} partition {}, using whole bucket splits.",
                    tableId,
                    partitionId,
                    e.getCause());
            return splits;
        }
        List<TieringSplit> result = new ArrayList<>();
        for (TieringSplit split : splits) {
            TieringLogSplit logSplit = split.asTieringLogSplit();
            long start = logSplit.getStartingOffset();
            long end = logSplit.getStoppingOffset();
            RemoteLogManifestInfo info = manifests.get(split.getTableBucket());
            if (info == null || start < 0 || info.getRemoteLogEndOffset() <= start) {
                result.add(split);
                continue;
            }
            long archivedEnd = Math.min(end, info.getRemoteLogEndOffset());
            int archivedSlots = maxSplits - (archivedEnd < end ? 1 : 0);
            List<Long> cuts = new ArrayList<>();
            if (archivedSlots > 1 && archivedEnd - start > 1) {
                try {
                    RemoteLogManifest manifest = readManifest(info);
                    if (manifest.getTableBucket().equals(split.getTableBucket())) {
                        cuts.addAll(
                                segmentCuts(
                                        manifest, start, archivedEnd, targetBytes, archivedSlots));
                    }
                } catch (IOException | RuntimeException e) {
                    // The manifest may have been replaced since the coordinator returned its path.
                    LOG.warn(
                            "Unable to read remote manifest for {}, keeping the archived range together.",
                            split.getTableBucket(),
                            e);
                }
            }
            if (archivedEnd < end) {
                cuts.add(archivedEnd);
            }
            if (cuts.isEmpty()) {
                result.add(split);
                continue;
            }
            cuts.add(end);
            long from = start;
            for (long to : cuts) {
                result.add(
                        TieringLogSplit.range(
                                split.getTablePath(),
                                split.getTableBucket(),
                                split.getPartitionName(),
                                from,
                                to,
                                start));
                from = to;
            }
        }
        return result;
    }

    private RemoteLogManifest readManifest(RemoteLogManifestInfo info) throws IOException {
        // Initialize the same filesystem credentials used by remote WAL readers.
        connection.getOrCreateRemoteFileDownloader();
        FsPath path = new FsPath(info.getRemoteLogManifestPath());
        try (FSDataInputStream input = path.getFileSystem().open(path)) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            IOUtils.copyBytes(input, bytes, false);
            return RemoteLogManifest.fromJsonBytes(bytes.toByteArray());
        }
    }

    /**
     * Groups the manifest's readable ranges, accounting for partial physical segments by byte
     * share.
     */
    static List<Long> segmentCuts(
            RemoteLogManifest manifest, long start, long end, long targetBytes, int maxGroups) {
        List<RemoteLogSegment> segments =
                manifest.normalizeLogicalRanges().getRemoteLogSegmentList();
        int first = 0;
        while (first < segments.size() && segments.get(first).logicalEndOffset() <= start) {
            first++;
        }
        int last = first;
        while (last < segments.size() && segments.get(last).logicalStartOffset() < end) {
            last++;
        }
        int count = last - first;
        if (count < 2) {
            return Collections.emptyList();
        }
        double[] cumulativeBytes = new double[count];
        long covered = start;
        double bytes = 0;
        for (int i = 0; i < count; i++) {
            RemoteLogSegment segment = segments.get(first + i);
            if (segment.logicalStartOffset() > covered || segment.segmentSizeInBytes() < 0) {
                return Collections.emptyList();
            }
            long next = Math.min(end, segment.logicalEndOffset());
            bytes +=
                    segment.segmentSizeInBytes()
                            * ((double) (next - covered)
                                    / (segment.remoteLogEndOffset()
                                            - segment.remoteLogStartOffset()));
            cumulativeBytes[i] = bytes;
            covered = next;
        }
        if (covered != end) {
            return Collections.emptyList();
        }
        int groups = Math.min(Math.min(count, maxGroups), (int) Math.ceil(bytes / targetBytes));
        List<Long> cuts = new ArrayList<>();
        int boundary = 0;
        for (int group = 1; group < groups; group++) {
            double goal = bytes * group / groups;
            int lastBoundary = count - (groups - group) - 1;
            while (boundary < lastBoundary
                    && Math.abs(cumulativeBytes[boundary + 1] - goal)
                            < Math.abs(cumulativeBytes[boundary] - goal)) {
                boundary++;
            }
            cuts.add(segments.get(first + boundary++).logicalEndOffset());
        }
        return cuts;
    }

    private KvSnapshots fetchLatestKvSnapshots(
            TablePath tablePath, @Nullable String partitionName) {
        try {
            return (partitionName == null
                            ? flussAdmin.getLatestKvSnapshots(tablePath)
                            : flussAdmin.getLatestKvSnapshots(tablePath, partitionName))
                    .get();
        } catch (Exception e) {
            String errorMessage =
                    partitionName == null
                            ? String.format(
                                    "Failed to get KV snapshot metadata for table %s", tablePath)
                            : String.format(
                                    "Failed to get KV snapshot metadata for table %s and partition %s",
                                    tablePath, partitionName);
            throw new FlussRuntimeException(
                    errorMessage, ExceptionUtils.stripCompletionException(e));
        }
    }

    private Optional<TieringSplit> generateSplitForPrimaryKeyTableBucket(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            KvSnapshots latestKvSnapshots,
            @Nullable Long lastCommittedBucketOffset,
            long latestBucketOffset) {
        if (latestBucketOffset <= 0) {
            LOG.debug(
                    "The latestBucketOffset {} is equals or less than 0, skip generating split for bucket {}",
                    latestBucketOffset,
                    tableBucket);
            return Optional.empty();
        }

        // the bucket is never been tiered, read kv snapshot is more efficient
        if (lastCommittedBucketOffset == null) {
            int bucket = tableBucket.getBucket();
            if (!latestKvSnapshots.getSnapshotId(bucket).isPresent()) {
                // bucket with non snapshot, scan log from earliest to latest offset
                return Optional.of(
                        new TieringLogSplit(
                                tablePath,
                                tableBucket,
                                partitionName,
                                EARLIEST_OFFSET,
                                latestBucketOffset,
                                0));
            } else {
                // bucket with snapshot, read kv to latest snapshotId + latestOffsetOfSnapshot
                checkState(latestKvSnapshots.getLogOffset(bucket).isPresent());
                return Optional.of(
                        new TieringSnapshotSplit(
                                tablePath,
                                tableBucket,
                                partitionName,
                                latestKvSnapshots.getSnapshotId(bucket).getAsLong(),
                                latestKvSnapshots.getLogOffset(bucket).getAsLong(),
                                0));
            }
        } else {
            // the bucket has been tiered, read bounded log
            if (lastCommittedBucketOffset < latestBucketOffset) {
                return Optional.of(
                        new TieringLogSplit(
                                tablePath,
                                tableBucket,
                                partitionName,
                                lastCommittedBucketOffset,
                                latestBucketOffset,
                                0));
            } else {
                LOG.debug(
                        "The lastCommittedBucketOffset {} is equals or bigger than latestBucketOffset {}, skip generating split for bucket {}",
                        lastCommittedBucketOffset,
                        latestBucketOffset,
                        tableBucket);
                return Optional.empty();
            }
        }
    }

    private Optional<TieringSplit> generateSplitForLogTableBucket(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable Long lastCommittedBucketOffset,
            long latestBucketOffset) {
        if (latestBucketOffset <= 0) {
            LOG.debug(
                    "The latestBucketOffset {} is equals or less than 0, skip generating split for bucket {}",
                    latestBucketOffset,
                    tableBucket);
            return Optional.empty();
        }

        // the bucket is never been tiered
        if (lastCommittedBucketOffset == null) {
            // the bucket is never been tiered, scan fluss log from the earliest offset
            return Optional.of(
                    new TieringLogSplit(
                            tablePath,
                            tableBucket,
                            partitionName,
                            EARLIEST_OFFSET,
                            latestBucketOffset));
        } else {
            // the bucket has been tiered, scan remain fluss log
            if (lastCommittedBucketOffset < latestBucketOffset) {
                return Optional.of(
                        new TieringLogSplit(
                                tablePath,
                                tableBucket,
                                partitionName,
                                lastCommittedBucketOffset,
                                latestBucketOffset));
            }
        }
        LOG.debug(
                "The lastCommittedBucketOffset {} is equals or bigger than latestBucketOffset {}, skip generating split for bucket {}",
                lastCommittedBucketOffset,
                latestBucketOffset,
                tableBucket);
        return Optional.empty();
    }
}
