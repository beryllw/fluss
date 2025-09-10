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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.conf.IcebergConfiguration;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.transforms.TransformUtils;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.iceberg.IcebergLakeCatalog.ICEBERG_CATALOG_DEFAULT_NAME;
import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;

/** Iceberg split planner. */
public class IcebergSplitPlanner implements Planner<IcebergSplit> {

    private final Configuration icebergConfig;
    private final TablePath tablePath;
    private final long snapshotId;

    public IcebergSplitPlanner(Configuration icebergConfig, TablePath tablePath, long snapshotId) {
        this.icebergConfig = icebergConfig;
        this.tablePath = tablePath;
        this.snapshotId = snapshotId;
    }

    @Override
    public List<IcebergSplit> plan() throws IOException {
        List<IcebergSplit> splits = new ArrayList<>();
        Catalog catalog = createIcebergCatalog(icebergConfig);
        Table table = catalog.loadTable(toIceberg(tablePath));
        Function<FileScanTask, Integer> bucketExtract = createBucketExtractor(table);
        Function<FileScanTask, List<String>> partitionExtract = createPartitionExtractor(table);
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan()
                        .useSnapshot(snapshotId)
                        .includeColumnStats()
                        .ignoreResiduals()
                        .planFiles()) {
            tasks.forEach(
                    task ->
                            splits.add(
                                    new IcebergSplit(
                                            task,
                                            bucketExtract.apply(task),
                                            partitionExtract.apply(task))));
        }
        return splits;
    }

    private Function<FileScanTask, Integer> createBucketExtractor(Table table) {
        PartitionSpec partitionSpec = table.spec();
        List<PartitionField> partitionFields = partitionSpec.fields();

        List<PartitionField> bucketFields =
                partitionFields.stream()
                        .filter(field -> TransformUtils.isBucketTransform(field.transform()))
                        .collect(Collectors.toList());

        if (bucketFields.size() != 1) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only one bucket key is supported for Iceberg at the moment, but found %d",
                            bucketFields.size()));
        }

        PartitionField bucketField = bucketFields.get(0);
        Types.StructType partitionType = partitionSpec.partitionType();
        int bucketFieldIndex =
                partitionType.fields().indexOf(partitionType.field(bucketField.fieldId()));

        return task -> task.file().partition().get(bucketFieldIndex, Integer.class);
    }

    private Function<FileScanTask, List<String>> createPartitionExtractor(Table table) {
        PartitionSpec partitionSpec = table.spec();
        List<PartitionField> partitionFields = partitionSpec.fields();
        Types.StructType partitionType = partitionSpec.partitionType();

        List<Integer> nonBucketFieldIndices =
                partitionFields.stream()
                        .filter(field -> !TransformUtils.isBucketTransform(field.transform()))
                        .map(
                                field ->
                                        partitionType
                                                .fields()
                                                .indexOf(partitionType.field(field.fieldId())))
                        .collect(Collectors.toList());

        return task ->
                nonBucketFieldIndices.stream()
                        .map(index -> task.partition().get(index, String.class))
                        .collect(Collectors.toList());
    }

    private Catalog createIcebergCatalog(Configuration configuration) {
        Map<String, String> icebergProps = configuration.toMap();
        String catalogName = icebergProps.getOrDefault("name", ICEBERG_CATALOG_DEFAULT_NAME);
        return buildIcebergCatalog(
                catalogName, icebergProps, IcebergConfiguration.from(configuration).get());
    }
}
