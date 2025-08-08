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

package com.alibaba.fluss.flink.lakehouse;

import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.lake.source.LakeSplit;
import com.alibaba.fluss.lake.source.Planner;
import com.alibaba.fluss.lake.source.RecordReader;
import com.alibaba.fluss.predicate.Predicate;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/** Test implementation of {@link com.alibaba.fluss.lake.source.LakeSource} for testing purpose. */
public class TestingLakeSource implements LakeSource<LakeSplit> {
    private Planner<LakeSplit> planner;

    public TestingLakeSource(Planner<LakeSplit> planner) {
        this.planner = planner;
    }

    @Override
    public void withProject(int[][] project) {}

    @Override
    public void withLimit(int limit) {}

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        throw new UnsupportedOperationException("Not impl.");
    }

    @Override
    public Planner<LakeSplit> createPlanner(PlannerContext context) throws IOException {
        return planner;
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<LakeSplit> context) throws IOException {
        throw new UnsupportedOperationException("Not impl.");
    }

    @Override
    public SimpleVersionedSerializer<LakeSplit> getSplitSerializer() {
        throw new UnsupportedOperationException("Not impl.");
    }

    /** Test implementation of {@link LakeSplit} for testing purpose. */
    public static class TestingLakeSplit implements LakeSplit {

        private final int bucketId;
        private final List<String> partitionList;

        public TestingLakeSplit(int bucketId, List<String> partitionList) {
            this.bucketId = bucketId;
            this.partitionList = partitionList;
        }

        @Override
        public int bucket() {
            return bucketId;
        }

        @Override
        public List<String> partition() {
            return partitionList;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestingLakeSplit)) {
                return false;
            }
            TestingLakeSplit that = (TestingLakeSplit) o;
            return Objects.equals(bucketId, that.bucketId)
                    && Objects.equals(partitionList, that.partitionList);
        }

        @Override
        public String toString() {
            return "TestingLakeSplit{"
                    + "bucketId="
                    + bucketId
                    + ", partitionList="
                    + partitionList
                    + '}';
        }
    }
}
