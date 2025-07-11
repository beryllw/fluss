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

package com.alibaba.fluss.lake.source1;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.predicate.Predicate;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * A generic interface for lake data sources that defines how to plan splits and read data. Any data
 * lake format supporting reading from data tiered in lake as Fluss records should implement this
 * interface.
 *
 * <p>This interface provides methods for projection, filtering, limiting to enable query engine to
 * push to lake source. Implementations must ensure that split planning and record reading
 * operations properly account for these pushed-down operations during execution.
 *
 * @param <Split> The type of data split, which must extend {@link LakeSplit}
 */
public interface LakeSource<Split extends LakeSplit> extends Serializable {

    /**
     * Applies column projection to the data source. it provides the field index paths that should
     * be used for a projection. The indices are 0-based and support fields within (possibly nested)
     * structures.
     *
     * <p>For nested, given the the following SQL, CREATE TABLE t (i INT, r ROW < d DOUBLE, b
     * BOOLEAN>, s STRING); SELECT s, r.d FROM t; the project will be [[2], [1, 0]]
     */
    void withProject(int[][] project);

    /** Applies a row limit to the data source. */
    void witLimit(int limit);

    /**
     * Pushes down filters to the data source for potential optimization.
     *
     * @param predicates The list of predicates to be pushed down
     * @return A {@link FilterPushDownResult} containing the predicates that were accepted by the
     *     source and those that remain to be evaluated
     */
    FilterPushDownResult withFilters(List<Predicate> predicates);

    /**
     * Creates a planner for plan splits to be read.
     *
     * @param context The planning context providing necessary planning information
     * @return A planner instance for this data source
     * @throws IOException if an error occurs during planner creation
     */
    Planner<Split> createPlanner(PlannerContext context) throws IOException;

    /**
     * Creates a record reader for reading data of data lake for the specified split .
     *
     * @param context The reader context containing the split to be read
     * @return A record reader instance for the given split
     * @throws IOException if an error occurs during reader creation
     */
    RecordReader createRecordReader(ReaderContext<Split> context) throws IOException;

    /**
     * Returns the serializer for the data split, used to transfer split information in distributed
     * environment.
     *
     * @return The serializer for the split
     */
    SimpleVersionedSerializer<Split> getSplitSerializer();

    /**
     * Context interface for planners, providing the snapshot id of the table in data-lake to plan
     * splits.
     */
    interface PlannerContext extends Serializable {
        long snapshotId();
    }

    /**
     * Context interface for record readers, providing access to the lake split being read.
     *
     * @param <Split> The type of lake split
     */
    interface ReaderContext<Split extends LakeSplit> extends Serializable {
        Split lakeSplit();
    }

    /**
     * Represents the result of a filter push down operation to lake source, indicating which
     * predicates were accepted by the source and which remain to be evaluated.
     */
    @PublicEvolving
    final class FilterPushDownResult {
        private final List<Predicate> acceptedPredicates;
        private final List<Predicate> remainingPredicates;

        private FilterPushDownResult(
                List<Predicate> acceptedPredicates, List<Predicate> remainingPredicates) {
            this.acceptedPredicates = acceptedPredicates;
            this.remainingPredicates = remainingPredicates;
        }

        /**
         * Creates a new FilterPushDownResult instance.
         *
         * @param acceptedPredicates The accepted predicates
         * @param remainingPredicates The remaining predicates
         * @return A new FilterPushDownResult instance
         */
        public static FilterPushDownResult of(
                List<Predicate> acceptedPredicates, List<Predicate> remainingPredicates) {
            return new FilterPushDownResult(acceptedPredicates, remainingPredicates);
        }

        /**
         * Returns the predicates that were accepted by the source.
         *
         * @return The list of accepted predicates
         */
        public List<Predicate> acceptedPredicates() {
            return acceptedPredicates;
        }

        /**
         * Returns the predicates that remain to be evaluated.
         *
         * @return The list of remaining predicates
         */
        public List<Predicate> remainingPredicates() {
            return remainingPredicates;
        }
    }
}
