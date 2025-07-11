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

package com.alibaba.fluss.predicate;

import java.io.Serializable;
import java.util.List;

/**
 * Non-leaf node in a {@link Predicate} tree. Its evaluation result depends on the results of its
 * children.
 */
public class CompoundPredicate implements Predicate {

    private final Function function;
    private final List<Predicate> children;

    public CompoundPredicate(Function function, List<Predicate> children) {
        this.function = function;
        this.children = children;
    }

    @Override
    public <T> T visit(PredicateVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** Evaluate the predicate result based on multiple {@link Predicate}s. */
    public abstract static class Function implements Serializable {

        @Override
        public int hashCode() {
            return this.getClass().getName().hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o != null && getClass() == o.getClass();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }
}
