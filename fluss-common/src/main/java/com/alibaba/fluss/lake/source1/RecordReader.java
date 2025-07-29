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

import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.utils.CloseableIterator;

import java.io.IOException;

/**
 * An interface for reading records from {@link LakeSplit}.
 *
 * <p>Implementations of this interface provide an iterator-style access to records, allowing
 * efficient sequential reading of potentially large datasets without loading all data into memory
 * at once. The reading should consider the pushed-down optimizations (project, filters, limits,
 * etc) from {@link LakeSource}.
 */
public interface RecordReader {

    /** Read a {@link LakeSplit} into a closeable iterator. */
    CloseableIterator<LogRecord> read() throws IOException;
}
