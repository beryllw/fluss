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

package org.apache.iceberg.data;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;

/**
 * GenericReader to read for records for iceberg. Extends from Iceberg {@link GenericReader} to
 * enable the {@link #open(FileScanTask)} method to be visible to Fluss.
 */
public class IcebergGenericReader extends GenericReader {

    public IcebergGenericReader(TableScan scan, boolean reuseContainers) {
        super(scan, reuseContainers);
    }
}
