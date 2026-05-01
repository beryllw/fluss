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

package org.apache.fluss.flink.tiering.event;

import org.apache.flink.api.connector.source.SourceEvent;

/**
 * SourceEvent used to represent a Fluss table's tiering is cancelled (e.g., the table was dropped).
 *
 * <p>Unlike {@link FailedTieringEvent} which represents an error during tiering, this event
 * indicates that the tiering is intentionally cancelled and should not be retried.
 */
public class CancelledTieringEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final long tableId;

    private final String cancelReason;

    public CancelledTieringEvent(long tableId, String cancelReason) {
        this.tableId = tableId;
        this.cancelReason = cancelReason;
    }

    public long getTableId() {
        return tableId;
    }

    public String cancelReason() {
        return cancelReason;
    }
}
