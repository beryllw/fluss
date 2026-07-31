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

package org.apache.fluss.utils.json;

import org.apache.fluss.lake.committer.PartitionMarkDoneState;
import org.apache.fluss.lake.committer.PartitionMarkDoneState.PartitionState;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Json serializer and deserializer for the {@link PartitionMarkDoneState} payload, e.g. {@code
 * {"partitions":{"5":{"update_time":1704153550000,"done_time":-1}}}}. Partitions are written in
 * ascending id order for deterministic output.
 *
 * <p>The payload schema version lives in the {@code TieringStateEntry} envelope, not here. The
 * serde validates the JSON structure and field types; value ranges are enforced by the {@link
 * PartitionState} constructor. Unknown fields are tolerated.
 */
public class PartitionMarkDoneStateJsonSerde
        implements JsonSerializer<PartitionMarkDoneState>,
                JsonDeserializer<PartitionMarkDoneState> {

    public static final PartitionMarkDoneStateJsonSerde INSTANCE =
            new PartitionMarkDoneStateJsonSerde();

    private static final String PARTITIONS_KEY = "partitions";
    private static final String UPDATE_TIME_KEY = "update_time";
    private static final String DONE_TIME_KEY = "done_time";

    @Override
    public void serialize(PartitionMarkDoneState state, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeObjectFieldStart(PARTITIONS_KEY);
        for (Map.Entry<Long, PartitionState> entry :
                new TreeMap<>(state.getPartitionStates()).entrySet()) {
            generator.writeObjectFieldStart(String.valueOf(entry.getKey()));
            generator.writeNumberField(UPDATE_TIME_KEY, entry.getValue().getUpdateTime());
            generator.writeNumberField(DONE_TIME_KEY, entry.getValue().getDoneTime());
            generator.writeEndObject();
        }
        generator.writeEndObject();
        generator.writeEndObject();
    }

    @Override
    public PartitionMarkDoneState deserialize(JsonNode node) {
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException(
                    "Corrupt partition mark-done state: expected a JSON object but got " + node);
        }

        Map<Long, PartitionState> partitionStates = new HashMap<>();
        JsonNode partitionsNode = node.get(PARTITIONS_KEY);
        if (partitionsNode != null && !partitionsNode.isNull()) {
            if (!partitionsNode.isObject()) {
                throw new IllegalArgumentException(
                        "Corrupt partition mark-done state: '"
                                + PARTITIONS_KEY
                                + "' must be a JSON object but got "
                                + partitionsNode);
            }
            Iterator<Map.Entry<String, JsonNode>> fields = partitionsNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                long partitionId;
                try {
                    partitionId = Long.parseLong(field.getKey());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "Corrupt partition mark-done state: invalid partitionId key '"
                                    + field.getKey()
                                    + "' in "
                                    + PARTITIONS_KEY);
                }
                if (partitionId <= 0) {
                    throw new IllegalArgumentException(
                            "Corrupt partition mark-done state: partitionId must be positive but got "
                                    + partitionId);
                }
                partitionStates.put(
                        partitionId, deserializePartitionState(partitionId, field.getValue()));
            }
        }

        return new PartitionMarkDoneState(partitionStates);
    }

    private static PartitionState deserializePartitionState(long partitionId, JsonNode node) {
        JsonNode updateTimeNode = node == null ? null : node.get(UPDATE_TIME_KEY);
        JsonNode doneTimeNode = node == null ? null : node.get(DONE_TIME_KEY);
        if (node == null
                || !node.isObject()
                || updateTimeNode == null
                || !updateTimeNode.canConvertToLong()
                || doneTimeNode == null
                || !doneTimeNode.canConvertToLong()) {
            throw new IllegalArgumentException(
                    "Corrupt partition mark-done state: partition "
                            + partitionId
                            + " must be an object with numeric '"
                            + UPDATE_TIME_KEY
                            + "' and '"
                            + DONE_TIME_KEY
                            + "' but got "
                            + node);
        }
        try {
            return new PartitionState(updateTimeNode.asLong(), doneTimeNode.asLong());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Corrupt partition mark-done state: partition "
                            + partitionId
                            + ": "
                            + e.getMessage());
        }
    }
}
