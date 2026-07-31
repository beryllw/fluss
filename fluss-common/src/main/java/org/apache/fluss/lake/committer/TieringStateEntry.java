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

package org.apache.fluss.lake.committer;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A keyed, versioned tiering-state entry carried by the lake offsets file alongside the bucket
 * offsets.
 *
 * <p>The entry is an opaque envelope: {@code stateKey} identifies the state owner (e.g. {@link
 * PartitionMarkDoneState#STATE_KEY}), {@code stateVersion} is the schema version of the payload,
 * and {@code payload} is a JSON object serialized as bytes. The transport and storage layers never
 * interpret the payload; the offsets-file serde may re-encode it as a JSON tree (all fields
 * preserved, byte-level layout not guaranteed). Entries with an unrecognized key or a
 * higher-than-supported version must be passed through unchanged so state written by a newer build
 * is never dropped.
 *
 * @since 0.9
 */
@PublicEvolving
public class TieringStateEntry {

    private final String stateKey;
    private final int stateVersion;
    private final byte[] payload;

    public TieringStateEntry(String stateKey, int stateVersion, byte[] payload) {
        checkArgument(
                stateKey != null && !stateKey.isEmpty(), "stateKey must be a non-empty string.");
        checkArgument(stateVersion > 0, "stateVersion must be positive but got %s.", stateVersion);
        this.stateKey = stateKey;
        this.stateVersion = stateVersion;
        this.payload = checkNotNull(payload, "payload must not be null.");
    }

    public String getStateKey() {
        return stateKey;
    }

    public int getStateVersion() {
        return stateVersion;
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TieringStateEntry that = (TieringStateEntry) o;
        return stateVersion == that.stateVersion
                && stateKey.equals(that.stateKey)
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateKey, stateVersion, Arrays.hashCode(payload));
    }

    @Override
    public String toString() {
        return "TieringStateEntry{"
                + "stateKey='"
                + stateKey
                + '\''
                + ", stateVersion="
                + stateVersion
                + ", payload="
                + Arrays.toString(payload)
                + '}';
    }
}
