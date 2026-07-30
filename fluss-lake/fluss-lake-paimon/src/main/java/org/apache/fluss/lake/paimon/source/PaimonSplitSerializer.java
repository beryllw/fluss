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

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamClass;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Serializer for paimon split. */
public class PaimonSplitSerializer implements SimpleVersionedSerializer<PaimonSplit> {

    private static final int VERSION_1 = 1;
    // VERSION_2 additionally persists the partition values.
    private static final int VERSION_2 = 2;

    @Override
    public int getVersion() {
        return VERSION_2;
    }

    @Override
    public byte[] serialize(PaimonSplit paimonSplit) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        DataSplit dataSplit = paimonSplit.dataSplit();
        InstantiationUtil.serializeObject(view, dataSplit);
        view.writeBoolean(paimonSplit.isBucketUnAware());
        List<String> partition = paimonSplit.partition();
        view.writeInt(partition.size());
        for (String value : partition) {
            view.writeUTF(value);
        }
        return out.toByteArray();
    }

    @Override
    public PaimonSplit deserialize(int version, byte[] serialized) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(serialized);
        DataSplit dataSplit;
        try {
            RelocatingObjectInputStream ois =
                    new RelocatingObjectInputStream(in, getClass().getClassLoader());
            dataSplit = (DataSplit) ois.readObject();
            DataInputStream dis = new DataInputStream(in);
            boolean isBucketUnAware = dis.readBoolean();
            if (version == VERSION_1) {
                // VERSION_1 did not store partition values separately, but string partitions were
                // exposed through DataSplit.partition(). Preserve that old behavior.
                return new PaimonSplit(
                        dataSplit, isBucketUnAware, readStringPartition(dataSplit.partition()));
            } else if (version == VERSION_2) {
                int size = dis.readInt();
                List<String> partition = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    partition.add(dis.readUTF());
                }
                return new PaimonSplit(dataSplit, isBucketUnAware, partition);
            } else {
                throw new IOException("Unsupported PaimonSplit serialization version: " + version);
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize PaimonSplit", e);
        }
    }

    private List<String> readStringPartition(BinaryRow partition) {
        if (partition == null || partition.getFieldCount() == 0) {
            return Collections.emptyList();
        }

        List<String> partitions = new ArrayList<>(partition.getFieldCount());
        for (int i = 0; i < partition.getFieldCount(); i++) {
            partitions.add(partition.getString(i).toString());
        }
        return partitions;
    }

    /**
     * An {@link java.io.ObjectInputStream} that restores state written before Paimon classes were
     * relocated (shaded): class names starting with the original {@code org.apache.paimon.} prefix
     * in the serialization stream are remapped to the actual (possibly relocated) class names at
     * deserialization time.
     *
     * <p>In non-relocated builds the actual prefix equals the original prefix, so the remapping
     * degrades to a no-op and behavior is unchanged.
     */
    static class RelocatingObjectInputStream
            extends InstantiationUtil.ClassLoaderObjectInputStream {

        // The prefix must NOT appear as a plain string literal: shade plugins rewrite matching
        // constant-pool strings, which would silently turn old and new prefixes into the same
        // string. Build it at runtime instead.
        private static final String ORIGINAL_PREFIX =
                String.join(".", "org", "apache", "paimon") + ".";

        // Derived from the actually loaded class, so any relocation prefix works; in
        // non-relocated builds it equals ORIGINAL_PREFIX.
        private static final String ACTUAL_PREFIX;

        static {
            String cls = DataSplit.class.getName();
            ACTUAL_PREFIX = cls.substring(0, cls.length() - "table.source.DataSplit".length());
        }

        private final String originalPrefix;
        private final String actualPrefix;

        RelocatingObjectInputStream(InputStream in, ClassLoader cl) throws IOException {
            this(in, cl, ORIGINAL_PREFIX, ACTUAL_PREFIX);
        }

        @VisibleForTesting
        RelocatingObjectInputStream(
                InputStream in, ClassLoader cl, String originalPrefix, String actualPrefix)
                throws IOException {
            super(in, cl);
            this.originalPrefix = originalPrefix;
            this.actualPrefix = actualPrefix;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            String name = desc.getName();
            if (!actualPrefix.equals(originalPrefix) && name.startsWith(originalPrefix)) {
                String relocated = actualPrefix + name.substring(originalPrefix.length());
                try {
                    return Class.forName(relocated, false, classLoader);
                } catch (ClassNotFoundException ignored) {
                    // fall back to the default resolution to keep the original exception path
                }
            }
            return super.resolveClass(desc);
        }
    }
}
