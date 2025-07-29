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

package com.alibaba.fluss.lake.paimon.utils;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;

import org.apache.paimon.data.Timestamp;

/** Wraps a Paimon row as a Fluss row. */
public class PaimonRowAsFlussRow implements InternalRow {

    private final org.apache.paimon.data.InternalRow paimonRow;

    public PaimonRowAsFlussRow(org.apache.paimon.data.InternalRow paimonRow) {
        this.paimonRow = paimonRow;
    }

    @Override
    public int getFieldCount() {
        return paimonRow.getFieldCount();
    }

    @Override
    public boolean isNullAt(int pos) {
        return paimonRow.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return paimonRow.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return paimonRow.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return paimonRow.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return paimonRow.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return paimonRow.getInt(pos);
    }

    @Override
    public float getFloat(int pos) {
        return paimonRow.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return paimonRow.getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return BinaryString.fromBytes(paimonRow.getString(pos).toBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(paimonRow.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        org.apache.paimon.data.Decimal paimonDecimal = paimonRow.getDecimal(pos, precision, scale);
        if (paimonDecimal.isCompact()) {
            return Decimal.fromUnscaledLong(paimonDecimal.toUnscaledLong(), precision, scale);
        } else {
            return Decimal.fromBigDecimal(paimonDecimal.toBigDecimal(), precision, scale);
        }
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        Timestamp timestamp = paimonRow.getTimestamp(pos, precision);
        if (TimestampNtz.isCompact(precision)) {
            return TimestampNtz.fromMillis(timestamp.getMillisecond());
        } else {
            return TimestampNtz.fromMillis(
                    timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
        }
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        Timestamp timestamp = paimonRow.getTimestamp(pos, precision);
        if (TimestampLtz.isCompact(precision)) {
            return TimestampLtz.fromEpochMillis(timestamp.getMillisecond());
        } else {
            return TimestampLtz.fromEpochMillis(
                    timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
        }
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return paimonRow.getBinary(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return paimonRow.getBinary(pos);
    }
}
