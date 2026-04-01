/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package herddb.model;

import herddb.codec.RecordSerializer;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.MapDataAccessor;
import herddb.utils.ObjectSizeUtils;
import herddb.utils.SizeAwareObject;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.Objects;

/**
 * A generic record
 *
 * @author enrico.olivelli
 */
public final class Record implements SizeAwareObject {

    /**
     * Constant size is internal data plus weak reference size (Bytes size is accounted during live estimation).
     * <p>
     * Record: header + 3 refs (key, value, cache).
     * With compressed oops: 12 + 3*4 = 24 bytes.
     * Without compressed oops: 16 + 3*8 = 40 bytes.
     * <p>
     * WeakReference (extends Reference): header + 4 refs (referent, queue, next, discovered).
     * With compressed oops: 12 + 4*4 + 4(padding) = 32 bytes.
     * Without compressed oops: 16 + 4*8 = 48 bytes.
     */
    private static final long CONSTANT_BYTE_SIZE = ObjectSizeUtils.COMPRESSED_OOPS
            ? (24 + 32)    // 56
            : (40 + 48);   // 88

    public static long estimateSize(Bytes key, byte[] value) {
        return key.getEstimatedSize() + Bytes.estimateSize(value) + CONSTANT_BYTE_SIZE;
    }

    public final Bytes key;
    public final Bytes value;
    private WeakReference<Map<String, Object>> cache;

    public Record(Bytes key, Bytes value) {
        this.key = key;
        this.value = value;
    }

    public Record(Bytes key, Bytes value, Map<String, Object> cache) {
        this.key = key;
        this.value = value;
        this.cache = new WeakReference<>(cache);
    }

    private Record(Bytes key, Bytes value, WeakReference<Map<String, Object>> cache) {
        this.key = key;
        this.value = value;
        this.cache = cache;
    }

    @Override
    public long getEstimatedSize() {
        return this.key.getEstimatedSize() + this.value.getEstimatedSize() + CONSTANT_BYTE_SIZE;
    }

    /**
     * Ensure that this value is not retaining strong references to a shared buffer
     *
     * @return the record itself or a copy
     */
    public Record nonShared() {

        if (key.isShared() || value.isShared()) {
            return new Record(key.nonShared(), value.nonShared(), cache);
        }

        return this;
    }

    public Map<String, Object> toBean(Table table) {
        WeakReference<Map<String, Object>> cachedRef = cache;
        Map<String, Object> res = cachedRef != null ? cachedRef.get() : null;
        if (res != null) {
            return res;
        }
        res = RecordSerializer.toBean(this, table);
        cache = new WeakReference<>(res);
        return res;
    }

    public DataAccessor getDataAccessor(Table table) {
        WeakReference<Map<String, Object>> cachedRef = cache;
        Map<String, Object> res = cachedRef != null ? cachedRef.get() : null;
        if (res != null) {
            return new MapDataAccessor(res, table.columnNames);
        } else {
            return RecordSerializer.buildRawDataAccessor(this, table);
        }
    }

    @Override
    public String toString() {
        return "Record{" + "key=" + key + ", value=" + value + '}';
    }

    public void clearCache() {
        cache = null;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 53 * hash + Objects.hashCode(this.key);
        hash = 53 * hash + Objects.hashCode(this.value);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Record other = (Record) obj;
        if (!Objects.equals(this.key, other.key)) {
            return false;
        }
        return Objects.equals(this.value, other.value);
    }

}
