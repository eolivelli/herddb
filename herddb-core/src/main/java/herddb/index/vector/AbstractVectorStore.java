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

package herddb.index.vector;

import herddb.utils.Bytes;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Abstract base class for vector stores.
 * Implementations store vectors keyed by primary key and support similarity search.
 *
 * @author enrico.olivelli
 */
public abstract class AbstractVectorStore implements AutoCloseable {

    protected final String vectorColumnName;

    protected AbstractVectorStore(String vectorColumnName) {
        this.vectorColumnName = vectorColumnName;
    }

    public String getVectorColumnName() {
        return vectorColumnName;
    }

    public abstract void addVector(Bytes pk, float[] vector);

    /**
     * Zero-copy counterpart of {@link #addVector(Bytes, float[])}. Stores from the
     * caller-owned buffer's current position up to its limit as a vector of
     * {@code remaining() / Float.BYTES} floats; the buffer is not retained past
     * this call.
     *
     * <p>Implementations that can consume the buffer directly (e.g. jvector-backed
     * stores wrapping it as a {@code BufferVectorFloat}) should override this method
     * to avoid the {@code float[]} materialization that the default implementation
     * performs as a compatibility fallback.
     *
     * <p>Use {@link ByteOrder#LITTLE_ENDIAN} for the native-SIMD fast path; big-endian
     * buffers still work correctly but only under the Panama fallback.
     */
    public void addVector(Bytes pk, ByteBuffer vector) {
        if (vector == null) {
            return;
        }
        float[] floats = new float[vector.remaining() / Float.BYTES];
        vector.asFloatBuffer().get(floats);
        addVector(pk, floats);
    }

    public abstract void removeVector(Bytes pk);

    public abstract int size();

    public abstract List<Map.Entry<Bytes, Float>> search(float[] queryVector, int topK);

    /**
     * Zero-copy counterpart of {@link #search(float[], int)}. Interprets the
     * caller-owned buffer's remaining bytes as the query vector; the buffer is not
     * retained past this call. See {@link #addVector(Bytes, ByteBuffer)} for the
     * copy / byte-order contract.
     */
    public List<Map.Entry<Bytes, Float>> search(ByteBuffer queryVector, int topK) {
        float[] floats = new float[queryVector.remaining() / Float.BYTES];
        queryVector.asFloatBuffer().get(floats);
        return search(floats, topK);
    }

    public abstract long estimatedMemoryUsageBytes();

    public abstract void start() throws Exception;

    /**
     * Visits every primary key currently stored in the vector store. Used by
     * the indexing-admin diagnostic CLI. The visitor returns {@code false} to
     * stop the traversal early.
     *
     * @param includeOnDisk if true, also visit primary keys that live only in
     *                      on-disk segments (ignored by in-memory stores)
     * @param visitor callback invoked for each PK; return false to stop
     */
    public void forEachPrimaryKey(boolean includeOnDisk, Predicate<Bytes> visitor) {
        throw new UnsupportedOperationException(
                "forEachPrimaryKey not implemented by " + getClass().getName());
    }

    @Override
    public void close() throws Exception {
    }
}
