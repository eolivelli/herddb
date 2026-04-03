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

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.types.VectorFloat;

/**
 * {@link RandomAccessVectorValues} implementation backed by a {@link VectorStorage}.
 *
 * <p>{@link #getVector(int)} is a single lock-free array lookup — no Integer boxing,
 * no hash computation, no ConcurrentHashMap segment traversal.
 *
 * <p>{@code isValueShared()} returns {@code false}: each slot holds an independent
 * {@code VectorFloat<?>} object, so callers need not copy before storing the reference.
 *
 * <p>{@code copy()} returns {@code this}: the storage is already thread-safe and
 * reusing the same instance is correct for concurrent graph-building threads.
 */
class VectorStorageRandomAccessVectorValues implements RandomAccessVectorValues {

    private final VectorStorage storage;
    private final int dimension;
    private final int size;
    /**
     * Offset added to the localNodeId before looking up in storage.
     * For shard-local graphs this equals {@code LiveGraphShard.startNodeId},
     * so that {@code getVector(localId)} returns the vector at global nodeId
     * {@code localId + offset}.  Zero for any non-shard use (pool building, etc.).
     */
    private final int offset;

    VectorStorageRandomAccessVectorValues(VectorStorage storage, int dimension) {
        this(storage, dimension, -1, 0);
    }

    VectorStorageRandomAccessVectorValues(VectorStorage storage, int dimension, int size) {
        this(storage, dimension, size, 0);
    }

    VectorStorageRandomAccessVectorValues(VectorStorage storage, int dimension, int size, int offset) {
        this.storage = storage;
        this.dimension = dimension;
        this.size = size;
        this.offset = offset;
    }

    @Override
    public VectorFloat<?> getVector(int localNodeId) {
        return storage.get(localNodeId + offset);
    }

    @Override
    public boolean isValueShared() {
        return false;
    }

    @Override
    public RandomAccessVectorValues copy() {
        return this;
    }

    @Override
    public int dimension() {
        return dimension;
    }

    @Override
    public int size() {
        if (size < 0) {
            throw new UnsupportedOperationException(
                    "VectorStorageRandomAccessVectorValues.size() is not supported without an explicit size");
        }
        return size;
    }
}
