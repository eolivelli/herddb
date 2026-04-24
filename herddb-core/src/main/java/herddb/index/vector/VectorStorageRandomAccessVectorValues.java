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
 * {@link RandomAccessVectorValues} implementation backed by a per-shard
 * {@link VectorStorage}. Each {@link VectorStorage} is owned by a single
 * {@code LiveGraphShard} and indexed by the shard's local ordinal
 * {@code [0, shardSize)} — no offset arithmetic is needed.
 *
 * <p>{@link #getVector(int)} is a single lock-free array lookup.
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

    VectorStorageRandomAccessVectorValues(VectorStorage storage, int dimension) {
        this(storage, dimension, -1);
    }

    VectorStorageRandomAccessVectorValues(VectorStorage storage, int dimension, int size) {
        this.storage = storage;
        this.dimension = dimension;
        this.size = size;
    }

    @Override
    public VectorFloat<?> getVector(int localNodeId) {
        return storage.get(localNodeId);
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
