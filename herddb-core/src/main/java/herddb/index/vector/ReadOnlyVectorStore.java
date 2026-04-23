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

import herddb.core.MemoryManager;
import herddb.log.LogSequenceNumber;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Lightweight read-only sibling of {@link PersistentVectorStore}, used by
 * shadow indexing-service replicas. It presents only the read-relevant subset
 * of the {@link AbstractVectorStore} surface and forbids writes at the API
 * boundary, delegating the heavy lifting (segment loading from remote storage
 * and on-disk graph search) to a {@link PersistentVectorStore} instance
 * booted in read-only mode via {@link PersistentVectorStore#setReadOnly}.
 *
 * <p>Shadows never have a live graph, never run compaction, and never write to
 * storage. Their segment list is refreshed from the remote storage via
 * {@link #reloadFromStatus(IndexStatus)} whenever the primary advertises a
 * new durable checkpoint.
 */
public final class ReadOnlyVectorStore extends AbstractVectorStore {

    private final PersistentVectorStore delegate;

    public ReadOnlyVectorStore(String indexName, String tableName, String tableSpaceUUID,
                               String vectorColumnName, Path tmpDirectory,
                               DataStorageManager dataStorageManager,
                               MemoryManager memoryManager,
                               int m, int beamWidth, float neighborOverflow, float alpha,
                               boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                               VectorSimilarityFunction similarityFunction) {
        super(vectorColumnName);
        // Compaction interval is irrelevant because the compaction thread is
        // never started in read-only mode; pass a non-zero placeholder so the
        // backoff math in PersistentVectorStore doesn't divide by zero.
        this.delegate = new PersistentVectorStore(
                indexName, tableName, tableSpaceUUID, vectorColumnName,
                tmpDirectory, dataStorageManager, memoryManager,
                m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, maxLiveGraphSize,
                /* compactionIntervalMs */ 60_000L,
                similarityFunction);
        this.delegate.setReadOnly(true);
    }

    /**
     * Test / advanced-use constructor that lets the caller pin the underlying
     * {@code indexUUID} (needed in tests that produce state with one instance
     * and read it back with another).
     */
    public ReadOnlyVectorStore(String indexName, String tableName, String tableSpaceUUID,
                               String vectorColumnName, String indexUUID, Path tmpDirectory,
                               DataStorageManager dataStorageManager,
                               MemoryManager memoryManager,
                               int m, int beamWidth, float neighborOverflow, float alpha,
                               boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                               VectorSimilarityFunction similarityFunction) {
        super(vectorColumnName);
        this.delegate = new PersistentVectorStore(
                indexName, tableName, tableSpaceUUID, vectorColumnName, indexUUID, tmpDirectory,
                dataStorageManager, memoryManager,
                m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, maxLiveGraphSize,
                /* compactionIntervalMs */ 60_000L,
                similarityFunction);
        this.delegate.setReadOnly(true);
    }

    @Override
    public void start() throws Exception {
        delegate.start();
    }

    @Override
    public List<Map.Entry<Bytes, Float>> search(float[] queryVector, int topK) {
        return delegate.search(queryVector, topK);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public long estimatedMemoryUsageBytes() {
        return delegate.estimatedMemoryUsageBytes();
    }

    @Override
    public void forEachPrimaryKey(boolean includeOnDisk, Predicate<Bytes> visitor) {
        delegate.forEachPrimaryKey(includeOnDisk, visitor);
    }

    @Override
    public void addVector(Bytes pk, float[] vector) {
        throw new UnsupportedOperationException(
                "ReadOnlyVectorStore does not accept writes (shadow replica)");
    }

    @Override
    public void removeVector(Bytes pk) {
        throw new UnsupportedOperationException(
                "ReadOnlyVectorStore does not accept deletes (shadow replica)");
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    /**
     * Re-loads the on-disk segment list from the given {@link IndexStatus}.
     * Called by the shadow engine whenever the primary advertises a new
     * durable checkpoint.
     */
    public void reloadFromStatus(IndexStatus newStatus) throws IOException, DataStorageManagerException {
        delegate.reloadFromStatus(newStatus);
    }

    /** LSN of the {@link IndexStatus} most recently loaded into this store, or {@code null}. */
    public LogSequenceNumber getLoadedLsn() {
        return delegate.getLoadedLsn();
    }

    public int getSegmentCount() {
        return delegate.getSegmentCount();
    }

    // Diagnostic passthroughs used by the admin CLI and shadow-status RPCs.

    public PersistentVectorStore unwrap() {
        return delegate;
    }
}
