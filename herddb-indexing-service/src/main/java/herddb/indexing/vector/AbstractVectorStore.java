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

package herddb.indexing.vector;

import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    /**
     * Returns a stable storage-level UUID for this store instance that can be
     * persisted in the {@link herddb.indexing.WatermarkSnapshot} so a restarting
     * indexing-service engine can re-attach to the same on-disk / remote
     * checkpoint without full DML log replay.
     *
     * <p>Returns {@code null} for stores that have no persistent identity
     * (e.g. {@link herddb.indexing.InMemoryVectorStore}). In that case the
     * checkpoint-and-save path skips UUID embedding.
     *
     * <p>{@link herddb.indexing.vector.PersistentVectorStore} overrides this to
     * return {@code getIndexUUID()}.
     */
    public String getStoreUUID() {
        return null;
    }

    /**
     * Adopts an externally-produced segment (e.g. an optimizer merge output) into
     * this store's active segment list. The segment's graph and map multipart files
     * must already be present in the underlying data-storage manager under the key
     * derived from {@code externalSegmentId}.
     *
     * <p>Idempotent: if a segment with the same {@code segmentUuid} is already loaded,
     * this method returns {@code false} immediately without loading it again.
     *
     * <p>The default implementation is a no-op (used by in-memory stores that have no
     * persistent segments). {@link herddb.indexing.vector.PersistentVectorStore} overrides
     * this with a full implementation.
     *
     * @param segmentUuid       the UUID of the segment (from the ZK registry)
     * @param externalSegmentId the 63-bit long segment ID allocated by the optimizer;
     *                          used to reconstruct the multipart storage key
     *                          ({@code indexUUID + "_seg" + externalSegmentId})
     * @param graphFilePath     logical path recorded in the segment's ZK znode
     * @param graphFileSize     exact byte-size of the graph multipart file
     * @param mapFilePath       logical path recorded in the segment's ZK znode
     * @param mapFileSize       exact byte-size of the map multipart file
     * @param generation        segment generation from the ZK znode
     * @return {@code true} if the segment was newly loaded; {@code false} if it was
     *         already present (idempotent re-fire) or if the store is non-persistent
     */
    public boolean adoptExternalSegment(String segmentUuid, long externalSegmentId,
            String graphFilePath, long graphFileSize,
            String mapFilePath, long mapFileSize,
            long generation) throws IOException, DataStorageManagerException {
        // No-op for non-persistent stores (e.g. InMemoryVectorStore).
        return false;
    }

    /**
     * Removes a segment from the active list by its UUID and releases its resources.
     * Called when the segment-assignment watcher reports that a segment previously
     * owned by this IS instance is now deprecated (e.g. superseded by an
     * optimizer-produced merge).
     *
     * <p>Idempotent: if the UUID is not found, this method returns immediately.
     * Does <em>not</em> queue the segment's files for deletion — the optimizer is
     * responsible for managing the lifecycle of files it produced.
     *
     * <p>The default implementation is a no-op.
     * {@link herddb.indexing.vector.PersistentVectorStore} overrides this.
     *
     * @param segmentUuid the UUID of the segment to remove
     */
    public void dropSegmentByUuid(String segmentUuid) {
        // No-op for non-persistent stores.
    }

    /**
     * Result of a {@link #dropSegmentByStorageKey(String)} call.
     *
     * <p>Used by the operator-facing {@code DeleteSegment} RPC (issue #617)
     * to report whether the segment was found and, when it was, the number
     * of live vectors that were lost as part of the removal — both as a
     * sanity check ("did I just delete a populated segment?") and as the
     * value the IS surfaces back to the {@code indexing-admin delete-segment}
     * CLI for the operator's audit log.
     */
    public static final class SegmentDropResult {
        public final boolean removed;
        public final long vectorsLost;

        public SegmentDropResult(boolean removed, long vectorsLost) {
            this.removed = removed;
            this.vectorsLost = vectorsLost;
        }

        public static final SegmentDropResult NOT_FOUND = new SegmentDropResult(false, 0L);
    }

    /**
     * Removes a segment from the active list by its <em>multipart storage key</em>
     * — i.e. the value returned by {@code PersistentVectorStore.segmentStorageKey}
     * (legacy {@code indexUUID + "_seg" + segmentId} or, for adopted segments,
     * the explicit {@code externalStorageKey}). Used by the operator-facing
     * {@code DeleteSegment} RPC (issue #617) to remove a corrupted segment
     * whose Phase B upload failed mid-flight, leaving it registered in IS
     * metadata without a fully-written graph file in remote storage.
     *
     * <p>Idempotent: returns {@link SegmentDropResult#NOT_FOUND} when no
     * segment with the given storage key is currently loaded.
     *
     * <p>Does <em>not</em> delete the segment's multipart files from the
     * underlying storage manager — that is the responsibility of the
     * caller (the {@code DeleteSegment} RPC handler), which only purges
     * remote files when explicitly requested via {@code purge_storage=true}.
     *
     * <p>The default implementation is a no-op.
     * {@link herddb.indexing.vector.PersistentVectorStore} overrides this.
     *
     * @param storageKey the segment's multipart storage key to remove
     * @return a {@link SegmentDropResult} describing whether the segment
     *         was removed and how many live vectors were lost
     */
    public SegmentDropResult dropSegmentByStorageKey(String storageKey) {
        // No-op for non-persistent stores.
        return SegmentDropResult.NOT_FOUND;
    }

    /**
     * Reconciles adopted (externally-produced) segments against the ZK-reported
     * snapshot. Any segment with a non-null external storage key whose UUID is
     * absent from {@code knownUuids} is dropped via {@link #dropSegmentByUuid}.
     *
     * <p>Called at IS startup, after the initial {@code watchIndex} scan, to
     * handle IS-was-down-while-optimizer-deleted scenarios.
     *
     * <p>The default implementation is a no-op (non-persistent stores have no
     * adopted segments). {@link herddb.indexing.vector.PersistentVectorStore} overrides.
     *
     * @param knownUuids segment UUIDs currently visible in the ZK registry
     */
    public void reconcileAdoptedSegments(Set<String> knownUuids) {
        // No-op for non-persistent stores.
    }

    @Override
    public void close() throws Exception {
    }
}
