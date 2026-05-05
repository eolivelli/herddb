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

package herddb.index;

import herddb.core.AbstractIndexManager;
import herddb.core.PostCheckpointAction;
import herddb.log.LogSequenceNumber;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Index which maps every key of a table to the page which contains the key.
 *
 * @author enrico.olivelli
 */
public interface KeyToPageIndex extends AutoCloseable {

    long getUsedMemory();

    boolean requireLoadAtStartup();

    long size();

    /**
     * Initialize the index
     *
     * @throws DataStorageManagerException
     */
    default void init() throws DataStorageManagerException {
    }

    void start(LogSequenceNumber sequenceNumber, boolean created) throws DataStorageManagerException;

    @Override
    void close();

    /**
     * Ensures that all data is persisted to memory
     */
    List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException;

    /**
     * Two-phase checkpoint, fuzzy-friendly. Implementations that own a
     * concurrent data structure (e.g. a BLink tree) capture all in-memory
     * state in {@link #prepareCheckpoint(LogSequenceNumber, boolean)} —
     * which the caller invokes while holding the per-table checkpoint write
     * lock — and defer the slow remote-storage I/O to
     * {@link #persistCheckpoint(KeyToPageCheckpointSnapshot)} which runs
     * <strong>outside</strong> the write lock. The default fallback runs
     * the legacy single-phase {@link #checkpoint(LogSequenceNumber, boolean)}
     * inside the persist phase, which is correct for in-memory-only
     * implementations (e.g. {@code ConcurrentMapKeyToPageIndex}) where
     * checkpoint is a no-op anyway.
     *
     * <p>Issue #403: shrinks the {@code TableManager} Phase-C exclusive
     * window from "BLink traversal + remote I/O" to "BLink traversal only",
     * eliminating multi-second commit-latency spikes during checkpoints
     * against remote storage.</p>
     *
     * @param sequenceNumber the post-flush LSN being snapshotted
     * @param pin            whether the resulting checkpoint must be pinned
     *                       on storage (prevents reclamation by GC)
     * @return an opaque snapshot to be passed to
     *         {@link #persistCheckpoint(KeyToPageCheckpointSnapshot)}
     */
    default KeyToPageCheckpointSnapshot prepareCheckpoint(
            LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException {
        return new SinglePhaseCheckpointSnapshot(sequenceNumber, pin);
    }

    /**
     * Persist a snapshot produced by
     * {@link #prepareCheckpoint(LogSequenceNumber, boolean)}.
     * Runs the slow remote I/O outside the per-table checkpoint write lock.
     */
    default List<PostCheckpointAction> persistCheckpoint(KeyToPageCheckpointSnapshot snapshot)
            throws DataStorageManagerException {
        SinglePhaseCheckpointSnapshot s = (SinglePhaseCheckpointSnapshot) snapshot;
        return checkpoint(s.sequenceNumber(), s.pin);
    }

    /**
     * Unpin a previously pinned checkpont (see
     * {@link #checkpoint(LogSequenceNumber, boolean)})
     *
     * @throws DataStorageManagerException
     */
    void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException;

    void truncate();

    void dropData();

    Stream<Map.Entry<Bytes, Long>> scanner(
            IndexOperation operation, StatementEvaluationContext context,
            TableContext tableContext, AbstractIndexManager index
    ) throws DataStorageManagerException, StatementExecutionException;

    void put(Bytes key, Long currentPage);

    /**
     * Attempt to put a new value in the index. The mapping will be update only if current mapping
     * matches given expected one (provide null if no mapping is expected).
     * <p>
     * If current mapping differs it will be left untouched
     * </p>
     *
     * @return {@code false} if the put wasn't executed
     */
    boolean put(Bytes key, Long newPage, Long expectedPage);

    boolean containsKey(Bytes key);

    Long get(Bytes key);

    Long remove(Bytes key);

    boolean isSortedAscending(int[] pkTypes);

    /**
     * Returns the byte-wise smallest key in the index, or {@code null} if the
     * index is empty. Implementations should run in O(log n) when possible.
     *
     * <p>Note: the returned bytes are byte-wise sorted. Callers that need
     * the natural-order MIN of a numeric column where the byte order does
     * not match the natural order (see {@link #isSortedAscending(int[])})
     * must not rely on this single value alone.</p>
     */
    default Bytes getMinKey() {
        throw new UnsupportedOperationException(
                "getMinKey not implemented by " + getClass().getName());
    }

    /**
     * Returns the byte-wise largest key in the index, or {@code null} if the
     * index is empty. Implementations should run in O(log n) when possible.
     *
     * <p>Same byte-vs-natural caveat as {@link #getMinKey()}.</p>
     */
    default Bytes getMaxKey() {
        throw new UnsupportedOperationException(
                "getMaxKey not implemented by " + getClass().getName());
    }

}
