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
package herddb.indexing.segment;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Watches the segment registry for an index and emits events to a
 * {@link SegmentAssignmentListener} when the local instance gains, retains, or
 * loses ownership of segments.
 *
 * <p>Mechanics:
 * <ul>
 *   <li>{@link #watchIndex(String, String)} arms a children watcher on the
 *       index znode and a data watcher on each segment znode.</li>
 *   <li>On any watcher fire we re-scan the index and diff against our local
 *       view; the diff produces the listener events.</li>
 *   <li>A heartbeat refresh runs every {@link #refreshIntervalMs} milliseconds
 *       as a defense against missed watcher fires (curator-test ZK is
 *       reliable, but production ZK can drop watchers across reconnects).</li>
 * </ul>
 *
 * <p>This class is thread-safe. {@link #close()} stops dispatch and unregisters
 * watchers (the underlying ZK client is owned by the caller).
 */
public final class SegmentAssignmentWatcher implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(SegmentAssignmentWatcher.class.getName());

    private final SegmentRegistryClient registry;
    private final int instanceId;
    private final SegmentAssignmentListener listener;
    private final ScheduledExecutorService dispatchExecutor;
    private final long refreshIntervalMs;

    /**
     * Per-segment cached view: last-seen metadata and the address keys (we need
     * the address keys to build the {@code onSegmentReleased} signal even if the
     * segment znode is gone).
     */
    private final Map<String, SegmentMetadata> known = new ConcurrentHashMap<>();

    /** Set of (tablespaceUuid, indexUuid) pairs currently being watched. */
    private final Set<IndexKey> watchedIndexes = ConcurrentHashMap.newKeySet();

    private volatile ScheduledFuture<?> refreshFuture;
    private volatile boolean closed;

    public SegmentAssignmentWatcher(SegmentRegistryClient registry,
                                    int instanceId,
                                    SegmentAssignmentListener listener) {
        this(registry, instanceId, listener, /* refreshIntervalMs= */ 30_000L);
    }

    public SegmentAssignmentWatcher(SegmentRegistryClient registry,
                                    int instanceId,
                                    SegmentAssignmentListener listener,
                                    long refreshIntervalMs) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.instanceId = instanceId;
        this.listener = Objects.requireNonNull(listener, "listener");
        this.refreshIntervalMs = refreshIntervalMs;
        this.dispatchExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "segment-assignment-watcher-" + instanceId);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Begins watching {@code (tablespaceUuid, indexUuid)}. Idempotent: calling
     * twice for the same index has no additional effect. Performs an initial scan
     * synchronously (blocking) so any existing assignments are emitted before
     * this call returns.
     */
    public void watchIndex(String tablespaceUuid, String indexUuid) throws SegmentRegistryException {
        if (closed) {
            throw new IllegalStateException("watcher is closed");
        }
        IndexKey key = new IndexKey(tablespaceUuid, indexUuid);
        watchedIndexes.add(key);
        scanIndex(key);
        if (refreshFuture == null) {
            refreshFuture = dispatchExecutor.scheduleAtFixedRate(this::refreshAll,
                    refreshIntervalMs, refreshIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    private void refreshAll() {
        if (closed) {
            return;
        }
        for (IndexKey key : watchedIndexes) {
            try {
                scanIndex(key);
            } catch (SegmentRegistryException e) {
                // Broad-ish catch is intentional: a transient registry error must not
                // kill the periodic refresher. Logged at WARNING; the next tick will
                // retry.
                LOGGER.log(Level.WARNING, "registry refresh failed for "
                        + key.tablespaceUuid + "/" + key.indexUuid + ": " + e.getMessage());
            }
        }
    }

    /**
     * One-shot scan of an index: re-list children with a watcher, re-read each
     * segment with a watcher, and diff against the local view.
     *
     * <p>Synchronised on the watcher instance to serialise overlapping scans
     * (children watcher + data watcher + heartbeat could otherwise race).
     */
    synchronized void scanIndex(IndexKey key) throws SegmentRegistryException {
        Watcher childrenWatcher = (WatchedEvent event) -> dispatchScan(key);
        List<VersionedSegmentMetadata> current = registry.listSegments(
                key.tablespaceUuid, key.indexUuid, childrenWatcher);

        Set<String> seen = new HashSet<>();
        for (VersionedSegmentMetadata v : current) {
            seen.add(v.metadata().getSegmentUuid());
            // Arm a per-znode data watcher.
            Watcher dataWatcher = (WatchedEvent event) -> dispatchScan(key);
            registry.getSegment(key.tablespaceUuid, key.indexUuid,
                    v.metadata().getSegmentUuid(), dataWatcher);
            applyDiff(v);
        }
        // Drop any local entries whose znode is gone, and emit released events
        // for segments we owned.
        Map<String, SegmentMetadata> snapshot = new HashMap<>(known);
        for (Map.Entry<String, SegmentMetadata> entry : snapshot.entrySet()) {
            if (!seen.contains(entry.getKey())) {
                known.remove(entry.getKey());
                if (entry.getValue().getOwnerInstanceId() == instanceId) {
                    fireReleased(null);
                }
            }
        }
    }

    private void dispatchScan(IndexKey key) {
        if (closed) {
            return;
        }
        // Hop to the dispatch thread so listener callbacks are serialised
        // and watcher firings don't block ZK's event thread.
        dispatchExecutor.execute(() -> {
            try {
                scanIndex(key);
            } catch (SegmentRegistryException e) {
                LOGGER.log(Level.WARNING, "watcher rescan failed for "
                        + key.tablespaceUuid + "/" + key.indexUuid + ": " + e.getMessage());
            }
        });
    }

    private void applyDiff(VersionedSegmentMetadata current) {
        SegmentMetadata m = current.metadata();
        SegmentMetadata previous = known.get(m.getSegmentUuid());
        known.put(m.getSegmentUuid(), m);

        boolean wasOwner = previous != null && previous.getOwnerInstanceId() == instanceId;
        boolean isOwner = m.getOwnerInstanceId() == instanceId;
        boolean isPending = m.getPendingOwnerInstanceId() == instanceId;
        boolean wasPending = previous != null && previous.getPendingOwnerInstanceId() == instanceId;

        if (isOwner && !wasOwner) {
            fireAssigned(current);
            return;
        }
        if (isPending && !wasPending) {
            firePending(current);
            return;
        }
        if (wasOwner && !isOwner) {
            fireReleased(previous);
        }
    }

    private void fireAssigned(VersionedSegmentMetadata seg) {
        try {
            listener.onSegmentAssigned(seg);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "listener.onSegmentAssigned threw for "
                    + seg.metadata().getSegmentUuid(), e);
        }
    }

    private void firePending(VersionedSegmentMetadata seg) {
        try {
            listener.onPendingAssignment(seg);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "listener.onPendingAssignment threw for "
                    + seg.metadata().getSegmentUuid(), e);
        }
    }

    private void fireReleased(SegmentMetadata previous) {
        try {
            listener.onSegmentReleased(previous);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "listener.onSegmentReleased threw for "
                    + (previous == null ? "<deleted>" : previous.getSegmentUuid()), e);
        }
    }

    @Override
    public void close() {
        closed = true;
        if (refreshFuture != null) {
            refreshFuture.cancel(true);
        }
        dispatchExecutor.shutdownNow();
        try {
            dispatchExecutor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Visible for tests: returns the watcher's last-seen ownership state for diagnostics. */
    public Map<String, SegmentMetadata> snapshotKnownSegments() {
        return new HashMap<>(known);
    }

    static final class IndexKey {
        final String tablespaceUuid;
        final String indexUuid;

        IndexKey(String tablespaceUuid, String indexUuid) {
            this.tablespaceUuid = tablespaceUuid;
            this.indexUuid = indexUuid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IndexKey)) {
                return false;
            }
            IndexKey that = (IndexKey) o;
            return Objects.equals(tablespaceUuid, that.tablespaceUuid)
                    && Objects.equals(indexUuid, that.indexUuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tablespaceUuid, indexUuid);
        }
    }
}
