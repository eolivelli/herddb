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

import herddb.index.blink.BLink;
import herddb.utils.Bytes;
import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Append-only PK set used by {@link PersistentVectorStore} to track
 * primary keys deleted during a long-running checkpoint or compaction
 * cycle so the new on-disk artefact can be purged of those PKs before
 * it becomes visible.
 *
 * <p>Backed by a {@link BLink} of {@code (Bytes -> Long)}; the {@code Long}
 * value is unused (always {@code 0L}) — this is a set, not a map. We reuse
 * the existing {@code BLink} infrastructure (rather than introducing a
 * dedicated set type) because:
 *
 * <ul>
 *   <li>memory is bounded by the index page replacement policy on the
 *       per-store {@link herddb.core.MemoryManager}: cold pages spill to
 *       the {@link herddb.storage.DataStorageManager} backing the set's
 *       {@link herddb.index.blink.BLinkIndexDataStorage};</li>
 *   <li>concurrent {@code add} operations are safe — {@code BLink.insert}
 *       handles its own locking; the previous {@code ConcurrentHashMap.newKeySet()}
 *       behaviour relied on the same property.</li>
 * </ul>
 *
 * <p>This addresses the same heap-pressure pattern as
 * <a href="https://github.com/eolivelli/herddb/issues/290">issue #290</a>:
 * with sustained delete traffic during a multi-minute checkpoint or
 * compaction, the previous unbounded {@code Set<Bytes>} grew without
 * limit and consumed an arbitrary amount of heap on top of the already
 * large baseline of resident segment metadata.
 *
 * <p>Lifecycle: instances are created at the start of a checkpoint or
 * compaction cycle by {@link PersistentVectorStore} and closed in a
 * {@code finally} block. Closing unloads resident pages and drops the
 * temporary backing index so no leftover state survives across cycles.
 */
final class PagedPkSet implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(PagedPkSet.class.getName());

    /** Sentinel value stored in the underlying BLink. The set semantics ignore it. */
    private static final Long PRESENT = 0L;

    private final BLink<Bytes, Long> blink;
    private final Runnable onDrop;

    /**
     * @param blink  the BLink used as the underlying paged storage
     * @param onDrop callback invoked after the BLink is closed, used by the
     *               production constructor to drop the temp storage; tests
     *               typically pass a no-op
     */
    PagedPkSet(BLink<Bytes, Long> blink, Runnable onDrop) {
        this.blink = blink;
        this.onDrop = onDrop;
    }

    /** Adds {@code pk} to the set. Idempotent. Safe to call concurrently. */
    void add(Bytes pk) {
        blink.insert(pk, PRESENT);
    }

    /** Returns {@code true} if {@code pk} has been added to the set. */
    boolean contains(Bytes pk) {
        return blink.search(pk) != null;
    }

    /** Number of PKs currently stored (live + paged-out). */
    long size() {
        return blink.size();
    }

    /**
     * Iterates over every stored PK, applying {@code action} to each. The
     * iteration order is the BLink's natural key order. The set must not be
     * mutated during iteration.
     */
    void forEach(Consumer<Bytes> action) {
        blink.scan(null, null).forEach(entry -> action.accept(entry.getKey()));
    }

    @Override
    public void close() throws IOException {
        try {
            blink.close();
        } catch (RuntimeException e) {
            // BLink.close() may surface unchecked IO failures from the
            // page replacement policy unloading paged-out nodes. We log
            // and continue so the storage drop still runs.
            LOGGER.log(Level.WARNING,
                    "paged PK set: blink close failed (continuing)", e);
        }
        onDrop.run();
    }
}
