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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Authority map used by {@link VectorIndexCompactor} to track, for each PK
 * present in the candidate segments of a compaction cycle, the highest-generation
 * segment that owns it (or {@link VectorIndexCompactor#LIVE_SHARD_SEGMENT_ID}
 * when a live shard supersedes the candidate).
 *
 * <p>Backed by a {@link BLink} of {@code (Bytes -> Long)}; the {@link Long} value
 * packs both the winning segment id and the winning generation:
 * <ul>
 *   <li>high 32 bits: generation (unsigned, capped at {@code 0xFFFFFFFFL})</li>
 *   <li>low 32 bits: signed {@code int} segment id (negative values
 *       — including {@link VectorIndexCompactor#LIVE_SHARD_SEGMENT_ID} —
 *       round-trip through a sign-extending narrowing cast)</li>
 * </ul>
 *
 * <p>Memory usage of the BLink is bounded by the index page replacement policy
 * configured on the per-store {@link herddb.core.MemoryManager}: cold pages are
 * paged out to the {@link herddb.storage.DataStorageManager} backing the
 * {@code BLink}'s {@link herddb.index.blink.BLinkIndexDataStorage}. This keeps
 * compaction heap usage bounded by the index-memory budget rather than by the
 * number of PKs in the index, which is what causes
 * <a href="https://github.com/eolivelli/herddb/issues/290">issue #290</a> when
 * the unbounded {@code HashMap} variant is used over hundreds of millions of
 * PKs.
 *
 * <p>Lifecycle: instances are created per compaction cycle by
 * {@link PersistentVectorStore} and closed in a {@code finally} block — closing
 * unloads the BLink's resident pages and drops the temporary backing index from
 * the storage manager so no leftover state survives across cycles. The class
 * implements {@link Closeable} (rather than {@link AutoCloseable}) so callers
 * can rethrow {@link IOException} from {@code close()}.
 */
final class CompactionAuthorityMap implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(CompactionAuthorityMap.class.getName());

    /**
     * Maximum representable generation when packed into the high 32 bits.
     * Beyond this, the encoder asserts to surface a misuse early — at one
     * checkpoint per second this corresponds to ~136 years of uptime.
     */
    static final long MAX_PACKABLE_GENERATION = 0xFFFFFFFFL;

    /**
     * Marker generation written for live-shard wins. Equal to
     * {@link #MAX_PACKABLE_GENERATION} so a live-shard entry always beats any
     * real on-disk segment generation under the {@code newGen > existingGen}
     * comparison performed by {@link #updateIfHigherGeneration(Bytes, long, int)}.
     */
    static final long LIVE_SHARD_GENERATION_MARKER = MAX_PACKABLE_GENERATION;

    private final BLink<Bytes, Long> blink;
    private final Runnable onDrop;

    /**
     * @param blink  the BLink storing PK -> packed (generation, segmentId)
     * @param onDrop callback invoked after the BLink is closed, used by the
     *               production constructor to drop the temp storage; tests
     *               typically pass a no-op
     */
    CompactionAuthorityMap(BLink<Bytes, Long> blink, Runnable onDrop) {
        this.blink = blink;
        this.onDrop = onDrop;
    }

    /**
     * Inserts the given PK with the supplied (generation, segmentId) tuple if no
     * entry exists yet, or replaces the existing entry when {@code newGeneration}
     * is strictly greater than the recorded generation. No-op when the existing
     * generation is greater or equal.
     */
    void updateIfHigherGeneration(Bytes pk, long newGeneration, int segmentId) {
        long encoded = encode(newGeneration, segmentId);
        Long existing = blink.search(pk);
        if (existing == null || newGeneration > decodeGeneration(existing)) {
            blink.insert(pk, encoded);
        }
    }

    /**
     * Returns the winning segment id for {@code pk}, or {@code null} if the
     * PK is not in the authority map. {@link VectorIndexCompactor#LIVE_SHARD_SEGMENT_ID}
     * indicates a live-shard win.
     */
    Integer getSegmentId(Bytes pk) {
        Long encoded = blink.search(pk);
        if (encoded == null) {
            return null;
        }
        return decodeSegmentId(encoded);
    }

    /** Returns the number of stored PKs (live + paged-out). Test-only. */
    long size() {
        return blink.size();
    }

    @Override
    public void close() throws IOException {
        // Close the BLink first so its resident pages stop being tracked by
        // the page replacement policy; then drop the underlying storage so
        // we don't leak temp pages across compaction cycles. Both steps are
        // best-effort: a failure on close should never prevent the temp
        // storage from being dropped.
        try {
            blink.close();
        } catch (RuntimeException e) {
            // BLink.close() may surface unchecked IO failures from the
            // page replacement policy unloading paged-out nodes. We log
            // and continue so the storage drop still runs.
            LOGGER.log(Level.WARNING,
                    "compaction authority map: blink close failed (continuing)", e);
        }
        onDrop.run();
    }

    /**
     * Packs {@code generation} (unsigned 32 bits) and {@code segmentId}
     * (signed int) into a single {@code long}.
     */
    static long encode(long generation, int segmentId) {
        if (generation < 0L || generation > MAX_PACKABLE_GENERATION) {
            throw new IllegalArgumentException(
                    "generation out of range for 32-bit packing: " + generation);
        }
        return (generation << 32) | (((long) segmentId) & 0xFFFFFFFFL);
    }

    /** Recovers the segment id from a packed value. Sign-extending narrowing cast. */
    static int decodeSegmentId(long encoded) {
        return (int) encoded;
    }

    /** Recovers the generation from a packed value. Unsigned shift. */
    static long decodeGeneration(long encoded) {
        return encoded >>> 32;
    }
}
