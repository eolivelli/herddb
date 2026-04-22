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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Global lock-free vector storage for all live graph shards in a VectorIndexManager.
 *
 * <p>Replaces {@code ConcurrentHashMap<Integer, VectorFloat<?>>} (which required Integer
 * autoboxing, hash computation, and volatile segment traversal on every getVector() call)
 * with a direct array lookup indexed by the globally-sequential nodeId.
 *
 * <p>Thread safety:
 * <ul>
 *   <li>{@link #get(int)} is fully lock-free: one volatile read of the array reference,
 *       one array element read.</li>
 *   <li>{@link #set(int, VectorFloat)} has a lock-free fast path when the backing array
 *       is already large enough, guarded by a <em>seqlock</em> on {@link #growSeq}: the
 *       writer reads {@code growSeq} before the write and again after, and retries if
 *       the counter changed (meaning a grow started or completed concurrently).  A write
 *       that completes without seeing any grow is guaranteed to have landed on the live
 *       backing array (either the copy loop read our freshly-written value, or no copy
 *       was in progress).  Growth itself is serialized by {@link #growAndSet}, which
 *       bumps {@code growSeq} to odd before the copy and back to even after the publish.
 *       Growth is rare (O(log N) over the store's lifetime), so retries are bounded.</li>
 *   <li>{@link #remove(int)} is synchronized so that its {@code array.set(nodeId, null)}
 *       excludes a concurrent {@link #growAndSet} copy loop: without the lock, a grow
 *       running in parallel with remove could snapshot the not-yet-nulled slot into the
 *       new array, leaking the reference.</li>
 * </ul>
 *
 * <p>Write-before-read ordering guarantee: callers must call {@code set(nodeId, vec)} before
 * passing {@code nodeId} to {@code GraphIndexBuilder.addGraphNode()}.  The atomic element
 * write in {@code set} (combined with the volatile array-reference read in {@code get})
 * establishes happens-before with any subsequent read, satisfying JLS 17.4.4.
 */
@SuppressFBWarnings({"UG_SYNC_SET_UNSYNC_GET", "VO_VOLATILE_INCREMENT"})
class VectorStorage {

    private volatile AtomicReferenceArray<VectorFloat<?>> array;

    /**
     * Seqlock counter for {@link #set}'s lock-free fast path.  Even values mean no grow
     * is in progress; odd values mean a grow is mid-copy.  {@link #growAndSet} bumps it
     * to odd before allocating/copying and back to even after publishing the new array.
     */
    private volatile int growSeq;

    VectorStorage(int initialCapacity) {
        array = new AtomicReferenceArray<>(Math.max(initialCapacity, 256));
    }

    /**
     * Returns the vector stored at {@code nodeId}, or {@code null} if not set.
     * Lock-free hot path.
     */
    VectorFloat<?> get(int nodeId) {
        AtomicReferenceArray<VectorFloat<?>> a = array; // single volatile read
        return nodeId < a.length() ? a.get(nodeId) : null;
    }

    /**
     * Stores {@code vec} at {@code nodeId}, growing the backing array if necessary.
     * Lock-free in the common case (array already large enough); falls back to the
     * synchronized {@link #growAndSet} when a grow is required or when a concurrent
     * grow raced past our write.
     */
    void set(int nodeId, VectorFloat<?> vec) {
        while (true) {
            int seq = growSeq;
            if ((seq & 1) != 0) {
                // Grow in progress: the copy loop may read our not-yet-written slot.
                // Spin until the grow publishes before we write.  The odd window
                // covers only the copy loop, so the spin is bounded.
                continue;
            }
            AtomicReferenceArray<VectorFloat<?>> a = array; // volatile read
            if (nodeId >= a.length()) {
                growAndSet(nodeId, vec);
                return;
            }
            a.set(nodeId, vec);
            if (growSeq == seq) {
                // No grow overlapped our write: either no copy ran, or the copy ran
                // entirely before our read of `seq` (so the new array already has our
                // slot's prior value and our write on the old array is not observable
                // to readers, but those readers saw a consistent pre-write state).
                return;
            }
            // A grow started (and possibly finished) between our read of `seq` and now.
            // Our write may have landed on the pre-grow array and been missed by the
            // copy loop; retry on the post-grow state.
        }
    }

    /**
     * Synchronized slow path: grow the backing array if needed, then write the slot.
     * Serializes with {@link #remove} and {@link #compact} so their {@code array.set}
     * / reassignment cannot race with the grow's copy loop.  Bumps {@link #growSeq}
     * around the grow so that lock-free {@link #set} writers can detect the overlap.
     */
    private synchronized void growAndSet(int nodeId, VectorFloat<?> vec) {
        if (nodeId >= array.length()) {
            growSeq++;  // now odd: grow in progress
            int newLen = Math.max(array.length() * 2, nodeId + 1);
            AtomicReferenceArray<VectorFloat<?>> na = new AtomicReferenceArray<>(newLen);
            for (int i = 0; i < array.length(); i++) {
                na.set(i, array.get(i));
            }
            array = na; // volatile write: visible to all threads before next set()
            growSeq++;  // back to even: grow done
        }
        array.set(nodeId, vec);
    }

    /**
     * Nulls out the slot at {@code nodeId}, allowing the vector to be garbage-collected.
     * Must be synchronized: without the lock a concurrent {@link #set} grow-and-copy
     * could copy the non-null slot into the new array after this method nulled the old one.
     */
    synchronized void remove(int nodeId) {
        if (nodeId < array.length()) {
            array.set(nodeId, null);
        }
    }

    /** Returns the current backing array length (may grow over time). */
    int length() {
        return array.length();
    }

    /**
     * Shrinks the backing array if less than half is in use.
     * Call after bulk removals (e.g., checkpoint Phase C) to reclaim memory.
     */
    synchronized void compact(int highestActiveNodeId) {
        int newLen = Math.max(256, highestActiveNodeId + 1);
        if (newLen < array.length() / 2) {
            AtomicReferenceArray<VectorFloat<?>> na = new AtomicReferenceArray<>(newLen);
            for (int i = 0; i < newLen && i < array.length(); i++) {
                na.set(i, array.get(i));
            }
            array = na;
        }
    }
}
