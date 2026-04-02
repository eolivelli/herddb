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
 *   <li>{@link #set(int, VectorFloat)} is synchronized only to handle array growth, which
 *       is rare (O(log N) times over the lifetime of the index).  Once the array is large
 *       enough the synchronized block exits immediately after the volatile array write.</li>
 *   <li>{@link #remove(int)} is synchronized for the same reason: a concurrent grow-and-copy
 *       inside {@code set} would copy the not-yet-nulled slot into the new array, leaking
 *       the reference.  Synchronizing remove prevents that window.</li>
 * </ul>
 *
 * <p>Write-before-read ordering guarantee: callers must call {@code set(nodeId, vec)} before
 * passing {@code nodeId} to {@code GraphIndexBuilder.addGraphNode()}.  The synchronized
 * write in {@code set} establishes happens-before with any subsequent volatile read in
 * {@code get}, satisfying JLS 17.4.4.
 */
@SuppressFBWarnings("UG_SYNC_SET_UNSYNC_GET")
class VectorStorage {

    private volatile AtomicReferenceArray<VectorFloat<?>> array;

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
     */
    synchronized void set(int nodeId, VectorFloat<?> vec) {
        if (nodeId >= array.length()) {
            int newLen = Math.max(array.length() * 2, nodeId + 1);
            AtomicReferenceArray<VectorFloat<?>> na = new AtomicReferenceArray<>(newLen);
            for (int i = 0; i < array.length(); i++) {
                na.set(i, array.get(i));
            }
            array = na; // volatile write: visible to all threads before next set()
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
