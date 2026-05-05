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

package herddb.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.lang.ref.Cleaner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Owns an off-heap {@link ByteBuf} slab carrying packed key bytes for an
 * index node (BLink, BRIN, or any future structure that needs to keep many
 * small {@code byte[]}s alive in memory).
 *
 * <h3>Lifecycle (Cleaner-anchored)</h3>
 * <ul>
 *   <li>One refcount on the slab is owned by this {@code IndexKeySlab}.</li>
 *   <li>Each shared-slab {@link Bytes} returned by
 *       {@link #wrap(int, int)} retains a strong reference to this
 *       {@code IndexKeySlab} (via the {@code slabOwner} anchor on
 *       {@link Bytes#fromSharedSlab(ByteBuf, int, int, Object)}). As long
 *       as any {@code Bytes} is reachable, the slab stays alive.</li>
 *   <li>When this {@code IndexKeySlab} and every {@code Bytes} it produced
 *       become GC-unreachable, the JDK {@link Cleaner} attached to it
 *       releases the slab once and the memory returns to the pooled
 *       allocator.</li>
 * </ul>
 * No use-after-free is possible under this design because the slab can only
 * be released after every reader has dropped its references.
 *
 * <p>Typical use, called once per index-page load:
 * <pre>
 *   IndexKeySlab owner = new IndexKeySlab(totalKeyBytes,
 *           HerdDBByteBufAllocators.indexPagesAllocator());
 *   for (each key) {
 *       int off = owner.append(keyBytes);
 *       Bytes offHeapKey = owner.wrap(off, keyBytes.length);
 *       map.put(offHeapKey, value);
 *   }
 * </pre>
 */
@SuppressFBWarnings("URF_UNREAD_FIELD")
public final class IndexKeySlab {

    private static final Logger LOGGER = Logger.getLogger(IndexKeySlab.class.getName());

    /**
     * Single shared {@link Cleaner} for all index-key slab releases. One
     * Cleaner thread is enough across all live slabs.
     */
    private static final Cleaner SLAB_CLEANER = Cleaner.create(r -> {
        Thread t = new Thread(r, "herddb-index-key-slab-cleaner");
        t.setDaemon(true);
        return t;
    });

    /**
     * Pack only when the aggregate key bytes are at least this many bytes.
     * Below the threshold the per-key overhead (slice metadata + Bytes
     * object) outweighs the off-heap savings.
     */
    public static final long OFFHEAP_KEY_BYTES_THRESHOLD = 4 * 1024L;

    private final ByteBuf slab;

    /**
     * Cleanup hook released by the JDK {@link Cleaner} when this
     * {@code IndexKeySlab} (and every shared-slab {@code Bytes} that
     * anchors it) becomes GC-unreachable. Field is unread on the
     * production code path; SpotBugs annotation justifies it.
     */
    private final Cleaner.Cleanable cleanable;

    private int writePos;

    /**
     * Allocates a slab of {@code totalKeyBytes} bytes from {@code allocator}
     * (typically {@link HerdDBByteBufAllocators#indexPagesAllocator()}).
     * The slab's refcount is initialised to 1; the JDK Cleaner releases it
     * once when this {@code IndexKeySlab} becomes GC-unreachable.
     *
     * @throws IllegalArgumentException if {@code totalKeyBytes} is negative
     *         or exceeds {@link Integer#MAX_VALUE}.
     */
    public IndexKeySlab(long totalKeyBytes, PooledByteBufAllocator allocator) {
        if (totalKeyBytes < 0L || totalKeyBytes > (long) Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "totalKeyBytes out of range: " + totalKeyBytes);
        }
        if (allocator == null) {
            throw new NullPointerException("allocator");
        }
        this.slab = allocator.directBuffer((int) totalKeyBytes);
        final ByteBuf slabRef = this.slab;
        this.cleanable = SLAB_CLEANER.register(this, () -> {
            if (slabRef.refCnt() > 0) {
                slabRef.release();
            } else if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.log(Level.FINE,
                        "IndexKeySlab cleanup observed already-released slab; "
                                + "no eager-release path exists today, "
                                + "investigate if this log appears.");
            }
        });
        this.writePos = 0;
    }

    /**
     * Appends {@code key.length} bytes to the slab and returns the offset
     * at which they were written.
     *
     * @return the slab-relative offset where the key was placed; pair this
     *         with {@code key.length} to call {@link #wrap(int, int)}.
     */
    public int append(byte[] key) {
        return append(key, 0, key.length);
    }

    /**
     * Appends {@code length} bytes from {@code key} starting at {@code offset}.
     */
    public int append(byte[] key, int offset, int length) {
        int writtenAt = writePos;
        if (length > 0) {
            slab.writeBytes(key, offset, length);
        }
        writePos += length;
        return writtenAt;
    }

    /**
     * Returns a non-owning shared-slab {@link Bytes} view into the slab at
     * {@code (offset, length)}, anchored against this {@code IndexKeySlab}
     * so the JDK Cleaner does not run while the {@code Bytes} is alive.
     */
    public Bytes wrap(int offset, int length) {
        return Bytes.fromSharedSlab(slab, offset, length, this);
    }

    /**
     * Returns the slab's allocated capacity in bytes, used by metrics and
     * accounting.
     */
    public int slabCapacity() {
        return slab.capacity();
    }

    /**
     * Test-only helper.
     */
    int slabRefCntForTesting() {
        return slab.refCnt();
    }
}
