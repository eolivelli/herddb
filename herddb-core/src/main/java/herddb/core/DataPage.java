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

package herddb.core;

import herddb.model.Record;
import herddb.utils.Bytes;
import herddb.utils.HerdDBByteBufAllocators;
import herddb.utils.ObjectSizeUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.lang.ref.Cleaner;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A page of data loaded in memory.
 *
 * <h3>Off-heap value storage (issue #399)</h3>
 * Immutable pages built from disk reads pack their {@code Record.value} bytes
 * into a single off-heap slab allocated from
 * {@link HerdDBByteBufAllocators#dataPagesAllocator()}. Each {@code Record}'s
 * value is a non-owning {@link Bytes#fromSharedSlab} view into the slab, so the
 * 17-million {@code byte[]} arrays that previously dominated the JVM heap on
 * bigann-class workloads now live in pooled native memory.
 *
 * <p>Lifecycle (Cleaner-anchored):
 * <ul>
 *   <li>{@code DataPage} owns one refcount on the slab.</li>
 *   <li>Each shared-slab {@code Bytes} retains a strong reference to
 *       {@code this} (the {@code slabOwner} anchor) so the JDK
 *       {@code Cleaner} attached to the page does not run while any reader
 *       still holds a {@code Record} extracted from the page.</li>
 *   <li>When {@code DataPage} and every {@code Bytes} on its slab become
 *       unreachable, the {@code Cleanable} releases the slab once and the
 *       memory returns to the pooled allocator.</li>
 * </ul>
 * No use-after-free is possible under this design because the slab can only
 * be released after every reader has dropped its references.
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class DataPage extends Page<TableManager> {

    private static final Logger LOGGER = Logger.getLogger(DataPage.class.getName());

    /**
     * Single shared {@link Cleaner} for all {@code DataPage} slab releases.
     * One Cleaner thread is enough; using a single shared instance avoids
     * the overhead of one daemon thread per page.
     */
    private static final Cleaner SLAB_CLEANER = Cleaner.create(r -> {
        Thread t = new Thread(r, "herddb-datapage-slab-cleaner");
        t.setDaemon(true);
        return t;
    });

    /**
     * Off-heap-pack only when the records' aggregate value bytes are at
     * least this many bytes. Below the threshold the per-record overhead
     * (slice metadata + Bytes object) outweighs the off-heap savings.
     */
    static final long OFFHEAP_VALUE_BYTES_THRESHOLD = 4 * 1024L;

    /**
     * Constant entry size accounting for HashMap$Node overhead.
     * <p>
     * HashMap$Node: header + hash(int) + key ref + value ref + next ref.
     * With compressed oops: 12 + 4 + 4 + 4 + 4 + 4(padding) = 32 bytes.
     * Without compressed oops: 16 + 4 + 4(padding) + 8 + 8 + 8 = 48 bytes.
     */
    public static final long CONSTANT_ENTRY_BYTE_SIZE = ObjectSizeUtils.COMPRESSED_OOPS ? 32L : 48L;

    public static long estimateEntrySize(Bytes key, byte[] value) {
        return Record.estimateSize(key, value) + DataPage.CONSTANT_ENTRY_BYTE_SIZE;
    }

    public static long estimateEntrySize(Bytes key, Bytes value) {
        return Record.estimateSize(key, value) + DataPage.CONSTANT_ENTRY_BYTE_SIZE;
    }

    public static long estimateEntrySize(Record record) {
        return record.getEstimatedSize() + DataPage.CONSTANT_ENTRY_BYTE_SIZE;
    }

    public final long maxSize;
    public final boolean immutable;

    private final Map<Bytes, Record> data;

    private final AtomicLong usedMemory;

    /**
     * Off-heap slab carrying every {@code Record.value} for an immutable
     * page that was slab-packed via
     * {@link #buildSlabPackedImmutable(TableManager, long, long, List)}.
     * {@code null} for on-heap-only pages (mutable pages, on-heap immutable
     * pages, or immutable pages whose values were too small to pack).
     */
    private final ByteBuf valueSlab;

    /**
     * Cleanup hook released by the JDK {@link Cleaner} when this page (and
     * every shared-slab {@code Bytes} that anchors it) becomes
     * GC-unreachable. {@code null} for pages without a slab.
     */
    @SuppressWarnings("PMD.UnusedPrivateField")
    private final Cleaner.Cleanable slabCleanable;

    /**
     * Access lock, exists only for mutable pages ({@code immutable == false})
     */
    public final ReadWriteLock pageLock;

    /**
     * Writability flag of mutable pages, mutable pages can switch to a not
     * writable state when flushed, to be accessed only under {@link #pageLock}
     */
    public boolean writable;

    protected DataPage(TableManager owner, long pageId, long maxSize, long estimatedSize, Map<Bytes, Record> data, boolean immutable) {
        this(owner, pageId, maxSize, estimatedSize, data, immutable, null);
    }

    private DataPage(TableManager owner, long pageId, long maxSize, long estimatedSize,
                     Map<Bytes, Record> data, boolean immutable, ByteBuf valueSlab) {
        super(owner, pageId);
        this.maxSize = maxSize;
        this.immutable = immutable;
        this.writable = !immutable;

        this.data = data;
        this.usedMemory = new AtomicLong(estimatedSize);

        pageLock = immutable ? null : new ReentrantReadWriteLock(false);

        this.valueSlab = valueSlab;
        if (valueSlab != null) {
            // Capture the slab into the cleanup action without keeping `this`
            // referenced — a Cleaner.Cleanable that captures its anchor would
            // never fire. The slab's refcount is initialised to 1 in
            // buildSlabPackedImmutable; the Cleanable releases it once.
            final ByteBuf slabRef = valueSlab;
            this.slabCleanable = SLAB_CLEANER.register(this, () -> {
                if (slabRef.refCnt() > 0) {
                    slabRef.release();
                } else if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.log(Level.FINE,
                            "DataPage slab cleanup observed already-released slab; "
                                    + "this is benign if releaseSlabEagerly() ran first");
                }
            });
        } else {
            this.slabCleanable = null;
        }
    }

    /**
     * Builds an immutable {@link DataPage} whose {@code Record.value} bytes
     * are packed into a single off-heap slab from
     * {@link HerdDBByteBufAllocators#dataPagesAllocator()}.
     *
     * <p>If the records' total value bytes are below
     * {@link #OFFHEAP_VALUE_BYTES_THRESHOLD} the page is built with
     * on-heap values (matching the previous behaviour and avoiding the
     * slab overhead for tiny pages).
     *
     * <p>Each shared-slab {@code Bytes} returned by
     * {@link Bytes#fromSharedSlab(ByteBuf, int, int, Object)} retains a
     * strong reference to the returned {@code DataPage} via the
     * {@code slabOwner} anchor; this is the use-after-free guard described
     * in the class-level Javadoc.
     */
    public static DataPage buildSlabPackedImmutable(TableManager owner, long pageId, long maxSize,
                                                     List<Record> records) {
        long totalValueBytes = 0L;
        for (Record r : records) {
            totalValueBytes += r.value.getLength();
        }
        if (totalValueBytes < OFFHEAP_VALUE_BYTES_THRESHOLD || totalValueBytes > Integer.MAX_VALUE) {
            // Fall back to the on-heap path: tiny pages (overhead dominates)
            // or pathologically large pages (slab won't fit a 32-bit ByteBuf
            // capacity) keep their byte[]-backed Bytes values.
            return buildOnHeapImmutable(owner, pageId, maxSize, records);
        }

        PooledByteBufAllocator allocator = HerdDBByteBufAllocators.dataPagesAllocator();
        // Allocate as direct memory so the bytes really live off-heap. The
        // slab's refcount is 1 here; the Cleanable in the constructor below
        // releases it exactly once when the page (and every shared-slab
        // Bytes anchored to it) becomes GC-unreachable.
        ByteBuf slab = allocator.directBuffer((int) totalValueBytes);
        try {
            Map<Bytes, Record> map = new HashMap<>(records.size());
            // We need a temporary reference to the page so each Record's
            // value can anchor against it. Build the page after populating
            // the map. To do this we use a small holder that we set later.
            DataPageHolder holder = new DataPageHolder();
            int writePos = 0;
            long estimatedPageSize = 0L;
            for (Record r : records) {
                int valueLen = r.value.getLength();
                if (valueLen > 0) {
                    slab.writeBytes(r.value.getBuffer(), r.value.getOffset(), valueLen);
                }
                Bytes offHeapValue = Bytes.fromSharedSlab(slab, writePos, valueLen, holder);
                Record packed = new Record(r.key, offHeapValue);
                map.put(packed.key, packed);
                estimatedPageSize += DataPage.estimateEntrySize(packed);
                writePos += valueLen;
            }
            DataPage page = new DataPage(owner, pageId, maxSize, estimatedPageSize, map, true, slab);
            holder.page = page;
            return page;
        } catch (RuntimeException t) {
            // Allocation succeeded; subsequent failure must release the slab
            // so we don't leak pooled memory.
            slab.release();
            throw t;
        }
    }

    /**
     * Builds an immutable {@link DataPage} with on-heap {@code Record.value}
     * bytes — the legacy behaviour, used as the fallback by
     * {@link #buildSlabPackedImmutable} for tiny / oversized pages.
     */
    public static DataPage buildOnHeapImmutable(TableManager owner, long pageId, long maxSize,
                                                 List<Record> records) {
        Map<Bytes, Record> map = new HashMap<>(records.size());
        long estimatedPageSize = 0L;
        for (Record r : records) {
            map.put(r.key, r);
            estimatedPageSize += DataPage.estimateEntrySize(r);
        }
        return new DataPage(owner, pageId, maxSize, estimatedPageSize, map, true);
    }

    /**
     * Holder that lets {@link #buildSlabPackedImmutable} hand each
     * shared-slab {@code Bytes} a reference that resolves to the
     * {@code DataPage} once construction completes. The {@code Bytes} only
     * needs an opaque {@code Object} for its GC-anchor role, so the holder
     * itself is sufficient — Java keeps the holder alive as long as any
     * {@code Bytes} references it, and the holder transitively keeps the
     * {@code DataPage} alive.
     */
    private static final class DataPageHolder {
        // package-private to allow read by tests if needed.
        volatile DataPage page;
    }

    /**
     * Returns {@code true} if this page packs its values into an off-heap
     * slab (issue #399). Used by metrics and tests.
     */
    public boolean hasOffHeapValueSlab() {
        return valueSlab != null;
    }

    /**
     * Returns the slab's current refcount, or 0 when this page is on-heap.
     * Test-only helper.
     */
    int slabRefCntForTesting() {
        return valueSlab == null ? 0 : valueSlab.refCnt();
    }

    /**
     * Returns the slab's allocated capacity in bytes, or 0 for on-heap pages.
     * Used by the {@code table_buffers_offheap_bytes} metric in step 3.
     */
    public long offHeapBytes() {
        return valueSlab == null ? 0L : valueSlab.capacity();
    }

    /**
     * Convert a {@link DataPage} to immutable.
     * <p>
     * Conversion can be done only after the page has been set as not writable.
     * </p>
     * <p>
     * Checks on immutable pages are faster (they not have to check volatile writable flag and lock it
     * before checking).
     * </p>
     *
     * @return immutable data page version
     */
    DataPage toImmutable() {

        if (immutable) {
            throw new IllegalStateException("page " + pageId + " already is immutable!");
        }

        if (writable) {
            throw new IllegalStateException("page " + pageId + " cannot be converted to immutable because still writable!");
        }

        return new DataPage(owner, pageId, maxSize, usedMemory.get(), data, true);

    }

    Record remove(Bytes key) {
        if (immutable) {
            throw new IllegalStateException("page " + pageId + " is immutable!");
        }

        final Record prev = data.remove(key);
        if (prev != null) {
            final long size = estimateEntrySize(prev);
            usedMemory.addAndGet(-size);
        }

        return prev;
    }

    public Record get(Bytes key) {
        return data.get(key);
    }

    boolean put(Record record) {
        if (immutable) {
            throw new IllegalStateException("page " + pageId + " is immutable!");
        }

        final Record prev = data.put(record.key, record);

        final long newSize = estimateEntrySize(record);
        if (newSize > maxSize) {
            throw new IllegalStateException(
                    "record too big to fit in any page " + newSize + " / " + maxSize + " bytes");
        }

        final long diff = prev == null
                ? newSize : newSize - estimateEntrySize(prev);

        final long target = maxSize - diff;

        final long old = usedMemory.getAndAccumulate(diff, (curr, change) -> curr > target ? curr : curr + diff);

        if (old > target) {
            /* Restore the previous record if there was one, otherwise remove.
             * Simply removing would lose the old record, creating a window where
             * keyToPage points to this page but the record is absent — which causes
             * "Inconsistency! no record in memory" errors during concurrent reads. */
            if (prev != null) {
                data.put(record.key, prev);
            } else {
                data.remove(record.key);
            }

            return false;
        }

        return true;
    }

    void putNoMemoryHandle(Record record) {
        if (immutable) {
            throw new IllegalStateException("page " + pageId + " is immutable!");
        }
        data.put(record.key, record);
    }

    void removeNoMemoryHandle(Record record) {
        if (immutable) {
            throw new IllegalStateException("page " + pageId + " is immutable!");
        }
        data.remove(record.key);
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public int size() {
        return data.size();
    }

    public Collection<Record> getRecordsForFlush() {
        return data.values();
    }

    public Set<Bytes> getKeysForDebug() {
        return data.keySet();
    }

    public long getUsedMemory() {
        return usedMemory.get();
    }

    /**
     * Companion method of {@link #putNoMemoryHandle(Record)} and {@link #removeNoMemoryHandle(Record)}
     * to handle memory counts externally.
     *
     * @param usedMemory used memory count to set
     */
    void setUsedMemory(long usedMemory) {
        this.usedMemory.set(usedMemory);
    }

    void clear() {
        if (immutable) {
            throw new IllegalStateException("page " + pageId + " is immutable!");
        }
        data.clear();
        usedMemory.set(0);
    }

    @Override
    public String toString() {
        return "DataPage{" + "pageId=" + pageId + ", immutable=" + immutable + ", writable=" + writable + ", usedMemory=" + usedMemory + '}';
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        return prime + (int) (pageId ^ (pageId >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DataPage other = (DataPage) obj;
        return pageId == other.pageId;
    }

    /**
     * Page deep equality. It checks every record in the page.
     */
    boolean deepEquals(DataPage other) {
        if (!equals(other)) {
            return false;
        }
        return data.equals(other.data);
    }

    void flushRecordsCache() {
        data.values().forEach(r -> r.clearCache());
    }


}
