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

package herddb.remote.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * One slab file with fixed-size cells, kept open for the JVM lifetime (issue #475).
 * <p>
 * Replaces the per-object cache file in {@link CachingObjectStorage} with a single,
 * pre-allocated container divided into {@code cellSize}-sized slots. Each cached
 * payload occupies exactly one cell; the in-memory {@code liveSlots} map records
 * the cell index, the actual payload length, and pin/eviction state. Free cells
 * are tracked in a lock-free deque. The file is opened once at construction and
 * closed only at shutdown — no per-object {@code open}/{@code close}/{@code create}/
 * {@code delete} syscalls happen during steady-state operation.
 * <p>
 * The cache is volatile: the slab file is deleted both on construction (a fresh
 * file is created with {@code CREATE_NEW}) and on {@link #close()}. There is no
 * persistent on-disk index, matching the issue's "cache is volatile and rebuilt
 * at every boot" requirement.
 * <p>
 * <b>Concurrency.</b> Reads and evictions race safely without holding any lock
 * across the async I/O. Each {@link Slot} carries a {@code pinCount}
 * ({@link AtomicInteger}) and a {@code volatile boolean evicted} flag.
 * Readers {@code tryPin} (CAS-increment that bails if {@code evicted == true},
 * then re-checks {@code evicted} after the increment); eviction sets
 * {@code evicted = true} <em>before</em> checking {@code pinCount}. The cell is
 * recycled by whichever path observes "{@code pinCount == 0} after the eviction
 * flag is set" — guarded by an {@link AtomicBoolean} so exactly one thread
 * recycles. This guarantees a recycled cell never serves stale bytes to an
 * in-flight reader.
 * <p>
 * <b>Caller contract.</b> {@link #tryStore} takes ownership of one refcount on
 * the supplied {@link ByteBuf} and releases it when the async write completes
 * (success or failure). Callers must serialize concurrent stores under the same
 * key (e.g. via {@link CachingObjectStorage}'s {@code inFlightWrites} dedup);
 * this class does not perform per-key dedup itself.
 *
 * @author enrico.olivelli
 */
public class SlabCacheFileStore implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(SlabCacheFileStore.class.getName());

    /**
     * Live entry handle — one per cached payload. Public so tests can introspect
     * {@link #cellIndex()} / {@link #payloadLength()}; pin/eviction state is
     * package-private (enforced via {@link SlabCacheFileStore} methods).
     */
    public static final class Slot {

        final int cellIndex;
        final int payloadLength;
        final AtomicInteger pinCount = new AtomicInteger();
        volatile boolean evicted;
        final AtomicBoolean recycled = new AtomicBoolean();

        Slot(int cellIndex, int payloadLength) {
            this.cellIndex = cellIndex;
            this.payloadLength = payloadLength;
        }

        public int cellIndex() {
            return cellIndex;
        }

        public int payloadLength() {
            return payloadLength;
        }
    }

    private final Path slabFile;
    private final int cellSize;
    private final int totalCells;
    private final AsynchronousFileChannel channel;
    private final ConcurrentLinkedDeque<Integer> freeCells = new ConcurrentLinkedDeque<>();
    private final ConcurrentHashMap<String, Slot> liveSlots = new ConcurrentHashMap<>();
    private final AtomicLong bytesInUse = new AtomicLong();
    private final AtomicLong admitCount = new AtomicLong();
    private final AtomicLong admitFullCount = new AtomicLong();
    private final AtomicLong evictCount = new AtomicLong();
    private volatile boolean closed;

    /**
     * Creates a fresh slab file at {@code slabFile} with {@code totalCells} cells of
     * {@code cellSize} bytes each. Any pre-existing file at the path is deleted first
     * (the cache is volatile). The file is pre-allocated to {@code totalCells * cellSize}
     * bytes so the OS can pick contiguous extents.
     *
     * Note: the file is sized to {@code totalCells * cellSize} via a one-byte tail
     * write. On ext4/xfs/apfs and other extent-based filesystems this creates a
     * sparse file (the holes materialise as the slab is written into); we do not
     * attempt to force contiguous on-disk extents. The kept-open
     * {@link AsynchronousFileChannel} is what avoids the per-admit
     * {@code open}/{@code close}/{@code create}/{@code delete} syscall cost — not
     * any layout guarantee of the underlying filesystem.
     *
     * @param slabFile path of the slab file (parent dir must exist)
     * @param cellSize cell size in bytes; must be {@code > 0}
     * @param totalCells number of cells; must be {@code >= 0} (zero is legal — the
     *                   tier will simply admit nothing and report full)
     */
    public SlabCacheFileStore(Path slabFile, int cellSize, int totalCells) throws IOException {
        if (cellSize <= 0) {
            throw new IllegalArgumentException("cellSize must be > 0, got " + cellSize);
        }
        if (totalCells < 0) {
            throw new IllegalArgumentException("totalCells must be >= 0, got " + totalCells);
        }
        this.slabFile = slabFile;
        this.cellSize = cellSize;
        this.totalCells = totalCells;
        if (slabFile.getParent() != null) {
            Files.createDirectories(slabFile.getParent());
        }
        Files.deleteIfExists(slabFile);
        this.channel = AsynchronousFileChannel.open(slabFile,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
        if (totalCells > 0) {
            try {
                // Size the file to totalCells * cellSize via a one-byte tail write.
                // AsynchronousFileChannel does not have a truncate-up operation; on
                // common extent-based filesystems this creates a sparse file (holes
                // are realised as the slab is written into).
                ByteBuffer one = ByteBuffer.allocate(1);
                CompletableFuture<Integer> latch = new CompletableFuture<>();
                channel.write(one, (long) totalCells * cellSize - 1, null,
                        new CompletionHandler<Integer, Void>() {
                            @Override
                            public void completed(Integer res, Void att) {
                                latch.complete(res);
                            }

                            @Override
                            public void failed(Throwable exc, Void att) {
                                latch.completeExceptionally(exc);
                            }
                        });
                latch.get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                try {
                    channel.close();
                } catch (IOException ignored) {
                    // Best-effort; we're already throwing.
                }
                throw new IOException("Interrupted while pre-allocating slab file " + slabFile, ie);
            } catch (ExecutionException ee) {
                try {
                    channel.close();
                } catch (IOException ignored) {
                    // Best-effort; we're already throwing.
                }
                Throwable cause = ee.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                throw new IOException("Failed to pre-allocate slab file " + slabFile, ee);
            }
        }
        for (int i = 0; i < totalCells; i++) {
            freeCells.add(i);
        }
        LOGGER.log(Level.INFO,
                "SlabCacheFileStore opened: file={0} cellSize={1} totalCells={2} totalBytes={3}",
                new Object[]{slabFile, cellSize, totalCells, (long) totalCells * cellSize});
    }

    public int cellSize() {
        return cellSize;
    }

    public int totalCells() {
        return totalCells;
    }

    public long bytesInUse() {
        return bytesInUse.get();
    }

    public long admitCount() {
        return admitCount.get();
    }

    public long admitFullCount() {
        return admitFullCount.get();
    }

    public long evictCount() {
        return evictCount.get();
    }

    public int liveCells() {
        return liveSlots.size();
    }

    public int freeCells() {
        return freeCells.size();
    }

    /**
     * Tries to admit {@code content} under {@code key}.
     * <p>
     * Resolves to:
     * <ul>
     *   <li>The newly-published {@link Slot} on success.</li>
     *   <li>{@code null} when the content is larger than {@link #cellSize()} (caller
     *       should fall through to a larger tier or to a per-file fallback).</li>
     *   <li>{@code null} when the tier is full ({@link #admitFullCount()} is incremented
     *       so operators can observe the rate of fall-through).</li>
     *   <li>Exceptionally on I/O failure; the cell is returned to the free list and
     *       the in-memory index is not updated.</li>
     * </ul>
     * The supplied {@link ByteBuf}'s refcount is released when the async write
     * completes (success or failure). Callers must serialize concurrent stores under
     * the same key — see class javadoc.
     */
    public CompletableFuture<Slot> tryStore(String key, ByteBuf content) {
        int len = content.readableBytes();
        if (len > cellSize) {
            content.release();
            return CompletableFuture.completedFuture(null);
        }
        if (closed) {
            content.release();
            CompletableFuture<Slot> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IOException("SlabCacheFileStore is closed: " + slabFile));
            return failed;
        }
        Integer cellIndexBoxed = freeCells.pollFirst();
        if (cellIndexBoxed == null) {
            admitFullCount.incrementAndGet();
            content.release();
            return CompletableFuture.completedFuture(null);
        }
        final int cellIndex = cellIndexBoxed;
        final long offset = (long) cellIndex * cellSize;
        final Slot newSlot = new Slot(cellIndex, len);
        final CompletableFuture<Slot> result = new CompletableFuture<>();

        try {
            ByteBuffer nio = content.nioBuffer(content.readerIndex(), len);
            channel.write(nio, offset, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer bytesWritten, Void attachment) {
                    try {
                        // Publish AFTER the bytes are durable on the channel so a
                        // concurrent reader that wins the race against the index put
                        // can never observe stale or partial bytes from a previously
                        // freed cell.
                        Slot prev = liveSlots.put(key, newSlot);
                        if (prev != null) {
                            // Caller didn't dedup — the previous slot under this key
                            // is now orphaned. Mark it evicted so its cell returns to
                            // the free list (lazily, after any in-flight readers
                            // unpin).
                            markEvictedSlot(prev);
                        }
                        admitCount.incrementAndGet();
                        bytesInUse.addAndGet(len);
                        result.complete(newSlot);
                    } finally {
                        content.release();
                    }
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    try {
                        // Roll back: never published, just return the cell.
                        freeCells.add(cellIndex);
                        result.completeExceptionally(exc);
                    } finally {
                        content.release();
                    }
                }
            });
        } catch (RuntimeException t) {
            // Synchronous failure (e.g. NonWritableChannelException / IllegalArgumentException
            // from AsynchronousFileChannel). ClosedChannelException is reported via the
            // handler's failed() callback, not thrown. Roll back inline.
            freeCells.add(cellIndex);
            content.release();
            result.completeExceptionally(t);
        }
        return result;
    }

    /**
     * Reads the full payload for {@code key}.
     * <p>
     * Resolves to a pooled direct {@link ByteBuf} carrying the payload bytes
     * (caller must release), or {@code null} on miss / concurrent eviction.
     * Resolves exceptionally on I/O failure.
     */
    public CompletableFuture<ByteBuf> read(String key) {
        Slot slot = liveSlots.get(key);
        if (slot == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (!tryPin(slot)) {
            return CompletableFuture.completedFuture(null);
        }
        return readPinned(slot, 0, slot.payloadLength);
    }

    /**
     * Reads at most {@code length} bytes starting at {@code offset} within the
     * payload for {@code key}.
     * <p>
     * Resolves to a pooled direct {@link ByteBuf} carrying the slice (caller
     * must release), or {@code null} on miss / concurrent eviction / out-of-range
     * offset. Resolves exceptionally on I/O failure.
     */
    public CompletableFuture<ByteBuf> readRange(String key, int offset, int length) {
        if (offset < 0 || length < 0) {
            CompletableFuture<ByteBuf> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalArgumentException(
                    "offset/length must be >= 0, got offset=" + offset + " length=" + length));
            return failed;
        }
        Slot slot = liveSlots.get(key);
        if (slot == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (!tryPin(slot)) {
            return CompletableFuture.completedFuture(null);
        }
        if (offset >= slot.payloadLength) {
            unpin(slot);
            return CompletableFuture.completedFuture(null);
        }
        int readable = Math.min(length, slot.payloadLength - offset);
        return readPinned(slot, offset, readable);
    }

    private CompletableFuture<ByteBuf> readPinned(Slot slot, int offsetInPayload, int length) {
        long fileOffset = (long) slot.cellIndex * cellSize + offsetInPayload;
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(length);
        ByteBuffer nio = byteBuf.nioBuffer(0, length);
        CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        try {
            channel.read(nio, fileOffset, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer bytesRead, Void attachment) {
                    // Order matters: unpin BEFORE completing the future so when a
                    // caller's read.get() returns, the slot's pinCount has already
                    // decremented and any racing markEvicted observes it as
                    // unpinned (and recycles synchronously). Otherwise a single-
                    // threaded test that evicts immediately after a successful read
                    // can race the JDK callback's unpin and observe the slot still
                    // pinned, deferring the recycle to a later async unpin and
                    // leaving the next admit with no free cell.
                    try {
                        if (bytesRead != null && bytesRead > 0) {
                            byteBuf.writerIndex(bytesRead);
                        }
                    } finally {
                        unpin(slot);
                    }
                    result.complete(byteBuf);
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    // Same unpin-before-complete contract as the success path.
                    try {
                        byteBuf.release();
                    } finally {
                        unpin(slot);
                    }
                    result.completeExceptionally(exc);
                }
            });
        } catch (RuntimeException t) {
            try {
                byteBuf.release();
            } finally {
                unpin(slot);
            }
            result.completeExceptionally(t);
        }
        return result;
    }

    /**
     * Marks {@code key} as evicted. Pinned readers continue; the cell is recycled
     * at the last unpin (or immediately if no readers are pinned).
     *
     * @return {@code true} if the key was resident and is now scheduled for recycle;
     *         {@code false} if the key was not in the index.
     */
    public boolean markEvicted(String key) {
        Slot slot = liveSlots.remove(key);
        if (slot == null) {
            return false;
        }
        markEvictedSlot(slot);
        return true;
    }

    private void markEvictedSlot(Slot slot) {
        slot.evicted = true;
        if (slot.pinCount.get() == 0) {
            recycle(slot);
        }
    }

    private boolean tryPin(Slot slot) {
        while (true) {
            if (slot.evicted) {
                return false;
            }
            int curr = slot.pinCount.get();
            if (slot.pinCount.compareAndSet(curr, curr + 1)) {
                if (slot.evicted) {
                    // Eviction won the race; back out our increment. The decrement
                    // may now observe pinCount == 0 with evicted == true and trigger
                    // the recycle.
                    unpin(slot);
                    return false;
                }
                return true;
            }
        }
    }

    private void unpin(Slot slot) {
        int now = slot.pinCount.decrementAndGet();
        if (now == 0 && slot.evicted) {
            recycle(slot);
        }
    }

    private void recycle(Slot slot) {
        if (!slot.recycled.compareAndSet(false, true)) {
            return;
        }
        bytesInUse.addAndGet(-slot.payloadLength);
        evictCount.incrementAndGet();
        freeCells.add(slot.cellIndex);
    }

    /** Returns true if {@code key} is currently resident (non-evicted) in this slab. */
    public boolean contains(String key) {
        return liveSlots.containsKey(key);
    }

    /**
     * Snapshot of the live keys. Test-only helper; the returned set is a live view
     * into the underlying concurrent map and is not stable across concurrent ops.
     */
    Set<String> liveKeys() {
        return liveSlots.keySet();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException pending = null;
        try {
            channel.close();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error closing slab channel: " + slabFile, e);
            pending = e;
        }
        try {
            Files.deleteIfExists(slabFile);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Could not delete slab file on close: " + slabFile, e);
            if (pending == null) {
                pending = e;
            }
        }
        if (pending != null) {
            throw pending;
        }
    }
}
