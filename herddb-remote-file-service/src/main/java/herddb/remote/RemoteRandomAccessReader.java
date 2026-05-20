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

package herddb.remote;

import edu.umd.cs.findbugs.annotations.Nullable;
import herddb.index.vector.BulkPrefetchReaderSupplier;
import herddb.index.vector.PinModeReaderSupplier;
import herddb.utils.VectorSearchRequestContext;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@link RandomAccessReader} that reads from a remote multipart file via
 * {@link RemoteFileServiceClient}. Buffers one {@code bufferSize}-sized window
 * at a time to avoid redundant network round-trips when jvector reads
 * sequentially within that window.
 *
 * <p>The buffer size is intentionally decoupled from the multipart write block
 * size (which is a GCS multipart-upload requirement and is typically 4 MiB).
 * For vector-index searches the buffer should be small — see issue #104 —
 * but still large enough to absorb a single jvector logical read (notably
 * {@code OnDiskGraphIndex.getVectorInto}, which reads {@code dimension * 4}
 * bytes for the re-rank raw vector) in one wire round-trip. The default is 16 KiB.
 * {@code writeBlockSize} is still passed to the server because it is what
 * {@code LocalObjectStorage.readRange} uses to locate the on-disk chunk file
 * ({@code blockIndex = offset / writeBlockSize}); changing it would break
 * chunk lookup.
 *
 * <p>Instances are NOT thread-safe (one per reader thread as expected by jvector).
 *
 * @author enrico.olivelli
 */
public class RemoteRandomAccessReader implements RandomAccessReader {

    private final RemoteFileServiceClient client;
    private final String path;
    private final long totalSize;
    private final int writeBlockSize;
    private final int bufferSize;
    private final OpStatsLogger clientReadLatency;
    private final Counter clientReadBytes;
    private final Counter clientReadRequests;
    private final SegmentBlockCache blockCache;
    /**
     * When {@code true}, every block loaded by {@link #ensureBlockLoaded} is
     * inserted into the frontier (pinned) region of the {@link SegmentBlockCache}
     * via {@link SegmentBlockCache#pinBlock} instead of the ordinary
     * {@link SegmentBlockCache#getBlock}. Used by the warmup BFS pass so that
     * entry-frontier Layer-0 blocks survive main-cache eviction pressure.
     * Set to {@code false} on all normal (non-warmup) readers.
     */
    private final boolean pinMode;

    private long position;
    /**
     * Pooled direct ByteBuf holding the currently-cached block. Owned by this reader
     * and released in {@link #close()} (and on every reload). All accessors
     * ({@link #readInt}, {@link #readLong}, {@link #readFloat}, {@link #readFully},
     * {@link #read}) operate at absolute offsets via Netty's getX(int) primitives,
     * which compile to Unsafe memcpy/load — no per-call temporary allocations.
     */
    @Nullable
    private ByteBuf blockBuffer;
    private long bufferedBlockIndex = -1;

    /**
     * Full constructor with separate write block size (routing / chunk lookup),
     * read buffer size (internal window for sequential reads), stats logger,
     * and shared {@link SegmentBlockCache}.
     *
     * <p>Both {@code statsLogger} and {@code blockCache} are mandatory and
     * non-null. Pass {@link NullStatsLogger#INSTANCE} to disable metrics and
     * {@link SegmentBlockCache#disabled()} to disable caching — this keeps
     * the hot path free of null checks.
     *
     * @param writeBlockSize the multipart chunk size used by the writer; must
     *                       be a multiple of the effective buffer size so that
     *                       a buffer window never crosses a chunk boundary
     * @param bufferSize     the read-side buffer window; capped to
     *                       {@code writeBlockSize} if larger
     * @param statsLogger    stats logger for client-side metrics; use
     *                       {@link NullStatsLogger#INSTANCE} to disable
     * @param blockCache     shared block cache; use
     *                       {@link SegmentBlockCache#disabled()} to disable
     */
    public RemoteRandomAccessReader(RemoteFileServiceClient client, String path,
                                    long totalSize, int writeBlockSize, int bufferSize,
                                    StatsLogger statsLogger,
                                    SegmentBlockCache blockCache) {
        this(client, path, totalSize, writeBlockSize, bufferSize, statsLogger, blockCache, false);
    }

    /**
     * Full constructor. All public convenience constructors delegate here.
     *
     * @param pinMode when {@code true} blocks are inserted into the frontier
     *     (pinned) region via {@link SegmentBlockCache#pinBlock}; when
     *     {@code false} blocks use normal eviction via
     *     {@link SegmentBlockCache#getBlock}
     */
    RemoteRandomAccessReader(RemoteFileServiceClient client, String path,
                             long totalSize, int writeBlockSize, int bufferSize,
                             StatsLogger statsLogger,
                             SegmentBlockCache blockCache, boolean pinMode) {
        Objects.requireNonNull(statsLogger, "statsLogger (use NullStatsLogger.INSTANCE to disable)");
        Objects.requireNonNull(blockCache, "blockCache (use SegmentBlockCache.disabled() to disable)");
        if (writeBlockSize <= 0) {
            throw new IllegalArgumentException("writeBlockSize must be > 0, got " + writeBlockSize);
        }
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be > 0, got " + bufferSize);
        }
        int effective = Math.min(bufferSize, writeBlockSize);
        if (writeBlockSize % effective != 0) {
            throw new IllegalArgumentException(
                    "bufferSize (" + bufferSize + ") must divide writeBlockSize ("
                            + writeBlockSize + ")");
        }
        this.client = client;
        this.path = path;
        this.totalSize = totalSize;
        this.writeBlockSize = writeBlockSize;
        this.bufferSize = effective;
        this.blockCache = blockCache;
        this.pinMode = pinMode;

        StatsLogger clientScope = statsLogger.scope("rfs").scope("client");
        this.clientReadLatency = clientScope.getOpStatsLogger("read_latency");
        this.clientReadBytes = clientScope.getCounter("read_bytes");
        this.clientReadRequests = clientScope.getCounter("read_requests");
    }

    /**
     * Convenience constructor without a block cache (uses
     * {@link SegmentBlockCache#disabled()}). A {@code null} stats logger is
     * normalised to {@link NullStatsLogger#INSTANCE}.
     */
    public RemoteRandomAccessReader(RemoteFileServiceClient client, String path,
                                    long totalSize, int writeBlockSize, int bufferSize,
                                    @Nullable StatsLogger statsLogger) {
        this(client, path, totalSize, writeBlockSize, bufferSize,
                statsLogger != null ? statsLogger : NullStatsLogger.INSTANCE,
                SegmentBlockCache.disabled());
    }

    /**
     * Convenience constructor for the case where the caller has no distinct
     * read-buffer size. Equivalent to
     * {@code RemoteRandomAccessReader(client, path, totalSize, blockSize, blockSize, statsLogger)}.
     */
    public RemoteRandomAccessReader(RemoteFileServiceClient client, String path,
                                    long totalSize, int blockSize,
                                    @Nullable StatsLogger statsLogger) {
        this(client, path, totalSize, blockSize, blockSize,
                statsLogger != null ? statsLogger : NullStatsLogger.INSTANCE,
                SegmentBlockCache.disabled());
    }

    /**
     * Convenience constructor without stats logging or caching — suitable for
     * tests and simple callers.
     */
    public RemoteRandomAccessReader(RemoteFileServiceClient client, String path,
                                    long totalSize, int blockSize) {
        this(client, path, totalSize, blockSize, blockSize,
                NullStatsLogger.INSTANCE, SegmentBlockCache.disabled());
    }

    @Override
    public void seek(long offset) throws IOException {
        this.position = offset;
    }

    @Override
    public long getPosition() throws IOException {
        return position;
    }

    @Override
    public long length() throws IOException {
        return totalSize;
    }

    @Override
    public int readInt() throws IOException {
        ensureBlockLoaded();
        int offsetInBlock = (int) (position % bufferSize);
        // Block boundary check: an int never spans two blocks because writeBlockSize
        // is constrained to be a multiple of bufferSize (see constructor); but the
        // logical end-of-file may sit mid-block. Fall back to readFully(byte[4]) only
        // if the int crosses a block boundary inside this buffered window.
        if (offsetInBlock + Integer.BYTES <= blockBuffer.readableBytes()) {
            int v = blockBuffer.getInt(offsetInBlock); // big-endian, matches the previous bit-shift code
            position += Integer.BYTES;
            return v;
        }
        byte[] tmp = new byte[Integer.BYTES];
        readFully(tmp);
        return ((tmp[0] & 0xFF) << 24)
                | ((tmp[1] & 0xFF) << 16)
                | ((tmp[2] & 0xFF) << 8)
                | (tmp[3] & 0xFF);
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public long readLong() throws IOException {
        ensureBlockLoaded();
        int offsetInBlock = (int) (position % bufferSize);
        if (offsetInBlock + Long.BYTES <= blockBuffer.readableBytes()) {
            long v = blockBuffer.getLong(offsetInBlock); // big-endian
            position += Long.BYTES;
            return v;
        }
        byte[] tmp = new byte[Long.BYTES];
        readFully(tmp);
        long v = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            v = (v << 8) | (tmp[i] & 0xFF);
        }
        return v;
    }

    @Override
    public void readFully(byte[] dest) throws IOException {
        int remaining = dest.length;
        int destOffset = 0;
        while (remaining > 0) {
            ensureBlockLoaded();
            int offsetInBlock = (int) (position % bufferSize);
            int available = blockBuffer.readableBytes() - offsetInBlock;
            int toCopy = Math.min(available, remaining);
            // getBytes(int srcIndex, byte[] dst, int dstIndex, int length) is a single
            // Unsafe memcpy from the pooled direct buffer into the heap array.
            blockBuffer.getBytes(offsetInBlock, dest, destOffset, toCopy);
            position += toCopy;
            destOffset += toCopy;
            remaining -= toCopy;
        }
    }

    @Override
    public void readFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            ensureBlockLoaded();
            int offsetInBlock = (int) (position % bufferSize);
            int available = blockBuffer.readableBytes() - offsetInBlock;
            int toCopy = Math.min(available, buffer.remaining());
            // Bounded view of the destination so getBytes copies exactly toCopy bytes
            // without overshooting; no temporary heap array.
            ByteBuffer view = buffer.duplicate();
            view.limit(view.position() + toCopy);
            blockBuffer.getBytes(offsetInBlock, view);
            buffer.position(buffer.position() + toCopy);
            position += toCopy;
        }
    }

    @Override
    public void readFully(long[] vector) throws IOException {
        for (int i = 0; i < vector.length; i++) {
            vector[i] = readLong();
        }
    }

    @Override
    public void read(int[] ints, int offset, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            ints[offset + i] = readInt();
        }
    }

    @Override
    public void read(float[] floats, int offset, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            floats[offset + i] = readFloat();
        }
    }

    @Override
    public void close() throws IOException {
        if (blockBuffer != null) {
            ReferenceCountUtil.safeRelease(blockBuffer);
            blockBuffer = null;
            bufferedBlockIndex = -1;
        }
    }

    /**
     * Non-blocking read of an arbitrary byte range. This override routes the
     * read through {@link SegmentBlockCache#getBlockAsync} so that jvector's
     * 2-slot async pipeline (enabled via
     * {@link io.github.jbellis.jvector.graph.GraphSearcher#setAsyncPipelineEnabled})
     * can overlap IO with similarity computation on the FusedPQ search path.
     *
     * <p><b>Thread-safety contract</b>: this method intentionally does NOT
     * touch {@link #position}, {@link #blockBuffer}, or
     * {@link #bufferedBlockIndex}. Async reads bypass this reader's sliding
     * window so they can safely interleave with synchronous calls on the same
     * reader instance (jvector's contract on
     * {@code RandomAccessReader.readRangeAsync}).
     *
     * <p><b>ByteBuf ownership</b>: each {@link ByteBuf} slice obtained from the
     * block cache is released inside the completion callback (after its bytes
     * have been copied to the returned on-heap {@link ByteBuffer}), so no
     * off-heap memory escapes this method.
     *
     * <p><b>Stats</b>: {@code clientReadRequests}, {@code clientReadBytes},
     * and {@code clientReadLatency} are updated via
     * {@link #fetchBlockFromRemoteAsync} for every cache-miss network call,
     * keeping the Grafana panels consistent with the sync path. The
     * {@link VectorSearchRequestContext} hit/miss/readFileRange counters are
     * updated once per {@code readRangeAsync} invocation on the single-block
     * fast path and per covering block on the multi-block path.
     */
    @Override
    public CompletableFuture<ByteBuffer> readRangeAsync(long offset, int length) {
        if (length <= 0) {
            return CompletableFuture.completedFuture(ByteBuffer.allocate(0));
        }
        if (offset < 0 || offset + length > totalSize) {
            CompletableFuture<ByteBuffer> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IOException(
                    "Read past end of file: offset=" + offset
                            + " length=" + length + " totalSize=" + totalSize));
            return failed;
        }

        long startBlock = offset / bufferSize;
        long endBlock = (offset + length - 1) / bufferSize;

        if (startBlock == endBlock) {
            // ---- Single-block fast path ----
            long blockOff = startBlock * (long) bufferSize;
            int blockLen = (int) Math.min(bufferSize, totalSize - blockOff);

            VectorSearchRequestContext ctx = VectorSearchRequestContext.current();
            boolean wasCached = blockCache.containsBlock(path, blockOff, bufferSize);
            long startNanos = System.nanoTime();

            return blockCache.getBlockAsync(path, blockOff, blockLen,
                    this::fetchBlockFromRemoteAsync)
                    .thenApply(buf -> {
                        long elapsedNanos = System.nanoTime() - startNanos;
                        if (ctx != null) {
                            if (wasCached) {
                                ctx.recordCacheHit();
                            } else {
                                ctx.recordCacheMiss();
                            }
                            ctx.recordReadFileRange((int) Math.min(bufferSize, totalSize - blockOff),
                                    elapsedNanos);
                        }
                        return sliceToHeap(buf, (int) (offset - blockOff), length);
                    });
        }

        // ---- Multi-block path ----
        // Issue all block fetches in parallel; splice the [offset, offset+length)
        // window once all buffers are ready. Each ByteBuf is released in the
        // finally block regardless of success or failure.
        int numBlocks = (int) (endBlock - startBlock + 1);
        // Capture cache-hit status and block metadata before dispatching, so
        // we can update VectorSearchRequestContext after allOf completes.
        VectorSearchRequestContext multiCtx = VectorSearchRequestContext.current();
        boolean[] wasCachedPerBlock = new boolean[numBlocks];
        long[] blockOffPerBlock = new long[numBlocks];
        int[] blockLenPerBlock = new int[numBlocks];
        long multiStartNanos = System.nanoTime();
        @SuppressWarnings("unchecked")
        CompletableFuture<ByteBuf>[] blockFutures = new CompletableFuture[numBlocks];
        for (int i = 0; i < numBlocks; i++) {
            long blockIdx = startBlock + i;
            long blockOff = blockIdx * (long) bufferSize;
            int blockLen = (int) Math.min(bufferSize, totalSize - blockOff);
            blockOffPerBlock[i] = blockOff;
            blockLenPerBlock[i] = blockLen;
            wasCachedPerBlock[i] = blockCache.containsBlock(path, blockOff, bufferSize);
            blockFutures[i] = blockCache.getBlockAsync(path, blockOff, blockLen,
                    this::fetchBlockFromRemoteAsync);
        }

        CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
        CompletableFuture.allOf(blockFutures).whenComplete((ignored, err) -> {
            if (err != null) {
                // Release any buffers that completed successfully before
                // the failure to avoid ByteBuf reference-count leaks.
                for (CompletableFuture<ByteBuf> f : blockFutures) {
                    if (f.isDone() && !f.isCompletedExceptionally() && !f.isCancelled()) {
                        ReferenceCountUtil.safeRelease(f.getNow(null));
                    }
                }
                result.completeExceptionally(err);
                return;
            }
            // Record per-block cache stats for the completed read.
            long multiElapsedNanos = System.nanoTime() - multiStartNanos;
            if (multiCtx != null) {
                for (int i = 0; i < numBlocks; i++) {
                    if (wasCachedPerBlock[i]) {
                        multiCtx.recordCacheHit();
                    } else {
                        multiCtx.recordCacheMiss();
                    }
                    multiCtx.recordReadFileRange(blockLenPerBlock[i], multiElapsedNanos);
                }
            }
            // All futures completed successfully; assemble the output byte array.
            try {
                byte[] out = new byte[length];
                int dstOff = 0;
                for (int i = 0; i < numBlocks; i++) {
                    ByteBuf buf = blockFutures[i].join(); // safe: allOf guarantees done
                    long blockOff = blockOffPerBlock[i];
                    int srcStart = (int) Math.max(0L, offset - blockOff);
                    int srcEnd = (int) Math.min((long) buf.readableBytes(), offset + length - blockOff);
                    int toCopy = srcEnd - srcStart;
                    if (toCopy > 0) {
                        buf.getBytes(srcStart, out, dstOff, toCopy);
                        dstOff += toCopy;
                    }
                }
                result.complete(ByteBuffer.wrap(out));
            } catch (RuntimeException e) {
                // Only RuntimeExceptions can escape buf.getBytes; narrow catch
                // avoids masking programming errors with a generic catch.
                result.completeExceptionally(e);
            } finally {
                // Release every buffer; the heap byte[] copy is already done.
                for (CompletableFuture<ByteBuf> f : blockFutures) {
                    ReferenceCountUtil.safeRelease(f.getNow(null));
                }
            }
        });
        return result;
    }

    /**
     * Async equivalent of {@link #fetchBlockFromRemote}: fires a non-blocking
     * {@code readFileRangeAsByteBufAsync} call, clamps {@code len} to avoid
     * reading past {@link #totalSize}, and updates the client-side counters
     * ({@code rfs_client_read_*}) on completion.
     *
     * <p>A {@code null} result from the client (block not found) is converted to
     * a failed future with {@link IOException}, mirroring the null-check in the
     * synchronous {@link #fetchBlockFromRemote}.
     *
     * <p>Back-pressure ({@code inflightReadBytes} semaphore) is handled
     * transparently inside {@link RemoteFileServiceClient#readFileRangeAsByteBufAsync},
     * so callers do not need to acquire/release it explicitly.
     */
    private CompletableFuture<ByteBuf> fetchBlockFromRemoteAsync(String p, long off, int len) {
        int actualLength = (int) Math.min((long) len, totalSize - off);
        if (actualLength <= 0) {
            CompletableFuture<ByteBuf> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IOException(
                    "Read past end of file: offset=" + off + " totalSize=" + totalSize));
            return failed;
        }
        long startNanos = System.nanoTime();
        return client.readFileRangeAsByteBufAsync(p, off, actualLength, writeBlockSize)
                // thenApply converts a null result to an exceptional completion,
                // mirroring the null-check in the synchronous fetchBlockFromRemote.
                .thenApply(fetched -> {
                    if (fetched == null) {
                        throw new CompletionException(new IOException(
                                "Block not found: path=" + p + " offset=" + off));
                    }
                    clientReadRequests.inc();
                    clientReadBytes.addCount(fetched.readableBytes());
                    return fetched;
                })
                .whenComplete((fetched, err) -> {
                    long elapsedNanos = System.nanoTime() - startNanos;
                    if (err != null) {
                        clientReadLatency.registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                    } else {
                        clientReadLatency.registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                    }
                });
    }

    /**
     * Copies bytes {@code [offsetInBlock, offsetInBlock + length)} from a
     * {@link ByteBuf} into a freshly-allocated on-heap {@link ByteBuffer}, then
     * releases the source buffer. The returned {@link ByteBuffer} is
     * independent of the pooled direct buffer.
     */
    private static ByteBuffer sliceToHeap(ByteBuf src, int offsetInBlock, int length) {
        try {
            byte[] out = new byte[length];
            src.getBytes(offsetInBlock, out);
            return ByteBuffer.wrap(out);
        } finally {
            ReferenceCountUtil.safeRelease(src);
        }
    }

    private void ensureBlockLoaded() throws IOException {
        long bufferIndex = position / bufferSize;
        if (bufferIndex == bufferedBlockIndex && blockBuffer != null) {
            return;
        }
        long bufferOffset = bufferIndex * (long) bufferSize;
        int requestLength = (int) Math.min(bufferSize, totalSize - bufferOffset);
        if (requestLength <= 0) {
            throw new IOException("Read past end of file: position=" + position
                    + " totalSize=" + totalSize);
        }
        // Decide hit vs miss *before* calling getBlock so the per-request
        // counter reflects whether the bytes came from memory or from a wire round-trip.
        // With SegmentBlockCache.disabled() containsBlock always returns
        // false, which correctly attributes every read as a miss.
        VectorSearchRequestContext ctx = VectorSearchRequestContext.current();
        boolean wasCached = blockCache.containsBlock(path, bufferOffset, bufferSize);
        long startNanos = System.nanoTime();
        ByteBuf data;
        try {
            if (pinMode) {
                // Pin-warmup BFS: insert this block into the frontier region so it
                // survives eviction pressure from the much larger main cache.
                data = blockCache.pinBlock(path, bufferOffset, bufferSize,
                        (p, off, len) -> fetchBlockFromRemote(p, off, len, bufferIndex));
            } else {
                data = blockCache.getBlock(path, bufferOffset, bufferSize,
                        (p, off, len) -> fetchBlockFromRemote(p, off, len, bufferIndex));
            }
        } catch (IOException e) {
            if (ctx != null && !wasCached) {
                ctx.recordCacheMiss();
            }
            throw e;
        }
        long elapsedNanos = System.nanoTime() - startNanos;
        if (ctx != null) {
            if (wasCached) {
                ctx.recordCacheHit();
            } else {
                ctx.recordCacheMiss();
            }
            // Even a cache hit represents one "logical read for a search",
            // so count it in the per-request readFileRange accumulator.
            ctx.recordReadFileRange(requestLength, elapsedNanos);
        }
        // Release the previously cached slice (if any) before reassigning.
        if (blockBuffer != null) {
            ReferenceCountUtil.safeRelease(blockBuffer);
        }
        blockBuffer = data;
        bufferedBlockIndex = bufferIndex;
    }

    /**
     * Loader callback invoked by {@link SegmentBlockCache} on a miss. Performs
     * the actual {@code readFileRange} wire round-trip, updates the client-side
     * counters ({@code rfs_client_read_*}), and returns a fresh pooled direct
     * {@link ByteBuf} that the cache (or, in pass-through mode, the caller)
     * takes ownership of.
     *
     * <p>The {@code len} argument is the nominal block size used for cache
     * keying; at end-of-file the actual fetch is clamped to the remaining
     * bytes so we don't request past {@link #totalSize}.
     */
    private ByteBuf fetchBlockFromRemote(String p, long off, int len, long bufferIndex)
            throws IOException {
        int actualLength = (int) Math.min((long) len, totalSize - off);
        if (actualLength <= 0) {
            throw new IOException("Read past end of file: offset=" + off + " totalSize=" + totalSize);
        }
        long startNanos = System.nanoTime();
        ByteBuf fetched;
        try {
            fetched = client.readFileRangeAsByteBuf(p, off, actualLength, writeBlockSize);
        } catch (RuntimeException e) {
            long elapsedNanos = System.nanoTime() - startNanos;
            clientReadLatency.registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
            throw new IOException("readFileRange failed: path=" + p + " offset=" + off
                    + " length=" + actualLength, e);
        }
        long elapsedNanos = System.nanoTime() - startNanos;
        if (fetched == null) {
            clientReadLatency.registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
            throw new IOException("Block not found: path=" + p + " bufferIndex=" + bufferIndex);
        }
        clientReadRequests.inc();
        clientReadBytes.addCount(fetched.readableBytes());
        clientReadLatency.registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
        return fetched;
    }

    /**
     * A {@link ReaderSupplier} that creates {@link RemoteRandomAccessReader} instances
     * for concurrent searcher threads (jvector calls {@code get()} per search thread).
     *
     * <p>To obtain a pin-mode supplier for the frontier-warmup BFS pass, call
     * {@link #withPinMode()}. The returned supplier creates readers that insert
     * every loaded block into the frontier (pinned) region of the block cache so
     * that entry-frontier Layer-0 blocks survive eviction pressure from the main
     * cache.
     */
    public static class Supplier implements ReaderSupplier, PinModeReaderSupplier,
            BulkPrefetchReaderSupplier {

        private static final Logger SUPPLIER_LOGGER =
                Logger.getLogger(Supplier.class.getName());

        private final RemoteFileServiceClient client;
        private final String path;
        private final long totalSize;
        private final int writeBlockSize;
        private final int bufferSize;
        private final StatsLogger statsLogger;
        private final SegmentBlockCache blockCache;
        /**
         * When {@code true}, {@link #get()} returns a pin-mode reader that
         * routes block loads through {@link SegmentBlockCache#pinBlock}.
         */
        private final boolean pinMode;

        public Supplier(RemoteFileServiceClient client, String path,
                        long totalSize, int writeBlockSize, int bufferSize,
                        StatsLogger statsLogger,
                        SegmentBlockCache blockCache) {
            this(client, path, totalSize, writeBlockSize, bufferSize, statsLogger, blockCache, false);
        }

        private Supplier(RemoteFileServiceClient client, String path,
                         long totalSize, int writeBlockSize, int bufferSize,
                         StatsLogger statsLogger,
                         SegmentBlockCache blockCache, boolean pinMode) {
            this.client = client;
            this.path = path;
            this.totalSize = totalSize;
            this.writeBlockSize = writeBlockSize;
            this.bufferSize = bufferSize;
            this.statsLogger = Objects.requireNonNull(statsLogger,
                    "statsLogger (use NullStatsLogger.INSTANCE to disable)");
            this.blockCache = Objects.requireNonNull(blockCache,
                    "blockCache (use SegmentBlockCache.disabled() to disable)");
            this.pinMode = pinMode;
        }

        /**
         * Convenience constructor without a block cache (uses
         * {@link SegmentBlockCache#disabled()}). {@code null} stats logger is
         * normalised to {@link NullStatsLogger#INSTANCE}.
         */
        public Supplier(RemoteFileServiceClient client, String path,
                        long totalSize, int writeBlockSize, int bufferSize,
                        @Nullable StatsLogger statsLogger) {
            this(client, path, totalSize, writeBlockSize, bufferSize,
                    statsLogger != null ? statsLogger : NullStatsLogger.INSTANCE,
                    SegmentBlockCache.disabled());
        }

        /**
         * Convenience constructor that uses the same value for the write block
         * size and the read-buffer size.
         */
        public Supplier(RemoteFileServiceClient client, String path,
                        long totalSize, int blockSize,
                        @Nullable StatsLogger statsLogger) {
            this(client, path, totalSize, blockSize, blockSize,
                    statsLogger != null ? statsLogger : NullStatsLogger.INSTANCE,
                    SegmentBlockCache.disabled());
        }

        /**
         * Convenience constructor without stats logging or caching.
         */
        public Supplier(RemoteFileServiceClient client, String path,
                        long totalSize, int blockSize) {
            this(client, path, totalSize, blockSize, blockSize,
                    NullStatsLogger.INSTANCE, SegmentBlockCache.disabled());
        }

        /**
         * Returns a new {@link Supplier} with the same configuration but with
         * {@code pinMode=true}. Readers produced by the returned supplier route
         * every block load through {@link SegmentBlockCache#pinBlock}, placing
         * the block into the frontier (pinned) region of the cache. Use this
         * for the one-time warmup BFS pass so that entry-frontier Layer-0 blocks
         * are eviction-resistant during subsequent searches.
         *
         * <p>Note: the returned supplier still targets the same
         * {@link SegmentBlockCache}, so pinned blocks are immediately visible to
         * all normal readers sharing the same cache instance.
         */
        public Supplier withPinMode() {
            return new Supplier(client, path, totalSize, writeBlockSize, bufferSize,
                    statsLogger, blockCache, true);
        }

        /**
         * Returns {@code true} when the underlying {@link SegmentBlockCache}
         * has a frontier (pinned) region configured. Callers can skip the
         * {@link #withPinMode()} BFS pass when this returns {@code false} (no
         * frontier budget has been allocated).
         */
        public boolean hasFrontierCacheActive() {
            return blockCache.isFrontierCacheActive();
        }

        /**
         * Bulk-prefetches up to {@code maxBytes} bytes starting at
         * {@code startOffset} from the remote file into the local
         * {@link SegmentBlockCache} (issue #619).
         *
         * <p>Issues one
         * {@link RemoteFileServiceClient#readFileRangeAsByteBufAsync} call per
         * underlying multipart chunk (chunks are {@code writeBlockSize} bytes
         * wide) and dispatches them in parallel. Each completed chunk is fed
         * to {@link SegmentBlockCache#bulkInsert} which slices it into
         * {@code bufferSize}-sized cache blocks. After this method returns,
         * subsequent reads via a normal {@link RemoteRandomAccessReader}
         * targeting the same {@code (path, bufferSize)} key space hit the
         * cache for every covered block — no further wire I/O.
         *
         * <p>If the cache is disabled
         * ({@code blockCache.isActive() == false}) the call is a no-op:
         * caching is off, so populating it would be pointless.
         *
         * <p>If the prefetch returns a {@code null} buffer for any chunk
         * (file deleted underneath us, or not yet uploaded) the call fails
         * with {@link IOException}; callers must treat this as best-effort
         * and continue with the per-block fallback path. All successfully
         * fetched buffers are released on the failure branch so no off-heap
         * memory leaks.
         *
         * @param startOffset starting byte offset; must be {@code >= 0} and
         *     aligned to {@code writeBlockSize}
         * @param maxBytes    upper bound on the number of bytes to read;
         *     clamped to {@code totalSize - startOffset}
         * @return number of bytes actually inserted into the cache
         * @throws IOException if any underlying read fails
         */
        @Override
        public long bulkPrefetchIntoCache(long startOffset, long maxBytes) throws IOException {
            if (!blockCache.isActive() || maxBytes <= 0) {
                return 0;
            }
            if (startOffset < 0) {
                throw new IllegalArgumentException(
                        "startOffset must be >= 0, got " + startOffset);
            }
            if (startOffset >= totalSize) {
                return 0;
            }
            if (startOffset % writeBlockSize != 0) {
                throw new IllegalArgumentException(
                        "startOffset must be aligned to multipart chunk size "
                                + writeBlockSize + ", got " + startOffset);
            }
            long endOffset = Math.min(totalSize, startOffset + maxBytes);
            if (endOffset <= startOffset) {
                return 0;
            }

            // Build one per-chunk request: chunks are writeBlockSize-aligned and
            // never span a multipart-chunk boundary (the storage backend forbids
            // it; readFileRangeAsByteBufAsync's recursive splitter only handles
            // a single boundary so we have to align here explicitly).
            List<CompletableFuture<ByteBuf>> futures = new ArrayList<>();
            List<Long> offsets = new ArrayList<>();
            long off = startOffset;
            while (off < endOffset) {
                long nextBoundary = ((off / writeBlockSize) + 1L) * writeBlockSize;
                long chunkEnd = Math.min(endOffset, nextBoundary);
                int len = (int) (chunkEnd - off);
                offsets.add(off);
                futures.add(client.readFileRangeAsByteBufAsync(path, off, len, writeBlockSize));
                off = chunkEnd;
            }

            long startNanos = System.nanoTime();
            try {
                // Block the calling thread (typically the checkpoint / compaction
                // worker that owns warmUpNewSegmentsBeforePublish) until every
                // chunk has resolved. We *want* to block: the warmup contract is
                // synchronous and the caller is on a dedicated executor.
                CompletableFuture.allOf(
                                futures.toArray(new CompletableFuture[0]))
                        .join();
            } catch (CompletionException ce) {
                // Drain any chunk that completed successfully before the failure.
                releaseCompletedFutures(futures);
                Throwable cause = ce.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                throw new IOException("Bulk prefetch failed for path=" + path
                        + " startOffset=" + startOffset, cause);
            }

            long insertedBytes = 0;
            // Track which futures still own a buffer that we need to release on
            // a failure mid-iteration (e.g., a null result from the server).
            int nextToConsume = 0;
            try {
                for (int i = 0; i < futures.size(); i++) {
                    ByteBuf data = futures.get(i).getNow(null);
                    if (data == null) {
                        // null = "block not found" — file deleted or not yet
                        // uploaded. Surface as an IOException; the warmup path
                        // treats it as best-effort and falls back to BFS.
                        nextToConsume = i + 1;
                        throw new IOException("Bulk prefetch: server returned null"
                                + " path=" + path + " offset=" + offsets.get(i));
                    }
                    // bulkInsert takes ownership of `data` and releases it.
                    long chunkOff = offsets.get(i);
                    nextToConsume = i + 1;
                    insertedBytes += blockCache.bulkInsert(path, chunkOff, bufferSize, data);
                }
                if (insertedBytes > 0) {
                    SUPPLIER_LOGGER.log(Level.FINE,
                            "bulkPrefetchIntoCache path={0}: inserted {1} bytes "
                                    + "({2} chunks) in {3} ms",
                            new Object[]{path, insertedBytes, futures.size(),
                                    (System.nanoTime() - startNanos) / 1_000_000L});
                }
                return insertedBytes;
            } catch (IOException | RuntimeException e) {
                // Release any chunk still owned by a pending future. Anything
                // already passed to bulkInsert has been consumed there.
                for (int i = nextToConsume; i < futures.size(); i++) {
                    CompletableFuture<ByteBuf> f = futures.get(i);
                    if (f.isDone() && !f.isCompletedExceptionally() && !f.isCancelled()) {
                        ReferenceCountUtil.safeRelease(f.getNow(null));
                    }
                }
                throw e;
            }
        }

        /**
         * Release every successfully-completed chunk buffer in {@code futures}
         * — used on the failure path of
         * {@link #bulkPrefetchIntoCache(long, long)} so that buffers fetched
         * before the failing chunk do not leak.
         */
        private static void releaseCompletedFutures(List<CompletableFuture<ByteBuf>> futures) {
            for (CompletableFuture<ByteBuf> f : futures) {
                if (f.isDone() && !f.isCompletedExceptionally() && !f.isCancelled()) {
                    ReferenceCountUtil.safeRelease(f.getNow(null));
                }
            }
        }

        @Override
        public RandomAccessReader get() throws IOException {
            return new RemoteRandomAccessReader(client, path, totalSize,
                    writeBlockSize, bufferSize, statsLogger, blockCache, pinMode);
        }

        @Override
        public void close() throws IOException {
            // client is shared; caller is responsible for closing it
        }
    }
}
