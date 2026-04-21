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

import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
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
 * bytes for the re-rank raw vector) in one gRPC call. The default is 16 KiB.
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
        // counter reflects whether the bytes came from memory or from gRPC.
        // With SegmentBlockCache.disabled() containsBlock always returns
        // false, which correctly attributes every read as a miss.
        VectorSearchRequestContext ctx = VectorSearchRequestContext.current();
        boolean wasCached = blockCache.containsBlock(path, bufferOffset, bufferSize);
        long startNanos = System.nanoTime();
        ByteBuf data;
        try {
            data = blockCache.getBlock(path, bufferOffset, bufferSize,
                    (p, off, len) -> fetchBlockFromRemote(p, off, len, bufferIndex));
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
     * the actual {@code readFileRange} gRPC call, updates the client-side
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
     */
    public static class Supplier implements ReaderSupplier {

        private final RemoteFileServiceClient client;
        private final String path;
        private final long totalSize;
        private final int writeBlockSize;
        private final int bufferSize;
        private final StatsLogger statsLogger;
        private final SegmentBlockCache blockCache;

        public Supplier(RemoteFileServiceClient client, String path,
                        long totalSize, int writeBlockSize, int bufferSize,
                        StatsLogger statsLogger,
                        SegmentBlockCache blockCache) {
            this.client = client;
            this.path = path;
            this.totalSize = totalSize;
            this.writeBlockSize = writeBlockSize;
            this.bufferSize = bufferSize;
            this.statsLogger = Objects.requireNonNull(statsLogger,
                    "statsLogger (use NullStatsLogger.INSTANCE to disable)");
            this.blockCache = Objects.requireNonNull(blockCache,
                    "blockCache (use SegmentBlockCache.disabled() to disable)");
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

        @Override
        public RandomAccessReader get() throws IOException {
            return new RemoteRandomAccessReader(client, path, totalSize,
                    writeBlockSize, bufferSize, statsLogger, blockCache);
        }

        @Override
        public void close() throws IOException {
            // client is shared; caller is responsible for closing it
        }
    }
}
