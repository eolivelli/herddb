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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.bookkeeper.stats.Counter;
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
    @Nullable
    private final OpStatsLogger clientReadLatency;
    @Nullable
    private final Counter clientReadBytes;
    @Nullable
    private final Counter clientReadRequests;

    private long position;
    private byte[] blockBuffer;
    private long bufferedBlockIndex = -1;

    /**
     * Full constructor with separate write block size (routing / chunk lookup)
     * and read buffer size (internal window for sequential reads).
     *
     * @param writeBlockSize the multipart chunk size used by the writer; must
     *                       be a multiple of the effective buffer size so that
     *                       a buffer window never crosses a chunk boundary
     * @param bufferSize     the read-side buffer window; capped to
     *                       {@code writeBlockSize} if larger
     * @param statsLogger    optional stats logger for client-side metrics (nullable)
     */
    public RemoteRandomAccessReader(RemoteFileServiceClient client, String path,
                                    long totalSize, int writeBlockSize, int bufferSize,
                                    @Nullable StatsLogger statsLogger) {
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

        if (statsLogger != null) {
            StatsLogger clientScope = statsLogger.scope("rfs").scope("client");
            this.clientReadLatency = clientScope.getOpStatsLogger("read_latency");
            this.clientReadBytes = clientScope.getCounter("read_bytes");
            this.clientReadRequests = clientScope.getCounter("read_requests");
        } else {
            this.clientReadLatency = null;
            this.clientReadBytes = null;
            this.clientReadRequests = null;
        }
    }

    /**
     * Convenience constructor for the case where the caller has no distinct
     * read-buffer size. Equivalent to
     * {@code RemoteRandomAccessReader(client, path, totalSize, blockSize, blockSize, statsLogger)}.
     */
    public RemoteRandomAccessReader(RemoteFileServiceClient client, String path,
                                    long totalSize, int blockSize,
                                    @Nullable StatsLogger statsLogger) {
        this(client, path, totalSize, blockSize, blockSize, statsLogger);
    }

    /**
     * Convenience constructor without stats logging (backward compatibility).
     * Equivalent to {@code RemoteRandomAccessReader(client, path, totalSize, blockSize, blockSize, null)}.
     */
    public RemoteRandomAccessReader(RemoteFileServiceClient client, String path,
                                    long totalSize, int blockSize) {
        this(client, path, totalSize, blockSize, blockSize, null);
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
        byte[] buf = new byte[4];
        readFully(buf);
        return ((buf[0] & 0xFF) << 24)
                | ((buf[1] & 0xFF) << 16)
                | ((buf[2] & 0xFF) << 8)
                | (buf[3] & 0xFF);
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public long readLong() throws IOException {
        byte[] buf = new byte[8];
        readFully(buf);
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (buf[i] & 0xFF);
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
            int available = blockBuffer.length - offsetInBlock;
            int toCopy = Math.min(available, remaining);
            System.arraycopy(blockBuffer, offsetInBlock, dest, destOffset, toCopy);
            position += toCopy;
            destOffset += toCopy;
            remaining -= toCopy;
        }
    }

    @Override
    public void readFully(ByteBuffer buffer) throws IOException {
        byte[] tmp = new byte[buffer.remaining()];
        readFully(tmp);
        buffer.put(tmp);
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
        byte[] buf = new byte[count * 4];
        readFully(buf);
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN);
        for (int i = 0; i < count; i++) {
            floats[offset + i] = bb.getFloat();
        }
    }

    @Override
    public void close() throws IOException {
        // stateless; nothing to close
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
        if (clientReadRequests != null) {
            clientReadRequests.inc();
        }
        long startNanos = System.nanoTime();
        byte[] data = client.readFileRange(path, bufferOffset, requestLength, writeBlockSize);
        long elapsedNanos = System.nanoTime() - startNanos;
        if (data == null) {
            if (clientReadLatency != null) {
                clientReadLatency.registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
            }
            throw new IOException("Block not found: path=" + path + " bufferIndex=" + bufferIndex);
        }
        if (clientReadLatency != null) {
            clientReadLatency.registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
        }
        if (clientReadBytes != null) {
            clientReadBytes.add((long) data.length);
        }
        blockBuffer = data;
        bufferedBlockIndex = bufferIndex;
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
        @Nullable
        private final StatsLogger statsLogger;

        public Supplier(RemoteFileServiceClient client, String path,
                        long totalSize, int writeBlockSize, int bufferSize,
                        @Nullable StatsLogger statsLogger) {
            this.client = client;
            this.path = path;
            this.totalSize = totalSize;
            this.writeBlockSize = writeBlockSize;
            this.bufferSize = bufferSize;
            this.statsLogger = statsLogger;
        }

        /**
         * Convenience constructor that uses the same value for the write block
         * size and the read-buffer size.
         */
        public Supplier(RemoteFileServiceClient client, String path,
                        long totalSize, int blockSize,
                        @Nullable StatsLogger statsLogger) {
            this(client, path, totalSize, blockSize, blockSize, statsLogger);
        }

        /**
         * Convenience constructor without stats logging (backward compatibility).
         */
        public Supplier(RemoteFileServiceClient client, String path,
                        long totalSize, int blockSize) {
            this(client, path, totalSize, blockSize, blockSize, null);
        }

        @Override
        public RandomAccessReader get() throws IOException {
            return new RemoteRandomAccessReader(client, path, totalSize, writeBlockSize, bufferSize, statsLogger);
        }

        @Override
        public void close() throws IOException {
            // client is shared; caller is responsible for closing it
        }
    }
}
