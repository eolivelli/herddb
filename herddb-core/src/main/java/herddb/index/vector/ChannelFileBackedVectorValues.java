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

import herddb.core.ClockProPolicy;
import herddb.core.PageReplacementPolicy;
import herddb.index.brin.BlockRangeIndex;
import herddb.index.brin.BlockRangeIndexMetadata;
import herddb.storage.DataStorageManagerException;
import herddb.utils.ObjectSizeUtils;
import herddb.utils.SizeAwareObject;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Channel-based (non-mmap) implementation of {@link FileBackedVectorValues}.
 * <p>
 * Uses an append-only dense file layout: vectors are written contiguously in
 * insertion order, and a {@code nodeId → fileOffset} index maps each nodeId
 * to the position of its vector in the file. This avoids sparse file holes
 * and improves Linux page-cache utilization compared to the old
 * {@code offset = nodeId * vectorByteSize} layout.
 * <p>
 * Two index strategies are available, selected automatically based on
 * {@code expectedSize}:
 * <ul>
 *   <li>{@link ArrayOffsetIndex} — O(1) lookup via {@link AtomicLongArray},
 *       used when expectedSize ≤ {@link #ARRAY_INDEX_THRESHOLD}</li>
 *   <li>{@link BrinOffsetIndex} — bounded-memory lookup via
 *       {@link BlockRangeIndex} with a {@link PageReplacementPolicy},
 *       used for very large graphs</li>
 * </ul>
 * <p>
 * Thread safety: concurrent writes to distinct nodeIds are safe because each
 * putVector atomically claims a unique file region via {@link AtomicLong},
 * and positional channel operations are thread-safe.
 */
class ChannelFileBackedVectorValues extends FileBackedVectorValues {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final ThreadLocal<ByteBuffer> BUFFER_CACHE = new ThreadLocal<>();

    static final String ARRAY_INDEX_THRESHOLD_PROPERTY = "herddb.vectorindex.dense.arraythreshold";
    static final long ARRAY_INDEX_THRESHOLD =
            Long.parseLong(System.getProperty(ARRAY_INDEX_THRESHOLD_PROPERTY, "10000000"));

    private final int dimension;
    private final long vectorByteSize;
    private final Path filePath;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final AtomicInteger count;
    private final AtomicLong appendPosition;
    private final OffsetIndex offsetIndex;

    // Per-thread read channels to avoid NativeThreadSet contention on the shared channel
    private final ConcurrentLinkedQueue<FileChannel> openReadChannels;
    private final ThreadLocal<FileChannel> readChannel;

    // Guarded by synchronized(this) for growing
    private volatile long fileSize;

    // Per-copy reusable buffer to avoid allocations in getVector (null for original instances)
    private final float[] sharedBuffer;
    private final VectorFloat<?> sharedVector;

    ChannelFileBackedVectorValues(int dimension, long expectedSize, Path tempDir) throws IOException {
        this.dimension = dimension;
        this.vectorByteSize = (long) dimension * Float.BYTES;
        this.filePath = Files.createTempFile(tempDir, "vec-rebuild-", ".tmp");
        this.raf = new RandomAccessFile(filePath.toFile(), "rw");
        this.channel = raf.getChannel();
        this.count = new AtomicInteger();
        this.appendPosition = new AtomicLong(0);
        this.openReadChannels = new ConcurrentLinkedQueue<>();
        this.readChannel = ThreadLocal.withInitial(this::openReadChannel);

        // No large pre-allocation; file starts empty and grows on demand
        this.fileSize = 0;
        this.sharedBuffer = null;
        this.sharedVector = null;

        if (expectedSize <= ARRAY_INDEX_THRESHOLD) {
            this.offsetIndex = new ArrayOffsetIndex((int) Math.max(expectedSize, 16));
        } else {
            this.offsetIndex = new BrinOffsetIndex();
        }
    }

    // Constructor for copy() — shares the same file, channels, index, and counters
    private ChannelFileBackedVectorValues(int dimension, Path filePath, RandomAccessFile raf,
                                           FileChannel channel, AtomicInteger count,
                                           AtomicLong appendPosition, OffsetIndex offsetIndex,
                                           ConcurrentLinkedQueue<FileChannel> openReadChannels,
                                           long fileSize, boolean shared) {
        this.dimension = dimension;
        this.vectorByteSize = (long) dimension * Float.BYTES;
        this.filePath = filePath;
        this.raf = raf;
        this.channel = channel;
        this.count = count;
        this.appendPosition = appendPosition;
        this.offsetIndex = offsetIndex;
        this.openReadChannels = openReadChannels;
        this.readChannel = ThreadLocal.withInitial(this::openReadChannel);
        this.fileSize = fileSize;
        if (shared) {
            this.sharedBuffer = new float[dimension];
            this.sharedVector = VTS.createFloatVector(sharedBuffer);
        } else {
            this.sharedBuffer = null;
            this.sharedVector = null;
        }
    }

    private ByteBuffer getOrAllocateBuffer() {
        ByteBuffer buf = BUFFER_CACHE.get();
        if (buf == null || buf.capacity() < vectorByteSize) {
            buf = ByteBuffer.allocateDirect((int) vectorByteSize).order(ByteOrder.nativeOrder());
            BUFFER_CACHE.set(buf);
        }
        buf.clear().limit((int) vectorByteSize);
        return buf;
    }

    @Override
    public void putVector(int nodeId, VectorFloat<?> vec) {
        // Claim the next contiguous slot in the file
        long offset = appendPosition.getAndAdd(vectorByteSize);
        long requiredSize = offset + vectorByteSize;

        if (requiredSize > fileSize) {
            growFile(requiredSize);
        }

        ByteBuffer buf = getOrAllocateBuffer();
        FloatBuffer fb = buf.asFloatBuffer();
        for (int i = 0; i < dimension; i++) {
            fb.put(vec.get(i));
        }
        writeFullyAt(buf, offset);
        offsetIndex.put(nodeId, offset);
        count.incrementAndGet();
    }

    @Override
    public void putVector(int nodeId, float[] floats) {
        // Claim the next contiguous slot in the file
        long offset = appendPosition.getAndAdd(vectorByteSize);
        long requiredSize = offset + vectorByteSize;

        if (requiredSize > fileSize) {
            growFile(requiredSize);
        }

        ByteBuffer buf = getOrAllocateBuffer();
        buf.asFloatBuffer().put(floats);
        writeFullyAt(buf, offset);
        offsetIndex.put(nodeId, offset);
        count.incrementAndGet();
    }

    private void writeFullyAt(ByteBuffer buf, long position) {
        try {
            while (buf.hasRemaining()) {
                position += channel.write(buf, position);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write vector", e);
        }
    }

    private FileChannel openReadChannel() {
        try {
            FileChannel ch = FileChannel.open(filePath, StandardOpenOption.READ);
            openReadChannels.add(ch);
            return ch;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open read channel", e);
        }
    }

    private void readFullyAt(ByteBuffer buf, long position) {
        try {
            FileChannel ch = readChannel.get();
            while (buf.hasRemaining()) {
                int n = ch.read(buf, position);
                if (n < 0) {
                    throw new IOException("Unexpected end of file at position " + position);
                }
                position += n;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read vector", e);
        }
    }

    private synchronized void growFile(long requiredSize) {
        if (requiredSize <= fileSize) {
            return;
        }
        try {
            long newSize = Math.max(requiredSize, fileSize == 0
                    ? Math.max(vectorByteSize * 64, requiredSize)
                    : fileSize * 2);
            raf.setLength(newSize);
            this.fileSize = newSize;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to grow vector file", e);
        }
    }

    @Override
    public int size() {
        return count.get();
    }

    @Override
    public int dimension() {
        return dimension;
    }

    @Override
    public VectorFloat<?> getVector(int nodeId) {
        long offset = offsetIndex.get(nodeId);
        if (offset < 0) {
            throw new IllegalArgumentException("No vector stored for nodeId " + nodeId);
        }
        ByteBuffer buf = getOrAllocateBuffer();
        readFullyAt(buf, offset);
        buf.flip();

        float[] floats = sharedBuffer != null ? sharedBuffer : new float[dimension];
        buf.asFloatBuffer().get(floats);
        return sharedVector != null ? sharedVector : VTS.createFloatVector(floats);
    }

    @Override
    public boolean isValueShared() {
        return true;
    }

    @Override
    public RandomAccessVectorValues copy() {
        return new ChannelFileBackedVectorValues(dimension, filePath, raf, channel, count,
                appendPosition, offsetIndex, openReadChannels, fileSize, true);
    }

    @Override
    public void close() throws IOException {
        if (sharedBuffer != null) {
            return; // copy — shared resources owned by the original
        }
        try {
            // Close all per-thread read channels
            FileChannel ch;
            while ((ch = openReadChannels.poll()) != null) {
                try {
                    ch.close();
                } catch (IOException ignored) {
                }
            }
            channel.close();
        } finally {
            try {
                raf.close();
            } finally {
                try {
                    Files.deleteIfExists(filePath);
                } finally {
                    offsetIndex.close();
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Offset index abstraction
    // -----------------------------------------------------------------------

    /**
     * Maps nodeId → file offset. Implementations must be thread-safe.
     */
    interface OffsetIndex {
        void put(int nodeId, long offset);

        /**
         * @return the file offset for the given nodeId, or -1 if not stored.
         */
        long get(int nodeId);

        void close();
    }

    // -----------------------------------------------------------------------
    // Array-backed index (O(1), for expectedSize <= ARRAY_INDEX_THRESHOLD)
    // -----------------------------------------------------------------------

    static final class ArrayOffsetIndex implements OffsetIndex {
        private volatile AtomicLongArray offsets;

        ArrayOffsetIndex(int initialCapacity) {
            AtomicLongArray arr = new AtomicLongArray(initialCapacity);
            for (int i = 0; i < initialCapacity; i++) {
                arr.set(i, -1L);
            }
            this.offsets = arr;
        }

        @Override
        public void put(int nodeId, long offset) {
            AtomicLongArray arr = offsets;
            if (nodeId >= arr.length()) {
                grow(nodeId);
                arr = offsets;
            }
            arr.set(nodeId, offset);
        }

        @Override
        public long get(int nodeId) {
            AtomicLongArray arr = offsets;
            if (nodeId >= arr.length()) {
                return -1;
            }
            return arr.get(nodeId);
        }

        private synchronized void grow(int requiredIndex) {
            AtomicLongArray arr = offsets;
            if (requiredIndex < arr.length()) {
                return;
            }
            int newLen = Math.max(requiredIndex + 1, arr.length() * 2);
            AtomicLongArray newArr = new AtomicLongArray(newLen);
            for (int i = 0; i < newLen; i++) {
                newArr.set(i, i < arr.length() ? arr.get(i) : -1L);
            }
            offsets = newArr;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    // -----------------------------------------------------------------------
    // BRIN-backed index (bounded memory, for very large graphs)
    // -----------------------------------------------------------------------

    static final class NodeIdKey implements Comparable<NodeIdKey>, SizeAwareObject {
        final int nodeId;

        NodeIdKey(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public int compareTo(NodeIdKey o) {
            return Integer.compare(nodeId, o.nodeId);
        }

        @Override
        public long getEstimatedSize() {
            // header + int field: 12+4=16 (compressed oops) or 16+4+4(padding)=24 (uncompressed)
            return ObjectSizeUtils.COMPRESSED_OOPS ? 16L : 24L;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof NodeIdKey && ((NodeIdKey) o).nodeId == nodeId;
        }

        @Override
        public int hashCode() {
            return nodeId;
        }
    }

    static final class OffsetValue implements SizeAwareObject {
        final long offset;

        OffsetValue(long offset) {
            this.offset = offset;
        }

        @Override
        public long getEstimatedSize() {
            // header + long field: 12+4(padding)+8=24 (compressed oops) or 16+8=24 (uncompressed)
            return 24L;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof OffsetValue && ((OffsetValue) o).offset == offset;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(offset);
        }
    }

    static final class BrinOffsetIndex implements OffsetIndex {

        private static final int BRIN_POLICY_CAPACITY =
                Integer.getInteger("herddb.vectorindex.dense.brin.policycapacity", 8192);
        private static final long BRIN_MAX_BLOCK_SIZE =
                Long.getLong("herddb.vectorindex.dense.brin.maxblocksize", 65536);

        private final BlockRangeIndex<NodeIdKey, OffsetValue> index;

        BrinOffsetIndex() {
            PageReplacementPolicy policy = new ClockProPolicy(BRIN_POLICY_CAPACITY);
            this.index = new BlockRangeIndex<>(BRIN_MAX_BLOCK_SIZE, policy);
            try {
                this.index.boot(BlockRangeIndexMetadata.empty());
            } catch (DataStorageManagerException e) {
                throw new RuntimeException("Failed to boot BRIN offset index", e);
            }
        }

        @Override
        public void put(int nodeId, long offset) {
            index.put(new NodeIdKey(nodeId), new OffsetValue(offset));
        }

        @Override
        public long get(int nodeId) {
            List<OffsetValue> results = index.search(new NodeIdKey(nodeId));
            if (results.isEmpty()) {
                return -1;
            }
            return results.get(0).offset;
        }

        @Override
        public void close() {
            // BlockRangeIndex is GC'd; no explicit close needed
        }
    }
}
