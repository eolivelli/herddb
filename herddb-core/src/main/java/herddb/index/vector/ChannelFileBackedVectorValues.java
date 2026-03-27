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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Channel-based (non-mmap) implementation of {@link FileBackedVectorValues}.
 * <p>
 * Uses positional {@link FileChannel#read(ByteBuffer, long)} and
 * {@link FileChannel#write(ByteBuffer, long)} for I/O, avoiding memory-mapped buffers.
 * This results in a lower virtual memory and page-cache footprint at the cost of
 * slightly higher per-access overhead due to system calls.
 * <p>
 * Thread safety: concurrent writes to distinct nodeIds are safe because each nodeId
 * maps to a non-overlapping file region and positional channel operations are thread-safe.
 */
class ChannelFileBackedVectorValues extends FileBackedVectorValues {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final ThreadLocal<ByteBuffer> BUFFER_CACHE = new ThreadLocal<>();

    private final int dimension;
    private final long vectorByteSize;
    private final Path filePath;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final AtomicInteger count;

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
        this.openReadChannels = new ConcurrentLinkedQueue<>();
        this.readChannel = ThreadLocal.withInitial(this::openReadChannel);

        long initialSize = Math.max(expectedSize, 16) * vectorByteSize;
        raf.setLength(initialSize);
        this.fileSize = initialSize;
        this.sharedBuffer = null;
        this.sharedVector = null;
    }

    // Constructor for copy() — shares the same file and read channel pool
    private ChannelFileBackedVectorValues(int dimension, Path filePath, RandomAccessFile raf,
                                           FileChannel channel, AtomicInteger count,
                                           ConcurrentLinkedQueue<FileChannel> openReadChannels,
                                           long fileSize, boolean shared) {
        this.dimension = dimension;
        this.vectorByteSize = (long) dimension * Float.BYTES;
        this.filePath = filePath;
        this.raf = raf;
        this.channel = channel;
        this.count = count;
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
        long offset = (long) nodeId * vectorByteSize;
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
        count.incrementAndGet();
    }

    @Override
    public void putVector(int nodeId, float[] floats) {
        long offset = (long) nodeId * vectorByteSize;
        long requiredSize = offset + vectorByteSize;

        if (requiredSize > fileSize) {
            growFile(requiredSize);
        }

        ByteBuffer buf = getOrAllocateBuffer();
        buf.asFloatBuffer().put(floats);
        writeFullyAt(buf, offset);
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
            long newSize = Math.max(requiredSize, fileSize * 2);
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
        long offset = (long) nodeId * vectorByteSize;
        ByteBuffer buf = getOrAllocateBuffer();
        readFullyAt(buf, offset);
        buf.flip();

        float[] floats = sharedBuffer != null ? sharedBuffer : new float[dimension];
        buf.asFloatBuffer().get(floats);
        return sharedVector != null ? sharedVector : VTS.createFloatVector(floats);
    }

    @Override
    public boolean isValueShared() {
        return sharedBuffer != null;
    }

    @Override
    public RandomAccessVectorValues copy() {
        return new ChannelFileBackedVectorValues(dimension, filePath, raf, channel, count,
                openReadChannels, fileSize, true);
    }

    @Override
    public void close() throws IOException {
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
                Files.deleteIfExists(filePath);
            }
        }
    }
}
