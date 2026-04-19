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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Memory-mapped implementation of {@link FileBackedVectorValues}.
 * <p>
 * The file is mapped in segments of up to {@link #DEFAULT_MAX_SEGMENT_SIZE} bytes each,
 * to stay within the {@code Integer.MAX_VALUE} limit of a single {@link MappedByteBuffer}.
 * The file grows automatically if the initial capacity is exceeded.
 */
class MmapFileBackedVectorValues extends FileBackedVectorValues {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    private final int dimension;
    private final long vectorByteSize;
    /**
     * Maximum segment size, rounded DOWN to a whole multiple of {@link #vectorByteSize}
     * so that a vector never straddles a segment boundary — a straddle would defeat
     * the zero-copy slice-and-wrap path in {@link #getVector(int)}.
     */
    private final int maxSegmentSize;
    private final Path filePath;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private AtomicInteger count = new AtomicInteger();

    // Guarded by synchronized(this) for remapping
    private volatile MappedByteBuffer[] segments;
    private volatile long mappedSize;

    // Copy instances share file/channels with the original and must NOT close them
    private final boolean isCopy;

    MmapFileBackedVectorValues(int dimension, long expectedSize, Path tempDir, int maxSegmentSize) throws IOException {
        this.dimension = dimension;
        this.vectorByteSize = (long) dimension * Float.BYTES;
        if (vectorByteSize > maxSegmentSize) {
            throw new IllegalArgumentException(
                    "vectorByteSize " + vectorByteSize + " exceeds maxSegmentSize " + maxSegmentSize
                    + "; a single vector does not fit in one mapped segment");
        }
        // Round down to a multiple of vectorByteSize so vectors never straddle segments.
        this.maxSegmentSize = (int) ((maxSegmentSize / vectorByteSize) * vectorByteSize);
        this.filePath = Files.createTempFile(tempDir, "vec-rebuild-", ".tmp");
        this.raf = new RandomAccessFile(filePath.toFile(), "rw");
        this.channel = raf.getChannel();

        long initialSize = Math.max(expectedSize, 16) * vectorByteSize;
        raf.setLength(initialSize);
        this.mappedSize = initialSize;
        this.segments = mapSegments(initialSize);
        this.isCopy = false;
    }

    // Constructor for copy() — shares the same file, optionally with a reusable read buffer
    private MmapFileBackedVectorValues(int dimension, Path filePath, RandomAccessFile raf,
                                        FileChannel channel, AtomicInteger count,
                                        MappedByteBuffer[] segments, long mappedSize,
                                        int maxSegmentSize) {
        this.dimension = dimension;
        this.vectorByteSize = (long) dimension * Float.BYTES;
        this.maxSegmentSize = maxSegmentSize;
        this.filePath = filePath;
        this.raf = raf;
        this.channel = channel;
        this.count = count;
        this.segments = segments;
        this.mappedSize = mappedSize;
        this.isCopy = true;
    }

    private MappedByteBuffer[] mapSegments(long totalSize) throws IOException {
        List<MappedByteBuffer> segs = new ArrayList<>();
        long offset = 0;
        while (offset < totalSize) {
            long remaining = totalSize - offset;
            int segSize = (int) Math.min(remaining, maxSegmentSize);
            MappedByteBuffer seg = channel.map(FileChannel.MapMode.READ_WRITE, offset, segSize);
            // Native order is required for jvector's native-SIMD fast path when the
            // segment is later wrapped as a VectorFloat in getVector.
            seg.order(ByteOrder.nativeOrder());
            segs.add(seg);
            offset += segSize;
        }
        return segs.toArray(new MappedByteBuffer[0]);
    }

    @Override
    public void putVector(int nodeId, VectorFloat<?> vec) {
        long offset = (long) nodeId * vectorByteSize;
        long requiredSize = offset + vectorByteSize;

        if (requiredSize > mappedSize) {
            growFile(requiredSize);
        }

        MappedByteBuffer[] segs = segments;
        for (int i = 0; i < dimension; i++) {
            long pos = offset + (long) i * Float.BYTES;
            int segIndex = (int) (pos / maxSegmentSize);
            int segOffset = (int) (pos % maxSegmentSize);
            segs[segIndex].putFloat(segOffset, vec.get(i));
        }
        count.incrementAndGet();
    }

    @Override
    public void putVector(int nodeId, float[] floats) {
        long offset = (long) nodeId * vectorByteSize;
        long requiredSize = offset + vectorByteSize;

        if (requiredSize > mappedSize) {
            growFile(requiredSize);
        }

        MappedByteBuffer[] segs = segments;
        for (int i = 0; i < dimension; i++) {
            long pos = offset + (long) i * Float.BYTES;
            int segIndex = (int) (pos / maxSegmentSize);
            int segOffset = (int) (pos % maxSegmentSize);
            segs[segIndex].putFloat(segOffset, floats[i]);
        }
        count.incrementAndGet();
    }

    private synchronized void growFile(long requiredSize) {
        if (requiredSize <= mappedSize) {
            return; // another thread already grew
        }
        try {
            long newSize = Math.max(requiredSize, mappedSize * 2);
            raf.setLength(newSize);
            this.segments = mapSegments(newSize);
            this.mappedSize = newSize;
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
        MappedByteBuffer[] segs = segments;
        int segIndex = (int) (offset / maxSegmentSize);
        int segOffset = (int) (offset % maxSegmentSize);
        // maxSegmentSize is aligned to vectorByteSize in the constructor, so a vector
        // never straddles a segment boundary — a single slice covers it.
        ByteBuffer view = segs[segIndex].duplicate();
        view.position(segOffset).limit(segOffset + (int) vectorByteSize);
        ByteBuffer slice = view.slice();
        slice.order(ByteOrder.nativeOrder());
        return VTS.wrapFloatVector(slice);
    }

    @Override
    public boolean isValueShared() {
        return true;
    }

    @Override
    public RandomAccessVectorValues copy() {
        return new MmapFileBackedVectorValues(dimension, filePath, raf, channel, count,
                segments, mappedSize, maxSegmentSize);
    }

    @Override
    public void close() throws IOException {
        if (isCopy) {
            return; // copy — shared resources owned by the original
        }
        // Force unmap is not strictly possible via public API, but the GC will handle it.
        // We close the channel and delete the file.
        try {
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
