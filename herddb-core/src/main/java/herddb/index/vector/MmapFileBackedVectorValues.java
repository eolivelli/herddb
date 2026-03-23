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
    private final int maxSegmentSize;
    private final Path filePath;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private AtomicInteger count = new AtomicInteger();

    // Guarded by synchronized(this) for remapping
    private volatile MappedByteBuffer[] segments;
    private volatile long mappedSize;

    // Per-copy reusable buffer to avoid allocations in getVector (null for original instances)
    private final float[] sharedBuffer;
    private final VectorFloat<?> sharedVector;

    MmapFileBackedVectorValues(int dimension, long expectedSize, Path tempDir, int maxSegmentSize) throws IOException {
        this.dimension = dimension;
        this.vectorByteSize = (long) dimension * Float.BYTES;
        this.maxSegmentSize = maxSegmentSize;
        this.filePath = Files.createTempFile(tempDir, "vec-rebuild-", ".tmp");
        this.raf = new RandomAccessFile(filePath.toFile(), "rw");
        this.channel = raf.getChannel();

        long initialSize = Math.max(expectedSize, 16) * vectorByteSize;
        raf.setLength(initialSize);
        this.mappedSize = initialSize;
        this.segments = mapSegments(initialSize);
        this.sharedBuffer = null;
        this.sharedVector = null;
    }

    // Constructor for copy() — shares the same file, optionally with a reusable read buffer
    private MmapFileBackedVectorValues(int dimension, Path filePath, RandomAccessFile raf,
                                        FileChannel channel, AtomicInteger count,
                                        MappedByteBuffer[] segments, long mappedSize,
                                        int maxSegmentSize, boolean shared) {
        this.dimension = dimension;
        this.vectorByteSize = (long) dimension * Float.BYTES;
        this.maxSegmentSize = maxSegmentSize;
        this.filePath = filePath;
        this.raf = raf;
        this.channel = channel;
        this.count = count;
        this.segments = segments;
        this.mappedSize = mappedSize;
        if (shared) {
            this.sharedBuffer = new float[dimension];
            this.sharedVector = VTS.createFloatVector(sharedBuffer);
        } else {
            this.sharedBuffer = null;
            this.sharedVector = null;
        }
    }

    private MappedByteBuffer[] mapSegments(long totalSize) throws IOException {
        List<MappedByteBuffer> segs = new ArrayList<>();
        long offset = 0;
        while (offset < totalSize) {
            long remaining = totalSize - offset;
            int segSize = (int) Math.min(remaining, maxSegmentSize);
            segs.add(channel.map(FileChannel.MapMode.READ_WRITE, offset, segSize));
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
        float[] floats = sharedBuffer != null ? sharedBuffer : new float[dimension];
        MappedByteBuffer[] segs = segments;
        for (int i = 0; i < dimension; i++) {
            long pos = offset + (long) i * Float.BYTES;
            int segIndex = (int) (pos / maxSegmentSize);
            int segOffset = (int) (pos % maxSegmentSize);
            floats[i] = segs[segIndex].getFloat(segOffset);
        }
        return sharedVector != null ? sharedVector : VTS.createFloatVector(floats);
    }

    @Override
    public boolean isValueShared() {
        return sharedBuffer != null;
    }

    @Override
    public RandomAccessVectorValues copy() {
        return new MmapFileBackedVectorValues(dimension, filePath, raf, channel, count,
                segments, mappedSize, maxSegmentSize, true);
    }

    @Override
    public void close() throws IOException {
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
