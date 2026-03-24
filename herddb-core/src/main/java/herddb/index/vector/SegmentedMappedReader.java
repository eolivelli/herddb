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

import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link RandomAccessReader} that maps a file into multiple segments to avoid
 * the {@link Integer#MAX_VALUE} limit of a single {@link MappedByteBuffer}.
 * <p>
 * Each segment is at most {@code maxSegmentSize} bytes (default 1 GiB).
 */
class SegmentedMappedReader implements RandomAccessReader {

    static final int DEFAULT_MAX_SEGMENT_SIZE = 1 << 30; // 1 GiB

    private final MappedByteBuffer[] segments;
    private final int maxSegmentSize;
    private final long fileSize;
    private long position;

    SegmentedMappedReader(Path path) throws IOException {
        this(path, DEFAULT_MAX_SEGMENT_SIZE);
    }

    SegmentedMappedReader(Path path, int maxSegmentSize) throws IOException {
        this.maxSegmentSize = maxSegmentSize;
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            this.fileSize = ch.size();
            List<MappedByteBuffer> segs = new ArrayList<>();
            long offset = 0;
            while (offset < fileSize) {
                long remaining = fileSize - offset;
                int segSize = (int) Math.min(remaining, maxSegmentSize);
                segs.add(ch.map(FileChannel.MapMode.READ_ONLY, offset, segSize));
                offset += segSize;
            }
            this.segments = segs.toArray(new MappedByteBuffer[0]);
        }
        this.position = 0;
    }

    private byte readByte() {
        int segIndex = (int) (position / maxSegmentSize);
        int segOffset = (int) (position % maxSegmentSize);
        byte b = segments[segIndex].get(segOffset);
        position++;
        return b;
    }

    @Override
    public void seek(long pos) {
        this.position = pos;
    }

    @Override
    public long getPosition() {
        return position;
    }

    @Override
    public int readInt() {
        // Read 4 bytes big-endian
        int b0 = readByte() & 0xFF;
        int b1 = readByte() & 0xFF;
        int b2 = readByte() & 0xFF;
        int b3 = readByte() & 0xFF;
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
    }

    @Override
    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public long readLong() {
        long hi = readInt() & 0xFFFFFFFFL;
        long lo = readInt() & 0xFFFFFFFFL;
        return (hi << 32) | lo;
    }

    @Override
    public void readFully(byte[] buf) {
        int off = 0;
        int remaining = buf.length;
        while (remaining > 0) {
            int segIndex = (int) (position / maxSegmentSize);
            int segOffset = (int) (position % maxSegmentSize);
            int available = maxSegmentSize - segOffset;
            int toRead = Math.min(remaining, available);
            ByteBuffer seg = segments[segIndex].duplicate();
            seg.position(segOffset);
            seg.get(buf, off, toRead);
            position += toRead;
            off += toRead;
            remaining -= toRead;
        }
    }

    @Override
    public void readFully(ByteBuffer dest) {
        int remaining = dest.remaining();
        while (remaining > 0) {
            int segIndex = (int) (position / maxSegmentSize);
            int segOffset = (int) (position % maxSegmentSize);
            int available = maxSegmentSize - segOffset;
            int toRead = Math.min(remaining, available);
            ByteBuffer seg = segments[segIndex].duplicate();
            seg.position(segOffset);
            seg.limit(segOffset + toRead);
            dest.put(seg);
            position += toRead;
            remaining -= toRead;
        }
    }

    @Override
    public void readFully(float[] dest) {
        for (int i = 0; i < dest.length; i++) {
            dest[i] = readFloat();
        }
    }

    @Override
    public void readFully(long[] dest) {
        for (int i = 0; i < dest.length; i++) {
            dest[i] = readLong();
        }
    }

    @Override
    public void read(int[] dest, int offset, int count) {
        for (int i = 0; i < count; i++) {
            dest[offset + i] = readInt();
        }
    }

    @Override
    public void read(float[] dest, int offset, int count) {
        for (int i = 0; i < count; i++) {
            dest[offset + i] = readFloat();
        }
    }

    @Override
    public void close() {
        // MappedByteBuffers are unmapped by GC; nothing to close explicitly.
    }

    @Override
    public long length() {
        return fileSize;
    }

    /**
     * A {@link ReaderSupplier} that creates {@link SegmentedMappedReader} instances,
     * replacing {@code SimpleMappedReader.Supplier} for files that may exceed 2 GB.
     */
    static class Supplier implements ReaderSupplier {

        private final Path path;

        Supplier(Path path) {
            this.path = path;
        }

        @Override
        public RandomAccessReader get() throws IOException {
            return new SegmentedMappedReader(path);
        }

        @Override
        public void close() {
            // nothing to close; each reader maps independently
        }
    }
}
