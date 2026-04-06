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

/**
 * A {@link RandomAccessReader} that reads from a remote multipart file via
 * {@link RemoteFileServiceClient}. Buffers one block at a time to avoid
 * redundant network round-trips when jvector reads sequentially within a block.
 *
 * <p>Instances are NOT thread-safe (one per reader thread as expected by jvector).
 *
 * @author enrico.olivelli
 */
public class RemoteRandomAccessReader implements RandomAccessReader {

    private final RemoteFileServiceClient client;
    private final String path;
    private final long totalSize;
    private final int blockSize;

    private long position;
    private byte[] blockBuffer;
    private long bufferedBlockIndex = -1;

    public RemoteRandomAccessReader(RemoteFileServiceClient client, String path,
                                    long totalSize, int blockSize) {
        this.client = client;
        this.path = path;
        this.totalSize = totalSize;
        this.blockSize = blockSize;
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
            int offsetInBlock = (int) (position % blockSize);
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
        long blockIndex = position / blockSize;
        if (blockIndex == bufferedBlockIndex && blockBuffer != null) {
            return;
        }
        long blockOffset = blockIndex * (long) blockSize;
        int requestLength = (int) Math.min(blockSize, totalSize - blockOffset);
        if (requestLength <= 0) {
            throw new IOException("Read past end of file: position=" + position
                    + " totalSize=" + totalSize);
        }
        byte[] data = client.readFileRange(path, blockOffset, requestLength, blockSize);
        if (data == null) {
            throw new IOException("Block not found: path=" + path + " blockIndex=" + blockIndex);
        }
        blockBuffer = data;
        bufferedBlockIndex = blockIndex;
    }

    /**
     * A {@link ReaderSupplier} that creates {@link RemoteRandomAccessReader} instances
     * for concurrent searcher threads (jvector calls {@code get()} per search thread).
     */
    public static class Supplier implements ReaderSupplier {

        private final RemoteFileServiceClient client;
        private final String path;
        private final long totalSize;
        private final int blockSize;

        public Supplier(RemoteFileServiceClient client, String path,
                        long totalSize, int blockSize) {
            this.client = client;
            this.path = path;
            this.totalSize = totalSize;
            this.blockSize = blockSize;
        }

        @Override
        public RandomAccessReader get() throws IOException {
            return new RemoteRandomAccessReader(client, path, totalSize, blockSize);
        }

        @Override
        public void close() throws IOException {
            // client is shared; caller is responsible for closing it
        }
    }
}
