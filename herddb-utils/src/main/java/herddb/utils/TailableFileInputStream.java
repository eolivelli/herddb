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

package herddb.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * An InputStream backed by a FileChannel that does not cache EOF.
 * <p>
 * Unlike {@link java.io.BufferedInputStream}, when the underlying file is appended to
 * after this stream has returned -1, subsequent reads will see the new data.
 * This makes it suitable for tailing a file that is being written to by another process.
 *
 * @author enrico.olivelli
 */
public class TailableFileInputStream extends InputStream {

    private final FileChannel channel;
    private final ByteBuffer buffer;
    private long markPosition = -1;

    public TailableFileInputStream(Path path) throws IOException {
        this(path, 64 * 1024);
    }

    public TailableFileInputStream(Path path, int bufferSize) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
        this.buffer = ByteBuffer.allocate(bufferSize);
        // Start with an empty buffer
        this.buffer.position(0);
        this.buffer.limit(0);
    }

    @Override
    public int read() throws IOException {
        if (!buffer.hasRemaining()) {
            int filled = fill();
            if (filled <= 0) {
                return -1;
            }
        }
        return buffer.get() & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return 0;
        }

        int totalRead = 0;
        while (totalRead < len) {
            if (!buffer.hasRemaining()) {
                int filled = fill();
                if (filled <= 0) {
                    return totalRead > 0 ? totalRead : -1;
                }
            }
            int toRead = Math.min(len - totalRead, buffer.remaining());
            buffer.get(b, off + totalRead, toRead);
            totalRead += toRead;
        }
        return totalRead;
    }

    private int fill() throws IOException {
        buffer.clear();
        int read = channel.read(buffer);
        buffer.flip();
        return read;
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }

    /**
     * Returns the current position in the file (bytes read so far).
     */
    public long position() throws IOException {
        return channel.position() - buffer.remaining();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        try {
            markPosition = position();
        } catch (IOException e) {
            markPosition = -1;
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        if (markPosition < 0) {
            throw new IOException("mark not set");
        }
        channel.position(markPosition);
        // Invalidate the buffer so the next read refills from the new channel position
        buffer.position(0);
        buffer.limit(0);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
