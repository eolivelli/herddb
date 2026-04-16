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

package herddb.remote.storage;

import io.netty.buffer.ByteBuf;
import javax.annotation.Nullable;

/**
 * Result of a {@link ObjectStorage#read(String)} operation.
 *
 * Supports both heap byte[] and Netty direct pooled ByteBuf for zero-copy I/O efficiency.
 * When using ByteBuf, the caller MUST release it after use via {@link #release()}.
 *
 * @author enrico.olivelli
 */
public final class ReadResult {

    public enum Status {
        FOUND,
        NOT_FOUND
    }

    private static final ReadResult NOT_FOUND_INSTANCE = new ReadResult(Status.NOT_FOUND, null, null);

    private final Status status;
    @Nullable
    private final byte[] content;
    @Nullable
    private final ByteBuf byteBuf;

    private ReadResult(Status status, @Nullable byte[] content, @Nullable ByteBuf byteBuf) {
        this.status = status;
        this.content = content;
        this.byteBuf = byteBuf;
        // Ensure only one is set
        if (content != null && byteBuf != null) {
            throw new IllegalArgumentException("Cannot have both content and byteBuf");
        }
    }

    /**
     * Create a found result with heap byte array (backward compatible).
     */
    public static ReadResult found(byte[] content) {
        return new ReadResult(Status.FOUND, content, null);
    }

    /**
     * Create a found result with Netty direct pooled ByteBuf.
     * <p>
     * IMPORTANT: Caller MUST call {@link #release()} when done to return the buffer to the pool.
     * The ByteBuf's refcount starts at 1.
     */
    public static ReadResult foundWithByteBuf(ByteBuf byteBuf) {
        if (byteBuf == null) {
            throw new IllegalArgumentException("byteBuf cannot be null");
        }
        return new ReadResult(Status.FOUND, null, byteBuf);
    }

    public static ReadResult notFound() {
        return NOT_FOUND_INSTANCE;
    }

    public Status status() {
        return status;
    }

    /**
     * Get content as byte array. If backed by ByteBuf, makes a copy.
     * @return content bytes, or null if NOT_FOUND
     */
    @Nullable
    public byte[] content() {
        if (content != null) {
            return content;
        }
        if (byteBuf != null) {
            byte[] copy = new byte[byteBuf.readableBytes()];
            byteBuf.getBytes(byteBuf.readerIndex(), copy);
            return copy;
        }
        return null;
    }

    /**
     * Get content as Netty ByteBuf. If backed by byte array, wraps it.
     * <p>
     * IMPORTANT: If this returns a pooled ByteBuf (from {@link #foundWithByteBuf}),
     * caller MUST call {@link #release()} when done. Wrapping a byte[] does not
     * require release.
     *
     * @return ByteBuf with content, or null if NOT_FOUND
     */
    @Nullable
    public ByteBuf byteBuf() {
        if (byteBuf != null) {
            return byteBuf;
        }
        if (content != null) {
            return io.netty.buffer.Unpooled.wrappedBuffer(content);
        }
        return null;
    }

    /**
     * Release the underlying ByteBuf if this result was created with
     * {@link #foundWithByteBuf(ByteBuf)}.
     *
     * Safe to call multiple times (refcount-aware). Only decrements if backing
     * a pooled ByteBuf. If backing a heap byte[], this is a no-op.
     */
    public void release() {
        if (byteBuf != null) {
            byteBuf.release();
        }
        // Heap byte[] doesn't need release
    }

    /**
     * Check if this result is backed by a pooled ByteBuf.
     * @return true if created with {@link #foundWithByteBuf(ByteBuf)}
     */
    public boolean isByteBufBacked() {
        return byteBuf != null;
    }
}
