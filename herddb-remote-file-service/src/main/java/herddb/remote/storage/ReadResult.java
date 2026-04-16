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
 * Backed by Netty direct pooled ByteBuf for zero-copy I/O efficiency.
 * Caller MUST call {@link #release()} when done to return buffer to the pool.
 *
 * @author enrico.olivelli
 */
public final class ReadResult {

    public enum Status {
        FOUND,
        NOT_FOUND
    }

    private static final ReadResult NOT_FOUND_INSTANCE = new ReadResult(Status.NOT_FOUND, null);

    private final Status status;
    @Nullable
    private final ByteBuf byteBuf;

    private ReadResult(Status status, @Nullable ByteBuf byteBuf) {
        this.status = status;
        this.byteBuf = byteBuf;
    }

    /**
     * Create a found result with Netty direct pooled ByteBuf.
     * <p>
     * IMPORTANT: Caller MUST call {@link #release()} when done to return the buffer to the pool.
     * The ByteBuf's refcount starts at 1.
     *
     * @param byteBuf non-null ByteBuf with content
     * @return ReadResult holding the ByteBuf (ownership transferred to result)
     */
    public static ReadResult found(ByteBuf byteBuf) {
        if (byteBuf == null) {
            throw new IllegalArgumentException("byteBuf cannot be null");
        }
        return new ReadResult(Status.FOUND, byteBuf);
    }

    /**
     * Create a not-found result.
     *
     * @return singleton NOT_FOUND result
     */
    public static ReadResult notFound() {
        return NOT_FOUND_INSTANCE;
    }

    public Status status() {
        return status;
    }

    /**
     * Get content as Netty ByteBuf with direct pooled memory.
     * <p>
     * IMPORTANT: Caller MUST call {@link #release()} on the result when done
     * to return the pooled buffer to Netty's allocator.
     *
     * @return ByteBuf with content, or null if NOT_FOUND
     */
    @Nullable
    public ByteBuf byteBuf() {
        return byteBuf;
    }

    /**
     * Get content as byte array copy (convenience method).
     * <p>
     * This makes a heap copy from the pooled direct ByteBuf.
     * Prefer using {@link #byteBuf()} directly for better performance.
     *
     * @return copy of content bytes, or null if NOT_FOUND
     */
    @Nullable
    public byte[] content() {
        if (byteBuf == null) {
            return null;
        }
        byte[] copy = new byte[byteBuf.readableBytes()];
        byteBuf.getBytes(byteBuf.readerIndex(), copy);
        return copy;
    }

    /**
     * Release the pooled ByteBuf, returning it to Netty's allocator.
     * <p>
     * Safe to call multiple times (refcount-aware via ByteBuf.release()).
     * If NOT_FOUND, this is a no-op (no buffer to release).
     * <p>
     * IMPORTANT: Must be called in a finally block to ensure proper cleanup.
     * Example:
     * <pre>
     * ReadResult result = storage.read(path).get();
     * try {
     *     process(result.byteBuf());
     * } finally {
     *     result.release();  // Always release
     * }
     * </pre>
     */
    public void release() {
        if (byteBuf != null) {
            byteBuf.release();
        }
    }
}
