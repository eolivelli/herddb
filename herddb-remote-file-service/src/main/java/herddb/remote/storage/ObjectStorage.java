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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Abstraction over an object/blob storage backend.
 * Enables local filesystem, S3, or other providers.
 *
 * <p>Multipart files: large files are split into fixed-size blocks stored at
 * {@code {path}.multipart/{blockIndex}}. The logical path is {@code path}.
 * Multipart and single-part files are mutually exclusive for a given logical path.
 *
 * @author enrico.olivelli
 */
public interface ObjectStorage extends AutoCloseable {

    /** Directory suffix used to store multipart file blocks. */
    String MULTIPART_SUFFIX = ".multipart";

    CompletableFuture<Void> write(String path, byte[] content);

    CompletableFuture<ReadResult> read(String path);

    /**
     * Writes one block of a multipart file.
     * Stored physically at {@code {path}.multipart/{blockIndex}}.
     */
    CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content);

    /**
     * Reads a range of bytes from a (possibly multipart) file.
     * The range must not span two blocks (client responsibility).
     *
     * @param path      logical file path
     * @param offset    byte offset in the logical file
     * @param length    number of bytes to read
     * @param blockSize size of each block in bytes
     */
    CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize);

    /**
     * Deletes a logical file: if {@code {path}.multipart/} exists all blocks are deleted,
     * otherwise the single-part file at {@code path} is deleted.
     */
    CompletableFuture<Boolean> deleteLogical(String path);

    /**
     * Lists logical paths under {@code prefix}, collapsing {@code {path}.multipart/{N}}
     * entries into their logical path {@code path}.
     */
    CompletableFuture<List<String>> listLogical(String prefix);

    CompletableFuture<Boolean> delete(String path);

    CompletableFuture<List<String>> list(String prefix);

    CompletableFuture<Integer> deleteByPrefix(String prefix);

    /**
     * Downloads the object at {@code path} and writes its content directly to
     * a local file, without materialising the full object in JVM heap or Netty
     * direct memory.
     *
     * <p>When {@code append} is {@code false} the destination file is created
     * (or replaced if it already exists). When {@code append} is {@code true}
     * the content is appended to the existing file (or the file is created if
     * absent). This lets callers reconstruct a multipart logical file by
     * downloading block 0 with {@code append=false} and all subsequent blocks
     * with {@code append=true}.
     *
     * <p>The default implementation falls back to {@link #read(String)} and
     * copies the returned {@link ByteBuf} via a {@link FileChannel}. Override
     * in storage backends that can stream natively (e.g.
     * {@link S3ObjectStorage} uses {@code AsyncResponseTransformer.toFile()}).
     *
     * @param path   object path (same key space as {@link #read(String)})
     * @param dest   destination file path
     * @param append when {@code true} content is appended; when {@code false}
     *               the file is created or replaced
     * @return a future that completes when the write is done; completes
     *         exceptionally if the object does not exist or an I/O error occurs
     */
    default CompletableFuture<Void> downloadToFile(String path, Path dest, boolean append) {
        return read(path).thenApply(result -> {
            // Guard: do NOT open or truncate the destination file when the object is absent.
            // Checking status before opening preserves dest integrity on NOT_FOUND.
            if (result.status() == ReadResult.Status.NOT_FOUND) {
                throw new UncheckedIOException(
                        new IOException("object not found: " + path));
            }
            ByteBuf buf = result.byteBuf();
            try {
                StandardOpenOption[] opts = append
                        ? new StandardOpenOption[]{
                                StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.APPEND}
                        : new StandardOpenOption[]{
                                StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.TRUNCATE_EXISTING};
                try (FileChannel ch = FileChannel.open(dest, opts)) {
                    // nioBuffer() returns a ByteBuffer view without heap copying
                    // for both direct and array-backed Netty buffers.
                    ByteBuffer nioBuf = buf.nioBuffer(buf.readerIndex(), buf.readableBytes());
                    while (nioBuf.hasRemaining()) {
                        ch.write(nioBuf);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } finally {
                result.release();
            }
            return (Void) null;
        });
    }

    @Override
    void close() throws Exception;
}
