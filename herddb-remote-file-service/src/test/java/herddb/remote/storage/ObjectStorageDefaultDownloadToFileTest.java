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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for the {@link ObjectStorage#downloadToFile(String, Path, boolean)}
 * <em>default</em> implementation (the FileChannel-based fallback in the interface).
 *
 * <p>Uses a minimal in-memory {@link ObjectStorage} stub so no S3 client, file
 * server, or Netty infrastructure is required.
 */
public class ObjectStorageDefaultDownloadToFileTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final byte[] DATA = {10, 20, 30, 40, 50};
    private static final byte[] DATA2 = {60, 70, 80};

    /** Minimal stub: stores a single path → byte[]. Returns NOT_FOUND for everything else. */
    private static ObjectStorage singletonStorage(String storedPath, byte[] bytes) {
        return new ObjectStorage() {
            @Override
            public CompletableFuture<Void> write(String path, byte[] content) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<ReadResult> read(String path) {
                if (storedPath.equals(path)) {
                    return CompletableFuture.completedFuture(
                            ReadResult.found(Unpooled.wrappedBuffer(bytes)));
                }
                return CompletableFuture.completedFuture(ReadResult.notFound());
            }

            @Override
            public CompletableFuture<Void> writeBlock(String path, long blockIndex,
                    byte[] content) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<ReadResult> readRange(String path, long offset,
                    int length, int blockSize) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<Boolean> deleteLogical(String path) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<List<String>> listLogical(String prefix) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<Boolean> delete(String path) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<List<String>> list(String prefix) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<Integer> deleteByPrefix(String prefix) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
            }
        };
    }

    /** append=false: destination file is created and contains exactly the stored bytes. */
    @Test
    public void createFileForFirstBlock() throws Exception {
        ObjectStorage storage = singletonStorage("seg/graph.multipart/0", DATA);
        Path dest = folder.newFile("block0.bin").toPath();
        storage.downloadToFile("seg/graph.multipart/0", dest, false).get();
        assertArrayEquals(DATA, Files.readAllBytes(dest));
    }

    /** append=true: bytes are appended to whatever is already in the destination. */
    @Test
    public void appendSubsequentBlock() throws Exception {
        // Write block 0 first.
        ObjectStorage storage0 = singletonStorage("seg/graph.multipart/0", DATA);
        Path dest = folder.newFile("out.bin").toPath();
        storage0.downloadToFile("seg/graph.multipart/0", dest, false).get();

        // Append block 1.
        ObjectStorage storage1 = singletonStorage("seg/graph.multipart/1", DATA2);
        storage1.downloadToFile("seg/graph.multipart/1", dest, true).get();

        byte[] written = Files.readAllBytes(dest);
        assertEquals(DATA.length + DATA2.length, written.length);
        for (int i = 0; i < DATA.length; i++) {
            assertEquals("block-0 byte " + i, DATA[i], written[i]);
        }
        for (int i = 0; i < DATA2.length; i++) {
            assertEquals("block-1 byte " + i, DATA2[i], written[DATA.length + i]);
        }
    }

    /**
     * NOT_FOUND: the future must complete exceptionally with {@link UncheckedIOException}
     * wrapping an {@link IOException}; the destination file must NOT be created or
     * truncated.
     */
    @Test
    public void notFoundCompletesExceptionallyAndDoesNotTouchDestFile() throws Exception {
        // Use a path that the stub does not recognise.
        ObjectStorage storage = singletonStorage("seg/graph.multipart/0", DATA);
        // Pre-populate a file with sentinel bytes.
        Path dest = folder.newFile("sentinel.bin").toPath();
        byte[] sentinel = {99, 98, 97};
        Files.write(dest, sentinel);

        CompletableFuture<Void> future = storage.downloadToFile("missing/path", dest, false);
        ExecutionException ex = null;
        try {
            future.get();
        } catch (ExecutionException e) {
            ex = e;
        }

        assertTrue("future must complete exceptionally on NOT_FOUND", ex != null);
        assertTrue("cause must be UncheckedIOException",
                ex.getCause() instanceof UncheckedIOException);
        IOException ioe = ((UncheckedIOException) ex.getCause()).getCause();
        assertTrue("wrapped IOException message must name the path",
                ioe.getMessage().contains("missing/path"));

        // The sentinel file must be intact — no truncation should have occurred.
        assertArrayEquals("destination file must not be modified on NOT_FOUND",
                sentinel, Files.readAllBytes(dest));
    }

    /**
     * NOT_FOUND with a non-existent destination (append=false): the file must NOT
     * be created. Verifies that the NOT_FOUND guard runs before the FileChannel
     * TRUNCATE_EXISTING / CREATE path.
     */
    @Test
    public void notFoundDoesNotCreateDestFile() throws Exception {
        ObjectStorage storage = singletonStorage("seg/graph.multipart/0", DATA);
        // Point at a file that does not yet exist.
        Path dest = folder.newFile("will-not-be-created.bin").toPath();
        Files.delete(dest); // ensure it does not exist

        CompletableFuture<Void> future = storage.downloadToFile("missing/path", dest, false);
        try {
            future.get();
        } catch (ExecutionException ignored) {
        }

        assertFalse("destination file must not be created on NOT_FOUND",
                Files.exists(dest));
    }
}
