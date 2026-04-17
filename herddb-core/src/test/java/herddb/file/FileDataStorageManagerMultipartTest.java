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

package herddb.file;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.storage.DataStorageManagerException;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.EOFException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileDataStorageManagerMultipartTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /** Sentinel byte pattern so cross-block reads are easy to verify. */
    private static byte pattern(long pos) {
        return (byte) ((pos * 31L) & 0xFF);
    }

    private static byte[] makePayload(int size) {
        byte[] out = new byte[size];
        for (int i = 0; i < size; i++) {
            out[i] = pattern(i);
        }
        return out;
    }

    @Test
    public void writeReadAcrossMultipleBlocks() throws Exception {
        byte[] payload = makePayload(1024 * 3 + 250); // 3 full blocks + short tail
        Path tempFile = folder.newFile("write.bin").toPath();
        Files.write(tempFile, payload);

        try (FileDataStorageManager dsm = new FileDataStorageManager(folder.newFolder().toPath())) {
            dsm.setMultipartBlockSizeForTests(1024);
            dsm.initIndex("ts", "idx");
            AtomicLong progressTotal = new AtomicLong();
            String dir = dsm.writeMultipartIndexFile(
                    "ts", "idx", "graph", tempFile, progressTotal::addAndGet);
            assertTrue(Files.isDirectory(java.nio.file.Paths.get(dir)));
            assertEquals(payload.length, progressTotal.get());
            // Verify 4 block files exist (3 full + 1 tail of 250 bytes).
            assertEquals(4L,
                    Files.list(java.nio.file.Paths.get(dir)).filter(p -> p.getFileName().toString().startsWith("block-"))
                            .count());

            try (ReaderSupplier supplier =
                         dsm.multipartIndexReaderSupplier("ts", "idx", "graph", payload.length);
                 RandomAccessReader reader = supplier.get()) {
                assertEquals(payload.length, reader.length());

                // Full read from start.
                byte[] readBack = new byte[payload.length];
                reader.readFully(readBack);
                assertArrayEquals(payload, readBack);

                // Read spanning block boundary.
                reader.seek(1000);
                byte[] span = new byte[100]; // crosses block 0/1
                reader.readFully(span);
                for (int i = 0; i < span.length; i++) {
                    assertEquals("byte " + (1000 + i),
                            pattern(1000 + i), span[i]);
                }

                // Read the short tail.
                reader.seek(1024L * 3);
                byte[] tail = new byte[250];
                reader.readFully(tail);
                for (int i = 0; i < tail.length; i++) {
                    assertEquals("tail byte " + i,
                            pattern(1024L * 3 + i), tail[i]);
                }
            }
        }
    }

    @Test
    public void writeEmptyFile() throws Exception {
        Path tempFile = folder.newFile("empty.bin").toPath();
        Files.write(tempFile, new byte[0]);

        try (FileDataStorageManager dsm = new FileDataStorageManager(folder.newFolder().toPath())) {
            dsm.initIndex("ts", "idx");
            String dir = dsm.writeMultipartIndexFile("ts", "idx", "map", tempFile, null);
            assertTrue(Files.isDirectory(java.nio.file.Paths.get(dir)));

            try (ReaderSupplier supplier =
                         dsm.multipartIndexReaderSupplier("ts", "idx", "map", 0);
                 RandomAccessReader reader = supplier.get()) {
                assertEquals(0L, reader.length());
            }
        }
    }

    @Test
    public void rewriteReplacesPreviousBlocks() throws Exception {
        Path tempFirst = folder.newFile("first.bin").toPath();
        Files.write(tempFirst, makePayload(1024 * 4)); // 4 blocks
        Path tempSecond = folder.newFile("second.bin").toPath();
        Files.write(tempSecond, makePayload(512)); // 1 short block

        try (FileDataStorageManager dsm = new FileDataStorageManager(folder.newFolder().toPath())) {
            dsm.setMultipartBlockSizeForTests(1024);
            dsm.initIndex("ts", "idx");
            String dir = dsm.writeMultipartIndexFile("ts", "idx", "graph", tempFirst, null);
            assertEquals(4L,
                    Files.list(java.nio.file.Paths.get(dir)).filter(p -> p.getFileName().toString().startsWith("block-"))
                            .count());

            dsm.writeMultipartIndexFile("ts", "idx", "graph", tempSecond, null);
            // Old block files must be gone.
            assertEquals(1L,
                    Files.list(java.nio.file.Paths.get(dir)).filter(p -> p.getFileName().toString().startsWith("block-"))
                            .count());
        }
    }

    @Test
    public void deleteRemovesDirectory() throws Exception {
        Path tempFile = folder.newFile("x.bin").toPath();
        Files.write(tempFile, makePayload(100));
        try (FileDataStorageManager dsm = new FileDataStorageManager(folder.newFolder().toPath())) {
            dsm.initIndex("ts", "idx");
            String dir = dsm.writeMultipartIndexFile("ts", "idx", "graph", tempFile, null);
            assertTrue(Files.exists(java.nio.file.Paths.get(dir)));

            dsm.deleteMultipartIndexFile("ts", "idx", "graph");
            assertFalse(Files.exists(java.nio.file.Paths.get(dir)));

            // Idempotent on missing directory.
            dsm.deleteMultipartIndexFile("ts", "idx", "graph");
        }
    }

    // ---------------------------------------------------------------------
    // Failure scenarios
    // ---------------------------------------------------------------------

    @Test
    public void readerSupplierFailsWhenDirectoryMissing() throws Exception {
        try (FileDataStorageManager dsm = new FileDataStorageManager(folder.newFolder().toPath())) {
            dsm.initIndex("ts", "idx");
            try {
                dsm.multipartIndexReaderSupplier("ts", "idx", "graph", 100);
                fail("expected DataStorageManagerException on missing directory");
            } catch (DataStorageManagerException expected) {
                assertTrue("message should mention the missing directory: " + expected.getMessage(),
                        expected.getMessage() != null && expected.getMessage().contains("multipart directory not found"));
            }
        }
    }

    @Test
    public void readerReadingPastEndOfFileThrowsEof() throws Exception {
        byte[] payload = makePayload(100);
        Path tempFile = folder.newFile("x.bin").toPath();
        Files.write(tempFile, payload);
        try (FileDataStorageManager dsm = new FileDataStorageManager(folder.newFolder().toPath())) {
            dsm.initIndex("ts", "idx");
            dsm.writeMultipartIndexFile("ts", "idx", "graph", tempFile, null);

            try (ReaderSupplier supplier =
                         dsm.multipartIndexReaderSupplier("ts", "idx", "graph", payload.length);
                 RandomAccessReader reader = supplier.get()) {
                reader.seek(payload.length);
                byte[] oneByte = new byte[1];
                try {
                    reader.readFully(oneByte);
                    fail("reading past EOF should throw EOFException");
                } catch (EOFException expected) {
                    // ok
                }
            }
        }
    }

    @Test
    public void readerReadingAcrossHoleInBlocksFailsCleanly() throws Exception {
        byte[] payload = makePayload(1024 * 3); // exactly 3 blocks at 1 KiB
        Path tempFile = folder.newFile("x.bin").toPath();
        Files.write(tempFile, payload);
        try (FileDataStorageManager dsm = new FileDataStorageManager(folder.newFolder().toPath())) {
            dsm.setMultipartBlockSizeForTests(1024);
            dsm.initIndex("ts", "idx");
            String dirStr = dsm.writeMultipartIndexFile("ts", "idx", "graph", tempFile, null);

            // Remove block 1 to simulate corruption.
            Path dir = java.nio.file.Paths.get(dirStr);
            Files.delete(dir.resolve("block-00000001"));

            try (ReaderSupplier supplier =
                         dsm.multipartIndexReaderSupplier("ts", "idx", "graph", payload.length);
                 RandomAccessReader reader = supplier.get()) {
                byte[] readBack = new byte[payload.length];
                try {
                    reader.readFully(readBack);
                    fail("reading with missing block must fail");
                } catch (java.io.IOException expected) {
                    // NoSuchFileException / IOException — surface any kind of IO error, content doesn't matter here
                }
            }
        }
    }

    @Test
    public void concurrentReadersHaveIndependentPositions() throws Exception {
        byte[] payload = makePayload(1024 * 4); // 4 blocks
        Path tempFile = folder.newFile("x.bin").toPath();
        Files.write(tempFile, payload);
        try (FileDataStorageManager dsm = new FileDataStorageManager(folder.newFolder().toPath())) {
            dsm.setMultipartBlockSizeForTests(1024);
            dsm.initIndex("ts", "idx");
            dsm.writeMultipartIndexFile("ts", "idx", "graph", tempFile, null);

            ExecutorService pool = Executors.newFixedThreadPool(4);
            try (ReaderSupplier supplier =
                         dsm.multipartIndexReaderSupplier("ts", "idx", "graph", payload.length)) {
                List<Future<Boolean>> futures = new ArrayList<>();
                for (int t = 0; t < 4; t++) {
                    final int starter = t;
                    futures.add(pool.submit((Callable<Boolean>) () -> {
                        // Each thread reads a different slice and checks the bytes.
                        try (RandomAccessReader reader = supplier.get()) {
                            for (int iter = 0; iter < 100; iter++) {
                                int start = (starter * 37 + iter * 13) % (payload.length - 200);
                                reader.seek(start);
                                byte[] buf = new byte[200];
                                reader.readFully(buf);
                                for (int i = 0; i < buf.length; i++) {
                                    if (buf[i] != pattern(start + i)) {
                                        return false;
                                    }
                                }
                            }
                        }
                        return true;
                    }));
                }
                for (Future<Boolean> f : futures) {
                    assertTrue("concurrent reader reported corrupted bytes",
                            f.get(30, TimeUnit.SECONDS));
                }
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    public void writeOverwritesExistingPartialDirectory() throws Exception {
        // Simulate a prior aborted write that left stray .tmp files around.
        byte[] payload = makePayload(1024 * 2);
        Path tempFile = folder.newFile("x.bin").toPath();
        Files.write(tempFile, payload);
        try (FileDataStorageManager dsm = new FileDataStorageManager(folder.newFolder().toPath())) {
            dsm.setMultipartBlockSizeForTests(1024);
            dsm.initIndex("ts", "idx");
            // First write creates 2 blocks.
            String dirStr = dsm.writeMultipartIndexFile("ts", "idx", "graph", tempFile, null);
            Path dir = java.nio.file.Paths.get(dirStr);
            // Drop a stray partial file that looks like a prior crash.
            Files.createFile(dir.resolve("block-00000007.tmp"));

            // Second write must wipe the dir entirely before redoing blocks.
            byte[] smaller = makePayload(500);
            Path temp2 = folder.newFile("y.bin").toPath();
            Files.write(temp2, smaller);
            dsm.writeMultipartIndexFile("ts", "idx", "graph", temp2, null);

            assertFalse("stray .tmp must be gone after overwrite",
                    Files.exists(dir.resolve("block-00000007.tmp")));
            // Exactly one block should remain.
            long blockCount = Files.list(dir)
                    .filter(p -> p.getFileName().toString().startsWith("block-"))
                    .count();
            assertEquals(1L, blockCount);
        }
    }
}
