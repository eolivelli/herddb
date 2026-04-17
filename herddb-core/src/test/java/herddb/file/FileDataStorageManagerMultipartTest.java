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
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.nio.file.Files;
import java.nio.file.Path;
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
}
