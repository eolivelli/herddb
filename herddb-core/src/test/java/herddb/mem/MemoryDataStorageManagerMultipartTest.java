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

package herddb.mem;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MemoryDataStorageManagerMultipartTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void writeReadDelete() throws Exception {
        byte[] payload = new byte[4096];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i & 0xFF);
        }
        Path tempFile = folder.newFile("write.bin").toPath();
        Files.write(tempFile, payload);

        try (MemoryDataStorageManager dsm = new MemoryDataStorageManager()) {
            AtomicLong progressTotal = new AtomicLong();
            String logicalPath = dsm.writeMultipartIndexFile(
                    "ts", "idx", "graph", tempFile, progressTotal::addAndGet);
            assertNotNull(logicalPath);
            assertEquals(payload.length, progressTotal.get());

            try (ReaderSupplier supplier =
                         dsm.multipartIndexReaderSupplier("ts", "idx", "graph", payload.length);
                 RandomAccessReader reader = supplier.get()) {
                byte[] readBack = new byte[payload.length];
                reader.readFully(readBack);
                assertArrayEquals(payload, readBack);

                reader.seek(100);
                byte[] slice = new byte[200];
                reader.readFully(slice);
                for (int i = 0; i < slice.length; i++) {
                    assertEquals((byte) ((100 + i) & 0xFF), slice[i]);
                }
                assertEquals(payload.length, reader.length());
            }

            dsm.deleteMultipartIndexFile("ts", "idx", "graph");
            try {
                dsm.multipartIndexReaderSupplier("ts", "idx", "graph", payload.length);
                org.junit.Assert.fail("expected DataStorageManagerException after delete");
            } catch (herddb.storage.DataStorageManagerException expected) {
                // ok
            }
        }
    }

    @Test
    public void writeEmptyFile() throws Exception {
        Path tempFile = folder.newFile("empty.bin").toPath();
        Files.write(tempFile, new byte[0]);

        try (MemoryDataStorageManager dsm = new MemoryDataStorageManager()) {
            AtomicLong progressTotal = new AtomicLong();
            dsm.writeMultipartIndexFile("ts", "idx", "map", tempFile, progressTotal::addAndGet);
            assertEquals(0L, progressTotal.get());

            try (ReaderSupplier supplier =
                         dsm.multipartIndexReaderSupplier("ts", "idx", "map", 0);
                 RandomAccessReader reader = supplier.get()) {
                assertEquals(0L, reader.length());
            }
        }
    }

    @Test
    public void eraseTablespaceDataRemovesMultipartFiles() throws Exception {
        Path tempFile = folder.newFile("data.bin").toPath();
        Files.write(tempFile, new byte[]{1, 2, 3, 4});

        try (MemoryDataStorageManager dsm = new MemoryDataStorageManager()) {
            dsm.writeMultipartIndexFile("ts", "idx", "graph", tempFile, null);
            dsm.writeMultipartIndexFile("other", "idx", "graph", tempFile, null);

            dsm.eraseTablespaceData("ts");

            // "other" tablespace still resolvable
            try (ReaderSupplier s = dsm.multipartIndexReaderSupplier("other", "idx", "graph", 4)) {
                try (RandomAccessReader r = s.get()) {
                    assertEquals(4L, r.length());
                }
            }
            try {
                dsm.multipartIndexReaderSupplier("ts", "idx", "graph", 4);
                org.junit.Assert.fail("expected wipe to remove 'ts' multipart data");
            } catch (herddb.storage.DataStorageManagerException expected) {
                // ok
            }
        }
    }
}
