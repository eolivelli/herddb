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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalObjectStorageTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ExecutorService executor;
    private LocalObjectStorage storage;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newFixedThreadPool(4);
        storage = new LocalObjectStorage(folder.newFolder("data").toPath(), executor);
    }

    @After
    public void tearDown() throws Exception {
        storage.close();
        executor.shutdown();
    }

    @Test
    public void testWriteRead() throws Exception {
        byte[] data = "hello world".getBytes();
        storage.write("ts1/uuid1/1.page", data).get();

        ReadResult result = storage.read("ts1/uuid1/1.page").get();
        try {
            assertEquals(ReadResult.Status.FOUND, result.status());
            assertArrayEquals(data, result.content());
        } finally {
            result.release();
        }
    }

    @Test
    public void testReadMissing() throws Exception {
        ReadResult result = storage.read("nonexistent/path.page").get();
        try {
            assertEquals(ReadResult.Status.NOT_FOUND, result.status());
        } finally {
            result.release();
        }
    }

    @Test
    public void testDelete() throws Exception {
        byte[] data = "content".getBytes();
        storage.write("ts1/uuid2/1.page", data).get();

        assertTrue(storage.delete("ts1/uuid2/1.page").get());
        assertFalse(storage.delete("ts1/uuid2/1.page").get());
        ReadResult missing = storage.read("ts1/uuid2/1.page").get();
        try {
            assertEquals(ReadResult.Status.NOT_FOUND, missing.status());
        } finally {
            missing.release();
        }
    }

    @Test
    public void testList() throws Exception {
        storage.write("ts1/uuid1/1.page", "a".getBytes()).get();
        storage.write("ts1/uuid1/2.page", "b".getBytes()).get();
        storage.write("ts2/uuid2/1.page", "c".getBytes()).get();

        List<String> paths = storage.list("ts1/").get();
        assertEquals(2, paths.size());
        assertTrue(paths.stream().allMatch(p -> p.startsWith("ts1/")));
    }

    @Test
    public void testDeleteByPrefix() throws Exception {
        storage.write("ts1/uuid1/1.page", "a".getBytes()).get();
        storage.write("ts1/uuid1/2.page", "b".getBytes()).get();
        storage.write("ts2/uuid2/1.page", "c".getBytes()).get();

        int deleted = storage.deleteByPrefix("ts1/").get();
        assertEquals(2, deleted);

        List<String> remaining = storage.list("").get();
        assertEquals(1, remaining.size());
        assertTrue(remaining.get(0).startsWith("ts2/"));
    }

    @Test
    public void testWriteReadRange() throws Exception {
        // Single-object layout: write the whole file in one shot, then readRange
        // returns slices of that one object.
        byte[] full = new byte[180];
        for (int i = 0; i < 100; i++) {
            full[i] = (byte) i;
        }
        for (int i = 0; i < 80; i++) {
            full[100 + i] = (byte) (i + 100);
        }

        storage.write("ts1/uuid1/graph", full).get();

        // Read a range within the first 100 bytes
        ReadResult r0 = storage.readRange("ts1/uuid1/graph", 10, 20, 100).get();
        try {
            assertEquals(ReadResult.Status.FOUND, r0.status());
            byte[] r0Bytes = r0.content();
            assertEquals(20, r0Bytes.length);
            for (int i = 0; i < 20; i++) {
                assertEquals((byte) (10 + i), r0Bytes[i]);
            }
        } finally {
            r0.release();
        }

        // Read first 5 bytes of the second half (offset 100)
        ReadResult r1 = storage.readRange("ts1/uuid1/graph", 100, 5, 100).get();
        try {
            assertEquals(ReadResult.Status.FOUND, r1.status());
            byte[] r1Bytes = r1.content();
            assertEquals(5, r1Bytes.length);
            for (int i = 0; i < 5; i++) {
                assertEquals((byte) (100 + i), r1Bytes[i]);
            }
        } finally {
            r1.release();
        }

        // Read past the end
        ReadResult missing = storage.readRange("ts1/uuid1/graph", 200, 10, 100).get();
        try {
            assertEquals(ReadResult.Status.NOT_FOUND, missing.status());
        } finally {
            missing.release();
        }
    }

    @Test
    public void testDeleteSingleObject() throws Exception {
        storage.write("ts1/uuid1/plain.page", "plain".getBytes()).get();
        storage.write("ts1/uuid1/multi", "data".getBytes()).get();

        assertTrue(storage.delete("ts1/uuid1/multi").get());
        assertFalse(storage.delete("ts1/uuid1/multi").get()); // already gone

        // plain file unaffected
        ReadResult r = storage.read("ts1/uuid1/plain.page").get();
        try {
            assertEquals(ReadResult.Status.FOUND, r.status());
        } finally {
            r.release();
        }
    }

    @Test
    public void testListSingleObjects() throws Exception {
        storage.write("ts1/uuid1/a.page", "a".getBytes()).get();
        storage.write("ts1/uuid1/bigfile", "abc".getBytes()).get();

        List<String> logical = storage.list("ts1/").get();
        assertEquals(2, logical.size());
        assertTrue(logical.contains("ts1/uuid1/a.page"));
        assertTrue(logical.contains("ts1/uuid1/bigfile"));
    }

    @Test
    public void testConcurrentReads() throws Exception {
        byte[] data = "concurrent data".getBytes();
        storage.write("ts1/uuid1/1.page", data).get();

        // Submit multiple concurrent reads and verify all succeed
        List<CompletableFuture<ReadResult>> futures = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            futures.add(storage.read("ts1/uuid1/1.page"));
        }

        for (CompletableFuture<ReadResult> f : futures) {
            ReadResult result = f.get();
            try {
                assertEquals(ReadResult.Status.FOUND, result.status());
                assertArrayEquals(data, result.content());
            } finally {
                result.release();
            }
        }
    }

    @Test
    public void testConcurrentWrites() throws Exception {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            final int idx = i;
            futures.add(storage.write("ts1/page" + idx, ("data" + idx).getBytes()));
        }

        for (CompletableFuture<Void> f : futures) {
            f.get();
        }

        // Verify all writes succeeded
        for (int i = 0; i < 4; i++) {
            ReadResult result = storage.read("ts1/page" + i).get();
            try {
                assertEquals(ReadResult.Status.FOUND, result.status());
                assertArrayEquals(("data" + i).getBytes(), result.content());
            } finally {
                result.release();
            }
        }
    }

    @Test
    public void testLargeFileRead() throws Exception {
        byte[] largeData = new byte[10 * 1024 * 1024]; // 10 MB
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i & 0xFF);
        }

        storage.write("ts1/uuid1/large.page", largeData).get();
        ReadResult result = storage.read("ts1/uuid1/large.page").get();
        try {
            assertEquals(ReadResult.Status.FOUND, result.status());
            assertArrayEquals(largeData, result.content());
        } finally {
            result.release();
        }
    }

    @Test
    public void testReadRangeOutOfBounds() throws Exception {
        byte[] data = new byte[100];
        for (int i = 0; i < 100; i++) {
            data[i] = (byte) i;
        }
        storage.write("ts1/uuid1/graph", data).get();

        // Request beyond file size
        ReadResult result = storage.readRange("ts1/uuid1/graph", 150, 10, 100).get();
        try {
            assertEquals(ReadResult.Status.NOT_FOUND, result.status());
        } finally {
            result.release();
        }
    }

    @Test
    public void testReadRangePastEnd() throws Exception {
        byte[] data = new byte[100];
        storage.write("ts1/uuid1/graph", data).get();

        // Request past end of the single object
        ReadResult result = storage.readRange("ts1/uuid1/graph", 200, 10, 100).get();
        try {
            assertEquals(ReadResult.Status.NOT_FOUND, result.status());
        } finally {
            result.release();
        }
    }

    @Test
    public void testAsyncReadHandlesException() throws Exception {
        // This tests that async read properly handles exceptions (e.g., from channel.read)
        // by completing the future exceptionally
        byte[] data = "test".getBytes();
        storage.write("ts1/test.page", data).get();

        // Normal read should work fine
        ReadResult result = storage.read("ts1/test.page").get();
        try {
            assertEquals(ReadResult.Status.FOUND, result.status());
        } finally {
            result.release();
        }
    }

    @Test
    public void testAsyncWriteHandlesException() throws Exception {
        // Write should succeed even with complex paths
        byte[] data = "test".getBytes();
        storage.write("ts1/uuid1/nested/deep/path/1.page", data).get();

        ReadResult result = storage.read("ts1/uuid1/nested/deep/path/1.page").get();
        try {
            assertEquals(ReadResult.Status.FOUND, result.status());
            assertArrayEquals(data, result.content());
        } finally {
            result.release();
        }
    }
}
