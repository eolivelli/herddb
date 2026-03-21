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

package herddb.index.vector;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class FileBackedVectorValuesTest {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testPutGetRoundtrip() throws Exception {
        int dimension = 4;
        Path tempDir = tempFolder.getRoot().toPath();

        try (FileBackedVectorValues fbv = new FileBackedVectorValues(dimension, 10, tempDir)) {
            float[] v0 = {1.0f, 2.0f, 3.0f, 4.0f};
            float[] v1 = {5.0f, 6.0f, 7.0f, 8.0f};
            float[] v2 = {-1.0f, 0.0f, 0.5f, -0.5f};

            fbv.putVector(0, v0);
            fbv.putVector(1, v1);
            fbv.putVector(2, v2);

            assertEquals(3, fbv.size());
            assertEquals(dimension, fbv.dimension());

            assertArrayEquals(v0, toFloatArray(fbv.getVector(0)), 0.0001f);
            assertArrayEquals(v1, toFloatArray(fbv.getVector(1)), 0.0001f);
            assertArrayEquals(v2, toFloatArray(fbv.getVector(2)), 0.0001f);
        }
    }

    @Test
    public void testPutGetWithVectorFloat() throws Exception {
        int dimension = 3;
        Path tempDir = tempFolder.getRoot().toPath();

        try (FileBackedVectorValues fbv = new FileBackedVectorValues(dimension, 5, tempDir)) {
            float[] data = {10.0f, 20.0f, 30.0f};
            VectorFloat<?> vec = VTS.createFloatVector(data);
            fbv.putVector(0, vec);

            assertArrayEquals(data, toFloatArray(fbv.getVector(0)), 0.0001f);
        }
    }

    @Test
    public void testConcurrentWrites() throws Exception {
        int dimension = 8;
        int numVectors = 1000;
        int numThreads = 8;
        Path tempDir = tempFolder.getRoot().toPath();

        try (FileBackedVectorValues fbv = new FileBackedVectorValues(dimension, numVectors, tempDir)) {
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < numVectors; i++) {
                final int nodeId = i;
                futures.add(executor.submit(() -> {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    float[] data = new float[dimension];
                    for (int d = 0; d < dimension; d++) {
                        data[d] = nodeId * 100.0f + d;
                    }
                    fbv.putVector(nodeId, data);
                }));
            }

            startLatch.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
            executor.shutdown();

            assertEquals(numVectors, fbv.size());

            // Verify all vectors
            for (int i = 0; i < numVectors; i++) {
                float[] expected = new float[dimension];
                for (int d = 0; d < dimension; d++) {
                    expected[d] = i * 100.0f + d;
                }
                assertArrayEquals("Mismatch at nodeId " + i, expected,
                        toFloatArray(fbv.getVector(i)), 0.0001f);
            }
        }
    }

    @Test
    public void testFileGrowthBeyondInitialCapacity() throws Exception {
        int dimension = 4;
        // Start with capacity for only 2 vectors
        Path tempDir = tempFolder.getRoot().toPath();

        try (FileBackedVectorValues fbv = new FileBackedVectorValues(dimension, 2, tempDir)) {
            // Insert 100 vectors — well beyond initial capacity
            for (int i = 0; i < 100; i++) {
                float[] data = new float[dimension];
                for (int d = 0; d < dimension; d++) {
                    data[d] = i + d * 0.1f;
                }
                fbv.putVector(i, data);
            }

            assertEquals(100, fbv.size());

            // Verify all vectors survived growth
            for (int i = 0; i < 100; i++) {
                float[] expected = new float[dimension];
                for (int d = 0; d < dimension; d++) {
                    expected[d] = i + d * 0.1f;
                }
                assertArrayEquals("Mismatch at nodeId " + i, expected,
                        toFloatArray(fbv.getVector(i)), 0.0001f);
            }
        }
    }

    @Test
    public void testCloseDeletesTempFile() throws Exception {
        int dimension = 4;
        Path tempDir = tempFolder.getRoot().toPath();
        Path filePath;

        FileBackedVectorValues fbv = new FileBackedVectorValues(dimension, 10, tempDir);
        fbv.putVector(0, new float[]{1, 2, 3, 4});

        // Find the temp file
        filePath = tempDir.toFile().listFiles()[0].toPath();
        assertTrue("File should exist before close", filePath.toFile().exists());

        fbv.close();
        assertFalse("File should be deleted after close", filePath.toFile().exists());
    }

    @Test
    public void testIsNotValueShared() throws Exception {
        int dimension = 2;
        Path tempDir = tempFolder.getRoot().toPath();

        try (FileBackedVectorValues fbv = new FileBackedVectorValues(dimension, 5, tempDir)) {
            assertFalse(fbv.isValueShared());
        }
    }

    @Test
    public void testCopySharesData() throws Exception {
        int dimension = 3;
        Path tempDir = tempFolder.getRoot().toPath();

        try (FileBackedVectorValues fbv = new FileBackedVectorValues(dimension, 5, tempDir)) {
            fbv.putVector(0, new float[]{1, 2, 3});

            RandomAccessVectorValues copy = fbv.copy();
            assertArrayEquals(new float[]{1, 2, 3}, toFloatArray(copy.getVector(0)), 0.0001f);
            assertEquals(1, copy.size());
            assertEquals(dimension, copy.dimension());
        }
    }

    @Test
    public void testMultipleSegments() throws Exception {
        int dimension = 4;
        int vectorBytes = dimension * Float.BYTES; // 16 bytes per vector
        // Use a tiny segment size so that vectors span multiple segments
        int maxSegmentSize = vectorBytes * 3; // 48 bytes per segment = 3 vectors per segment
        Path tempDir = tempFolder.getRoot().toPath();
        int numVectors = 20; // will require ~7 segments

        try (FileBackedVectorValues fbv = new FileBackedVectorValues(dimension, numVectors, tempDir, maxSegmentSize)) {
            for (int i = 0; i < numVectors; i++) {
                float[] data = new float[dimension];
                for (int d = 0; d < dimension; d++) {
                    data[d] = i * 10.0f + d;
                }
                fbv.putVector(i, data);
            }

            assertEquals(numVectors, fbv.size());

            // Verify all vectors across segment boundaries
            for (int i = 0; i < numVectors; i++) {
                float[] expected = new float[dimension];
                for (int d = 0; d < dimension; d++) {
                    expected[d] = i * 10.0f + d;
                }
                assertArrayEquals("Mismatch at nodeId " + i, expected,
                        toFloatArray(fbv.getVector(i)), 0.0001f);
            }

            // Verify copy also works across segments
            RandomAccessVectorValues copy = fbv.copy();
            for (int i = 0; i < numVectors; i++) {
                float[] expected = new float[dimension];
                for (int d = 0; d < dimension; d++) {
                    expected[d] = i * 10.0f + d;
                }
                assertArrayEquals("Copy mismatch at nodeId " + i, expected,
                        toFloatArray(copy.getVector(i)), 0.0001f);
            }
        }
    }

    @Test
    public void testMultipleSegmentsWithGrowth() throws Exception {
        int dimension = 2;
        int vectorBytes = dimension * Float.BYTES; // 8 bytes per vector
        // 2 vectors per segment
        int maxSegmentSize = vectorBytes * 2;
        Path tempDir = tempFolder.getRoot().toPath();

        // Start with small expectedSize so the file must grow and remap with multiple segments
        try (FileBackedVectorValues fbv = new FileBackedVectorValues(dimension, 2, tempDir, maxSegmentSize)) {
            // Insert well beyond initial capacity
            for (int i = 0; i < 50; i++) {
                fbv.putVector(i, new float[]{i, i + 0.5f});
            }

            assertEquals(50, fbv.size());

            for (int i = 0; i < 50; i++) {
                assertArrayEquals("Mismatch at nodeId " + i,
                        new float[]{i, i + 0.5f},
                        toFloatArray(fbv.getVector(i)), 0.0001f);
            }
        }
    }

    @Test
    public void testVectorStraddlingSegmentBoundary() throws Exception {
        // Ensure a vector whose bytes cross a segment boundary is handled correctly.
        // dimension=4 => 16 bytes per vector. Segment size = 24 bytes.
        // Vector 0: bytes [0..15] -> segment 0
        // Vector 1: bytes [16..31] -> straddles segment 0 (bytes 16-23) and segment 1 (bytes 24-31)
        int dimension = 4;
        int maxSegmentSize = 24;
        Path tempDir = tempFolder.getRoot().toPath();

        try (FileBackedVectorValues fbv = new FileBackedVectorValues(dimension, 10, tempDir, maxSegmentSize)) {
            float[] v0 = {1.0f, 2.0f, 3.0f, 4.0f};
            float[] v1 = {5.0f, 6.0f, 7.0f, 8.0f};
            float[] v2 = {9.0f, 10.0f, 11.0f, 12.0f};

            fbv.putVector(0, v0);
            fbv.putVector(1, v1);
            fbv.putVector(2, v2);

            assertArrayEquals(v0, toFloatArray(fbv.getVector(0)), 0.0001f);
            assertArrayEquals(v1, toFloatArray(fbv.getVector(1)), 0.0001f);
            assertArrayEquals(v2, toFloatArray(fbv.getVector(2)), 0.0001f);
        }
    }

    @Test
    public void testConcurrentWritesWithMultipleSegments() throws Exception {
        int dimension = 8;
        int vectorBytes = dimension * Float.BYTES; // 32 bytes
        int maxSegmentSize = vectorBytes * 5; // 5 vectors per segment
        int numVectors = 200;
        int numThreads = 8;
        Path tempDir = tempFolder.getRoot().toPath();

        try (FileBackedVectorValues fbv = new FileBackedVectorValues(dimension, numVectors, tempDir, maxSegmentSize)) {
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < numVectors; i++) {
                final int nodeId = i;
                futures.add(executor.submit(() -> {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    float[] data = new float[dimension];
                    for (int d = 0; d < dimension; d++) {
                        data[d] = nodeId * 100.0f + d;
                    }
                    fbv.putVector(nodeId, data);
                }));
            }

            startLatch.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
            executor.shutdown();

            assertEquals(numVectors, fbv.size());

            for (int i = 0; i < numVectors; i++) {
                float[] expected = new float[dimension];
                for (int d = 0; d < dimension; d++) {
                    expected[d] = i * 100.0f + d;
                }
                assertArrayEquals("Mismatch at nodeId " + i, expected,
                        toFloatArray(fbv.getVector(i)), 0.0001f);
            }
        }
    }

    @Test
    public void testSparseNodeIds() throws Exception {
        int dimension = 2;
        Path tempDir = tempFolder.getRoot().toPath();

        try (FileBackedVectorValues fbv = new FileBackedVectorValues(dimension, 2, tempDir)) {
            // Insert at non-sequential positions
            fbv.putVector(0, new float[]{1, 2});
            fbv.putVector(50, new float[]{3, 4});
            fbv.putVector(100, new float[]{5, 6});

            assertArrayEquals(new float[]{1, 2}, toFloatArray(fbv.getVector(0)), 0.0001f);
            assertArrayEquals(new float[]{3, 4}, toFloatArray(fbv.getVector(50)), 0.0001f);
            assertArrayEquals(new float[]{5, 6}, toFloatArray(fbv.getVector(100)), 0.0001f);
        }
    }

    private float[] toFloatArray(VectorFloat<?> vec) {
        float[] result = new float[vec.length()];
        for (int i = 0; i < result.length; i++) {
            result[i] = vec.get(i);
        }
        return result;
    }
}
