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

package herddb.remote;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.remote.storage.LocalObjectStorage;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #645: end-to-end restart coverage for the direct-S3 upload path.
 *
 * <p>The bench failure that motivated this test (Run 9 BIGANN 200M) showed
 * the indexing service crash-looping on restart after running with
 * {@code indexing.s3.direct.write.enabled=true}: segments uploaded via the
 * direct-S3 path were unreadable after a JVM restart and the load path
 * silently fell back to the gRPC file-server which had no record of those
 * blocks. These tests pin the invariants that prevent that regression:
 *
 * <ol>
 *   <li>Files uploaded via {@code writeMultipartIndexFile} on DSM A must be
 *       readable via {@code multipartIndexReaderSupplier} and
 *       {@code downloadMultipartIndexFile} on a freshly-constructed DSM B
 *       against the same backing object storage, without any in-memory
 *       state carried over.</li>
 *   <li>The probe cache must not be required — DSM B starts empty and
 *       discovers the bulk layout strictly via {@code existsObject} HEAD
 *       probes. After a successful probe the cache fills as expected.</li>
 *   <li>Many segments (graph + map files for each, mirroring the FusedPQ
 *       compaction shape) must all be readable after a single restart.</li>
 *   <li>A mixed layout (some files written via direct-S3, some legacy
 *       per-block) must round-trip correctly through a restart — the bulk
 *       probe must say {@code true} for the direct-S3 ones and {@code
 *       false} for the legacy ones, routing each read through the right
 *       code path.</li>
 * </ol>
 *
 * <p>The test uses {@link LocalObjectStorage} as the direct-S3 stand-in:
 * it durably persists objects to a local directory so the simulated
 * restart (close DSM A, allocate DSM B against the same directory)
 * faithfully reproduces the IS-restart-against-MinIO failure mode.
 */
public class RemoteFileDataStorageManagerDirectUploadRestartTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private ExecutorService metadataExecutor;
    private Path objectStorageDir;
    private LocalObjectStorage objectStorageA;
    private RemoteFileServiceClient stubClient;
    private RemoteFileDataStorageManager dsmA;

    @Before
    public void setUp() throws IOException {
        metadataExecutor = Executors.newSingleThreadExecutor();
        objectStorageDir = tmpFolder.newFolder("object-storage").toPath();
        objectStorageA = new LocalObjectStorage(objectStorageDir, metadataExecutor);
        // gRPC stub client with fast-fail config — any accidental fallback
        // to the file-server path must surface immediately (1 s timeout,
        // 0 retries) instead of blocking the test for minutes.
        Map<String, Object> fastFailConfig = new HashMap<>();
        fastFailConfig.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 1L);
        fastFailConfig.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 0);
        stubClient = new RemoteFileServiceClient(Collections.emptyList(), fastFailConfig);
        Path metaDir = tmpFolder.newFolder("meta-a").toPath();
        Path tmpDirA = tmpFolder.newFolder("tmp-a").toPath();
        dsmA = new RemoteFileDataStorageManager(metaDir, tmpDirA, Integer.MAX_VALUE, stubClient);
        dsmA.setDirectObjectStorage(objectStorageA);
        dsmA.enableDirectUpload(16L * 1024 * 1024);
    }

    @After
    public void tearDown() throws Exception {
        if (dsmA != null) {
            dsmA.close();
        }
        if (objectStorageA != null) {
            objectStorageA.close();
        }
        if (stubClient != null) {
            stubClient.close();
        }
        if (metadataExecutor != null) {
            metadataExecutor.shutdown();
        }
    }

    /**
     * Helper: writes {@code content} to a temp file and returns its path so
     * the test can hand it to {@code writeMultipartIndexFile}.
     */
    private Path writeTempFile(String name, byte[] content) throws IOException {
        Path f = tmpFolder.newFile(name).toPath();
        Files.write(f, content);
        return f;
    }

    private static byte[] randomBytes(int size, long seed) {
        Random rng = new Random(seed);
        byte[] data = new byte[size];
        rng.nextBytes(data);
        return data;
    }

    /**
     * Allocates a fresh DSM bound to the same backing object storage as
     * DSM A — the simulated restart. The new DSM has its own metaDir and
     * tmpDir (mirroring real IS behaviour: ephemeral local state) and
     * starts with empty {@code bulkLayoutCache} and {@code bulkLocalCache}
     * maps, so every bulk-layout discovery on it must go through the
     * authoritative HEAD probe against the shared storage.
     */
    private RemoteFileDataStorageManager spawnRestartDsm() throws IOException {
        // Reuse the SAME on-disk directory as objectStorageA. A new
        // LocalObjectStorage instance is the on-disk equivalent of a
        // fresh S3 client connecting to the same bucket.
        LocalObjectStorage objectStorageB = new LocalObjectStorage(
                objectStorageDir, metadataExecutor);
        Path metaDir = tmpFolder.newFolder("meta-b-" + System.nanoTime()).toPath();
        Path tmpDir = tmpFolder.newFolder("tmp-b-" + System.nanoTime()).toPath();
        RemoteFileDataStorageManager dsmB = new RemoteFileDataStorageManager(
                metaDir, tmpDir, Integer.MAX_VALUE, stubClient);
        dsmB.setDirectObjectStorage(objectStorageB);
        dsmB.enableDirectUpload(16L * 1024 * 1024);
        return dsmB;
    }

    /**
     * Issue #645 — primary regression test.
     *
     * <p>A multipart file uploaded via the direct-S3 path on DSM A must be
     * readable end-to-end on a freshly-allocated DSM B against the same
     * backing storage. This mirrors the IS restart: the in-memory
     * {@code bulkLayoutCache} on DSM A is gone, the probe on DSM B fires a
     * HEAD against the shared storage, finds the {@code .bulk} object,
     * caches the positive result, and materialises the local cache file
     * for the memory-mapped reader.
     */
    @Test
    public void directUploadedFileIsReadableAfterRestart() throws Exception {
        byte[] payload = randomBytes(64 * 1024 + 17, 11L);
        Path src = writeTempFile("graph-a.bin", payload);
        dsmA.writeMultipartIndexFile("ts", "uuid-restart-1", "graph", src, null);
        assertTrue("file must be visible on the writer DSM",
                dsmA.multipartIndexFileExists("ts", "uuid-restart-1", "graph"));

        // Close DSM A — clears in-memory bulkLayoutCache and bulkLocalCache.
        dsmA.close();
        dsmA = null;

        RemoteFileDataStorageManager dsmB = spawnRestartDsm();
        try {
            // multipartIndexFileExists must say "true" via the HEAD probe
            // (the cache is empty on DSM B). This is the lenient-probe
            // entry point.
            assertTrue("bulk file must be discoverable on a fresh DSM via HEAD probe",
                    dsmB.multipartIndexFileExists("ts", "uuid-restart-1", "graph"));

            // downloadMultipartIndexFile must take the bulk branch and
            // reconstruct the bytes identically.
            Path dest = tmpFolder.newFile("graph-a-out.bin").toPath();
            dsmB.downloadMultipartIndexFile(
                    "ts", "uuid-restart-1", "graph", payload.length, dest);
            assertArrayEquals("download must reconstruct original bytes",
                    payload, Files.readAllBytes(dest));

            // multipartIndexReaderSupplier must return a MappedChunkReader
            // (the bulk-layout reader supplier), NOT a RemoteRandomAccessReader
            // backed by the gRPC client.
            ReaderSupplier supplier = dsmB.multipartIndexReaderSupplier(
                    "ts", "uuid-restart-1", "graph", payload.length);
            assertNotNull(supplier);
            String className = supplier.getClass().getName();
            assertTrue("supplier must be MappedChunkReader-backed, was "
                            + className,
                    className.contains("MappedChunkReader"));

            // The reader must serve the file: read a 4-byte window from
            // offset 0 and compare against the original payload.
            try (RandomAccessReader reader = supplier.get()) {
                reader.seek(0L);
                byte[] head = new byte[4];
                reader.readFully(head);
                byte[] expected = new byte[4];
                System.arraycopy(payload, 0, expected, 0, 4);
                assertArrayEquals("first 4 bytes read via supplier must match",
                        expected, head);
            }
        } finally {
            dsmB.close();
        }
    }

    /**
     * Issue #645: many segments (mirroring the FusedPQ shape: graph + map
     * per segment) uploaded across the writer's lifetime must ALL be
     * readable after a single restart. This is the closest unit-level
     * approximation of the bench failure (Run 9: ~36 segments missing on
     * restart).
     */
    @Test
    public void manySegmentsAreReadableAfterRestart() throws Exception {
        // Use a modest count — the loop is O(segments) and each iteration
        // does a real local I/O round-trip via LocalObjectStorage. 12
        // segments × 2 files each = 24 multipart objects, enough to expose
        // any per-file state leakage without slowing the build.
        final int segments = 12;
        List<byte[]> graphPayloads = new ArrayList<>(segments);
        List<byte[]> mapPayloads = new ArrayList<>(segments);
        for (int i = 0; i < segments; i++) {
            byte[] graphPayload = randomBytes(8 * 1024 + i, 100L + i);
            byte[] mapPayload = randomBytes(4 * 1024 + i, 200L + i);
            graphPayloads.add(graphPayload);
            mapPayloads.add(mapPayload);
            Path graphTemp = writeTempFile("graph-" + i + ".bin", graphPayload);
            Path mapTemp = writeTempFile("map-" + i + ".bin", mapPayload);
            dsmA.writeMultipartIndexFile("ts", "uuid-seg" + i, "graph", graphTemp, null);
            dsmA.writeMultipartIndexFile("ts", "uuid-seg" + i, "map", mapTemp, null);
        }

        // Close DSM A — wipes in-memory caches.
        dsmA.close();
        dsmA = null;

        RemoteFileDataStorageManager dsmB = spawnRestartDsm();
        try {
            for (int i = 0; i < segments; i++) {
                Path graphOut = tmpFolder.newFile("graph-" + i + "-out.bin").toPath();
                Path mapOut = tmpFolder.newFile("map-" + i + "-out.bin").toPath();
                dsmB.downloadMultipartIndexFile(
                        "ts", "uuid-seg" + i, "graph",
                        graphPayloads.get(i).length, graphOut);
                dsmB.downloadMultipartIndexFile(
                        "ts", "uuid-seg" + i, "map",
                        mapPayloads.get(i).length, mapOut);
                assertArrayEquals("graph segment " + i + " must round-trip",
                        graphPayloads.get(i), Files.readAllBytes(graphOut));
                assertArrayEquals("map segment " + i + " must round-trip",
                        mapPayloads.get(i), Files.readAllBytes(mapOut));
            }
        } finally {
            dsmB.close();
        }
    }

    /**
     * Issue #645: an installation that has a mix of legacy per-block
     * files (written before direct-S3 was enabled) and direct-S3 bulk
     * files (written after) must route reads correctly after restart.
     * The bulk probe must answer {@code true} for the direct files and
     * {@code false} for the legacy ones; the read paths must follow.
     *
     * <p>The legacy file is materialised by writing per-block content
     * directly into the {@link LocalObjectStorage} (mirroring what the
     * old gRPC write path would have produced); the bulk file is written
     * via the normal direct-upload API on DSM A.
     */
    @Test
    public void mixedLegacyAndBulkFilesSurviveRestart() throws Exception {
        // Direct-S3 bulk file via DSM A's normal direct-upload path.
        byte[] bulkPayload = randomBytes(2048, 7L);
        Path bulkSrc = writeTempFile("mixed-bulk.bin", bulkPayload);
        dsmA.writeMultipartIndexFile("ts", "uuid-bulk", "graph", bulkSrc, null);

        // Legacy per-block file: write the blocks directly via the backing
        // object storage so DSM A's bulk-write path is never invoked for
        // this logical path. Reuse the storage instance bound to DSM A so
        // the blocks live in the same directory.
        byte[] legacyPayload = randomBytes(1024, 9L);
        String legacyLogical = "ts/uuid-legacy/multipart/graph";
        objectStorageA.writeBlock(legacyLogical, 0L, legacyPayload).get();

        // Pre-condition: legacy .bulk variant must NOT exist (so the probe
        // will route to gRPC). For this test we cannot exercise the gRPC
        // path (no real file server is running) — but we can assert the
        // probe answers {@code false} correctly, which is the only thing
        // the routing decision depends on.
        assertFalse("legacy .bulk object must be absent before restart",
                objectStorageA.existsObject(legacyLogical + ".bulk").get());

        dsmA.close();
        dsmA = null;

        RemoteFileDataStorageManager dsmB = spawnRestartDsm();
        try {
            // Bulk file: probe must say true and download must succeed.
            Path bulkOut = tmpFolder.newFile("mixed-bulk-out.bin").toPath();
            dsmB.downloadMultipartIndexFile("ts", "uuid-bulk", "graph",
                    bulkPayload.length, bulkOut);
            assertArrayEquals("bulk file must round-trip",
                    bulkPayload, Files.readAllBytes(bulkOut));

            // Legacy file: the bulk probe must answer false on the fresh
            // DSM. We invoke multipartIndexFileExists which is the
            // lenient-probe entry point — the bulk check returns false,
            // then the gRPC presence check runs (fast-failing on the
            // empty stub) and also returns false. The key behaviour we
            // pin: the bulk probe must NOT incorrectly say true for a
            // file that only has the legacy layout.
            boolean legacyVisible = dsmB.multipartIndexFileExists(
                    "ts", "uuid-legacy", "graph");
            assertFalse("legacy file must not be reachable as bulk on the fresh DSM"
                            + " (no gRPC server in this unit test)",
                    legacyVisible);
        } finally {
            dsmB.close();
        }
    }

    /**
     * Issue #645: a cold-start restart that immediately reads many files
     * must issue exactly one HEAD probe per logical path (i.e. probe
     * caching works on the restarted DSM too — once a positive answer
     * has been observed, subsequent reads of the same logical path
     * reuse the cached answer).
     *
     * <p>This guards against an O(reads × HEAD) regression that would
     * blow up the cold-start probe cost on the first few minutes after
     * restart.
     */
    @Test
    public void coldStartProbeIsCachedAcrossReads() throws Exception {
        byte[] payload = randomBytes(1024, 13L);
        Path src = writeTempFile("cached.bin", payload);
        dsmA.writeMultipartIndexFile("ts", "uuid-cached", "graph", src, null);
        dsmA.close();
        dsmA = null;

        // Wrap LocalObjectStorage with a counter so we can assert the
        // exact number of HEAD probes issued by DSM B.
        ProbeCountingLocalObjectStorage countingStorage =
                new ProbeCountingLocalObjectStorage(objectStorageDir, metadataExecutor);
        Path metaDir = tmpFolder.newFolder("meta-counting").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp-counting").toPath();
        RemoteFileDataStorageManager dsmB = new RemoteFileDataStorageManager(
                metaDir, tmpDir, Integer.MAX_VALUE, stubClient);
        dsmB.setDirectObjectStorage(countingStorage);
        dsmB.enableDirectUpload(16L * 1024 * 1024);
        try {
            assertEquals("no probes before any read", 0, countingStorage.headCount());

            // First read: 1 HEAD probe.
            Path out1 = tmpFolder.newFile("cached-out1.bin").toPath();
            dsmB.downloadMultipartIndexFile(
                    "ts", "uuid-cached", "graph", payload.length, out1);
            assertEquals("first read issues exactly one HEAD",
                    1, countingStorage.headCount());

            // Second + third reads: must hit the cache, no new HEADs.
            Path out2 = tmpFolder.newFile("cached-out2.bin").toPath();
            dsmB.downloadMultipartIndexFile(
                    "ts", "uuid-cached", "graph", payload.length, out2);
            Path out3 = tmpFolder.newFile("cached-out3.bin").toPath();
            dsmB.downloadMultipartIndexFile(
                    "ts", "uuid-cached", "graph", payload.length, out3);
            assertEquals("second and third reads must be probe-cache hits",
                    1, countingStorage.headCount());

            // multipartIndexFileExists on the same logical path must also
            // be a cache hit.
            assertTrue(dsmB.multipartIndexFileExists("ts", "uuid-cached", "graph"));
            assertEquals("multipartIndexFileExists must be a cache hit",
                    1, countingStorage.headCount());
        } finally {
            dsmB.close();
        }
    }

    /**
     * {@link LocalObjectStorage} subclass that counts every
     * {@code existsObject} (HEAD) probe so the cold-start cache test can
     * assert the exact number of round-trips.
     */
    private static final class ProbeCountingLocalObjectStorage extends LocalObjectStorage {

        private final java.util.concurrent.atomic.AtomicInteger headProbes =
                new java.util.concurrent.atomic.AtomicInteger();

        ProbeCountingLocalObjectStorage(Path baseDir, ExecutorService executor)
                throws IOException {
            super(baseDir, executor);
        }

        @Override
        public java.util.concurrent.CompletableFuture<Boolean> existsObject(String path) {
            headProbes.incrementAndGet();
            // Delegate to the default contract from ObjectStorage which uses
            // read() and now obeys the tri-state contract (issue #645).
            return super.existsObject(path);
        }

        int headCount() {
            return headProbes.get();
        }
    }
}
