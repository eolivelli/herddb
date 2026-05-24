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
import static org.junit.Assert.assertTrue;
import herddb.remote.storage.LocalObjectStorage;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #650: verifies the single-object multipart layout end-to-end against
 * {@link RemoteFileDataStorageManager#writeMultipartIndexFile} and
 * {@link RemoteFileDataStorageManager#multipartIndexReaderSupplier}.
 *
 * <p>The write path goes through {@code directObjectStorage.uploadFile}
 * (a single S3 object — no {@code .multipart/{N}} per-block layout, no
 * {@code .bulk} suffix). The read path uses a no-server
 * {@link RemoteFileServiceClient} stub: the reader supplier returned by
 * {@code multipartIndexReaderSupplier} is constructed with the stub client,
 * and the actual reads in this test go via the
 * {@link #downloadMultipartIndexFile} path which uses the wired
 * {@link LocalObjectStorage} directly.
 *
 * <p>Boundary sizes exercised: sub-block (1 KiB), exact 1 block (4 MiB),
 * exact 3 blocks (12 MiB) and 3 blocks + partial trailing (12 MiB + 1234 B).
 * For each size we assert that exactly ONE object lands in the inner storage
 * at the logical path — no extra keys under {@code .multipart/{N}}, no
 * {@code .bulk} suffix — and the reconstructed bytes match the original.
 */
public class RemoteFileDataStorageManagerSingleObjectLayoutTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /** Must equal {@code RemoteFileDataStorageManager.MULTIPART_BLOCK_SIZE}. */
    private static final int BLOCK_SIZE = 4 * 1024 * 1024;

    private ExecutorService metadataExecutor;
    private LocalObjectStorage objectStorage;
    private Path storageDir;
    private RemoteFileServiceClient stubClient;
    private RemoteFileDataStorageManager dsm;

    @Before
    public void setUp() throws IOException {
        metadataExecutor = Executors.newSingleThreadExecutor();
        storageDir = tmpFolder.newFolder("storage").toPath();
        objectStorage = new LocalObjectStorage(storageDir, metadataExecutor);

        // No-server client: the only API we touch via the supplier path is
        // client.getBlockSize() during the reader construction. We exercise
        // the actual reads via downloadMultipartIndexFile, which goes
        // through directObjectStorage and does not need a real server.
        stubClient = new RemoteFileServiceClient(Collections.emptyList());

        Path metaDir = tmpFolder.newFolder("meta").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        dsm = new RemoteFileDataStorageManager(metaDir, tmpDir, Integer.MAX_VALUE, stubClient);
        dsm.setDirectObjectStorage(objectStorage);
        // Direct-S3 uploads are now the only multipart write path (#650).
        dsm.enableDirectUpload(1L << 30);
    }

    @After
    public void tearDown() throws Exception {
        if (dsm != null) {
            dsm.close();
        }
        if (stubClient != null) {
            stubClient.close();
        }
        if (metadataExecutor != null) {
            metadataExecutor.shutdown();
        }
    }

    private static byte[] randomBytes(int size, long seed) {
        Random rng = new Random(seed);
        byte[] data = new byte[size];
        rng.nextBytes(data);
        return data;
    }

    /**
     * Lists every key currently present under {@code prefix} in the
     * {@link LocalObjectStorage}. Used to assert single-object layout —
     * exactly one entry at the logical path.
     */
    private List<String> listAll(String prefix) throws Exception {
        return objectStorage.list(prefix).get();
    }

    /**
     * Writes a payload via {@link RemoteFileDataStorageManager#writeMultipartIndexFile}
     * and asserts:
     * <ul>
     *   <li>exactly ONE object exists in the inner storage under the segment prefix;</li>
     *   <li>that object's key is exactly the logical multipart path
     *       ({@code tableSpace/uuid/multipart/fileType}) — no
     *       {@code .multipart/{N}} per-block suffix, no {@code .bulk}
     *       trailing suffix;</li>
     *   <li>round-tripping via {@code downloadMultipartIndexFile} yields the
     *       original bytes byte-for-byte.</li>
     * </ul>
     */
    private void writeAndAssertSingleObject(String tableSpace, String uuid,
                                            String fileType, byte[] payload) throws Exception {
        Path tempFile = tmpFolder.newFile().toPath();
        Files.write(tempFile, payload);
        String returnedLogicalPath = dsm.writeMultipartIndexFile(
                tableSpace, uuid, fileType, tempFile, deltaBytes -> { });
        String expectedLogicalPath = tableSpace + "/" + uuid + "/multipart/" + fileType;
        assertEquals("writeMultipartIndexFile must return the logical path",
                expectedLogicalPath, returnedLogicalPath);

        // The segment's storage namespace should hold exactly ONE key — the
        // single S3 object at the logical path. No .multipart/{N}, no .bulk.
        List<String> keysUnderSegment = listAll(tableSpace + "/" + uuid + "/");
        assertEquals("single-object layout: exactly one key per multipart file, "
                + "got: " + keysUnderSegment, 1, keysUnderSegment.size());
        String key = keysUnderSegment.get(0);
        assertEquals("object must be at the logical path, not a sub-key",
                expectedLogicalPath, key);
        assertFalse("no .bulk suffix in single-object layout",
                key.endsWith(".bulk"));
        assertFalse("no .multipart/{N} per-block suffix in single-object layout",
                key.contains("/.multipart/") || key.matches(".*/multipart/[^/]+/\\d+$"));

        // Read back via downloadMultipartIndexFile (uses directObjectStorage)
        // and confirm byte-identical reconstruction.
        Path dest = tmpFolder.newFile().toPath();
        dsm.downloadMultipartIndexFile(tableSpace, uuid, fileType, payload.length, dest);
        byte[] roundTripped = Files.readAllBytes(dest);
        assertArrayEquals("multipart write/read must be byte-identical for size "
                + payload.length, payload, roundTripped);

        // Sanity-check the reader supplier wires up against the same logical
        // path. multipartIndexReaderSupplier construction must succeed; we do
        // not exercise actual range reads here because that would route via
        // the gRPC client which has no servers in this test.
        ReaderSupplier supplier = dsm.multipartIndexReaderSupplier(
                tableSpace, uuid, fileType, payload.length);
        assertTrue("ReaderSupplier must be returned", supplier != null);
        // Defensive: try to close any handle the supplier might have opened
        // up-front. The remote-supplier implementation is lazy so this is a
        // no-op; we still call it to flush any eager resource grab.
        try {
            // Some ReaderSupplier impls expose AutoCloseable through the
            // returned RandomAccessReader, not the supplier itself; we don't
            // open one here. The supplier's own close() (if AutoCloseable) is
            // best-effort.
            if (supplier instanceof AutoCloseable) {
                ((AutoCloseable) supplier).close();
            }
        } catch (Exception ignored) {
            // Best-effort: supplier teardown failures are unrelated to the
            // single-object layout contract under test.
        }
    }

    @Test
    public void subBlockPayloadRoundTripsAsSingleObject() throws Exception {
        byte[] payload = randomBytes(1024, 1L);          // 1 KiB — < 4 MiB
        writeAndAssertSingleObject("ts", "uuidA", "graph", payload);
    }

    @Test
    public void exactlyOneBlockPayloadRoundTripsAsSingleObject() throws Exception {
        byte[] payload = randomBytes(BLOCK_SIZE, 2L);    // 4 MiB
        writeAndAssertSingleObject("ts", "uuidB", "graph", payload);
    }

    @Test
    public void exactlyThreeBlocksPayloadRoundTripsAsSingleObject() throws Exception {
        byte[] payload = randomBytes(3 * BLOCK_SIZE, 3L); // 12 MiB
        writeAndAssertSingleObject("ts", "uuidC", "graph", payload);
    }

    @Test
    public void threeBlocksPlusPartialPayloadRoundTripsAsSingleObject() throws Exception {
        byte[] payload = randomBytes(3 * BLOCK_SIZE + 1234, 4L); // 12 MiB + 1234 B
        writeAndAssertSingleObject("ts", "uuidD", "graph", payload);
    }

    @Test
    public void twelveAndAHalfMiBPayloadRoundTripsAsSingleObject() throws Exception {
        // Spec sentinel from the issue: 12.5 MiB exact.
        int size = 12 * 1024 * 1024 + 512 * 1024;
        byte[] payload = randomBytes(size, 5L);
        writeAndAssertSingleObject("ts", "uuidE", "map", payload);
    }
}
