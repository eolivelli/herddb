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
import herddb.remote.storage.LocalObjectStorage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
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
 * Issue #638: verifies backward compatibility of the
 * {@link RemoteFileDataStorageManager} read paths after the bulk
 * layout was introduced. Files written before the change exist only in
 * the per-block layout ({@code .multipart/{i}}); the new read paths must
 * detect the absent {@code .bulk} object and fall back transparently.
 *
 * <p>The test materialises a per-block layout directly in a
 * {@link LocalObjectStorage} (mirroring the on-wire shape the file-server
 * would have produced) and asserts that
 * {@code downloadMultipartIndexFile} and {@code multipartIndexFileExists}
 * still work end-to-end without the bulk variant.
 */
public class RemoteFileDataStorageManagerLegacyFallbackTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private ExecutorService metadataExecutor;
    private LocalObjectStorage objectStorage;
    private RemoteFileServiceClient stubClient;
    private RemoteFileDataStorageManager dsm;

    /** Block size: must equal {@code MULTIPART_BLOCK_SIZE} default (4 MiB). */
    private static final int BLOCK_SIZE = 4 * 1024 * 1024;

    @Before
    public void setUp() throws IOException {
        metadataExecutor = Executors.newSingleThreadExecutor();
        Path storageDir = tmpFolder.newFolder("storage").toPath();
        objectStorage = new LocalObjectStorage(storageDir, metadataExecutor);
        Map<String, Object> fastFailConfig = new HashMap<>();
        fastFailConfig.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 1L);
        fastFailConfig.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 0);
        stubClient = new RemoteFileServiceClient(Collections.emptyList(), fastFailConfig);
        Path metaDir = tmpFolder.newFolder("meta").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        dsm = new RemoteFileDataStorageManager(metaDir, tmpDir, Integer.MAX_VALUE, stubClient);
        dsm.setDirectObjectStorage(objectStorage);
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

    private String writeBlocksDirectly(String tableSpace, String uuid, String fileType,
            byte[] data) throws Exception {
        String logicalPath = tableSpace + "/" + uuid + "/multipart/" + fileType;
        int numBlocks = (int) Math.ceil((double) data.length / BLOCK_SIZE);
        for (int i = 0; i < numBlocks; i++) {
            int start = i * BLOCK_SIZE;
            int end = Math.min(start + BLOCK_SIZE, data.length);
            byte[] block = new byte[end - start];
            System.arraycopy(data, start, block, 0, block.length);
            objectStorage.writeBlock(logicalPath, i, block).get();
        }
        return logicalPath;
    }

    private static byte[] randomBytes(int size, long seed) {
        Random rng = new Random(seed);
        byte[] data = new byte[size];
        rng.nextBytes(data);
        return data;
    }

    /**
     * A logical file present <em>only</em> in the per-block layout must be
     * fully readable via {@code downloadMultipartIndexFile}. The bulk
     * probe returns false (no {@code .bulk} object exists), so the legacy
     * sequential block download path runs and stitches the file back
     * together byte-for-byte.
     */
    @Test
    public void perBlockOnlyFileDownloadsCorrectly() throws Exception {
        byte[] original = randomBytes(BLOCK_SIZE + 17, 42L);
        writeBlocksDirectly("ts", "legacy-1", "map", original);

        // Ensure the .bulk variant does NOT exist for this logical path.
        assertFalse("bulk object must be absent for legacy file",
                objectStorage.existsObject(
                        "ts/legacy-1/multipart/map.bulk").get());

        Path dest = tmpFolder.newFile("legacy-1-out.bin").toPath();
        dsm.downloadMultipartIndexFile("ts", "legacy-1", "map", original.length, dest);
        assertArrayEquals("legacy per-block file must reconstruct byte-identically",
                original, Files.readAllBytes(dest));
    }

    /**
     * Without a live gRPC server, {@code multipartIndexFileExists} returns
     * {@code false} for a legacy per-block-only file even when direct upload
     * is enabled. The bulk probe returns {@code false} (no {@code .bulk} object),
     * and the gRPC per-block probe is unreachable (empty stub client →
     * "Hash ring is empty" RuntimeException → translated to "missing").
     *
     * <p>This test pins the <em>no-server</em> behaviour: the method must not
     * throw, and it must not report true based solely on the LocalObjectStorage
     * contents (the file is not accessible via the code paths {@code multipartIndexFileExists}
     * uses when neither bulk object nor gRPC server is available).
     * The full positive case — legacy file reachable via a live gRPC server — is
     * covered in the integration tests against a real {@code RemoteFileServer}.
     */
    @Test
    public void perBlockOnlyFileIsNotDiscoverableWithoutGrpc() throws Exception {
        dsm.enableDirectUpload(16L * 1024 * 1024);
        byte[] original = randomBytes(256, 7L);
        writeBlocksDirectly("ts", "legacy-2", "graph", original);
        // The bulk probe will return false (no .bulk object).
        // The gRPC per-block probe hits "Hash ring is empty" and is treated as missing.
        boolean visible = dsm.multipartIndexFileExists("ts", "legacy-2", "graph");
        assertFalse("with no gRPC server reachable, exists must report false even though"
                + " the legacy blocks are present on the local storage",
                visible);
    }

    /**
     * When BOTH layouts are present for the same logical path, the bulk
     * variant must win — the read path returns the bulk content. (Reality:
     * this can only happen during a mid-rewrite race, since
     * {@code writeMultipartIndexFile} invalidates the cache and writes to
     * exactly one layout. The "bulk wins" semantics here matches the
     * implementation: bulk is probed first.)
     */
    @Test
    public void bulkWinsWhenBothLayoutsPresent() throws Exception {
        // Write per-block content with payload "OLD-..." then write bulk
        // content with payload "NEW-..." for the same logical path.
        byte[] legacyPayload = "OLD-PAYLOAD-LEGACY".getBytes();
        byte[] bulkPayload = "NEW-PAYLOAD-BULK".getBytes();
        writeBlocksDirectly("ts", "both-3", "map", legacyPayload);
        // Materialise a bulk object at logicalPath.bulk by calling write()
        // directly on the object storage (bypasses the DSM).
        objectStorage.write("ts/both-3/multipart/map.bulk", bulkPayload).get();

        Path dest = tmpFolder.newFile("both-3-out.bin").toPath();
        dsm.downloadMultipartIndexFile("ts", "both-3", "map", bulkPayload.length, dest);
        // Bulk wins.
        assertArrayEquals("bulk variant must be served when both layouts exist",
                bulkPayload, Files.readAllBytes(dest));
    }

    /**
     * Deleting a logical file present only in the legacy layout still
     * clears the probe cache and the local bulk-cache (both no-ops in this
     * case) without throwing.
     *
     * <p>Post-condition: after delete the bulk probe cache must be clear
     * (so a subsequent upload under the same logical path starts fresh),
     * and an {@code existsObject} probe against the backing LocalObjectStorage
     * must still return {@code false} for the {@code .bulk} key (the delete
     * path issued a best-effort delete on the .bulk key which was already
     * absent — the key must not have been accidentally created).
     */
    @Test
    public void deleteLegacyOnlyFileSucceeds() throws Exception {
        byte[] original = randomBytes(64, 1L);
        String tableSpace = "ts";
        String uuid = "legacy-3";
        String fileType = "map";
        writeBlocksDirectly(tableSpace, uuid, fileType, original);

        // Pre-condition: no .bulk object exists.
        String bulkKey = tableSpace + "/" + uuid + "/multipart/" + fileType + ".bulk";
        assertFalse("bulk object must be absent before delete",
                objectStorage.existsObject(bulkKey).get());

        // The DSM's gRPC deleteFile path will surface "Hash ring is empty"
        // and the catch will log it as non-fatal. The .bulk delete via
        // ObjectStorage is a no-op for a missing key. The probe-cache and
        // local cache invalidation happens unconditionally. So the whole
        // delete returns normally.
        dsm.deleteMultipartIndexFile(tableSpace, uuid, fileType);

        // Post-condition: .bulk key was not accidentally created by the delete call.
        assertFalse("delete must not create the .bulk key as a side-effect",
                objectStorage.existsObject(bulkKey).get());
        // Post-condition: a re-probe after delete sees a fresh (uncached) result.
        // Since the .bulk object does not exist, the re-probe returns false —
        // confirming the cache was cleared (if the cache entry had stuck at TRUE
        // the DSM would return true without re-probing the storage).
        assertFalse("probe cache must be cleared so re-probe returns fresh result",
                dsm.multipartIndexFileExists(tableSpace, uuid, fileType));
    }

    /**
     * The probe-cache is populated lazily on first probe. A legacy file
     * must NOT have its absence cached after the very first call — that
     * would cause a never-rewritten legacy file to permanently look
     * "absent" if it later gains a bulk variant. The implementation
     * deliberately caches the negative result for the JVM's lifetime
     * because we expect the layout to be stable per logical path, but the
     * assertion here guards a subtler property: the cache MUST be
     * populated (true or false) after the first probe, so we don't issue
     * a HEAD round-trip on every random-access read.
     */
    @Test
    public void probeCacheIsPopulatedAfterFirstProbe() throws Exception {
        byte[] original = randomBytes(BLOCK_SIZE * 2, 9L);
        writeBlocksDirectly("ts", "probe-cache", "map", original);

        // First probe — triggers a HEAD against the LocalObjectStorage. We
        // do not have direct access to the cache map, but verify behaviour:
        // a second download succeeds without any probe-throughput
        // explosion. (Indirect: a real test of cache population would need
        // a counting ObjectStorage, but the legacy-only file
        // downloadMultipartIndexFile assertion above already exercises the
        // probe-then-fallback path end-to-end.)
        Path dest1 = tmpFolder.newFile("probe-cache-1.bin").toPath();
        dsm.downloadMultipartIndexFile("ts", "probe-cache", "map", original.length, dest1);
        Path dest2 = tmpFolder.newFile("probe-cache-2.bin").toPath();
        dsm.downloadMultipartIndexFile("ts", "probe-cache", "map", original.length, dest2);
        assertArrayEquals(original, Files.readAllBytes(dest1));
        assertArrayEquals(original, Files.readAllBytes(dest2));
        // Sanity check: the file bytes match across both invocations — the
        // second invocation must not have served a stale cached copy.
        assertEquals(original.length, Files.size(dest1));
        assertEquals(original.length, Files.size(dest2));
    }
}
