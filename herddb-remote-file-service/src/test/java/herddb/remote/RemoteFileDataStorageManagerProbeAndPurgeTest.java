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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * pr-reviewer follow-up #2 for issue #617: dedicated probe-and-purge contract
 * tests for {@link RemoteFileDataStorageManager#multipartIndexFileExists}
 * (the safety gate the {@code DeleteSegment} engine path keys off) and
 * {@link RemoteFileDataStorageManager#deleteMultipartIndexFile} (the
 * {@code --purge-storage} purger).
 *
 * <p>The matrix:
 * <ul>
 *   <li><b>(a)</b> probe returns {@code false} when the multipart object is
 *       absent — the gate must not block a delete the operator initiated for
 *       an already-orphaned segment record;</li>
 *   <li><b>(b)</b> probe returns {@code true} when the multipart object is
 *       fully present — the gate must protect the operator from deleting a
 *       healthy segment;</li>
 *   <li><b>(c)</b> {@code deleteMultipartIndexFile} is idempotent and
 *       invalidates the {@link SegmentBlockCache} so a subsequent reader on
 *       the same logical path cannot serve stale bytes;</li>
 *   <li><b>(d)</b> probe still returns {@code true} on a block-0 that is at
 *       least 4 bytes long but otherwise truncated/corrupted — this
 *       documents the contract gap that the operator's {@code --force} flag
 *       covers (the gate cannot distinguish "complete file" from "block-0
 *       only", so {@code --force} remains required when the rest of the
 *       file is known to be corrupt).</li>
 * </ul>
 *
 * <p>End-to-end coverage of {@code DeleteSegment} that exercises this gate
 * indirectly lives in {@code herddb.indexing.ShadowDeleteSegmentE2ETest}
 * (in the indexing-service module).
 */
public class RemoteFileDataStorageManagerProbeAndPurgeTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;
    private RemoteFileDataStorageManager storage;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("remote").toPath());
        server.start();
        client = new RemoteFileServiceClient(Arrays.asList("localhost:" + server.getPort()));
        storage = new RemoteFileDataStorageManager(
                folder.newFolder("metadata").toPath(),
                folder.newFolder("tmp").toPath(),
                1000,
                client);
        storage.start();
    }

    @After
    public void tearDown() throws Exception {
        if (storage != null) {
            storage.close();
        }
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    /**
     * Writes a synthetic graph file via the high-level multipart writer so
     * the produced object matches exactly what {@code PersistentVectorStore}
     * would have written at Phase B time (issue #617 reproduction shape).
     */
    private long writeMultipartGraph(String tableSpace, String uuid, byte[] data) throws Exception {
        Path tempFile = folder.newFile("graph-" + uuid + ".bin").toPath();
        Files.write(tempFile, data);
        try {
            storage.writeMultipartIndexFile(tableSpace, uuid, "graph", tempFile, null);
        } finally {
            Files.deleteIfExists(tempFile);
        }
        return data.length;
    }

    /**
     * (a) absent object → probe returns {@code false}. This is the path the
     * issue #617 reproduction hits when the Phase B upload failed before
     * block-0 was ever published: the IS metadata still references the
     * segment, but the remote storage has no object for it. With the gate
     * keying off this probe, {@code DeleteSegment} with {@code force=false}
     * is accepted because the safety check (graph file IS reachable) does
     * not fire.
     */
    @Test(timeout = 30_000)
    public void probeReturnsFalseWhenObjectIsAbsent() {
        assertFalse("probe must return false when no multipart object exists",
                storage.multipartIndexFileExists("ts1", "uuid-absent", "graph"));
    }

    /**
     * (b) fully-present object → probe returns {@code true}. This is the
     * happy path: the gate must protect the operator from deleting a
     * healthy segment without going through {@code --force}.
     */
    @Test(timeout = 60_000)
    public void probeReturnsTrueWhenObjectIsFullyPresent() throws Exception {
        // Multi-block payload so the probe does not just inspect a partial
        // block-0 — exactly matches the production "complete graph file"
        // contract that the gate is supposed to detect.
        byte[] payload = new byte[64 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i & 0xFF);
        }
        writeMultipartGraph("ts1", "uuid-present", payload);
        assertTrue("probe must return true when the multipart object exists",
                storage.multipartIndexFileExists("ts1", "uuid-present", "graph"));
    }

    /**
     * (c) {@code deleteMultipartIndexFile} is idempotent and invalidates
     * the block cache. We verify:
     * <ol>
     *   <li>two back-to-back calls on the same key succeed (the second
     *       must not throw — the multipart files are already gone);</li>
     *   <li>a block populated in the {@link SegmentBlockCache} via a real
     *       read is removed from the cache by the delete call, so a
     *       subsequent reader on the same logical path cannot serve stale
     *       bytes from a rewritten segment that happens to hit the same
     *       key.</li>
     * </ol>
     *
     * <p>{@link SegmentBlockCache} is {@code final}, so we install a real
     * one with a small capacity and assert via the public
     * {@link SegmentBlockCache#containsBlock(String, long, int)} accessor
     * — that is exactly the same surface used in production to decide
     * whether a block load is needed.
     */
    @Test(timeout = 60_000)
    public void deleteMultipartIndexFileIsIdempotentAndInvalidatesCache() throws Exception {
        byte[] payload = new byte[8 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) ((i * 31) & 0xFF);
        }
        writeMultipartGraph("ts1", "uuid-purge", payload);
        assertTrue("precondition: object must exist before purge",
                storage.multipartIndexFileExists("ts1", "uuid-purge", "graph"));

        // Install a real (enabled) SegmentBlockCache so we can observe the
        // invalidation effect via the public containsBlock() accessor.
        // 1 MiB capacity is plenty for the 8 KiB payload.
        SegmentBlockCache cache = new SegmentBlockCache(1L * 1024 * 1024);
        storage.setSegmentBlockCache(cache,
                org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE);

        // Force a block to land in the cache by running a real read. The
        // RemoteRandomAccessReader populates the cache on first access via
        // the supplier returned by multipartIndexReaderSupplier.
        io.github.jbellis.jvector.disk.ReaderSupplier rs =
                storage.multipartIndexReaderSupplier("ts1", "uuid-purge", "graph",
                        (long) payload.length);
        try (io.github.jbellis.jvector.disk.RandomAccessReader r = rs.get()) {
            r.seek(0L);
            r.readInt(); // pulls block-0 into the cache
        }
        String logicalPath = "ts1/uuid-purge/multipart/graph";
        // The cache may use a writeBlockSize different from payload.length,
        // so we cannot pin down the exact (offset, length) pair without
        // duplicating the production block-sizing math. Instead, walk a
        // small set of candidate block sizes via the public containsBlock
        // helper — at least one of them must hit, otherwise the read
        // above did not populate the cache and the test cannot prove the
        // invalidation contract.
        int[] candidateLengths = new int[]{
            4 * 1024 * 1024, 1024 * 1024, 64 * 1024, payload.length
        };
        boolean wasCached = false;
        for (int len : candidateLengths) {
            if (cache.containsBlock(logicalPath, 0L, len)) {
                wasCached = true;
                break;
            }
        }
        assertTrue("precondition: a block must be cached after the read,"
                        + " otherwise the invalidation assertion is vacuous",
                wasCached);

        // First delete must succeed AND invalidate the cache.
        storage.deleteMultipartIndexFile("ts1", "uuid-purge", "graph");
        assertFalse("object must be gone after delete",
                storage.multipartIndexFileExists("ts1", "uuid-purge", "graph"));
        for (int len : candidateLengths) {
            assertFalse("cached block at length=" + len
                            + " must be evicted after deleteMultipartIndexFile",
                    cache.containsBlock(logicalPath, 0L, len));
        }

        // Second delete must also succeed (idempotent) — no exception, no
        // surfaced error. The implementation logs at WARNING but does not
        // propagate.
        storage.deleteMultipartIndexFile("ts1", "uuid-purge", "graph");
        assertFalse("object must remain gone after the second delete call",
                storage.multipartIndexFileExists("ts1", "uuid-purge", "graph"));
    }

    /**
     * (d) Documents the contract gap that {@code --force} covers: when
     * block-0 is present but truncated/corrupted, the probe still returns
     * {@code true}. The default probe reads the first 4 bytes via the
     * multipart reader supplier — that succeeds as long as block-0 has at
     * least 4 bytes, even if the rest of the file is missing or corrupted.
     *
     * <p>This is the exact reproduction of the issue #617 scenario where an
     * S3 multipart upload published block-0 then failed on a later block:
     * the operator running {@code delete-segment} without {@code --force}
     * sees the safety gate trip ("graph file IS reachable"), reads the
     * documentation, and re-runs with {@code --force} once they have
     * confirmed via the file-server logs that the upload never completed.
     */
    @Test(timeout = 30_000)
    public void probeReturnsTrueOnPartialBlockZeroAtLeast4Bytes() throws Exception {
        // Write just block-0 directly via the lower-level writeFileBlock RPC
        // — that bypasses the multipart writer's "all blocks or none"
        // semantics and reproduces the Phase B "upload failed after
        // block-0" failure mode the issue #617 operator path covers.
        // Must match remoteMultipartPath: tableSpace/uuid/multipart/fileType.
        String logicalPath = "ts1/uuid-partial/multipart/graph";
        byte[] partial = new byte[16]; // > 4 bytes; the probe reads 4 bytes
        for (int i = 0; i < partial.length; i++) {
            partial[i] = (byte) i;
        }
        client.writeFileBlock(logicalPath, 0L, partial);

        // Sanity: the multipart writer would have written multiple blocks
        // for a real graph file. Here only block-0 is present — yet the
        // probe must still return true because it only inspects block-0.
        assertTrue("probe returns true on a partial block-0 — documents the"
                        + " contract gap that --force covers (issue #617)",
                storage.multipartIndexFileExists("ts1", "uuid-partial", "graph"));

        // Round-tripping the partial bytes through writeMultipartFile is
        // not possible (the input stream framing would always produce a
        // matching file); this direct block-0 write is the only way to
        // produce a "partial" state in the test harness, and it mirrors
        // exactly what an S3 backend produces on a partial multipart
        // upload failure.
        assertNotNull("client must still be wired for follow-up purge calls", client);
    }

}
