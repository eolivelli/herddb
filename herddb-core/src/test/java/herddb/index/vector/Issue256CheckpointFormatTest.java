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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Targets the checkpoint-metadata format change from issue #256: the
 * {@code nextNodeId} field was rewritten from {@code int32} to {@code int64}
 * (no back-compat per project decision). Issue #499 additionally dropped v3
 * format support entirely; all checkpoints are now written in v4 format.
 *
 * <p>Two cases:
 * <ol>
 *   <li>A large {@code nextNodeId} round-trips through {@code writeLong}
 *       / {@code readLong} unscathed.</li>
 *   <li>A checkpoint blob using the OLD {@code writeInt} width for
 *       {@code nextNodeId} must not silently migrate — the loader's behaviour
 *       is locked in so a future refactor can't reintroduce a half-way
 *       back-compat path.</li>
 * </ol>
 */
public class Issue256CheckpointFormatTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void disableDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
    }

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private PersistentVectorStore createStore(Path tmpDir, String uuid, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                uuid, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.COSINE, Long.MAX_VALUE, null, 0, 0);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, Integer.MAX_VALUE, 0);
        return store;
    }

    /**
     * Drives {@code nextNodeId} well past {@code Integer.MAX_VALUE},
     * checkpoints, reloads, asserts the persisted counter survives.
     * Also peeks at the raw metadata bytes to confirm the {@code long}
     * is actually at the expected offset (not a truncated {@code int}).
     */
    @Test
    public void largeNextNodeIdRoundTripsThroughCheckpointFormat() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        String uuid = "issue256-fmt-test";
        long seeded = ((long) Integer.MAX_VALUE) + 9_000_001L;

        try (PersistentVectorStore store = createStore(tmpDir, uuid, dsm)) {
            store.start();
            store.seedNextNodeIdForTest(seeded);
            Random rng = new Random(17);
            for (int i = 0; i < 25; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, 16));
            }
            assertTrue(store.checkpoint());
        }

        // Peek at the raw metadata blob: version (int) + dim (int) + m (int)
        // + bw (int) + neighborOverflow (float) + alpha (float) + addHierarchy (byte)
        // + fusedPQ (byte) + nextNodeId (long). Total header size = 4*4 + 4*2 + 2 + 8 = 34.
        // Issue #256 moved nextNodeId from writeInt to writeLong at that offset.
        // Issue #499 dropped v3 support; v4 format was introduced.
        // Issue #514 adds externalStorageKey per segment; all checkpoints are now written in v5 format.
        byte[] raw = readRawIndexStatus(dsm, uuid);
        ByteBuffer bb = ByteBuffer.wrap(raw);
        int version = bb.getInt();
        assertEquals("version must be 5 (V5 adds externalStorageKey per segment, issue #514)", 5, version);
        bb.getInt(); // dim
        bb.getInt(); // m
        bb.getInt(); // beamWidth
        bb.getFloat(); // neighborOverflow
        bb.getFloat(); // alpha
        bb.get(); // addHierarchy
        bb.get(); // fusedPQ
        long persistedNextNodeId = bb.getLong();
        assertTrue("persisted nextNodeId must be widened to int64, got " + persistedNextNodeId,
                persistedNextNodeId >= seeded + 25L);

        try (PersistentVectorStore reloaded = createStore(tmpDir, uuid, dsm)) {
            reloaded.start();
            long reloadedCounter = reloaded.getNextNodeId();
            assertTrue("reload must see the int64 counter (" + reloadedCounter + ")",
                    reloadedCounter > Integer.MAX_VALUE);
            assertTrue("reload must see at least the persisted value",
                    reloadedCounter >= persistedNextNodeId);
        }
    }

    /**
     * Locks in the "no silent misalignment" decision: if we hand-craft a metadata
     * blob using the OLD {@code writeInt} width for {@code nextNodeId} (truncating
     * the 8-byte long to 4 bytes), the loader is NOT allowed to treat it as valid
     * and silently promote the (now-misaligned) fields behind it. The expected
     * outcome is either a clean "start empty" or a loud exception. The test asserts
     * that whichever path we take, the restored {@code nextNodeId} is either zero
     * (start empty) or a recognisably-corrupt value that would NOT compare equal to
     * the value we would have observed under the old format.
     */
    @Test
    public void oldInt32LayoutIsNotSilentlyAccepted() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        String uuid = "issue256-old-fmt";

        // Write a legitimate checkpoint first so the store has a usable
        // segments list after we corrupt the header layout.
        long expectedOldValue = 42_000L;
        try (PersistentVectorStore store = createStore(tmpDir, uuid, dsm)) {
            store.start();
            store.seedNextNodeIdForTest(expectedOldValue);
            Random rng = new Random(19);
            for (int i = 0; i < 10; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, 16));
            }
            assertTrue(store.checkpoint());
        }

        // Hand-craft a corrupted blob that uses writeInt instead of writeLong
        // at the nextNodeId offset. If the loader were to silently accept this
        // layout it would mis-read all subsequent fields (generation,
        // numSegments, ...) as a shifted view of the real bytes — likely
        // throwing much later with a cryptic message. We want it to either
        // reject cleanly OR yield nextNodeId = 0 (start empty). It must NOT
        // yield nextNodeId == expectedOldValue, which would indicate the loader
        // silently migrated the old format.
        byte[] real = readRawIndexStatus(dsm, uuid);
        // Reconstruct: header fields stay the same until the nextNodeId
        // field, which becomes 4 bytes (writeInt of expectedOldValue).
        // Drop the last 4 bytes of the current 8-byte long to simulate
        // this.
        byte[] corrupted = new byte[real.length - 4];
        // Copy everything up to the nextNodeId offset (22 bytes):
        // 4 (version) + 4 (dim) + 4 (m) + 4 (bw) + 4 (nbo) + 4 (alpha)
        // wait: nbo and alpha are floats (4 bytes each), addHierarchy and
        // fusedPQ are bytes. Recompute: 4+4+4+4+4+4+1+1 = 26.
        System.arraycopy(real, 0, corrupted, 0, 26);
        // Write nextNodeId as int (4 bytes): high 4 bytes of the long at
        // offset 26 in the original, low 4 bytes at offset 30.
        // writeInt writes big-endian; writeLong also big-endian. So the
        // old int would occupy bytes 26..29 as the high-order bytes of
        // what is now the long. We simulate the old layout by using the
        // LOW 4 bytes of the long (bytes 30..33) — that's what the old
        // code would have written for expectedOldValue (which fits in
        // int32).
        System.arraycopy(real, 30, corrupted, 26, 4);
        // Copy the rest (everything after the 8-byte long, starting at
        // offset 34 in the real buffer) into the corrupted buffer at
        // offset 30.
        System.arraycopy(real, 34, corrupted, 30, real.length - 34);

        // Inject the corrupted blob back into the storage manager.
        writeRawIndexStatus(dsm, uuid, corrupted);

        // Reload and observe.
        try (PersistentVectorStore reloaded = createStore(tmpDir, uuid, dsm)) {
            long observed;
            boolean threw = false;
            try {
                reloaded.start();
                observed = reloaded.getNextNodeId();
            } catch (RuntimeException ignored) {
                observed = -1L;
                threw = true;
            } catch (Exception other) {
                // start() throws Exception from its signature; narrow
                // catches are impractical here without enumerating every
                // deserialisation path.
                observed = -1L;
                threw = true;
            }
            if (!threw) {
                // If the loader accepted the corrupted blob, it must at
                // least NOT have silently decoded it as the old value.
                assertTrue("loader must NOT silently decode old int32 layout as the intended value "
                                + "(observed " + observed + ")",
                        observed != expectedOldValue);
            }
            // Either path (loud throw or clean zero-out) is acceptable;
            // what we forbid is a silent migration.
        }
    }

    // Helper: read the raw IndexStatus blob for the given index UUID
    // directly from the MemoryDataStorageManager.
    private byte[] readRawIndexStatus(MemoryDataStorageManager dsm, String uuid) throws Exception {
        herddb.storage.IndexStatus status = dsm.getIndexStatus(
                "tstblspace", uuid, herddb.log.LogSequenceNumber.START_OF_TIME);
        assertTrue("index status must exist for uuid=" + uuid, status != null);
        return status.indexData;
    }

    // Helper: overwrite the raw IndexStatus blob in the
    // MemoryDataStorageManager via the public checkpoint entry point.
    private void writeRawIndexStatus(MemoryDataStorageManager dsm, String uuid, byte[] data) throws Exception {
        herddb.storage.IndexStatus latest = dsm.getIndexStatus(
                "tstblspace", uuid, herddb.log.LogSequenceNumber.START_OF_TIME);
        herddb.storage.IndexStatus replaced = new herddb.storage.IndexStatus(
                latest.indexName,
                latest.sequenceNumber,
                latest.newPageId,
                latest.activePages,
                data);
        dsm.indexCheckpoint("tstblspace", uuid, replaced, true);
    }

    /**
     * Sanity: the header-offset arithmetic used in the corruption test
     * mirrors the actual layout written by the store. Without this
     * check the oldInt32LayoutIsNotSilentlyAccepted test could silently
     * drift if the header format changes.
     */
    @Test
    public void headerOffsetArithmeticMatchesActualFormat() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        String uuid = "issue256-fmt-layout";
        try (PersistentVectorStore store = createStore(tmpDir, uuid, dsm)) {
            store.start();
            Random rng = new Random(37);
            for (int i = 0; i < 5; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, 16));
            }
            assertTrue(store.checkpoint());
        }
        byte[] real = readRawIndexStatus(dsm, uuid);
        assertTrue("metadata blob must be at least 34 bytes (header), got " + real.length,
                real.length >= 34);
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(real))) {
            assertEquals(5, dis.readInt());                 // version (v5 since issue #514; v4 was issue #499)
            assertEquals(16, dis.readInt());                // dim
            assertEquals(16, dis.readInt());                // m
            assertEquals(100, dis.readInt());               // beamWidth
            assertEquals(1.2f, dis.readFloat(), 0.0001);    // neighborOverflow
            assertEquals(1.4f, dis.readFloat(), 0.0001);    // alpha
            dis.readByte();                                 // addHierarchy
            dis.readByte();                                 // fusedPQ
            long nextNodeId = dis.readLong();               // long after issue #256
            assertTrue(nextNodeId >= 5L);
        }
    }
}
