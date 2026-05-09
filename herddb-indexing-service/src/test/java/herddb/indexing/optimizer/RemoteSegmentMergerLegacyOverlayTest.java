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
package herddb.indexing.optimizer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.TombstoneOverlay;
import herddb.indexing.segment.TombstoneOverlayManager;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link RemoteSegmentMerger} correctly applies tombstone
 * overlays from <em>legacy</em> znodes — i.e. znodes written before the
 * {@code overlayGeneration} field was introduced (issue #484, review item B.1#1
 * from the first pr-reviewer pass).
 *
 * <p>The merger MUST NOT silently treat {@code overlayGeneration == 0 &&
 * tombstonePath != null} as "no tombstones" — that would resurrect deleted
 * vectors. Instead, it probes the bounded generation window
 * {@code 1..LEGACY_OVERLAY_GENERATION_PROBE_MAX} highest-first and applies the
 * highest existing overlay. If no overlay file exists in the probed window,
 * the merger throws {@link RemoteSegmentMerger.TombstoneLoadFailedException}
 * so the run aborts and the engine logs {@code mergeFailures}.
 */
public class RemoteSegmentMergerLegacyOverlayTest {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final String TS_UUID = "ts-legacy";
    private static final String IDX_UUID = "idx-legacy";
    private static final int DIM = 16;
    private static final int VECTORS_PER_SEGMENT = 50;

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private MemoryDataStorageManager dsm;
    private Path tmpDir;

    @Before
    public void setUp() throws Exception {
        dsm = new MemoryDataStorageManager();
        tmpDir = tmp.newFolder("merger-tmp").toPath();
    }

    private SegmentMetadata writeInputAndPublishOverlay(
            String segUuid, long segId, int seed,
            int[] tombstonedOrdinals, long overlayGeneration,
            String tombstonePath) throws Exception {
        // Write the map file with all VECTORS_PER_SEGMENT entries.
        Path mapFile = Files.createTempFile(tmpDir, "input-", ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(mapFile.toFile()));
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(VECTORS_PER_SEGMENT);
            Random rng = new Random(seed);
            for (int local = 0; local < VECTORS_PER_SEGMENT; local++) {
                dos.writeInt(local);
                byte[] pkBytes = Bytes.from_string(segUuid + "-" + local).to_array();
                dos.writeInt(pkBytes.length);
                dos.write(pkBytes);
                VectorFloat<?> v = VTS.createFloatVector(DIM);
                for (int j = 0; j < DIM; j++) {
                    v.set(j, rng.nextFloat() * 2.0f - 1.0f);
                }
                dos.writeInt(v.length());
                for (int j = 0; j < v.length(); j++) {
                    dos.writeInt(Float.floatToIntBits(v.get(j)));
                }
            }
        }
        long mapSize = Files.size(mapFile);
        String multipartUuid = IDX_UUID + "_seg" + segId;
        String mapPath = dsm.writeMultipartIndexFile(TS_UUID, multipartUuid, "map", mapFile, null);
        Files.deleteIfExists(mapFile);

        // Write a real overlay file at the requested generation (so the
        // merger's probe finds it). Mirrors what TombstoneOverlayManager.flush
        // would have done for a pre-overlayGeneration IS.
        if (tombstonedOrdinals != null && tombstonedOrdinals.length > 0) {
            TombstoneOverlay overlay = new TombstoneOverlay(segUuid,
                    new LogSequenceNumber(1L, 100L), overlayGeneration, tombstonedOrdinals);
            Path overlayFile = Files.createTempFile(tmpDir, "overlay-", ".bin");
            Files.write(overlayFile, overlay.serialize());
            dsm.writeMultipartIndexFile(TS_UUID,
                    TombstoneOverlayManager.multipartUuidFor(IDX_UUID, segUuid),
                    TombstoneOverlayManager.fileTypeFor(overlayGeneration),
                    overlayFile, null);
            Files.deleteIfExists(overlayFile);
        }

        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID).tableName("docs")
                .indexUuid(IDX_UUID).indexName("docs_v1")
                .state(SegmentState.ACTIVE).ownerInstanceId(0)
                .segmentId(segId)
                .graphPath("g/" + segUuid)
                .mapPath(mapPath)
                .tombstonePath(tombstonePath)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(mapSize * 2L).mapFileSize(mapSize)
                .vectorCount(VECTORS_PER_SEGMENT).generation(1L)
                .createdAtEpochMillis(0L)
                // Legacy: no overlayGeneration set (defaults to 0L).
                .build();
    }

    @Test
    public void legacyZnodeWithExistingOverlayAppliesTombstones() throws Exception {
        // Input A is "legacy": tombstonePath is set but overlayGeneration is 0.
        // Its overlay file exists at generation=5 (a typical published generation).
        int[] tombstonedA = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        SegmentMetadata a = writeInputAndPublishOverlay(
                "legacy-A", /* segId */ 100L, /* seed */ 0xA,
                tombstonedA, /* overlayGeneration on DSM */ 5L,
                "tombstones-path-A");
        SegmentMetadata b = writeInputAndPublishOverlay(
                "B", /* segId */ 200L, /* seed */ 0xB,
                /* no tombstones */ null, 0L, null);

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir, DIM, /* M */ 8, /* beamWidth */ 32, 1.2f, 1.4f,
                VectorSimilarityFunction.EUCLIDEAN);

        SegmentMetadata merged = merger.merge(List.of(a, b), 0);
        assertNotNull("merger must produce an output", merged);
        assertTrue("vectorCount must reflect tombstone-filtered A: 40 + 50 = 90",
                merged.getVectorCount() == 90L);

        // The merged map file MUST NOT contain any of the 10 tombstoned A PKs.
        Set<Bytes> mergedPks = readMergedPks(merged);
        for (int ord : tombstonedA) {
            assertTrue("legacy tombstoned PK 'legacy-A-" + ord + "' must NOT appear in merged segment",
                    !mergedPks.contains(Bytes.from_string("legacy-A-" + ord)));
        }
        // All B PKs must be present.
        for (int i = 0; i < VECTORS_PER_SEGMENT; i++) {
            assertTrue(mergedPks.contains(Bytes.from_string("B-" + i)));
        }
    }

    @Test
    public void legacyZnodeWithNoExistingOverlayThrowsTombstoneLoadFailed() throws Exception {
        // Input A is "legacy": tombstonePath is set but the overlay file is
        // missing entirely from the DSM (no probe generation ever existed).
        // The merger must REFUSE the merge — silently treating this as "no
        // tombstones" would resurrect deleted vectors.
        SegmentMetadata a = writeInputAndPublishOverlay(
                "legacy-orphan-A", 100L, 0xA, /* no overlay file written */ null, 0L,
                "tombstones-orphan-A");
        SegmentMetadata b = writeInputAndPublishOverlay(
                "B", 200L, 0xB, null, 0L, null);

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir, DIM, 8, 32, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);
        try {
            merger.merge(List.of(a, b), 0);
            fail("expected TombstoneLoadFailedException for legacy znode with no overlay file");
        } catch (RemoteSegmentMerger.TombstoneLoadFailedException ok) {
            assertTrue(ok.getMessage().contains("legacy-orphan-A"));
            assertTrue(ok.getMessage().contains("0")); // generation reported as 0
        }
    }

    @Test
    public void modernZnodeWithGenerationLoadsExactGeneration() throws Exception {
        // Input A is "modern": both tombstonePath AND overlayGeneration=3 are set.
        // The merger must load EXACTLY generation 3, not probe.
        int[] tombstonedA = new int[]{42, 43, 44};
        SegmentMetadata a = writeInputAndPublishOverlay(
                "modern-A", 100L, 0xA, tombstonedA, 3L, "tombstones-A-gen3");
        // Modify the metadata to set overlayGeneration=3 explicitly (the helper
        // builds the znode with overlayGeneration=0 by default).
        SegmentMetadata aModern = a.toBuilder().overlayGeneration(3L).build();
        SegmentMetadata b = writeInputAndPublishOverlay("B", 200L, 0xB, null, 0L, null);

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir, DIM, 8, 32, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);
        SegmentMetadata merged = merger.merge(List.of(aModern, b), 0);
        assertNotNull(merged);
        assertTrue("vectorCount must reflect tombstone-filtered A: 47 + 50 = 97",
                merged.getVectorCount() == 97L);
        Set<Bytes> mergedPks = readMergedPks(merged);
        for (int ord : tombstonedA) {
            assertTrue("modern tombstoned PK 'modern-A-" + ord + "' must NOT appear",
                    !mergedPks.contains(Bytes.from_string("modern-A-" + ord)));
        }
    }

    private Set<Bytes> readMergedPks(SegmentMetadata merged) throws Exception {
        ReaderSupplier supplier = dsm.multipartIndexReaderSupplier(
                merged.getTablespaceUuid(),
                merged.getIndexUuid() + "_seg" + merged.getSegmentId(), "map",
                /* sizeHint */ Long.MAX_VALUE);
        try (RandomAccessReader r = supplier.get()) {
            r.seek(0L);
            // Read entryCount first, then read exactly entryCount entries
            // (no EOF-driven probing). Each entry is fixed-shape:
            //   int ordinal, int pkLen, byte[pkLen] pkBytes, int floatCount,
            //   int[floatCount] floatBits.
            byte[] hdr = new byte[4];
            r.readFully(hdr);
            int entryCount = ((hdr[0] & 0xff) << 24) | ((hdr[1] & 0xff) << 16)
                    | ((hdr[2] & 0xff) << 8) | (hdr[3] & 0xff);
            Set<Bytes> out = new HashSet<>(entryCount * 2);
            byte[] intBuf = new byte[4];
            for (int i = 0; i < entryCount; i++) {
                r.readFully(intBuf); // ordinal — not used here
                r.readFully(intBuf);
                int pkLen = ((intBuf[0] & 0xff) << 24) | ((intBuf[1] & 0xff) << 16)
                        | ((intBuf[2] & 0xff) << 8) | (intBuf[3] & 0xff);
                byte[] pk = new byte[pkLen];
                r.readFully(pk);
                r.readFully(intBuf);
                int floatCount = ((intBuf[0] & 0xff) << 24) | ((intBuf[1] & 0xff) << 16)
                        | ((intBuf[2] & 0xff) << 8) | (intBuf[3] & 0xff);
                byte[] floats = new byte[floatCount * Float.BYTES];
                r.readFully(floats);
                out.add(Bytes.from_array(pk));
            }
            return out;
        }
    }
}
