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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.TombstoneOverlayManager;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link RemoteSegmentMerger} surfaces overlay-load failures
 * via {@link RemoteSegmentMerger.TombstoneLoadFailedException} rather than
 * silently treating the input as having no tombstones (issue #484, review
 * item B.5 from the first pr-reviewer pass).
 *
 * <p>This is a hard data-integrity invariant: if the merger can't load an
 * overlay it was supposed to apply, it MUST refuse the merge — silently
 * skipping the overlay would resurrect every tombstoned vector in the
 * affected segment.
 */
public class RemoteSegmentMergerOverlayLoadFailureTest {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final String TS_UUID = "ts-overlay-fail";
    private static final String IDX_UUID = "idx-overlay-fail";
    private static final int DIM = 8;
    private static final int VECTORS_PER_SEGMENT = 20;

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private MemoryDataStorageManager dsm;
    private Path tmpDir;

    @Before
    public void setUp() throws Exception {
        dsm = new MemoryDataStorageManager();
        tmpDir = tmp.newFolder("merger-tmp").toPath();
        // Synthetic inputs carry no real graph multipart file — the streaming
        // dispatch in {@code RemoteSegmentGraphMerger.merge} automatically
        // routes to {@code mergeLegacy} when {@code graphFileSize <= 0} for
        // any input.
    }

    private SegmentMetadata writeInput(String segUuid, long segId, int seed,
                                       String tombstonePath, long overlayGen) throws Exception {
        Path mapFile = Files.createTempFile(tmpDir, "input-", ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(mapFile.toFile()));
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(VECTORS_PER_SEGMENT);
            Random rng = new Random(seed);
            for (int local = 0; local < VECTORS_PER_SEGMENT; local++) {
                dos.writeInt(local);
                byte[] pk = (segUuid + "-" + local).getBytes();
                dos.writeInt(pk.length);
                dos.write(pk);
                VectorFloat<?> v = VTS.createFloatVector(DIM);
                for (int j = 0; j < DIM; j++) {
                    v.set(j, rng.nextFloat());
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
        return SegmentMetadata.builder()
                .segmentUuid(segUuid).tablespaceUuid(TS_UUID).tableName("docs")
                .indexUuid(IDX_UUID).indexName("docs_v1")
                .state(SegmentState.ACTIVE).ownerInstanceId(0).segmentId(segId)
                .graphPath("g/" + segUuid).mapPath(mapPath)
                .tombstonePath(tombstonePath)
                .overlayGeneration(overlayGen)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                // sizeBytes == mapFileSize → derived graphFileSize == 0
                // forces RemoteSegmentGraphMerger.merge into mergeLegacy
                .sizeBytes(mapSize)
                .mapFileSize(mapSize)
                .vectorCount(VECTORS_PER_SEGMENT).generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    @Test
    public void modernZnodeWithMissingOverlayFileThrows() throws Exception {
        // Segment carries overlayGeneration=7 and tombstonePath != null, but
        // the corresponding overlay file (multipart "tombstones-7") does NOT
        // exist on the DSM. The merger MUST throw rather than treat as "no
        // tombstones". The engine then logs mergeFailures and retries next tick.
        SegmentMetadata a = writeInput("seg-A", 100L, 0xA, "tombstones-A-gen7", 7L);
        SegmentMetadata b = writeInput("seg-B", 200L, 0xB, null, 0L);

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir,
                new StaticIndexMergeConfigProvider(new IndexMergeConfig(
                        /* graphM */ 8, /* beamWidth */ 32, 1.2f, 1.4f,
                        VectorSimilarityFunction.EUCLIDEAN)));
        try {
            merger.merge(List.of(a, b), /* newOwnerInstance */ 0);
            fail("expected TombstoneLoadFailedException for missing overlay file");
        } catch (RemoteSegmentMerger.TombstoneLoadFailedException ok) {
            assertTrue("error message should reference the segment uuid: " + ok.getMessage(),
                    ok.getMessage().contains("seg-A"));
            assertTrue("error message should reference the overlay generation 7: " + ok.getMessage(),
                    ok.getMessage().contains("7"));
        }

        // No merger output should have been uploaded — the failure happens
        // BEFORE the build phase. Probe the DSM for any output starting with
        // the expected prefix; nothing matching merged segments must exist.
        // (We can't enumerate the in-memory DSM directly; the assertion is
        // implicit: if the merger had got far enough to upload, it would
        // have first finished `loadTombstonedOrdinalsBestEffort` for both
        // inputs, which we just proved fails.)
    }

    @Test
    public void modernZnodeWithCorruptOverlayFileThrows() throws Exception {
        // Same as above but the overlay file exists with garbage contents — the
        // deserializer in TombstoneOverlay.deserialize will throw on the version
        // header. Wrapped in TombstoneLoadFailedException by the merger.
        SegmentMetadata a = writeInput("seg-A2", 100L, 0xA, "tombstones-A-gen3", 3L);
        SegmentMetadata b = writeInput("seg-B2", 200L, 0xB, null, 0L);

        // Plant a garbage overlay file at the expected key.
        Path corruptOverlay = Files.createTempFile(tmpDir, "corrupt-overlay-", ".bin");
        Files.write(corruptOverlay, new byte[]{
                // Bogus version header (current format expects 1).
                (byte) 0x12, (byte) 0x34, (byte) 0x56, (byte) 0x78,
                // Plus some padding bytes.
                0, 0, 0, 0
        });
        dsm.writeMultipartIndexFile(TS_UUID,
                TombstoneOverlayManager.multipartUuidFor(IDX_UUID, "seg-A2"),
                TombstoneOverlayManager.fileTypeFor(3L),
                corruptOverlay, null);
        Files.deleteIfExists(corruptOverlay);

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir,
                new StaticIndexMergeConfigProvider(new IndexMergeConfig(
                        8, 32, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN)));
        try {
            merger.merge(List.of(a, b), 0);
            fail("expected TombstoneLoadFailedException for corrupt overlay file");
        } catch (RemoteSegmentMerger.TombstoneLoadFailedException ok) {
            assertTrue(ok.getMessage().contains("seg-A2"));
        }
    }

    @Test
    public void noTombstonePathBypassesOverlayLoad() throws Exception {
        // Sanity check: when tombstonePath is null, the merger never tries to
        // load an overlay (no DSM call, no exception possible). This is the
        // common case for fresh segments produced by the merger itself.
        SegmentMetadata a = writeInput("seg-clean-A", 100L, 0xA, null, 0L);
        SegmentMetadata b = writeInput("seg-clean-B", 200L, 0xB, null, 0L);

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir,
                new StaticIndexMergeConfigProvider(new IndexMergeConfig(
                        8, 32, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN)));
        SegmentMetadata merged = merger.merge(List.of(a, b), 0);
        assertTrue("two clean inputs must merge successfully",
                merged != null && merged.getVectorCount() == 2L * VECTORS_PER_SEGMENT);
    }
}
