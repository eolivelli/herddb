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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentState;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration tests for {@link RemoteSegmentMerger} against a
 * {@link MemoryDataStorageManager} (issue #484).
 *
 * <p>Drives the production merger through realistic inputs (real graph and
 * map files written via the DSM) and asserts:
 * <ul>
 *   <li>{@code merge()} returns a fresh ACTIVE {@link SegmentMetadata} with
 *       a 63-bit non-negative random {@code segmentId}, generation
 *       = {@code max(input) + 1}, and a graphPath/mapPath pointing to a
 *       newly-uploaded multipart pair on the DSM;</li>
 *   <li>{@code abandon()} deletes both the graph and map multipart files
 *       so an aborted run doesn't leak artefacts;</li>
 *   <li>mismatched tablespace/index uuids in the input list throw an
 *       IllegalArgumentException — the engine should never call us with a
 *       cross-index batch but we defend the invariant.</li>
 * </ul>
 */
public class RemoteSegmentMergerTest {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final String TS_UUID = "ts-rsm";
    private static final String IDX_UUID = "idx-rsm";
    private static final int DIM = 32;
    private static final int VECTORS_PER_SEGMENT = 200;

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private MemoryDataStorageManager dsm;
    private Path tmpDir;

    @Before
    public void setUp() throws Exception {
        dsm = new MemoryDataStorageManager();
        tmpDir = tmp.newFolder("merger-tmp").toPath();
        // This suite produces synthetic inputs with only a map file (no real
        // graph file). The streaming dispatch in {@code RemoteSegmentGraphMerger}
        // requires a real graph multipart file per input ({@code graphFileSize > 0});
        // without one it automatically routes to the legacy in-memory rebuild
        // path. No explicit flag flip is needed — the absent graph file is
        // itself the route.
    }

    private SegmentMetadata writeInputSegmentToDsm(String uuid, long segmentId,
                                                   long generation, int seedBase) throws Exception {
        Path mapFile = Files.createTempFile(tmpDir, "input-", ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(mapFile.toFile()));
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(VECTORS_PER_SEGMENT);
            Random rng = new Random(seedBase);
            for (int local = 0; local < VECTORS_PER_SEGMENT; local++) {
                dos.writeInt(local);
                byte[] pkBytes = Bytes.from_string("pk-" + uuid + "-" + local).to_array();
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
        String multipartUuid = IDX_UUID + "_seg" + segmentId;
        String mapPath = dsm.writeMultipartIndexFile(TS_UUID, multipartUuid, "map", mapFile, null);
        Files.deleteIfExists(mapFile);

        return SegmentMetadata.builder()
                .segmentUuid(uuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .segmentId(segmentId)
                .graphPath("g/" + uuid)
                .mapPath(mapPath)
                .baseLsn(new LogSequenceNumber(1L, 100L * generation))
                .sizeBytes(mapSize)  // == mapFileSize → derived graphFileSize == 0
                // forces RemoteSegmentGraphMerger.merge into mergeLegacy
                .mapFileSize(mapSize)
                .vectorCount(VECTORS_PER_SEGMENT)
                .generation(generation)
                .createdAtEpochMillis(0L)
                .build();
    }

    @Test
    public void mergeProducesActiveSegmentWithRandomSegmentId() throws Exception {
        SegmentMetadata a = writeInputSegmentToDsm("seg-A", 100L, /* gen */ 1L, 0xA);
        SegmentMetadata b = writeInputSegmentToDsm("seg-B", 200L, /* gen */ 2L, 0xB);

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir,
                new StaticIndexMergeConfigProvider(new IndexMergeConfig(
                        /* graphM */ 16, /* beamWidth */ 64,
                        /* neighborOverflow */ 1.2f, /* alpha */ 1.4f,
                        VectorSimilarityFunction.EUCLIDEAN)));
        SegmentMetadata merged = merger.merge(List.of(a, b), /* newOwnerInstance */ 0);

        assertNotNull("merger must produce an output for healthy inputs", merged);
        assertEquals(SegmentState.ACTIVE, merged.getState());
        assertEquals(TS_UUID, merged.getTablespaceUuid());
        assertEquals(IDX_UUID, merged.getIndexUuid());
        assertEquals("generation = max(input) + 1", 3L, merged.getGeneration());
        assertEquals("vector count = sum of inputs (no tombstones)",
                2L * VECTORS_PER_SEGMENT, merged.getVectorCount());
        assertEquals("merged segment starts with no overlay", 0L, merged.getOverlayGeneration());
        assertNull("merged segment has no tombstone path", merged.getTombstonePath());

        // segmentId must be a 63-bit non-negative random — collision-free vs the inputs
        // (segments 100 and 200), and not the sentinel.
        assertNotEquals(SegmentMetadata.NO_SEGMENT_ID, merged.getSegmentId());
        assertTrue("segmentId must be non-negative", merged.getSegmentId() >= 0L);
        assertNotEquals("segmentId must differ from both inputs",
                a.getSegmentId(), merged.getSegmentId());
        assertNotEquals(b.getSegmentId(), merged.getSegmentId());

        // The output multipart files must be addressable under the new segmentId.
        String multipartUuid = IDX_UUID + "_seg" + merged.getSegmentId();
        assertNotNull(dsm.multipartIndexReaderSupplier(TS_UUID, multipartUuid, "graph",
                /* sizeHint */ 1L));
        assertNotNull(dsm.multipartIndexReaderSupplier(TS_UUID, multipartUuid, "map",
                /* sizeHint */ 1L));

        assertEquals(1L, merger.getInvocationCount());
    }

    @Test
    public void abandonRemovesUploadedFiles() throws Exception {
        SegmentMetadata a = writeInputSegmentToDsm("seg-A", 100L, 1L, 0x1);
        SegmentMetadata b = writeInputSegmentToDsm("seg-B", 200L, 2L, 0x2);

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir,
                new StaticIndexMergeConfigProvider(new IndexMergeConfig(
                        16, 64, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN)));
        SegmentMetadata merged = merger.merge(List.of(a, b), 0);
        assertNotNull(merged);

        // Pre-condition: files exist on the DSM.
        String multipartUuid = IDX_UUID + "_seg" + merged.getSegmentId();
        dsm.multipartIndexReaderSupplier(TS_UUID, multipartUuid, "graph", 1L);

        merger.abandon(merged);

        // The MemoryDataStorageManager throws on missing keys, which is exactly
        // what we want to assert.
        try {
            dsm.multipartIndexReaderSupplier(TS_UUID, multipartUuid, "graph", 1L);
            org.junit.Assert.fail("graph file must be deleted after abandon");
        } catch (Exception ok) {
            // expected
        }
        try {
            dsm.multipartIndexReaderSupplier(TS_UUID, multipartUuid, "map", 1L);
            org.junit.Assert.fail("map file must be deleted after abandon");
        } catch (Exception ok) {
            // expected
        }
    }

    @Test
    public void mismatchedTablespaceRejected() throws Exception {
        SegmentMetadata a = writeInputSegmentToDsm("seg-A", 100L, 1L, 0x1);
        // Synthesise an input from a different tablespace — without writing to
        // the DSM (the merger throws before it gets there).
        SegmentMetadata b = SegmentMetadata.builder()
                .segmentUuid("seg-other")
                .tablespaceUuid("other-ts")
                .tableName("docs").indexUuid(IDX_UUID).indexName("docs_v1")
                .state(SegmentState.ACTIVE).ownerInstanceId(0)
                .segmentId(200L)
                .graphPath("g/other").mapPath("m/other")
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(100L).vectorCount(1L).generation(1L).createdAtEpochMillis(0L)
                .build();
        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir,
                new StaticIndexMergeConfigProvider(new IndexMergeConfig(
                        16, 64, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN)));
        try {
            merger.merge(List.of(a, b), 0);
            org.junit.Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            assertTrue(ok.getMessage().contains("tablespaceUuid"));
        }
    }

    @Test
    public void noSegmentIdRejected() throws Exception {
        SegmentMetadata a = writeInputSegmentToDsm("seg-A", 100L, 1L, 0x1);
        SegmentMetadata b = SegmentMetadata.builder()
                .segmentUuid("seg-noid")
                .tablespaceUuid(TS_UUID).tableName("docs")
                .indexUuid(IDX_UUID).indexName("docs_v1")
                .state(SegmentState.ACTIVE).ownerInstanceId(0)
                .segmentId(SegmentMetadata.NO_SEGMENT_ID)  // sentinel
                .graphPath("g/no").mapPath("m/no")
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(100L).vectorCount(1L).generation(1L).createdAtEpochMillis(0L)
                .build();
        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir,
                new StaticIndexMergeConfigProvider(new IndexMergeConfig(
                        16, 64, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN)));
        try {
            merger.merge(List.of(a, b), 0);
            org.junit.Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            assertTrue(ok.getMessage().contains("segmentId"));
        }
    }

    @Test
    public void emptyInputReturnsNull() throws Exception {
        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir,
                new StaticIndexMergeConfigProvider(new IndexMergeConfig(
                        16, 64, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN)));
        assertNull("empty input → null output", merger.merge(new ArrayList<>(), 0));
    }
}
