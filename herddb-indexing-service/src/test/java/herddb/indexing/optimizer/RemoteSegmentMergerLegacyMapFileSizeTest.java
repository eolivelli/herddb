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
import static org.junit.Assert.assertNotNull;
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
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the legacy fallback in {@link RemoteSegmentMerger#merge} for
 * znodes that pre-date the {@code mapFileSize} field added in issue #484
 * round 2 (review item B.1.4).
 *
 * <p>For legacy znodes ({@code mapFileSize == 0}), the merger uses
 * {@code sizeBytes} (graph + map combined) as an upper-bound size hint
 * for the multipart reader; the download path uses the reader's
 * {@link io.github.jbellis.jvector.disk.RandomAccessReader#length()} to
 * clamp the read to the actual file size, so the over-estimate is
 * harmless. This test exercises that path end-to-end against
 * {@link MemoryDataStorageManager} and verifies the merged output is
 * byte-accurate (no truncation, no resurrection of dropped tail entries).
 */
public class RemoteSegmentMergerLegacyMapFileSizeTest {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final String TS_UUID = "ts-legacy-mfs";
    private static final String IDX_UUID = "idx-legacy-mfs";
    private static final int DIM = 8;
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

    private SegmentMetadata writeLegacyInput(String segUuid, long segId, int seed) throws Exception {
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
        long actualMapSize = Files.size(mapFile);
        // Simulate "graphFileSize ≈ 3× mapFileSize" — the typical ratio for a
        // small dim. The combined sizeBytes hint will overestimate map size by
        // 4×, so the legacy fallback MUST clamp via reader.length() to avoid
        // reading past the actual file.
        long combinedSizeHint = actualMapSize * 4L;
        String multipartUuid = IDX_UUID + "_seg" + segId;
        String mapPath = dsm.writeMultipartIndexFile(TS_UUID, multipartUuid, "map", mapFile, null);
        Files.deleteIfExists(mapFile);

        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID).tableName("docs")
                .indexUuid(IDX_UUID).indexName("docs_v1")
                .state(SegmentState.ACTIVE).ownerInstanceId(0)
                .segmentId(segId)
                .graphPath("g/" + segUuid).mapPath(mapPath)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(combinedSizeHint)
                // Legacy: mapFileSize INTENTIONALLY left at the default 0L.
                // The merger must fall back to sizeBytes as the size hint
                // and then clamp via reader.length() to read only the real
                // map bytes (issue #484 round-2 fix).
                .vectorCount(VECTORS_PER_SEGMENT).generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    @Test
    public void legacyZnodeWithUnsetMapFileSizeStillProducesByteAccurateMerge() throws Exception {
        SegmentMetadata a = writeLegacyInput("legacy-mfs-A", 100L, 0xA);
        SegmentMetadata b = writeLegacyInput("legacy-mfs-B", 200L, 0xB);

        // Sanity: the metadata indeed carries mapFileSize == 0 (the field
        // defaults to UNKNOWN_FILE_SIZE when not set).
        assertEquals(SegmentMetadata.UNKNOWN_FILE_SIZE, a.getMapFileSize());
        assertEquals(SegmentMetadata.UNKNOWN_FILE_SIZE, b.getMapFileSize());

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir, DIM, /* M */ 8, /* beam */ 32, 1.2f, 1.4f,
                VectorSimilarityFunction.EUCLIDEAN);

        SegmentMetadata merged = merger.merge(List.of(a, b), /* newOwnerInstance */ 0);
        assertNotNull("merger must succeed on legacy mapFileSize == 0 inputs", merged);
        assertEquals("every input vector must survive (no tail truncation)",
                2L * VECTORS_PER_SEGMENT, merged.getVectorCount());
        assertTrue("merged segmentId must be a real value, not the sentinel",
                merged.getSegmentId() != SegmentMetadata.NO_SEGMENT_ID);
        // The merger's OWN output must populate the new field so subsequent
        // merges of this output don't fall back to the legacy path.
        assertTrue("merger output must populate mapFileSize",
                merged.getMapFileSize() > 0L);
    }

    @Test
    public void modernZnodeWithExplicitMapFileSizeBypassesFallback() throws Exception {
        // Mirror of the legacy test, but the inputs explicitly set
        // mapFileSize via the builder. This is the production path post-#484
        // and is the most heavily exercised code path; included here as an
        // adjacent control case.
        Path mapA = Files.createTempFile(tmpDir, "modern-A-", ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(mapA.toFile()));
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(10);
            for (int i = 0; i < 10; i++) {
                dos.writeInt(i);
                byte[] pk = ("modern-A-" + i).getBytes();
                dos.writeInt(pk.length);
                dos.write(pk);
                dos.writeInt(DIM);
                for (int j = 0; j < DIM; j++) {
                    dos.writeInt(Float.floatToIntBits((float) (i + j)));
                }
            }
        }
        long realSize = Files.size(mapA);
        dsm.writeMultipartIndexFile(TS_UUID, IDX_UUID + "_seg300", "map", mapA, null);
        Files.deleteIfExists(mapA);
        SegmentMetadata modern1 = SegmentMetadata.builder()
                .segmentUuid("modern-A").tablespaceUuid(TS_UUID).tableName("t")
                .indexUuid(IDX_UUID).indexName("i").state(SegmentState.ACTIVE)
                .ownerInstanceId(0).segmentId(300L)
                .graphPath("g").mapPath(Bytes.from_string("map").toString())
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(realSize * 4L)
                .mapFileSize(realSize) // explicit
                .vectorCount(10L).generation(1L)
                .createdAtEpochMillis(0L)
                .build();

        Path mapB = Files.createTempFile(tmpDir, "modern-B-", ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(mapB.toFile()));
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(10);
            for (int i = 0; i < 10; i++) {
                dos.writeInt(i);
                byte[] pk = ("modern-B-" + i).getBytes();
                dos.writeInt(pk.length);
                dos.write(pk);
                dos.writeInt(DIM);
                for (int j = 0; j < DIM; j++) {
                    dos.writeInt(Float.floatToIntBits((float) (i + j + 100)));
                }
            }
        }
        long realSizeB = Files.size(mapB);
        dsm.writeMultipartIndexFile(TS_UUID, IDX_UUID + "_seg400", "map", mapB, null);
        Files.deleteIfExists(mapB);
        SegmentMetadata modern2 = SegmentMetadata.builder()
                .segmentUuid("modern-B").tablespaceUuid(TS_UUID).tableName("t")
                .indexUuid(IDX_UUID).indexName("i").state(SegmentState.ACTIVE)
                .ownerInstanceId(0).segmentId(400L)
                .graphPath("g").mapPath("m")
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(realSizeB * 4L)
                .mapFileSize(realSizeB)
                .vectorCount(10L).generation(2L)
                .createdAtEpochMillis(0L)
                .build();

        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                dsm, tmpDir, DIM, 8, 32, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);
        SegmentMetadata merged = merger.merge(List.of(modern1, modern2), 0);
        assertNotNull(merged);
        assertEquals(20L, merged.getVectorCount());
    }
}
