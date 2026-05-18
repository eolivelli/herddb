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
package herddb.indexing.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Targeted unit test for the tombstone-filter path in
 * {@link RemoteSegmentGraphMerger} (issue #484).
 *
 * <p>Builds two input segments:
 * <ul>
 *   <li>Input A: 100 vectors, with the first 50 ordinals tombstoned.</li>
 *   <li>Input B: 100 vectors, untombstoned.</li>
 * </ul>
 *
 * <p>Asserts that the merged segment contains exactly:
 * <ul>
 *   <li>The 50 surviving PKs from input A (ordinals 50..99).</li>
 *   <li>All 100 PKs from input B.</li>
 * </ul>
 *
 * <p>Tombstoned PKs from A must NOT appear in the merged map file — proving
 * the tombstone bitset filter actually drops them rather than letting them
 * resurrect through the merge.
 */
public class RemoteSegmentGraphMergerTombstoneFilterTest {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final String TS_UUID = "ts-tomb";
    private static final String IDX_UUID = "idx-tomb";
    private static final int DIM = 16;
    private static final int VECTORS_PER_SEGMENT = 100;
    private static final int M = 8;
    private static final int BEAM_WIDTH = 32;

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private MemoryDataStorageManager dsm;
    private Path tmpDir;

    @Before
    public void setUp() throws Exception {
        dsm = new MemoryDataStorageManager();
        tmpDir = tmp.newFolder("merger-tmp").toPath();
    }

    private void writeMapForSegment(long segmentId, String pkPrefix) throws Exception {
        Path mapFile = Files.createTempFile(tmpDir, "input-", ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(mapFile.toFile()));
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(VECTORS_PER_SEGMENT);
            Random rng = new Random((int) segmentId * 0xDEADBEEFL);
            for (int local = 0; local < VECTORS_PER_SEGMENT; local++) {
                dos.writeInt(local);
                byte[] pkBytes = Bytes.from_string(pkPrefix + "-" + local).to_array();
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
        dsm.writeMultipartIndexFile(TS_UUID, IDX_UUID + "_seg" + segmentId, "map", mapFile, null);
        Files.deleteIfExists(mapFile);
        // Side-effect: the test holds the size for later assertions.
        sizes[(int) segmentId] = mapSize;
    }

    /** Map sizes recorded by {@link #writeMapForSegment} for later input wiring. */
    private final long[] sizes = new long[10];

    @Test
    public void halfTombstonedSegmentLosesExactlyTombstonedPks() throws Exception {
        // Input A: tombstones ordinals 0..49.
        writeMapForSegment(/* segmentId */ 1L, "a");
        // Input B: no tombstones.
        writeMapForSegment(2L, "b");

        int[] tombstonedA = new int[VECTORS_PER_SEGMENT / 2];
        for (int i = 0; i < tombstonedA.length; i++) {
            tombstonedA[i] = i;
        }
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs = new ArrayList<>();
        inputs.add(new RemoteSegmentGraphMerger.RemoteSegmentInput(
                TS_UUID, IDX_UUID, "uuid-A", 1L, sizes[1], /* generation */ 1L,
                tombstonedA));
        inputs.add(new RemoteSegmentGraphMerger.RemoteSegmentInput(
                TS_UUID, IDX_UUID, "uuid-B", 2L, sizes[2], /* generation */ 2L,
                /* tombstoned */ new int[0]));

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, M, BEAM_WIDTH, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);
        long outputSegmentId = 9_876_543_210L;
        RemoteSegmentGraphMerger.MergeOutput output = merger.merge(
                inputs, TS_UUID, IDX_UUID, outputSegmentId, DIM);
        assertNotNull(output);
        assertEquals("50 from A + 100 from B = 150 surviving", 150, output.vectorCount);
        assertEquals(50L, output.droppedTombstones);
        assertEquals(0L, output.droppedDuplicates);

        Set<Bytes> mergedPks = readMergedPks(output);
        assertEquals(150, mergedPks.size());
        // All B PKs must be present.
        for (int i = 0; i < VECTORS_PER_SEGMENT; i++) {
            assertTrue("input-B PK b-" + i + " missing",
                    mergedPks.contains(Bytes.from_string("b-" + i)));
        }
        // Only the un-tombstoned A PKs (ordinals 50..99) must be present.
        for (int i = 0; i < VECTORS_PER_SEGMENT; i++) {
            Bytes pk = Bytes.from_string("a-" + i);
            if (i < 50) {
                assertFalse("tombstoned PK a-" + i + " must NOT appear in merged segment",
                        mergedPks.contains(pk));
            } else {
                assertTrue("surviving PK a-" + i + " missing", mergedPks.contains(pk));
            }
        }
    }

    @Test
    public void allOrdinalsTombstonedDropsEntireSegment() throws Exception {
        writeMapForSegment(1L, "a");
        writeMapForSegment(2L, "b");

        int[] allTombstoned = new int[VECTORS_PER_SEGMENT];
        for (int i = 0; i < VECTORS_PER_SEGMENT; i++) {
            allTombstoned[i] = i;
        }
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs = List.of(
                new RemoteSegmentGraphMerger.RemoteSegmentInput(
                        TS_UUID, IDX_UUID, "uuid-A", 1L, sizes[1], 1L, allTombstoned),
                new RemoteSegmentGraphMerger.RemoteSegmentInput(
                        TS_UUID, IDX_UUID, "uuid-B", 2L, sizes[2], 2L, new int[0]));

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, M, BEAM_WIDTH, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);
        RemoteSegmentGraphMerger.MergeOutput output = merger.merge(
                inputs, TS_UUID, IDX_UUID, 1L, DIM);
        assertNotNull(output);
        assertEquals("only input-B's 100 vectors survive", 100, output.vectorCount);
        assertEquals(VECTORS_PER_SEGMENT, output.droppedTombstones);
        Set<Bytes> mergedPks = readMergedPks(output);
        for (int i = 0; i < VECTORS_PER_SEGMENT; i++) {
            assertFalse(mergedPks.contains(Bytes.from_string("a-" + i)));
            assertTrue(mergedPks.contains(Bytes.from_string("b-" + i)));
        }
    }

    private Set<Bytes> readMergedPks(RemoteSegmentGraphMerger.MergeOutput output) throws Exception {
        ReaderSupplier supplier = dsm.multipartIndexReaderSupplier(
                output.tablespaceUuid, output.indexUuid + "_seg" + output.segmentId,
                "map", output.mapFileSize);
        try (RandomAccessReader r = supplier.get()) {
            r.seek(0L);
            byte[] all = new byte[(int) output.mapFileSize];
            r.readFully(all);
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(all));
            int entryCount = dis.readInt();
            Set<Bytes> out = new HashSet<>(entryCount * 2);
            for (int i = 0; i < entryCount; i++) {
                dis.readInt(); // ordinal
                int pkLen = dis.readInt();
                byte[] pk = new byte[pkLen];
                dis.readFully(pk);
                int floatCount = dis.readInt();
                for (int j = 0; j < floatCount; j++) {
                    dis.readInt();
                }
                out.add(Bytes.from_array(pk));
            }
            return out;
        }
    }
}
