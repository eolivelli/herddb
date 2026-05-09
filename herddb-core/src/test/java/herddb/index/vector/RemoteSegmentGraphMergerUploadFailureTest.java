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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the orphan-graph cleanup branch of {@link RemoteSegmentGraphMerger}:
 * when the graph file uploads successfully but the map upload fails, the
 * merger MUST delete the orphan graph artefact so an aborted run doesn't leak
 * multipart files (issue #484, review item B.4 from the first pr-reviewer pass).
 */
public class RemoteSegmentGraphMergerUploadFailureTest {

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final String TS_UUID = "ts-upfail";
    private static final String IDX_UUID = "idx-upfail";
    private static final int DIM = 8;
    private static final int VECTORS_PER_SEGMENT = 30;

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private MemoryDataStorageManager backingDsm;
    private Path tmpDir;

    @Before
    public void setUp() throws Exception {
        backingDsm = new MemoryDataStorageManager();
        tmpDir = tmp.newFolder("merger-tmp").toPath();
    }

    private List<RemoteSegmentGraphMerger.RemoteSegmentInput> writeTwoInputs() throws Exception {
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs = new ArrayList<>(2);
        for (int seg = 0; seg < 2; seg++) {
            Path mapFile = Files.createTempFile(tmpDir, "input-", ".tmp");
            try (BufferedOutputStream bos = new BufferedOutputStream(
                    new FileOutputStream(mapFile.toFile()));
                 DataOutputStream dos = new DataOutputStream(bos)) {
                dos.writeInt(VECTORS_PER_SEGMENT);
                Random rng = new Random(seg);
                for (int local = 0; local < VECTORS_PER_SEGMENT; local++) {
                    dos.writeInt(local);
                    byte[] pk = ("seg" + seg + "-" + local).getBytes();
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
            backingDsm.writeMultipartIndexFile(TS_UUID, IDX_UUID + "_seg" + seg, "map", mapFile, null);
            Files.deleteIfExists(mapFile);
            inputs.add(new RemoteSegmentGraphMerger.RemoteSegmentInput(
                    TS_UUID, IDX_UUID, "seg-uuid-" + seg, seg, mapSize,
                    /* generation */ 1L + seg, new int[0]));
        }
        return inputs;
    }

    @Test
    public void mapUploadFailureCleansUpOrphanGraph() throws Exception {
        // Use a fresh throwing DSM so the merger reads inputs back from the
        // SAME storage instance that simulates the map-upload failure. The
        // fault is armed only AFTER the input fixtures have been seeded so
        // the seed writes never trip the trigger.
        List<String> deletes = new ArrayList<>();
        ThrowingMapUploadDsm throwingDsm = new ThrowingMapUploadDsm(
                new AtomicInteger(), deletes);
        backingDsm = throwingDsm;
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs = writeTwoInputs();
        throwingDsm.armMapUploadFailure();

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                throwingDsm, tmpDir, /* M */ 8, /* beam */ 32, 1.2f, 1.4f,
                VectorSimilarityFunction.EUCLIDEAN);

        long outputSegmentId = 555L;
        try {
            merger.merge(inputs, TS_UUID, IDX_UUID, outputSegmentId, DIM);
            fail("expected DataStorageManagerException from the map-upload failure");
        } catch (DataStorageManagerException ok) {
            assertTrue(ok.getMessage().contains("simulated map upload failure"));
        }

        // The merger must have attempted to delete the orphan graph file on the
        // way out (the cleanup branch in RemoteSegmentGraphMerger.merge).
        String expectedKey = TS_UUID + "/" + IDX_UUID + "_seg" + outputSegmentId + "/graph";
        assertTrue("orphan-graph cleanup must have been invoked, observed deletes: " + deletes,
                deletes.contains(expectedKey));

        // Belt-and-suspenders: the orphan graph file is gone from the backing
        // DSM (the throwing decorator delegates deletes to the backing DSM).
        try {
            backingDsm.multipartIndexReaderSupplier(TS_UUID,
                    IDX_UUID + "_seg" + outputSegmentId, "graph", 1L);
            fail("orphan graph file must have been deleted");
        } catch (DataStorageManagerException ok) {
            // expected — the in-memory DSM throws on missing keys
        }
    }

    @Test
    public void deleteOutputAfterEverythingSucceededRemovesBoth() throws Exception {
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs = writeTwoInputs();
        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                backingDsm, tmpDir, 8, 32, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);
        RemoteSegmentGraphMerger.MergeOutput output = merger.merge(
                inputs, TS_UUID, IDX_UUID, /* outputSegmentId */ 777L, DIM);
        assertTrue(output != null);

        merger.deleteOutput(output);

        // Both files removed from the backing DSM.
        try {
            backingDsm.multipartIndexReaderSupplier(TS_UUID,
                    IDX_UUID + "_seg777", "graph", 1L);
            fail("graph file must have been deleted");
        } catch (DataStorageManagerException ok) {
            // expected
        }
        try {
            backingDsm.multipartIndexReaderSupplier(TS_UUID,
                    IDX_UUID + "_seg777", "map", 1L);
            fail("map file must have been deleted");
        } catch (DataStorageManagerException ok) {
            // expected
        }

        // deleteOutput is idempotent — calling it again on already-deleted output
        // must NOT throw.
        merger.deleteOutput(output);
    }

    @Test
    public void deleteOutputOnNullIsNoOp() throws Exception {
        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                backingDsm, tmpDir, 8, 32, 1.2f, 1.4f, VectorSimilarityFunction.EUCLIDEAN);
        merger.deleteOutput(null);
        // No exception; that's the assertion.
    }

    /**
     * DSM decorator that delegates to a backing {@link MemoryDataStorageManager}
     * for everything the merger touches except for the second
     * {@code writeMultipartIndexFile} call (the map-file upload, which always
     * follows the graph upload). That call throws to simulate a partial-upload
     * failure so the orphan-graph cleanup branch can be exercised.
     *
     * <p>Subclassing {@link MemoryDataStorageManager} avoids the ~30
     * abstract-method ceremony that subclassing
     * {@code herddb.storage.DataStorageManager} directly would require.
     */
    private static final class ThrowingMapUploadDsm extends MemoryDataStorageManager {
        private final List<String> deletes;
        /** Set to true to enable the throw-on-next-map-upload trigger. */
        private volatile boolean throwOnNextMapUpload = false;

        ThrowingMapUploadDsm(AtomicInteger unused, List<String> deletes) {
            this.deletes = deletes;
        }

        void armMapUploadFailure() {
            this.throwOnNextMapUpload = true;
        }

        @Override
        public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                              Path tempFile, LongConsumer progress)
                throws IOException, DataStorageManagerException {
            if ("map".equals(fileType) && throwOnNextMapUpload) {
                throwOnNextMapUpload = false;
                throw new DataStorageManagerException("simulated map upload failure");
            }
            return super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
        }

        @Override
        public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
                throws DataStorageManagerException {
            deletes.add(tableSpace + "/" + uuid + "/" + fileType);
            super.deleteMultipartIndexFile(tableSpace, uuid, fileType);
        }
    }
}
