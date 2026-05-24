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
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #650: verifies the IS-side wiring of the file-server prewarm path —
 * {@code PersistentVectorStore.prewarmFileServerForSegment(VectorSegment)}
 * dispatches one {@code prewarmMultipartIndexFile} call per segment with the
 * canonical parameters (4 MiB block size, parallelism=8, graph filetype,
 * exact graphFileSize); and {@code warmUpNewSegmentsBeforePublish} skips
 * prewarm entirely when {@code setPrewarmFileServer(false)}.
 */
public class PersistentVectorStorePrewarmFileServerTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /** Recorded {@code prewarmMultipartIndexFile} call. */
    private static final class PrewarmCall {
        final String tableSpace;
        final String uuid;
        final String fileType;
        final long fileSize;
        final int blockSize;
        final int parallelism;

        PrewarmCall(String tableSpace, String uuid, String fileType,
                    long fileSize, int blockSize, int parallelism) {
            this.tableSpace = tableSpace;
            this.uuid = uuid;
            this.fileType = fileType;
            this.fileSize = fileSize;
            this.blockSize = blockSize;
            this.parallelism = parallelism;
        }
    }

    /**
     * Memory-backed DSM that records every {@code prewarmMultipartIndexFile}
     * call. {@code MemoryDataStorageManager} is public; subclassing it
     * directly lets us stay in the {@code herddb.indexing.vector} package so
     * we can call {@code PersistentVectorStore.prewarmFileServerForSegment}
     * (which is package-private) without reflection.
     */
    static final class RecordingPrewarmDsm extends MemoryDataStorageManager {
        final List<PrewarmCall> calls =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void prewarmMultipartIndexFile(String tableSpace, String uuid,
                                              String fileType, long fileSize,
                                              int blockSize, int parallelism)
                throws DataStorageManagerException {
            calls.add(new PrewarmCall(tableSpace, uuid, fileType, fileSize,
                    blockSize, parallelism));
        }
    }

    private PersistentVectorStore newStore(Path tmpDir, DataStorageManager dsm,
                                           String indexUuid) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0,
                1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(
                "vidx650", "vectable", "tstblspace", "vec", indexUuid, tmpDir,
                dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
    }

    private static VectorSegment minimalSegment(int segmentId, long graphFileSize) {
        VectorSegment seg = new VectorSegment(segmentId);
        // No externalStorageKey — segmentStorageKey uses the legacy
        // {indexUUID}_seg{segmentId} formula. graphFileSize > 0 is the only
        // gate the prewarm-for-segment code checks.
        seg.graphFileSize = graphFileSize;
        return seg;
    }

    @Test
    public void prewarmFileServerForSegmentDispatchesOnePrewarmWithCanonicalParams() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        RecordingPrewarmDsm dsm = new RecordingPrewarmDsm();
        String indexUuid = "uuid-prewarm-direct";
        try (PersistentVectorStore store = newStore(tmpDir, dsm, indexUuid)) {
            // Default state: prewarmFileServer enabled, parallelism = 8.
            // (The constructor wires those defaults.)
            store.start();

            long graphFileSize = 12L * 1024 * 1024 + 1234L; // 3+ blocks, non-aligned
            VectorSegment seg = minimalSegment(/*segmentId*/ 42, graphFileSize);

            boolean dispatched = store.prewarmFileServerForSegment(seg);
            assertTrue("prewarm must dispatch when graphFileSize > 0", dispatched);
            assertEquals("exactly one prewarm call must be issued", 1, dsm.calls.size());

            PrewarmCall c = dsm.calls.get(0);
            assertEquals("tableSpace must be the store's tableSpaceUUID",
                    "tstblspace", c.tableSpace);
            assertEquals("uuid must follow the {indexUUID}_seg{segmentId} convention",
                    indexUuid + "_seg" + 42, c.uuid);
            assertEquals("fileType must be 'graph'", "graph", c.fileType);
            assertEquals("fileSize must equal the segment's graphFileSize",
                    graphFileSize, c.fileSize);
            assertEquals("blockSize must be MULTIPART_BLOCK_SIZE (4 MiB)",
                    4 * 1024 * 1024, c.blockSize);
            assertEquals("default parallelism must be 8", 8, c.parallelism);
        }
    }

    @Test
    public void zeroByteSegmentSkipsPrewarm() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        RecordingPrewarmDsm dsm = new RecordingPrewarmDsm();
        try (PersistentVectorStore store = newStore(tmpDir, dsm, "uuid-zero")) {
            store.start();
            VectorSegment seg = minimalSegment(1, 0L);
            boolean dispatched = store.prewarmFileServerForSegment(seg);
            assertFalse("zero-byte segment must not dispatch a prewarm call", dispatched);
            assertTrue("DSM must record no calls for a zero-byte segment", dsm.calls.isEmpty());
        }
    }

    @Test
    public void warmUpNewSegmentsBeforePublishSkipsPrewarmWhenDisabled() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        RecordingPrewarmDsm dsm = new RecordingPrewarmDsm();
        try (PersistentVectorStore store = newStore(tmpDir, dsm, "uuid-disabled")) {
            store.start();
            // Turn the prewarm gate OFF.
            store.setPrewarmFileServer(false);

            VectorSegment seg = minimalSegment(/*segmentId*/ 7,
                    /*graphFileSize*/ 8L * 1024 * 1024 + 17);

            // warmUpNewSegmentsBeforePublish is private — invoke via reflection.
            Method m = PersistentVectorStore.class.getDeclaredMethod(
                    "warmUpNewSegmentsBeforePublish", List.class, String.class);
            m.setAccessible(true);
            m.invoke(store, Collections.singletonList(seg), "unit-test");

            assertTrue("with prewarmFileServer disabled, warmUpNewSegmentsBeforePublish "
                    + "must NOT dispatch any prewarm RPC; got " + dsm.calls.size(),
                    dsm.calls.isEmpty());
        }
    }
}
