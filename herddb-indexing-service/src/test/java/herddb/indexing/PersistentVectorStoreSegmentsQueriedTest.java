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
package herddb.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import herddb.utils.VectorSearchRequestContext;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link VectorSearchRequestContext#getSegmentsQueried()} counts
 * exactly the number of on-disk segments (Phase 1) plus in-memory shards
 * (Phase 2/3) queried during a {@link PersistentVectorStore#search} call
 * (issue #541).
 *
 * <p>The acceptance criterion from the issue is:
 * <pre>
 *   segments_queried == N  (or >= N with in-memory shards included)
 * </pre>
 */
public class PersistentVectorStoreSegmentsQueriedTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);
    }

    /**
     * With only live in-memory shards (no checkpoint yet), searching must still
     * record at least one shard queried when there are vectors in the store.
     */
    @Test
    public void testSegmentsQueriedWithLiveShardsOnly() {
        Path tmpDir;
        try {
            tmpDir = tmpFolder.newFolder("live-shards").toPath();
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        PersistentVectorStore store = createStore(tmpDir);
        for (int i = 0; i < 20; i++) {
            store.addVector(Bytes.from_int(i), new float[]{i * 0.1f, (20 - i) * 0.1f});
        }

        VectorSearchRequestContext ctx = VectorSearchRequestContext.begin();
        try {
            List<Map.Entry<Bytes, Float>> results = store.search(new float[]{0.5f, 0.5f}, 5);
            assertTrue("Should return results", !results.isEmpty());
            long queried = ctx.getSegmentsQueried();
            assertTrue("At least one in-memory shard must be recorded as queried; got " + queried,
                    queried >= 1);
        } finally {
            VectorSearchRequestContext.end();
        }

        // After end(), current() must return null — no leakage.
        assertNull(VectorSearchRequestContext.current());
    }

    /**
     * After a checkpoint, vectors are flushed to one on-disk segment.
     * A search must record that on-disk segment in segments_queried.
     */
    @Test
    public void testSegmentsQueriedAfterCheckpoint() throws Exception {
        Path tmpDir = tmpFolder.newFolder("after-checkpoint").toPath();
        PersistentVectorStore store = createStore(tmpDir);

        // Add vectors and checkpoint to flush to disk.
        for (int i = 0; i < 50; i++) {
            store.addVector(Bytes.from_int(i), new float[]{i * 0.1f, (50 - i) * 0.1f});
        }
        store.checkpoint();

        // After checkpoint, vectors are in at least one on-disk segment.
        int onDiskSegments = store.getSegmentCount();
        assertTrue("Store must have at least 1 on-disk segment after checkpoint",
                onDiskSegments >= 1);

        VectorSearchRequestContext ctx = VectorSearchRequestContext.begin();
        try {
            List<Map.Entry<Bytes, Float>> results = store.search(new float[]{2.0f, 3.0f}, 5);
            assertTrue("Should return results after checkpoint", !results.isEmpty());
            long queried = ctx.getSegmentsQueried();
            // Must have visited at least all on-disk segments; in-memory shards
            // may or may not be non-empty but on-disk ones are always present.
            assertTrue(
                    "segments_queried (" + queried + ") must be >= on-disk segment count ("
                            + onDiskSegments + ")",
                    queried >= onDiskSegments);
        } finally {
            VectorSearchRequestContext.end();
        }
    }

    /**
     * When no {@link VectorSearchRequestContext} is active, searching must not
     * throw — the null-guard in {@code searchInternal} must be respected.
     */
    @Test
    public void testSearchWithoutContextDoesNotThrow() {
        Path tmpDir;
        try {
            tmpDir = tmpFolder.newFolder("no-ctx").toPath();
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        PersistentVectorStore store = createStore(tmpDir);
        for (int i = 0; i < 10; i++) {
            store.addVector(Bytes.from_int(i), new float[]{i * 0.5f, 1.0f});
        }

        // No VectorSearchRequestContext.begin() — current() returns null.
        assertNull(VectorSearchRequestContext.current());
        List<Map.Entry<Bytes, Float>> results = store.search(new float[]{1.0f, 1.0f}, 3);
        assertTrue("Results must still be returned without an active context",
                !results.isEmpty());
    }

    /**
     * segments_queried is scoped to the request context: a second search on the
     * same store must start from 0 in a fresh context.
     */
    @Test
    public void testSegmentsQueriedResetsBetweenRequests() {
        Path tmpDir;
        try {
            tmpDir = tmpFolder.newFolder("two-requests").toPath();
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        PersistentVectorStore store = createStore(tmpDir);
        for (int i = 0; i < 20; i++) {
            store.addVector(Bytes.from_int(i), new float[]{i * 0.1f, (20 - i) * 0.1f});
        }

        long firstQueried;
        VectorSearchRequestContext ctx1 = VectorSearchRequestContext.begin();
        try {
            store.search(new float[]{0.5f, 0.5f}, 3);
            firstQueried = ctx1.getSegmentsQueried();
        } finally {
            VectorSearchRequestContext.end();
        }

        // Second independent request.
        VectorSearchRequestContext ctx2 = VectorSearchRequestContext.begin();
        try {
            store.search(new float[]{0.9f, 0.1f}, 3);
            long secondQueried = ctx2.getSegmentsQueried();
            // Both requests visit the same shards, so counts should be equal.
            assertEquals("segments_queried must be consistent across independent requests",
                    firstQueried, secondQueried);
        } finally {
            VectorSearchRequestContext.end();
        }
    }
}
