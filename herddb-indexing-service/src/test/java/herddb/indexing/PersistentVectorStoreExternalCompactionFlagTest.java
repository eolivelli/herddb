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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the {@code indexing.optimizer.enabled} flag, plumbed through
 * {@link PersistentVectorStore#setExternalCompactionEnabled}: when enabled, the
 * in-IS {@code vectorIndexCompactionThread} must NOT start; when disabled, it
 * must.
 */
public class PersistentVectorStoreExternalCompactionFlagTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);
    }

    private Thread compactionThreadOf(PersistentVectorStore store) throws Exception {
        Method m = PersistentVectorStore.class.getDeclaredMethod("getVectorIndexCompactionThread");
        m.setAccessible(true);
        return (Thread) m.invoke(store);
    }

    @Test
    public void legacyModeStartsCompactionLoop() throws Exception {
        Path tmpDir = tmpFolder.newFolder("legacy").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            // No setExternalCompactionEnabled call — default is legacy.
            assertFalse(store.isExternalCompactionEnabled());
            store.start();
            Thread t = compactionThreadOf(store);
            assertNotNull("legacy mode must launch the in-IS compaction thread", t);
            assertTrue("compaction thread must be alive in legacy mode", t.isAlive());
            assertEquals("persistent-vector-store-vidxcompaction-testidx", t.getName());
        }
    }

    @Test
    public void externalCompactionModeSuppressesCompactionLoop() throws Exception {
        Path tmpDir = tmpFolder.newFolder("external").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.setExternalCompactionEnabled(true);
            assertTrue(store.isExternalCompactionEnabled());
            store.start();
            Thread t = compactionThreadOf(store);
            assertNull("external compaction mode must NOT launch the in-IS compaction thread", t);

            // Sanity: tailer + checkpoint path still works (no segmented-v2 publisher attached
            // here, just verifying we can still ingest and checkpoint without the compaction loop).
            int dim = 32;
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), dim));
            }
            assertTrue(store.checkpoint());
        }
    }

    @Test
    public void flagDefaultsToFalse() throws Exception {
        Path tmpDir = tmpFolder.newFolder("default").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            assertFalse("flag must default to false (legacy mode)", store.isExternalCompactionEnabled());
        }
    }

    @Test
    public void flagPropertyConstantIsAvailableInIndexingServerConfiguration() {
        // Smoke check: the new property constant must be wired up where the IS will read it.
        assertEquals("indexing.optimizer.enabled",
                IndexingServerConfiguration.PROPERTY_INDEX_OPTIMIZER_ENABLED);
        assertFalse(IndexingServerConfiguration.PROPERTY_INDEX_OPTIMIZER_ENABLED_DEFAULT);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }
}
