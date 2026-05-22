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
import herddb.core.MemoryManager;
import herddb.file.FileDataStorageManager;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Setter/getter wiring tests for the per-cycle output-node cap (issue #643).
 *
 * <p>The cap field {@code vectorIndexCompactionMaxOutputNodes} is what
 * {@link PersistentVectorStore#runCompactionCycle} passes to
 * {@link VectorIndexCompactor#chooseSegmentsToMerge}'s 9-arg overload. The
 * policy-level behaviour is covered by
 * {@link VectorIndexCompactorMaxOutputNodesTest}; this test pins the public
 * API surface — default (disabled), positive set, clamp on negative — so the
 * wiring through {@link herddb.indexing.IndexingServiceEngine} (which reads
 * the config key {@code vector.index.compaction.maxOutputNodes} and calls
 * {@link PersistentVectorStore#setCompactionMaxOutputNodes(long)}) stays
 * correct.
 */
public class PersistentVectorStoreCompactionMaxOutputNodesTest {

    private static final String TABLE_SPACE = "tstblspace";
    private static final String INDEX_NAME = "mon";
    private static final String INDEX_UUID = "mon_idx_uuid";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir, FileDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(INDEX_NAME, "testtable", TABLE_SPACE,
                "vector_col", INDEX_UUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0, Long.MAX_VALUE);
    }

    @Test
    public void defaultIsDisabled() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            assertEquals("default must be 0 (cap disabled — purely additive on upgrade)",
                    0L, store.getCompactionMaxOutputNodes());
        }
    }

    @Test
    public void positiveValuePassesThrough() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // Typical operator-recommended value: maxLiveGraphSize × maxCount
            // (2,000,000 for maxLiveGraphSize=100,000 and maxCount=20).
            store.setCompactionMaxOutputNodes(2_000_000L);
            assertEquals(2_000_000L, store.getCompactionMaxOutputNodes());

            // Also re-tune to a different value, simulating a live operator change.
            store.setCompactionMaxOutputNodes(500_000L);
            assertEquals(500_000L, store.getCompactionMaxOutputNodes());

            // Long.MAX_VALUE passes through unchanged (a positive value).
            store.setCompactionMaxOutputNodes(Long.MAX_VALUE);
            assertEquals(Long.MAX_VALUE, store.getCompactionMaxOutputNodes());
        }
    }

    @Test
    public void zeroExplicitlyDisablesTheCap() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // Start enabled, then disable via 0.
            store.setCompactionMaxOutputNodes(123_456_789L);
            assertEquals(123_456_789L, store.getCompactionMaxOutputNodes());

            store.setCompactionMaxOutputNodes(0L);
            assertEquals("0 must disable the cap", 0L,
                    store.getCompactionMaxOutputNodes());
        }
    }

    @Test
    public void negativeValueClampsToZero() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // Negative values are not meaningful for a node count; the setter
            // must clamp to 0 (disabled) rather than store a nonsense value
            // that the picker would then have to defensively re-check.
            store.setCompactionMaxOutputNodes(-5L);
            assertEquals("negative must clamp to 0 (disabled)",
                    0L, store.getCompactionMaxOutputNodes());

            store.setCompactionMaxOutputNodes(Long.MIN_VALUE);
            assertEquals("Long.MIN_VALUE must clamp to 0 (disabled)",
                    0L, store.getCompactionMaxOutputNodes());

            // After a clamp, a subsequent positive set must still work
            // (defensive: pin that the clamp does not poison the field).
            store.setCompactionMaxOutputNodes(1_000_000L);
            assertEquals(1_000_000L, store.getCompactionMaxOutputNodes());
        }
    }
}
