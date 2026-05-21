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
 * Setter/getter wiring tests for the per-segment graduation cap (issue #631).
 *
 * <p>The cap field {@code vectorIndexCompactionTargetBytes} is what
 * {@link PersistentVectorStore#runCompactionCycle} passes to
 * {@link VectorIndexCompactor#chooseSegmentsToMerge}. The policy-level
 * behaviour is covered by {@link VectorIndexCompactorChooseTest}; this test
 * pins the public API surface — default, positive set, disable via {@code 0} /
 * negative — so the wiring through {@link herddb.indexing.IndexingServiceEngine}
 * (which reads the config key and calls {@link
 * PersistentVectorStore#setCompactionTargetBytes(long)}) stays correct.
 */
public class PersistentVectorStoreCompactionTargetBytesTest {

    private static final String TABLE_SPACE = "tstblspace";
    private static final String INDEX_NAME = "tgt";
    private static final String INDEX_UUID = "tgt_idx_uuid";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir, FileDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(INDEX_NAME, "testtable", TABLE_SPACE,
                "vector_col", INDEX_UUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0, Long.MAX_VALUE);
    }

    @Test
    public void defaultIsEightGibibytes() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            assertEquals("default must match the optimizer-pod cap (8 GiB)",
                    8L * 1024L * 1024L * 1024L, store.getCompactionTargetBytes());
        }
    }

    @Test
    public void positiveValuePassesThrough() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setCompactionTargetBytes(123_456_789L);
            assertEquals(123_456_789L, store.getCompactionTargetBytes());

            // Also re-tune to a larger value, simulating a live operator change.
            store.setCompactionTargetBytes(64L * 1024L * 1024L * 1024L);
            assertEquals(64L * 1024L * 1024L * 1024L,
                    store.getCompactionTargetBytes());
        }
    }

    @Test
    public void zeroOrNegativeClampsToMaxValueDisablingTheCap() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setCompactionTargetBytes(0L);
            assertEquals("0 must clamp to Long.MAX_VALUE (disabled)",
                    Long.MAX_VALUE, store.getCompactionTargetBytes());

            store.setCompactionTargetBytes(-1L);
            assertEquals("negative must clamp to Long.MAX_VALUE (disabled)",
                    Long.MAX_VALUE, store.getCompactionTargetBytes());

            // And an explicit Long.MAX_VALUE is preserved (already disabled).
            store.setCompactionTargetBytes(Long.MAX_VALUE);
            assertEquals(Long.MAX_VALUE, store.getCompactionTargetBytes());
        }
    }
}
