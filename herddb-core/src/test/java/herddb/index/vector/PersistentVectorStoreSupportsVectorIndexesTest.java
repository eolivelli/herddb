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
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link PersistentVectorStore#start()} refuses to run on a
 * {@link herddb.storage.DataStorageManager} that reports it does not support
 * vector indexes (e.g. the BookKeeper-backed one in cluster mode).
 */
public class PersistentVectorStoreSupportsVectorIndexesTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /** Memory DSM that pretends it cannot host vector indexes. */
    private static final class UnsupportedDsm extends MemoryDataStorageManager {
        @Override
        public boolean supportsVectorIndexes() {
            return false;
        }
    }

    @Test
    public void startFailsClearlyOnUnsupportedBackend() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        try (PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "ts", "vector_col",
                tmpDir, new UnsupportedDsm(), mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE)) {
            try {
                store.start();
                fail("start() should have failed on an unsupported DSM");
            } catch (DataStorageManagerException expected) {
                assertTrue("error must mention vector indexes and the DSM class",
                        expected.getMessage() != null
                                && expected.getMessage().contains("Vector indexes")
                                && expected.getMessage().contains("UnsupportedDsm"));
            }
        }
    }
}
