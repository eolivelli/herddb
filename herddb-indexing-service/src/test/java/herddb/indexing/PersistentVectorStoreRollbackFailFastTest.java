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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.MemoryManager;
import herddb.file.FileDataStorageManager;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.log.LogSequenceNumber;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Validates fix B4 from the second pr-reviewer pass: rolling back a binary that
 * does not understand the on-disk metadata format MUST fail fast at boot rather
 * than silently emptying the index.
 */
public class PersistentVectorStoreRollbackFailFastTest {

    private static final String TABLE_SPACE = "tstblspace";
    private static final String INDEX_UUID = "rollback_test_idx";

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void unknownFutureVersionThrowsAtStart() throws Exception {
        Path baseDir = tmp.newFolder("data").toPath();
        Path tmpDir = tmp.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        dsm.initIndex(TABLE_SPACE, INDEX_UUID);

        // Forge an IndexStatus payload claiming version 99 — the loader must reject.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(99); // unknown future version
            dos.writeInt(32); // dimension
            dos.writeInt(16); // m
            dos.writeInt(100); // beamWidth
            dos.writeFloat(1.2f);
            dos.writeFloat(1.4f);
            dos.writeByte(1);
            dos.writeByte(1);
            dos.writeLong(0L); // nextNodeId
            dos.writeLong(0L); // generation
            dos.writeInt(0);   // numSegments
            dos.writeInt(0);   // pendingDeletes
        }
        IndexStatus status = new IndexStatus("rollback-idx",
                LogSequenceNumber.START_OF_TIME, 0L,
                Collections.emptySet(), baos.toByteArray());
        dsm.indexCheckpoint(TABLE_SPACE, INDEX_UUID, status, false);

        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "rollback-idx", "tbl", TABLE_SPACE,
                "v", INDEX_UUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0, Long.MAX_VALUE);
        try {
            store.start();
            fail("expected DataStorageManagerException for unsupported version");
        } catch (DataStorageManagerException ok) {
            assertTrue("error must mention version 99: " + ok.getMessage(),
                    ok.getMessage().contains("99"));
            assertTrue("error must mention rollback / re-deploy guidance",
                    ok.getMessage().contains("re-deploy") || ok.getMessage().contains("rollback")
                            || ok.getMessage().contains("re-create"));
        } finally {
            try {
                store.close();
            } catch (Exception ignored) {
            }
        }
    }
}
