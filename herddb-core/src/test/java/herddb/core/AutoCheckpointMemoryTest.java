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

package herddb.core;

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.model.TransactionContext.NO_TRANSACTION;
import static org.junit.Assert.assertTrue;
import herddb.core.indexes.MockRemoteVectorIndexService;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that a checkpoint works correctly with a remote vector index service.
 * In remote mode, vector index memory estimation returns 0 (no local state),
 * so memory-triggered checkpoints are not relevant for vector indexes.
 * This test verifies the basic checkpoint+vector index lifecycle doesn't fail.
 */
public class AutoCheckpointMemoryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testCheckpointWithRemoteVectorIndex() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        final int dimension = 8;
        final int numRows = 50;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.setRemoteVectorIndexService(new MockRemoteVectorIndexService());
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            for (int i = 1; i <= numRows; i++) {
                float[] vec = randomVec(dimension, i);
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, vec));
            }

            // Checkpoint should succeed with remote vector index
            manager.checkpoint();

            // Verify the index is still present after checkpoint
            assertTrue("vidx must be present after checkpoint",
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").containsKey("vidx"));
        }
    }

    private static float[] randomVec(int dim, int seed) {
        float[] v = new float[dim];
        float norm = 0;
        for (int i = 0; i < dim; i++) {
            v[i] = (float) Math.sin(seed * (i + 1));
            norm += v[i] * v[i];
        }
        norm = (float) Math.sqrt(norm);
        if (norm > 0) {
            for (int i = 0; i < dim; i++) {
                v[i] /= norm;
            }
        }
        return v;
    }
}
