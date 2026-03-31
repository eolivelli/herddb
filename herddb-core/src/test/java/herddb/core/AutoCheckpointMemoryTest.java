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

import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.vector.VectorIndexManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that a checkpoint is automatically triggered when a tablespace's
 * estimated memory usage exceeds the configured limit.
 */
public class AutoCheckpointMemoryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testMemoryTriggeredCheckpoint() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        final int dimension = 8;
        final int numRows = 500;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            // Use a long time-based period so only the memory trigger fires
            manager.setCheckpointPeriod(600_000);
            // Set a low memory limit so the memory trigger fires after a few hundred vectors
            // Each vector: 8 floats * 4 bytes * 5.0 multiplier = 160 bytes estimated
            // 500 vectors ≈ 80 KB estimated vector memory (plus table data)
            // Set limit low enough that it will be exceeded
            manager.setCheckpointMemoryLimit(10_000);
            manager.setVectorMemoryMultiplier(5.0);

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Insert enough vector data to exceed the memory limit
            for (int i = 1; i <= numRows; i++) {
                float[] vec = randomVec(dimension, i);
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, vec));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            long lastCheckpoint = manager.getLastCheckPointTs();

            // Wait for the activator to detect memory pressure and trigger checkpoint
            // The activator polls every 1 second
            boolean checkpointTriggered = false;
            for (int i = 0; i < 30; i++) {
                Thread.sleep(1000);
                if (manager.getLastCheckPointTs() != lastCheckpoint) {
                    checkpointTriggered = true;
                    break;
                }
            }

            assertTrue("A memory-triggered checkpoint should have occurred", checkpointTriggered);
            // After checkpoint, live vectors should have been flushed to disk
            assertTrue("Live node count should be less than total after checkpoint",
                    vim.getLiveNodeCount() < numRows);
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
