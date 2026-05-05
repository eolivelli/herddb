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
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #403: end-to-end validation that the refactored
 * {@code TableManager} Phase C — which splits the per-table checkpoint into
 * an under-lock snapshot and an out-of-lock persistence — is functionally
 * equivalent to the legacy single-phase checkpoint across a restart.
 *
 * <p>Three distinct checkpoint cycles are exercised, each followed by an
 * INSERT batch that lands in pages flushed by the next checkpoint. After
 * the manager is closed and reopened, every row inserted across all cycles
 * must be visible — this tests both the under-lock LSN snapshotting (issue
 * #157) and the post-Phase-C ordering of {@code keyToPage.persistCheckpoint}
 * vs. {@code dataStorageManager.tableCheckpoint}.</p>
 */
public class FuzzyCheckpointRecoveryMidPhaseCTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void recoverAcrossMultipleFuzzyCheckpoints() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        String nodeId = "localhost";

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1",
                    Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)",
                    Collections.emptyList());

            // Cycle 1: 30 inserts, then checkpoint.
            for (int i = 0; i < 30; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("c1-" + i, i));
            }
            manager.checkpoint();

            // Cycle 2: 30 more inserts, then checkpoint.
            for (int i = 0; i < 30; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("c2-" + i, 100 + i));
            }
            manager.checkpoint();

            // Cycle 3: 30 more inserts, then checkpoint.
            for (int i = 0; i < 30; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("c3-" + i, 200 + i));
            }
            manager.checkpoint();

            // Tail: 15 more inserts WITHOUT a final checkpoint — these must be
            // recovered from the WAL on restart.
            for (int i = 0; i < 15; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("c4-" + i, 300 + i));
            }
        }

        // Restart: every row across every cycle must be visible.
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            long total;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                total = s.consume().size();
            }
            assertEquals("expected all 105 rows visible after restart",
                    105L, total);

            // Spot-check each cycle's tail row.
            assertScalarMatch(manager, "SELECT n1 FROM tblspace1.t1 WHERE k1='c1-29'", 29);
            assertScalarMatch(manager, "SELECT n1 FROM tblspace1.t1 WHERE k1='c2-29'", 129);
            assertScalarMatch(manager, "SELECT n1 FROM tblspace1.t1 WHERE k1='c3-29'", 229);
            assertScalarMatch(manager, "SELECT n1 FROM tblspace1.t1 WHERE k1='c4-14'", 314);

            // A fresh checkpoint after recovery must succeed (validates that
            // the post-recovery in-memory state is consistent).
            manager.checkpoint();
        }
    }

    private static void assertScalarMatch(DBManager manager, String query, int expected)
            throws Exception {
        try (DataScanner s = scan(manager, query, Collections.emptyList())) {
            java.util.List<herddb.utils.DataAccessor> rows = s.consume();
            assertEquals("query " + query + " row count", 1, rows.size());
            Object got = rows.get(0).get("n1");
            assertEquals("query " + query, (Integer) expected, got);
        }
    }
}
