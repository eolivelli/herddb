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

import static herddb.core.TestUtils.beginTransaction;
import static herddb.core.TestUtils.commitTransaction;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Cluster-mode regression test for issue #145 — the same race as
 * {@link CheckpointStaleLsnRecoveryTest} but with
 * {@link herddb.cluster.BookkeeperCommitLogManager} (ZooKeeper + embedded BookKeeper from
 * {@link ReplicatedLogtestcase}).
 */
@Category(ClusterTest.class)
public class CheckpointStaleLsnRecoveryBookKeeperTest extends ReplicatedLogtestcase {

    /**
     * Transaction committed during Phase B must survive a restart against the BookKeeper
     * commit log without a duplicate-key recovery error.
     */
    @Test
    public void testRecoveryAfterCommitDuringPhaseB_BookKeeper() throws Exception {
        try (DBManager manager = startDBManager("node1")) {
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1",
                    Collections.singleton("node1"), "node1", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)", Collections.emptyList());
            for (int i = 0; i < 30; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("pre" + i, i));
            }
            manager.checkpoint();

            long tx = beginTransaction(manager, "tblspace1");
            for (int i = 0; i < 5; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("tx" + i, 100 + i),
                        new TransactionContext(tx));
            }

            AtomicReference<Throwable> hookError = new AtomicReference<>();
            AbstractTableManager t1Manager = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1");
            ((TableManager) t1Manager).setDuringPhaseBAction(() -> {
                try {
                    commitTransaction(manager, "tblspace1", tx);
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            manager.checkpoint();
            if (hookError.get() != null) {
                throw new AssertionError("commit-during-Phase-B hook failed", hookError.get());
            }
        }

        // Restart on the same node directory: ZK + BK state persists in testEnv, so the
        // BookkeeperCommitLog sees the same ledgers. Recovery must complete cleanly.
        try (DBManager manager = startDBManager("node1")) {
            manager.waitForTablespace("tblspace1", 10000);

            long total;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                total = s.consume().size();
            }
            assertEquals(35, total);

            long txRows;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE n1>=100",
                    Collections.emptyList())) {
                txRows = s.consume().size();
            }
            assertEquals(5, txRows);
        }
    }
}
