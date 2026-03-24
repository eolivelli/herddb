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
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * Integration tests for concurrent DML during checkpoint with the BookKeeper
 * commit log backend (ZooKeeper + BookKeeper via ZKTestEnv).
 */
public class ConcurrentCheckpointBookKeeperTest extends ReplicatedLogtestcase {

    /**
     * BookKeeper backend: DML during checkpoint does not block and data is consistent.
     */
    @Test
    public void testCheckpointAllowsConcurrentDML_BookKeeper() throws Exception {
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

            AtomicInteger dmlCount = new AtomicInteger(0);
            AtomicReference<Throwable> dmlError = new AtomicReference<>();

            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    int v = dmlCount.incrementAndGet();
                    execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                            Arrays.asList("during" + v, 999));
                } catch (Throwable t) {
                    dmlError.set(t);
                }
            });

            manager.checkpoint();

            if (dmlError.get() != null) {
                throw new AssertionError("DML during checkpoint with BookKeeper failed", dmlError.get());
            }

            assertEquals(1, dmlCount.get());

            // Verify the row inserted during checkpoint is visible
            long count;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE n1=999", Collections.emptyList())) {
                count = s.consume().size();
            }
            assertEquals(1, count);
        }
    }

    /**
     * BookKeeper backend: concurrent DML during checkpoint with HASH index — index stays consistent.
     */
    @Test
    public void testConcurrentDMLWithHashIndex_BookKeeper() throws Exception {
        try (DBManager manager = startDBManager("node1")) {
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1",
                    Collections.singleton("node1"), "node1", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)", Collections.emptyList());
            execute(manager, "CREATE INDEX idx_n1 ON tblspace1.t1(n1)", Collections.emptyList());
            for (int i = 0; i < 30; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("pre" + i, 1));
            }
            manager.checkpoint();

            AtomicInteger inserts = new AtomicInteger(0);
            AtomicReference<Throwable> error = new AtomicReference<>();

            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    int v = inserts.incrementAndGet();
                    execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                            Arrays.asList("during" + v, 2));
                } catch (Throwable t) {
                    error.set(t);
                }
            });

            manager.checkpoint();

            if (error.get() != null) {
                throw new AssertionError("DML with HASH index during checkpoint (BK) failed", error.get());
            }

            // n1=2 rows: only those inserted during checkpoint
            long countN2;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE n1=2", Collections.emptyList())) {
                countN2 = s.consume().size();
            }
            assertEquals(inserts.get(), countN2);
        }
    }
}
