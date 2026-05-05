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
import static org.junit.Assert.assertTrue;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #403 (PR review BLOCK follow-up): verifies that concurrent
 * {@code TableManager.checkpoint(...)} invocations on the same table are
 * serialized by the per-table {@code checkpointSerializerLock}, preventing
 * the race where two checkpoints overlap their out-of-lock Phase C-persist
 * blocks (and Phase B) and corrupt the persisted manifest /
 * {@code TableStatus} blob.
 *
 * <p>The race vector is the activator-thread-driven
 * {@code TableSpaceManager.runLocalTableCheckPoints()} (no
 * {@code checkpointMutex}) racing with an admin-driven
 * {@code manager.checkpoint()}. With the issue #403 fuzzy refactor, the
 * BLink {@code persistCheckpoint(...)} mutates {@code currentManifest},
 * {@code previousByNodeId} and {@code lastPreserveSet} outside the
 * {@code checkpointLock}; without serialization the mutations would tear.</p>
 *
 * <p>This test starts two threads that simultaneously drive
 * {@code tableManager.checkpoint(false)} on the SAME table, then verifies
 * the persisted state is consistent and a clean restart loads every row.</p>
 */
public class FuzzyCheckpointConcurrentInvocationSerializedTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void concurrentTableCheckpointsDoNotCorruptManifest() throws Exception {
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
            for (int i = 0; i < 100; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }

            AbstractTableManager t1Manager = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1");
            assertTrue(t1Manager instanceof TableManager);
            TableManager tm = (TableManager) t1Manager;

            // Drive ROUNDS rounds; each round starts two threads that BOTH call
            // tableManager.checkpoint(false) on the same TableManager, racing
            // their entire bodies. Without the per-table serializer lock the
            // race corrupts internal state; with it, the second invocation
            // blocks on the lock and runs after the first.
            final int rounds = 5;
            final AtomicInteger startedAtomic = new AtomicInteger();
            for (int round = 0; round < rounds; round++) {
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch go = new CountDownLatch(1);
                AtomicReference<Throwable> err = new AtomicReference<>();
                Runnable body = () -> {
                    try {
                        ready.countDown();
                        go.await();
                        startedAtomic.incrementAndGet();
                        tm.checkpoint(false);
                    } catch (Throwable t) {
                        // Broad catch: worker thread; surface failure to main.
                        err.compareAndSet(null, t);
                    }
                };
                Thread a = new Thread(body, "concurrent-ckpt-a-" + round);
                Thread b = new Thread(body, "concurrent-ckpt-b-" + round);
                a.start();
                b.start();
                assertTrue(ready.await(10, TimeUnit.SECONDS));
                go.countDown();
                a.join(60_000);
                b.join(60_000);
                assertTrue("checkpoint thread A still alive", !a.isAlive());
                assertTrue("checkpoint thread B still alive", !b.isAlive());
                if (err.get() != null) {
                    throw new AssertionError("concurrent checkpoint round " + round + " failed",
                            err.get());
                }

                // Insert more rows between rounds so the next pair has dirty
                // pages to flush.
                for (int i = 0; i < 20; i++) {
                    execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                            Arrays.asList("r" + round + "-" + i, 1000 * (round + 1) + i));
                }
            }
            assertEquals(2 * rounds, startedAtomic.get());

            try (DataScanner s = scan(manager,
                    "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                assertEquals(100L + 20L * rounds, s.consume().size());
            }
        }

        // Reopen — recovery loads the last persisted manifest plus the WAL
        // tail. A torn manifest would surface here as an
        // IllegalStateException ("no record in memory for K") or a
        // duplicate-key failure during replay.
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            try (DataScanner s = scan(manager,
                    "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                assertEquals("expected 200 rows after restart following concurrent checkpoints",
                        200L, s.consume().size());
            }

            // A fresh checkpoint after recovery must succeed.
            manager.checkpoint();
        }
    }
}
