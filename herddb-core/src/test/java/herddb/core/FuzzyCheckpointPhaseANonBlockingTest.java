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
import static org.junit.Assert.assertTrue;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.storage.DataStorageManagerException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #403: verifies that {@code TableSpaceManager} Phase A's slow
 * remote-storage writes — {@code dataStorageManager.writeTables(...)} and
 * {@code dataStorageManager.writeTransactionsAtCheckpoint(...)} — run
 * <strong>outside</strong> the {@code generalLock} write window, so concurrent
 * INSERT commits proceed with bounded latency.
 *
 * <p>The test uses a {@link FileDataStorageManager} subclass whose
 * {@code writeTables(...)} sleeps for {@value #SLOW_PERSIST_MS} ms. With the
 * issue #403 fix in place, the tablespace generalLock has been released
 * before {@code writeTables} runs, so an INSERT thread completes well within
 * the slow-persist sleep duration.</p>
 */
public class FuzzyCheckpointPhaseANonBlockingTest {

    /** Sleep injected inside {@code writeTables} to model slow remote I/O. */
    private static final long SLOW_PERSIST_MS = 1500L;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void insertCompletesWhileWriteTablesIsSlow() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        String nodeId = "localhost";

        SlowWriteTablesStorage slowStorage = new SlowWriteTablesStorage(dataPath);

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                slowStorage,
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
            for (int i = 0; i < 50; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("pre" + i, i));
            }
            slowStorage.armSlowdown(false);
            manager.checkpoint();

            // Arm the slowdown for the next checkpoint.
            slowStorage.armSlowdown(true);

            AtomicReference<Throwable> ckptError = new AtomicReference<>();
            Thread ckptThread = new Thread(() -> {
                try {
                    manager.checkpoint();
                } catch (Throwable t) {
                    // Broad catch: this runs on a worker thread; any failure
                    // (checked or unchecked) must be surfaced to the main thread
                    // so the test reports it cleanly. CLAUDE.md compliance.
                    ckptError.set(t);
                }
            }, "fuzzy-ckpt");
            ckptThread.start();

            assertTrue("writeTables never entered",
                    slowStorage.writeTablesEntered.await(30, TimeUnit.SECONDS));

            // While writeTables is still sleeping inside Phase-A-persist, run an
            // INSERT — it must NOT be blocked by the tablespace generalLock.
            long t0 = System.nanoTime();
            execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                    Arrays.asList("post-during-writeTables", 999));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

            ckptThread.join(SLOW_PERSIST_MS * 4);
            if (ckptError.get() != null) {
                throw new AssertionError("checkpoint failed", ckptError.get());
            }
            assertTrue("checkpoint thread did not finish in time", !ckptThread.isAlive());

            assertTrue("INSERT during writeTables took too long: " + elapsedMs
                            + " ms (slow-persist sleep = " + SLOW_PERSIST_MS + " ms);"
                            + " issue #403 fix is missing or regressed",
                    elapsedMs < SLOW_PERSIST_MS / 2);

            assertTrue("writeTables did not actually sleep, the test fixture is broken",
                    slowStorage.writeTablesDurationMs.get() >= SLOW_PERSIST_MS);
        }
    }

    /**
     * {@link FileDataStorageManager} that sleeps inside
     * {@code writeTables(...)} when armed. Models the multi-second
     * shared-storage publishing latency observed in production.
     */
    private static final class SlowWriteTablesStorage extends FileDataStorageManager {

        private volatile boolean slow;
        final CountDownLatch writeTablesEntered = new CountDownLatch(1);
        final AtomicLong writeTablesDurationMs = new AtomicLong();

        SlowWriteTablesStorage(Path basePath) {
            super(basePath);
        }

        void armSlowdown(boolean slow) {
            this.slow = slow;
        }

        @Override
        public Collection<PostCheckpointAction> writeTables(
                String tableSpace, LogSequenceNumber sequenceNumber,
                List<Table> tables, List<Index> indexlist, boolean prepareActions
        ) throws DataStorageManagerException {
            if (slow) {
                writeTablesEntered.countDown();
                long t0 = System.nanoTime();
                try {
                    Thread.sleep(SLOW_PERSIST_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DataStorageManagerException(e);
                }
                writeTablesDurationMs.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0));
            }
            return super.writeTables(tableSpace, sequenceNumber, tables, indexlist, prepareActions);
        }

        @Override
        public Collection<PostCheckpointAction> writeTransactionsAtCheckpoint(
                String tableSpace, LogSequenceNumber sequenceNumber, Collection<Transaction> transactions
        ) throws DataStorageManagerException {
            return super.writeTransactionsAtCheckpoint(tableSpace, sequenceNumber, transactions);
        }
    }
}
