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
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.storage.DataStorageManagerException;
import herddb.storage.TableStatus;
import java.nio.file.Path;
import java.util.Arrays;
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
 * Issue #403: verifies that the slow remote-storage I/O of {@code TableManager}
 * Phase C — {@code dataStorageManager.tableCheckpoint(...)} — runs
 * <strong>outside</strong> the {@code checkpointLock} write window, so
 * concurrent {@code INSERT} commits complete with bounded latency even when
 * the persistence I/O is artificially slow.
 *
 * <p>The test uses a {@link FileDataStorageManager} subclass whose
 * {@code tableCheckpoint(...)} sleeps for {@value #SLOW_PERSIST_MS} ms. With
 * the issue #403 fix in place, the checkpoint write lock has already been
 * released before {@code tableCheckpoint} runs, so a concurrent INSERT thread
 * completes within tens of ms — well under the slow-persist sleep duration.</p>
 */
public class FuzzyCheckpointPhaseCNonBlockingTest {

    /** Sleep injected inside {@code tableCheckpoint} to model slow remote I/O. */
    private static final long SLOW_PERSIST_MS = 1500L;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void insertCompletesWhileTableCheckpointIsSlow() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        String nodeId = "localhost";

        SlowTableCheckpointStorage slowStorage = new SlowTableCheckpointStorage(dataPath);

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
            // First checkpoint runs without injected slowness.
            slowStorage.armSlowdown(false);
            manager.checkpoint();

            // Arm the slowdown for the next checkpoint.
            slowStorage.armSlowdown(true);

            // Drive a checkpoint on a worker thread; tableCheckpoint will sleep.
            AtomicReference<Throwable> ckptError = new AtomicReference<>();
            Thread ckptThread = new Thread(() -> {
                try {
                    manager.checkpoint();
                } catch (Throwable t) {
                    ckptError.set(t);
                }
            }, "fuzzy-ckpt");
            ckptThread.start();

            // Wait until the slow tableCheckpoint phase has been entered — by
            // this point the issue #403 refactor must have already released the
            // checkpoint write lock.
            assertTrue("tableCheckpoint never entered",
                    slowStorage.tableCheckpointEntered.await(30, TimeUnit.SECONDS));

            // While the slow persist is still running, run an INSERT and assert
            // it completes much faster than the sleep duration.
            long t0 = System.nanoTime();
            execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                    Arrays.asList("post-during-persist", 999));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

            ckptThread.join(SLOW_PERSIST_MS * 4);
            if (ckptError.get() != null) {
                throw new AssertionError("checkpoint failed", ckptError.get());
            }
            assertTrue("checkpoint thread did not finish in time", !ckptThread.isAlive());

            assertTrue("INSERT during tableCheckpoint took too long: " + elapsedMs
                            + " ms (slow-persist sleep = " + SLOW_PERSIST_MS + " ms);"
                            + " issue #403 fix is missing or regressed",
                    elapsedMs < SLOW_PERSIST_MS / 2);

            // Final sanity check: the slowdown actually fired.
            assertTrue("tableCheckpoint did not actually sleep, the test fixture is broken",
                    slowStorage.tableCheckpointDurationMs.get() >= SLOW_PERSIST_MS);
        }
    }

    /**
     * {@link FileDataStorageManager} that sleeps inside
     * {@code tableCheckpoint(...)} when armed. Models the multi-second remote
     * I/O latency observed in production against GCS / file-server backends.
     */
    private static final class SlowTableCheckpointStorage extends FileDataStorageManager {

        private volatile boolean slow;
        final CountDownLatch tableCheckpointEntered = new CountDownLatch(1);
        final AtomicLong tableCheckpointDurationMs = new AtomicLong();

        SlowTableCheckpointStorage(Path basePath) {
            super(basePath);
        }

        void armSlowdown(boolean slow) {
            this.slow = slow;
        }

        @Override
        public List<PostCheckpointAction> tableCheckpoint(String tableSpace, String uuid,
                TableStatus tableStatus, boolean pin) throws DataStorageManagerException {
            if (slow) {
                tableCheckpointEntered.countDown();
                long t0 = System.nanoTime();
                try {
                    Thread.sleep(SLOW_PERSIST_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DataStorageManagerException(e);
                }
                tableCheckpointDurationMs.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0));
            }
            return super.tableCheckpoint(tableSpace, uuid, tableStatus, pin);
        }
    }
}
