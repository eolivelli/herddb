/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.cluster.bookkeeper;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.core.ClusterTest;
import herddb.log.LogEntryFactory;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.model.TableSpace;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.util.UUID;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * Regression tests for the fix introduced in issue #385 that refreshes the
 * error state inside {@code closeCurrentWriter()}'s catch block.
 *
 * <p>Before the fix, {@code closeCurrentWriter(true)} captured
 * {@code hadWriteError} <em>before</em> calling
 * {@code waitForAllPendingWrites()}.  If the bookie was briefly unavailable,
 * in-flight writes set {@code errorOccurredDuringWrite=true} <em>during</em>
 * the wait, but the stale snapshot ({@code hadWriteError=false}) caused the
 * catch block to call {@code signalLogFailed()}, permanently failing the log.
 *
 * <p>After the fix the catch block re-reads
 * {@code writer.errorOccurredDuringWrite} so that transient write errors
 * discovered during the wait are treated identically to errors that occurred
 * before the rotation began — swallowed for non-fencing failures so that
 * {@code openNewLedger()} can create a fresh, healthy ledger.
 */
@Category(ClusterTest.class)
public class CloseCurrentWriterRotationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    /**
     * Exercises {@code closeCurrentWriter(true)} with in-flight writes to a
     * briefly-paused bookie.
     *
     * <p>The scenario:
     * <ol>
     *   <li>A very small {@code maxLedgerSizeBytes} is configured so that
     *       size-based rotation fires after just a handful of entries.</li>
     *   <li>The bookie is paused before writing begins.  With pre-ack
     *       {@code localLength} counting, every submission immediately
     *       increments {@code localLength} even though BK has not acknowledged
     *       the entry yet.  The rotation threshold is therefore crossed
     *       quickly, while a batch of writes is still in-flight (queued in the
     *       BK client's pending-add pipeline).</li>
     *   <li>The rotation attempt calls
     *       {@code closeCurrentWriter(waitForPendingAdds=true)}, which invokes
     *       {@code waitForAllPendingWrites()}.  Those in-flight writes either
     *       fail (if the bookie stays paused past BK's timeout) or succeed
     *       (if the bookie resumes in time).  Either outcome must leave the
     *       log in a non-failed state.</li>
     *   <li>The bookie is resumed and a final synchronous write is issued.
     *       The log must not be permanently failed, and the write must land on
     *       a new ledger.</li>
     * </ol>
     *
     * <p>Without the {@code closeCurrentWriter} catch-block re-read fix, the
     * test fails with {@code assertFalse(log.isFailed())} because
     * {@code signalLogFailed()} was called incorrectly when in-flight writes
     * set {@code errorOccurredDuringWrite=true} after the initial
     * {@code hadWriteError=false} snapshot.
     */
    @Test
    public void testSizeRotationWithInFlightWritesDoesNotFailLog() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        // A small entry (beginTransaction) is ~20 bytes.  1024-byte ledger cap
        // means ~50 entries trigger a roll — enough async submissions for
        // several to be in-flight during the rotation's waitForAllPendingWrites().
        final int maxLedgerSize = 1024;
        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man,
                        serverConfiguration, NullStatsLogger.INSTANCE)) {
            logManager.setMaxLedgerSizeBytes(maxLedgerSize);
            man.start();
            logManager.start();

            try (BookkeeperCommitLog log = logManager.createCommitLog(tableSpaceUUID, name, nodeid)) {
                log.setWriteLedgerHeader(false);
                log.startWriting(1);

                long initialLedgerId = log.getWriter().getLedgerId();

                // Pause bookie: all subsequent appendAsync calls queue up in
                // BK's client pipeline without being acknowledged.
                testEnv.pauseBookie();

                // Submit many async writes. localLength grows immediately with
                // each submission (pre-ack), so isWritable() flips to false
                // after ~50 entries and triggers rotation while earlier writes
                // are still pending acknowledgement.
                final int entries = 200;
                for (int i = 0; i < entries; i++) {
                    try {
                        log.log(LogEntryFactory.beginTransaction(i + 1), false);
                    } catch (LogNotAvailableException ignored) {
                        // A rotation attempt may itself fail-fast here if BK
                        // rejects the new-ledger creation; we tolerate that
                        // and let the final sync write below drive recovery.
                    }
                }

                // Resume bookie so queued writes drain and the next sync write
                // can succeed on a fresh ledger.
                testEnv.resumeBookie();

                // The final sync write must succeed after the transient outage.
                LogSequenceNumber lsn = log.log(LogEntryFactory.beginTransaction(9999), true)
                        .getLogSequenceNumber();
                assertNotNull("post-recovery sync write must return a valid LSN", lsn);

                // The log must NOT be permanently failed: the bookie was only
                // briefly paused and the catch-block re-read must have swallowed
                // the transient error instead of calling signalLogFailed().
                assertFalse("log must not be permanently failed after transient bookie pause",
                        log.isFailed());

                // Rotation must have happened: the post-recovery write is on a
                // different ledger from the one we started with.
                assertNotEquals("ledger must have rotated after size threshold was crossed",
                        initialLedgerId, log.getWriter().getLedgerId());
            }
        }
    }
}
