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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.cluster.BookKeeperCommitLogTailer;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.LedgersInfo;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.log.CommitLogResult;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #331: when a ledger is permanently dropped from the
 * active-ledger list (i.e. all remaining active ledgers have higher IDs), the
 * {@link BookKeeperCommitLogTailer} must stop rather than retrying forever.
 *
 * <p>Scenario reproduced here:
 * <ol>
 *   <li>Write entries to "writer A" (potentially spanning several ledgers in the
 *       BK local environment) and capture the last position as {@code lastLsnA}.
 *   <li>Write entries to "writer B" (creates ledgers with higher IDs).
 *   <li>Simulate {@code dropOldLedgers}: remove from ZooKeeper every active ledger
 *       whose ID is ≤ {@code lastLsnA.ledgerId}, so only newer ledgers remain.
 *   <li>Start a tailer with a watermark equal to {@code lastLsnA}.
 *   <li>Assert that the tailer detects the permanent gap and stops within a few
 *       seconds rather than looping forever.
 * </ol>
 */
public class BookKeeperCommitLogTailerPermanentGapTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void setUp() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder("zk").toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void tearDown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    /**
     * Verifies that the tailer stops (sets {@code isRunning() == false}) when it
     * discovers that the ledger it needs has been permanently removed from the
     * active-ledger list and all remaining ledgers have higher IDs.
     *
     * <p>Before the fix, the tailer would loop forever retrying the missing ledger.
     */
    @Test
    public void testTailerStopsOnPermanentLedgerGap() throws Exception {
        String tableSpaceUUID = "test-permanent-gap-uuid";

        try (ZookeeperMetadataStorageManager metadataManager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            metadataManager.start();
            metadataManager.ensureDefaultTableSpace("writer-node", "writer-node", 0, 1);

            ServerConfiguration serverConfig = new ServerConfiguration();
            serverConfig.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH,
                    ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);

            try (BookkeeperCommitLogManager clManager = new BookkeeperCommitLogManager(
                    metadataManager, serverConfig, NullStatsLogger.INSTANCE)) {
                clManager.start();

                // --- Step 1: write entries with writer A ---
                // In the embedded BK environment each sync write may roll to a new
                // ledger; that is fine.  We capture the LSN of the last write to
                // determine which ledger the tailer will try to read from.
                LogSequenceNumber lastLsnA;
                try (BookkeeperCommitLog writerLogA = clManager.createCommitLog(
                        tableSpaceUUID, "default", "writer-node")) {
                    writerLogA.startWriting(1);
                    writerLogA.log(LogEntryFactory.noop(), true);
                    writerLogA.log(LogEntryFactory.noop(), true);
                    CommitLogResult lastResultA = writerLogA.log(LogEntryFactory.noop(), true);
                    lastLsnA = lastResultA.getLogSequenceNumber();
                }

                long targetLedger = lastLsnA.ledgerId;

                // --- Step 2: write entries with writer B (higher ledger IDs) ---
                // Failures in close() are expected in the embedded BK environment when
                // two sequential writers use the same tablespace UUID — the close of
                // writer A may cause writer B's in-flight write to land on a
                // BKLedgerClosedException. We swallow those to keep the test focused on
                // the permanent-gap detection behaviour.
                try (BookkeeperCommitLog writerLogB = clManager.createCommitLog(
                        tableSpaceUUID, "default", "writer-node")) {
                    writerLogB.startWriting(1);
                    writerLogB.log(LogEntryFactory.noop(), false);
                    writerLogB.log(LogEntryFactory.noop(), false);
                    writerLogB.log(LogEntryFactory.noop(), false);
                } catch (Exception ignored) {
                    // Swallow write errors from the embedded test environment — we only
                    // care that higher ledger IDs were registered in ZooKeeper.
                }

                // --- Step 3: simulate dropOldLedgers ---
                // Remove ALL active ledgers with ID ≤ targetLedger from ZK so that
                // only ledgers with higher IDs remain.  This exactly mirrors what
                // dropOldLedgers does when IS-0 is temporarily offline: it drops
                // everything up to and including IS-0's last seen ledger.
                LedgersInfo ledgersInfo = metadataManager.getActualLedgersList(tableSpaceUUID);
                List<Long> activeBefore = ledgersInfo.getActiveLedgers();
                List<Long> toRemove = new ArrayList<>();
                for (long id : activeBefore) {
                    if (id <= targetLedger) {
                        toRemove.add(id);
                    }
                }
                for (long id : toRemove) {
                    ledgersInfo.removeLedger(id);
                }
                metadataManager.saveActualLedgersList(tableSpaceUUID, ledgersInfo);

                // Sanity: after the simulated drop, at least one ledger with a higher
                // ID must remain — otherwise ensureOpenReader would see an empty list and
                // return silently (no exception thrown, tailer keeps spinning).
                LedgersInfo afterDrop = metadataManager.getActualLedgersList(tableSpaceUUID);
                List<Long> remaining = afterDrop.getActiveLedgers();
                boolean hasNewerLedger = false;
                for (long id : remaining) {
                    if (id > targetLedger) {
                        hasNewerLedger = true;
                        break;
                    }
                }
                assertTrue(
                        "Test precondition: at least one active ledger with ID > " + targetLedger
                        + " must exist after the simulated drop; actual: " + remaining,
                        hasNewerLedger);

                // --- Step 4: start a tailer pointing into the dropped ledger ---
                // This reproduces the IS-0 restart scenario: the tailer resumes
                // from the last processed position, which is inside a ledger that
                // has now been dropped.
                BookKeeperCommitLogTailer tailer = new BookKeeperCommitLogTailer(
                        testEnv.getAddress(),
                        testEnv.getTimeout(),
                        testEnv.getPath(),
                        ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT,
                        tableSpaceUUID,
                        lastLsnA,
                        (lsn, entry) -> {
                            // No entries expected — the required ledger is gone.
                        }
                );

                Thread tailerThread = new Thread(tailer, "test-permanent-gap-tailer");
                tailerThread.setDaemon(true);
                tailerThread.start();

                // --- Step 5: assert the tailer stops quickly ---
                // The fix makes this happen on the first followTheLeader call:
                // ensureOpenReader checks the ZK list, finds targetLedger absent
                // and all remaining ledgers newer → throws LogNotAvailableException
                // with permanent=true → run() sets running=false and exits.
                // Allow 10 s to be generous with slow CI environments.
                long deadline = System.currentTimeMillis() + 10_000;
                while (tailer.isRunning() && System.currentTimeMillis() < deadline) {
                    Thread.sleep(200);
                }

                assertFalse(
                        "Tailer must stop when the required ledger is permanently dropped "
                        + "(all remaining active ledgers have higher IDs). "
                        + "watermark=" + lastLsnA + " targetLedger=" + targetLedger
                        + " remainingActive=" + remaining,
                        tailer.isRunning());

                tailer.close();
                tailerThread.join(5_000);
            }
        }
    }
}
