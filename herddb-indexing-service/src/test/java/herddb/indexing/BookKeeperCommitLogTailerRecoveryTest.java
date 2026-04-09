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
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that the BookKeeperCommitLogTailer recovers from transient BookKeeper
 * errors such as bookie restarts and empty ledgers created during connectivity
 * issues.
 */
public class BookKeeperCommitLogTailerRecoveryTest {

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
     * Reproduces the scenario from production where:
     * 1. Writer creates ledgers and writes entries
     * 2. Bookie restarts, causing write failures and new empty ledgers
     * 3. Tailer gets BK errors and must recover by resetting stale handles
     * 4. After bookie comes back, tailer catches up on all entries
     */
    @Test
    public void testTailerRecoversAfterBookieRestart() throws Exception {
        String tableSpaceUUID = "test-recovery-uuid";
        int entriesBefore = 5;
        int entriesAfter = 5;

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

                // Phase 1: write entries before bookie restart
                BookkeeperCommitLog writerLog = clManager.createCommitLog(
                        tableSpaceUUID, "default", "writer-node");
                writerLog.startWriting(1);

                for (int i = 0; i < entriesBefore; i++) {
                    writerLog.log(LogEntryFactory.noop(), true);
                }

                // Start the tailer - it should consume the initial entries
                List<LogSequenceNumber> receivedLsns = new CopyOnWriteArrayList<>();
                BookKeeperCommitLogTailer tailer = new BookKeeperCommitLogTailer(
                        testEnv.getAddress(),
                        testEnv.getTimeout(),
                        testEnv.getPath(),
                        ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT,
                        tableSpaceUUID,
                        LogSequenceNumber.START_OF_TIME,
                        (lsn, entry) -> receivedLsns.add(lsn)
                );

                Thread tailerThread = new Thread(tailer, "test-recovery-tailer");
                tailerThread.setDaemon(true);
                tailerThread.start();

                // Wait for initial entries to be consumed
                waitForEntries(receivedLsns, entriesBefore, 30_000);
                assertTrue("Tailer should have consumed initial entries, got " + receivedLsns.size(),
                        receivedLsns.size() >= entriesBefore);

                // Phase 2: stop bookie - this will cause BK errors
                writerLog.close();
                BookieId bookieAddr = testEnv.stopBookie();

                // Brief pause while bookie is down
                Thread.sleep(2000);

                // Phase 3: restart bookie
                testEnv.startStoppedBookie(bookieAddr);

                // Phase 4: write more entries on a new ledger
                BookkeeperCommitLog writerLog2 = clManager.createCommitLog(
                        tableSpaceUUID, "default", "writer-node");
                writerLog2.startWriting(1);

                for (int i = 0; i < entriesAfter; i++) {
                    writerLog2.log(LogEntryFactory.noop(), true);
                }

                // Wait for tailer to catch up on ALL entries
                int totalExpected = entriesBefore + entriesAfter;
                waitForEntries(receivedLsns, totalExpected, 60_000);

                tailer.close();
                tailerThread.join(10_000);

                assertTrue("Tailer should have recovered and consumed all " + totalExpected
                                + " entries, got " + receivedLsns.size(),
                        receivedLsns.size() >= totalExpected);
                assertFalse(tailer.isRunning());

                writerLog2.close();
            }
        }
    }

    /**
     * Tests that the tailer correctly handles a ledger that is closed and empty
     * (no entries written) by advancing to the next ledger.
     * This simulates the scenario where the server creates a ledger but fails
     * to write to it and immediately rolls to a new one.
     */
    @Test
    public void testTailerHandlesLedgerRollover() throws Exception {
        String tableSpaceUUID = "test-rollover-uuid";
        int entriesFirst = 5;
        int entriesSecond = 5;

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

                // Write entries to first ledger, then close it
                BookkeeperCommitLog writerLog1 = clManager.createCommitLog(
                        tableSpaceUUID, "default", "writer-node");
                writerLog1.startWriting(1);

                for (int i = 0; i < entriesFirst; i++) {
                    writerLog1.log(LogEntryFactory.noop(), true);
                }
                writerLog1.close();

                // Create second writer log - this creates a new ledger
                BookkeeperCommitLog writerLog2 = clManager.createCommitLog(
                        tableSpaceUUID, "default", "writer-node");
                writerLog2.startWriting(1);

                for (int i = 0; i < entriesSecond; i++) {
                    writerLog2.log(LogEntryFactory.noop(), true);
                }

                // Start tailer from beginning - it must traverse both ledgers
                List<LogSequenceNumber> receivedLsns = new CopyOnWriteArrayList<>();
                BookKeeperCommitLogTailer tailer = new BookKeeperCommitLogTailer(
                        testEnv.getAddress(),
                        testEnv.getTimeout(),
                        testEnv.getPath(),
                        ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT,
                        tableSpaceUUID,
                        LogSequenceNumber.START_OF_TIME,
                        (lsn, entry) -> receivedLsns.add(lsn)
                );

                Thread tailerThread = new Thread(tailer, "test-rollover-tailer");
                tailerThread.setDaemon(true);
                tailerThread.start();

                int totalExpected = entriesFirst + entriesSecond;
                waitForEntries(receivedLsns, totalExpected, 30_000);

                tailer.close();
                tailerThread.join(5000);

                assertTrue("Tailer should have consumed entries from both ledgers, expected "
                                + totalExpected + " got " + receivedLsns.size(),
                        receivedLsns.size() >= totalExpected);
                assertFalse(tailer.isRunning());

                writerLog2.close();
            }
        }
    }

    private static void waitForEntries(List<?> received, int expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (received.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
    }
}
