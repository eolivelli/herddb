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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.cluster.BookKeeperCommitLogTailer;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Table;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the BookKeeperCommitLogTailer by writing entries to a BK commit log
 * and verifying they are consumed by the tailer.
 */
public class BookKeeperCommitLogTailerTest {

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

    @Test
    public void testTailEntriesFromBookKeeper() throws Exception {
        String tableSpaceUUID = "test-ts-uuid";

        try (ZookeeperMetadataStorageManager metadataManager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            metadataManager.start();
            // Register the default tablespace so ledger metadata paths exist
            metadataManager.ensureDefaultTableSpace(
                    "writer-node", "writer-node", 0, 1);

            ServerConfiguration serverConfig = new ServerConfiguration();
            serverConfig.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH,
                    ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);

            try (BookkeeperCommitLogManager clManager = new BookkeeperCommitLogManager(
                    metadataManager, serverConfig, NullStatsLogger.INSTANCE)) {
                clManager.start();

                BookkeeperCommitLog writerLog = clManager.createCommitLog(
                        tableSpaceUUID, "default", "writer-node");
                writerLog.startWriting(1);

                // Write some entries
                Table table = Table.builder()
                        .name("mytable")
                        .tablespace("default")
                        .column("pk", ColumnTypes.STRING)
                        .column("data", ColumnTypes.STRING)
                        .primaryKey("pk")
                        .build();

                LogEntry createTable = LogEntryFactory.createTable(table, null);
                CommitLogResult r1 = writerLog.log(createTable, true);
                assertNotNull(r1.getLogSequenceNumber());

                int numEntries = 10;
                for (int i = 0; i < numEntries; i++) {
                    writerLog.log(LogEntryFactory.noop(), true);
                }

                // Collect entries via tailer
                List<LogSequenceNumber> receivedLsns =
                        new CopyOnWriteArrayList<>();

                BookKeeperCommitLogTailer tailer = new BookKeeperCommitLogTailer(
                        testEnv.getAddress(),
                        testEnv.getTimeout(),
                        testEnv.getPath(),
                        ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT,
                        tableSpaceUUID,
                        LogSequenceNumber.START_OF_TIME,
                        (lsn, entry) -> receivedLsns.add(lsn)
                );

                Thread tailerThread = new Thread(tailer, "test-bk-tailer");
                tailerThread.setDaemon(true);
                tailerThread.start();

                // Wait for entries to be consumed
                long deadline = System.currentTimeMillis() + 30_000;
                // We expect at least numEntries + 1 (createTable) + possibly a noop header
                while (receivedLsns.size() < numEntries + 1
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(200);
                }

                tailer.close();
                tailerThread.join(5000);

                assertTrue("Expected at least " + (numEntries + 1) + " entries, got "
                        + receivedLsns.size(), receivedLsns.size() >= numEntries + 1);
                assertFalse(tailer.isRunning());
                assertTrue(tailer.getEntriesProcessed() >= numEntries + 1);

                writerLog.close();
            }
        }
    }

    /**
     * Issue #459: {@code BookKeeperCommitLogTailer.getBatchesProcessed()}
     * must be {@code 0} on a freshly-constructed tailer, must advance once
     * the tailer drains the leader's first wave of entries, and must keep
     * advancing as additional waves arrive — confirming the
     * "{@code entriesProcessed > entriesBefore} → bump batchesProcessed"
     * delta in {@code BookKeeperCommitLogTailer.run()} is wired correctly.
     */
    @Test
    public void testBatchesProcessedAdvancesAcrossMultipleWaves() throws Exception {
        String tableSpaceUUID = "test-ts-uuid-batches";

        try (ZookeeperMetadataStorageManager metadataManager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            metadataManager.start();
            metadataManager.ensureDefaultTableSpace(
                    "writer-node", "writer-node", 0, 1);

            ServerConfiguration serverConfig = new ServerConfiguration();
            serverConfig.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH,
                    ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);

            try (BookkeeperCommitLogManager clManager = new BookkeeperCommitLogManager(
                    metadataManager, serverConfig, NullStatsLogger.INSTANCE)) {
                clManager.start();

                BookkeeperCommitLog writerLog = clManager.createCommitLog(
                        tableSpaceUUID, "default", "writer-node");
                writerLog.startWriting(1);

                Table table = Table.builder()
                        .name("mytable")
                        .tablespace("default")
                        .column("pk", ColumnTypes.STRING)
                        .column("data", ColumnTypes.STRING)
                        .primaryKey("pk")
                        .build();
                CommitLogResult r1 = writerLog.log(LogEntryFactory.createTable(table, null), true);
                assertNotNull(r1.getLogSequenceNumber());

                // First wave.
                int firstWave = 5;
                for (int i = 0; i < firstWave; i++) {
                    writerLog.log(LogEntryFactory.noop(), true);
                }

                List<LogSequenceNumber> received = new CopyOnWriteArrayList<>();
                BookKeeperCommitLogTailer tailer = new BookKeeperCommitLogTailer(
                        testEnv.getAddress(),
                        testEnv.getTimeout(),
                        testEnv.getPath(),
                        ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT,
                        tableSpaceUUID,
                        LogSequenceNumber.START_OF_TIME,
                        (lsn, entry) -> received.add(lsn)
                );
                Thread tailerThread = new Thread(tailer, "test-bk-tailer-batches");
                tailerThread.setDaemon(true);

                assertEquals("freshly-constructed tailer reports zero batches",
                        0L, tailer.getBatchesProcessed());

                tailerThread.start();
                try {
                    // Wait for the first wave (CREATE_TABLE + firstWave NOOPs)
                    // to drain.
                    long deadline = System.currentTimeMillis() + 30_000;
                    while (received.size() < firstWave + 1
                            && System.currentTimeMillis() < deadline) {
                        Thread.sleep(100);
                    }
                    assertTrue("first wave drained: received=" + received.size(),
                            received.size() >= firstWave + 1);
                    long batchesAfterFirst = tailer.getBatchesProcessed();
                    assertTrue("at least one batch must be counted after the "
                                    + "first wave drained, got " + batchesAfterFirst,
                            batchesAfterFirst >= 1L);

                    // Second wave: append more entries while the tailer is
                    // following.  followTheLeader() will return again after
                    // these land, so batchesProcessed must strictly advance.
                    int secondWave = 3;
                    for (int i = 0; i < secondWave; i++) {
                        writerLog.log(LogEntryFactory.noop(), true);
                    }
                    deadline = System.currentTimeMillis() + 30_000;
                    while (received.size() < firstWave + 1 + secondWave
                            && System.currentTimeMillis() < deadline) {
                        Thread.sleep(100);
                    }
                    assertTrue("second wave drained: received=" + received.size(),
                            received.size() >= firstWave + 1 + secondWave);

                    // Assert progress: the per-batch counter must move strictly
                    // forward (no off-by-one, no stuck-at-1).
                    long batchesAfterSecond = tailer.getBatchesProcessed();
                    assertTrue("batch counter must advance after the second wave: "
                                    + "before=" + batchesAfterFirst
                                    + " after=" + batchesAfterSecond,
                            batchesAfterSecond > batchesAfterFirst);

                    // Sanity: batches <= entries — every batch carries at
                    // least one entry, so this invariant must always hold.
                    assertTrue("batches (" + batchesAfterSecond + ") must be "
                                    + "<= entries (" + tailer.getEntriesProcessed() + ")",
                            batchesAfterSecond <= tailer.getEntriesProcessed());
                } finally {
                    tailer.close();
                    tailerThread.join(5_000);
                }
                writerLog.close();
            }
        }
    }

    @Test
    public void testSpeculativeReadsDisabledByDefault() throws Exception {
        // Issue #180: the tailer is a sequential follower; speculative reads
        // only add load. Verify the built-in default flips them off.
        BookKeeperCommitLogTailer tailer = new BookKeeperCommitLogTailer(
                testEnv.getAddress(),
                testEnv.getTimeout(),
                testEnv.getPath(),
                ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT,
                "test-ts-spec-defaults",
                LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> { /* no-op */ }
        );
        Thread t = new Thread(tailer, "test-bk-tailer-spec-defaults");
        t.setDaemon(true);
        t.start();
        try {
            ClientConfiguration cfg = waitForClientConfig(tailer);
            assertEquals("firstSpeculativeReadTimeout default must be 0",
                    0, cfg.getFirstSpeculativeReadTimeout());
            assertEquals("maxSpeculativeReadTimeout default must be 0",
                    0, cfg.getMaxSpeculativeReadTimeout());
        } finally {
            tailer.close();
            t.join(5_000);
        }
    }

    @Test
    public void testV2WireProtocolEnabledByDefault() throws Exception {
        // Issue #392: the indexing-service tailer must default to BK V2
        // (no protobuf framing on readEntries) — verifies the default flows
        // through the BookKeeperCommitLogTailer entry point, not just the
        // BookkeeperCommitLogManager constructor.
        BookKeeperCommitLogTailer tailer = new BookKeeperCommitLogTailer(
                testEnv.getAddress(),
                testEnv.getTimeout(),
                testEnv.getPath(),
                ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT,
                "test-ts-v2-defaults",
                LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> { /* no-op */ }
        );
        Thread t = new Thread(tailer, "test-bk-tailer-v2-defaults");
        t.setDaemon(true);
        t.start();
        try {
            ClientConfiguration cfg = waitForClientConfig(tailer);
            assertTrue("IS tailer must default to V2 wire protocol",
                    cfg.getUseV2WireProtocol());
        } finally {
            tailer.close();
            t.join(5_000);
        }
    }

    @Test
    public void testV2WireProtocolOperatorOverride() throws Exception {
        // Issue #392: an operator must still be able to fall back to V3 by
        // setting bookkeeper.useV2WireProtocol=false on the IS tailer's
        // bookkeeper-client properties.
        Properties bkProps = new Properties();
        bkProps.setProperty("bookkeeper.useV2WireProtocol", "false");

        BookKeeperCommitLogTailer tailer = new BookKeeperCommitLogTailer(
                testEnv.getAddress(),
                testEnv.getTimeout(),
                testEnv.getPath(),
                ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT,
                "test-ts-v2-override",
                LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> { /* no-op */ },
                bkProps
        );
        Thread t = new Thread(tailer, "test-bk-tailer-v2-override");
        t.setDaemon(true);
        t.start();
        try {
            ClientConfiguration cfg = waitForClientConfig(tailer);
            assertFalse("operator-supplied bookkeeper.useV2WireProtocol=false must win on IS tailer",
                    cfg.getUseV2WireProtocol());
        } finally {
            tailer.close();
            t.join(5_000);
        }
    }

    @Test
    public void testBookkeeperPropertiesArePassedThrough() throws Exception {
        // Issue #180: the bookkeeper.* prefix must reach the BK client. A
        // non-default value for firstSpeculativeReadTimeout confirms both
        // the passthrough (change #3) and that the operator override wins
        // over the built-in default (change #4).
        Properties bkProps = new Properties();
        bkProps.setProperty("bookkeeper.firstSpeculativeReadTimeout", "50");
        bkProps.setProperty("bookkeeper.readTimeout", "17");

        BookKeeperCommitLogTailer tailer = new BookKeeperCommitLogTailer(
                testEnv.getAddress(),
                testEnv.getTimeout(),
                testEnv.getPath(),
                ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT,
                "test-ts-bk-props",
                LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> { /* no-op */ },
                bkProps
        );
        Thread t = new Thread(tailer, "test-bk-tailer-props");
        t.setDaemon(true);
        t.start();
        try {
            ClientConfiguration cfg = waitForClientConfig(tailer);
            assertEquals("operator-supplied firstSpeculativeReadTimeout must win",
                    50, cfg.getFirstSpeculativeReadTimeout());
            assertEquals("arbitrary bookkeeper.* property must be visible",
                    "17", cfg.getProperty("readTimeout"));
        } finally {
            tailer.close();
            t.join(5_000);
        }
    }

    private static ClientConfiguration waitForClientConfig(
            BookKeeperCommitLogTailer tailer) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            ClientConfiguration cfg = tailer.getClientConfigurationForTest();
            if (cfg != null) {
                return cfg;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("tailer did not initialize BK client within 15s");
    }

    @Test
    public void testTailerWatermarkAdvances() throws Exception {
        String tableSpaceUUID = "test-ts-uuid-2";

        try (ZookeeperMetadataStorageManager metadataManager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            metadataManager.start();
            metadataManager.ensureDefaultTableSpace(
                    "writer-node", "writer-node", 0, 1);

            ServerConfiguration serverConfig = new ServerConfiguration();
            serverConfig.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH,
                    ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);

            try (BookkeeperCommitLogManager clManager = new BookkeeperCommitLogManager(
                    metadataManager, serverConfig, NullStatsLogger.INSTANCE)) {
                clManager.start();

                BookkeeperCommitLog writerLog = clManager.createCommitLog(
                        tableSpaceUUID, "default", "writer-node");
                writerLog.startWriting(1);

                for (int i = 0; i < 5; i++) {
                    writerLog.log(LogEntryFactory.noop(), true);
                }

                List<LogSequenceNumber> received = new CopyOnWriteArrayList<>();
                BookKeeperCommitLogTailer tailer = new BookKeeperCommitLogTailer(
                        testEnv.getAddress(),
                        testEnv.getTimeout(),
                        testEnv.getPath(),
                        ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT,
                        tableSpaceUUID,
                        LogSequenceNumber.START_OF_TIME,
                        (lsn, entry) -> received.add(lsn)
                );

                Thread tailerThread = new Thread(tailer, "test-bk-tailer-2");
                tailerThread.setDaemon(true);
                tailerThread.start();

                long deadline = System.currentTimeMillis() + 30_000;
                while (received.size() < 5 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(200);
                }

                LogSequenceNumber watermark = tailer.getWatermark();
                assertNotNull(watermark);
                assertFalse("Watermark should have advanced from START_OF_TIME",
                        watermark.isStartOfTime());

                tailer.close();
                tailerThread.join(5000);

                writerLog.close();
            }
        }
    }
}
