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

import static org.junit.Assert.*;

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
import java.util.concurrent.CopyOnWriteArrayList;
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
