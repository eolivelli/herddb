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
package herddb.cluster;

import herddb.log.CommitLog;
import herddb.log.CommitLogTailing;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.server.ServerConfiguration;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.NullStatsLogger;

/**
 * Tails a BookKeeper-based commit log using the follower protocol.
 * Uses {@link BookkeeperCommitLog#followTheLeader} to consume entries.
 *
 * @author enrico.olivelli
 */
public class BookKeeperCommitLogTailer implements CommitLogTailing {

    private static final Logger LOGGER = Logger.getLogger(BookKeeperCommitLogTailer.class.getName());
    private static final int MAX_CONSECUTIVE_ERRORS = 10;

    private final String zkAddress;
    private final int zkSessionTimeout;
    private final String zkPath;
    private final String bkLedgersPath;
    private final String tableSpaceUUID;
    private final EntryConsumer consumer;

    private volatile LogSequenceNumber watermark;
    private volatile boolean running = true;
    private long entriesProcessed;
    private int consecutiveErrors;

    private ZookeeperMetadataStorageManager metadataManager;
    private BookkeeperCommitLogManager commitLogManager;
    private BookkeeperCommitLog commitLog;

    public BookKeeperCommitLogTailer(
            String zkAddress,
            int zkSessionTimeout,
            String zkPath,
            String bkLedgersPath,
            String tableSpaceUUID,
            LogSequenceNumber startFrom,
            EntryConsumer consumer
    ) {
        this.zkAddress = zkAddress;
        this.zkSessionTimeout = zkSessionTimeout;
        this.zkPath = zkPath;
        this.bkLedgersPath = bkLedgersPath;
        this.tableSpaceUUID = tableSpaceUUID;
        this.watermark = startFrom;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        LOGGER.log(Level.INFO, "BookKeeperCommitLogTailer starting, zk={0}, tableSpaceUUID={1}, watermark={2}",
                new Object[]{zkAddress, tableSpaceUUID, watermark});
        try {
            initBookKeeper();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize BookKeeper connection", e);
            running = false;
            return;
        }

        while (running) {
            try (CommitLog.FollowerContext context = commitLog.startFollowing(watermark)) {
                consecutiveErrors = 0;
                while (running) {
                    try {
                        commitLog.followTheLeader(watermark, (lsn, entry) -> {
                            if (!running) {
                                return false;
                            }
                            consumer.accept(lsn, entry);
                            watermark = lsn;
                            entriesProcessed++;
                            return running;
                        }, context);
                        consecutiveErrors = 0;
                    } catch (LogNotAvailableException e) {
                        if (!running) {
                            break;
                        }
                        consecutiveErrors++;
                        LOGGER.log(Level.WARNING,
                                "Error tailing BookKeeper log (consecutive errors: {0}), retrying",
                                consecutiveErrors);
                        LOGGER.log(Level.FINE, "Tailing error details", e);
                        if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                            LOGGER.log(Level.WARNING,
                                    "Too many consecutive errors ({0}), reinitializing BookKeeper connection",
                                    consecutiveErrors);
                            break;
                        }
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            running = false;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                if (running) {
                    LOGGER.log(Level.SEVERE, "BookKeeperCommitLogTailer error in context lifecycle", e);
                }
            }

            if (running) {
                closeBookKeeper();
                try {
                    Thread.sleep(2000);
                    initBookKeeper();
                    consecutiveErrors = 0;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    running = false;
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failed to reinitialize BookKeeper connection", e);
                    running = false;
                }
            }
        }

        closeBookKeeper();
        LOGGER.info("BookKeeperCommitLogTailer stopped");
    }

    private void initBookKeeper() throws Exception {
        metadataManager = new ZookeeperMetadataStorageManager(zkAddress, zkSessionTimeout, zkPath);
        metadataManager.start();

        ServerConfiguration serverConfig = new ServerConfiguration();
        serverConfig.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH, bkLedgersPath);

        commitLogManager = new BookkeeperCommitLogManager(metadataManager, serverConfig, NullStatsLogger.INSTANCE);
        commitLogManager.start();

        // Create a commit log instance for following (not writing)
        // The localNodeId is just for identification in logs
        commitLog = commitLogManager.createCommitLog(tableSpaceUUID, "indexing-tailer", "indexing-tailer");
    }

    private void closeBookKeeper() {
        if (commitLog != null) {
            try {
                commitLog.close();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error closing commit log", e);
            }
            commitLog = null;
        }
        if (commitLogManager != null) {
            commitLogManager.close();
            commitLogManager = null;
        }
        if (metadataManager != null) {
            try {
                metadataManager.close();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error closing metadata manager", e);
            }
            metadataManager = null;
        }
    }

    @Override
    public LogSequenceNumber getWatermark() {
        return watermark;
    }

    @Override
    public long getEntriesProcessed() {
        return entriesProcessed;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void close() {
        running = false;
    }
}
