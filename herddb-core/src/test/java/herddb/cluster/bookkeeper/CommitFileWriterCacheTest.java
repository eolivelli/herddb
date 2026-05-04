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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.core.ClusterTest;
import herddb.log.LogEntryFactory;
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
 * Verifies the local-cache optimisation introduced in issue #385:
 * {@code CommitFileWriter} caches the closed state in a {@code volatile boolean
 * outClosed} and tracks requested bytes in an {@code AtomicLong localLength}
 * (incremented before each {@code appendAsync} call) so that {@code isWritable()}
 * never needs to acquire the intrinsic {@code synchronized} lock on the BookKeeper
 * {@link org.apache.bookkeeper.client.api.WriteHandle} instance.
 */
@Category(ClusterTest.class)
public class CommitFileWriterCacheTest {

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
     * After {@link BookkeeperCommitLog#close()} the {@code outClosed} flag on
     * the (now-unreferenced) writer must be {@code true}.  This proves that
     * {@code CommitFileWriter.close()} sets the local flag, so any thread still
     * holding a reference to the old writer (captured before the write-lock
     * acquisition) will observe {@code isWritable() == false} without entering
     * the BK-level synchronized block.
     */
    @Test
    public void testOutClosedSetOnClose() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man,
                        serverConfiguration, NullStatsLogger.INSTANCE)) {
            man.start();
            logManager.start();

            try (BookkeeperCommitLog log = logManager.createCommitLog(tableSpaceUUID, name, nodeid)) {
                log.startWriting(1);

                // write a couple of entries so the writer is fully initialised
                log.log(LogEntryFactory.beginTransaction(1), true).getLogSequenceNumber();
                log.log(LogEntryFactory.beginTransaction(2), true).getLogSequenceNumber();

                BookkeeperCommitLog.CommitFileWriter w = log.getWriter();
                assertFalse("writer should not be closed before log.close()", w.isOutClosed());
                assertTrue("localLength should be positive after writes", w.getLocalLength() > 0);

                // close the log — this calls CommitFileWriter.close() under the write lock
                log.close();

                // The writer reference we captured must now have outClosed == true
                assertTrue("outClosed must be true after CommitFileWriter.close()", w.isOutClosed());
            }
        }
    }

    /**
     * Verifies that the {@code localLength} counter drives the ledger-roll
     * threshold.  We set {@code maxLedgerSizeBytes} to a small value and keep
     * writing until the ledger ID changes, then assert that:
     * <ul>
     *   <li>the old writer's {@code localLength} crossed {@code maxLedgerSizeBytes}
     *       (proving it was the size check that triggered the roll, not some
     *       unrelated condition), and</li>
     *   <li>the new writer starts with a fresh, smaller {@code localLength}.</li>
     * </ul>
     */
    @Test
    public void testLocalLengthTriggersLedgerRoll() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man,
                        serverConfiguration, NullStatsLogger.INSTANCE)) {
            man.start();
            logManager.start();

            try (BookkeeperCommitLog log = logManager.createCommitLog(tableSpaceUUID, name, nodeid)) {
                // Set a small ledger size so that a few dozen entries trigger a roll
                final long maxSize = 512L;
                log.setMaxLedgerSizeBytes(maxSize);
                // Suppress the ledger-header NOOP so all localLength bytes come
                // from explicit application writes; easier to reason about counts.
                log.setWriteLedgerHeader(false);
                log.startWriting(1);

                long initialLedgerId = log.getWriter().getLedgerId();
                // Capture the initial writer reference before rotation so we can
                // inspect its final localLength after the roll has happened.
                BookkeeperCommitLog.CommitFileWriter initialWriter = log.getWriter();
                long rolledLedgerId = initialLedgerId;

                // Keep writing until the ledger rolls (new ledger ID)
                for (int i = 0; i < 10_000 && rolledLedgerId == initialLedgerId; i++) {
                    log.log(LogEntryFactory.beginTransaction(i + 1), true).getLogSequenceNumber();
                    BookkeeperCommitLog.CommitFileWriter currentWriter = log.getWriter();
                    if (currentWriter != null) {
                        rolledLedgerId = currentWriter.getLedgerId();
                    }
                }

                assertNotEquals("ledger should have rolled once localLength exceeded maxLedgerSizeBytes",
                        initialLedgerId, rolledLedgerId);

                // The initial writer's localLength must be >= maxSize: this is what
                // made isWritable() return false and triggered the roll.
                // localLength counts requested (pre-acknowledgement) bytes so it can
                // slightly over-count, but it must be at least maxSize.
                assertTrue("initial writer localLength must be >= maxLedgerSizeBytes at rotation,"
                        + " got: " + initialWriter.getLocalLength(),
                        initialWriter.getLocalLength() >= maxSize);

                // The current (new) writer starts accumulating from zero, so its
                // localLength is well below the old ledger's accumulated size.
                BookkeeperCommitLog.CommitFileWriter newWriter = log.getWriter();
                assertTrue("new writer localLength should be less than maxLedgerSizeBytes * 2,"
                        + " got: " + newWriter.getLocalLength(),
                        newWriter.getLocalLength() < maxSize * 2);

                // The new writer must be open (not closed)
                assertFalse("new writer must not be closed", newWriter.isOutClosed());
            }
        }
    }

    /**
     * Verifies that the {@code localLength} counter increments monotonically
     * and consistently for a known sequence of fixed-size entries, so that the
     * threshold comparison in {@code isWritable()} is accurate.
     */
    @Test
    public void testLocalLengthAccumulates() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man,
                        serverConfiguration, NullStatsLogger.INSTANCE)) {
            man.start();
            logManager.start();

            try (BookkeeperCommitLog log = logManager.createCommitLog(tableSpaceUUID, name, nodeid)) {
                // Use a very large ledger size so no roll happens during the test
                log.setMaxLedgerSizeBytes(Long.MAX_VALUE);
                log.setWriteLedgerHeader(false);
                log.startWriting(1);

                BookkeeperCommitLog.CommitFileWriter writer = log.getWriter();
                assertEquals("localLength must start at zero", 0L, writer.getLocalLength());

                final int writes = 5;
                long sizeAfterFirstWrite = 0;
                for (int i = 0; i < writes; i++) {
                    // Use a NOOP to get a deterministic, uniform entry size
                    log.log(LogEntryFactory.noop(), true).getLogSequenceNumber();
                    long len = writer.getLocalLength();
                    assertTrue("localLength must be positive after write " + i, len > 0);
                    if (i == 0) {
                        sizeAfterFirstWrite = len;
                    } else {
                        // Each entry must add the same number of bytes
                        assertEquals("localLength must grow by a fixed amount per NOOP entry",
                                sizeAfterFirstWrite * (i + 1), len);
                    }
                }
            }
        }
    }
}
