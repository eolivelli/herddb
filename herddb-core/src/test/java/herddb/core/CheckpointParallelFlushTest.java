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
import static org.junit.Assert.fail;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Regression tests for issue #202's Phase-B parallel page flush. The frozen-pages loop
 * and {@code drainPendingNewPages} now fan their per-page writes out through the
 * DBManager checkpoint-flush executor; these tests prove it by:
 *
 * <ul>
 *   <li>wrapping the {@link MemoryDataStorageManager} so every {@code writePage} call
 *       bumps an in-flight counter and briefly sleeps — the MAX observed in-flight
 *       must exceed 1 when many pages exist, proving the calls actually overlap;</li>
 *   <li>injecting a failure on the N-th {@code writePage} and asserting the whole
 *       checkpoint fails with a recognisable cause, not a silent partial-success.</li>
 * </ul>
 */
public class CheckpointParallelFlushTest {

    @Test
    public void parallelFrozenPagesFlushRunsConcurrently() throws Exception {
        // Small page size so INSERTs create many frozen newPages at Phase A snapshot.
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 256);
        // Keep async worker pool threaded (default) so the checkpoint-flush executor
        // gets its configured parallelism.
        config.set(ServerConfiguration.PROPERTY_CHECKPOINT_FLUSH_THREADS, 8);
        config.set(ServerConfiguration.PROPERTY_CHECKPOINT_FLUSH_PARALLELISM, 8);

        InFlightTrackingStorage ds = new InFlightTrackingStorage();
        ds.pageWriteDelayMillis = 40L;
        String nodeId = "localhost";
        try (DBManager manager = new DBManager(nodeId,
                new MemoryMetadataStorageManager(), ds, new MemoryCommitLogManager(),
                null, null, config, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1",
                    Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, s1 string)",
                    Collections.emptyList());
            // 60 rows × ~60-byte values at 256-byte page size → many frozen newPages.
            for (int i = 0; i < 60; i++) {
                execute(manager,
                        "INSERT INTO tblspace1.t1(k1,s1) values(?,?)",
                        Arrays.asList("k" + i, "payload-" + i + "-padding-padding-padding"));
            }

            ds.resetCounters();
            manager.checkpoint();

            assertTrue("expected writePage to be invoked during checkpoint, got "
                            + ds.writePageCount.get(),
                    ds.writePageCount.get() > 0);
            assertTrue("expected MAX in-flight writePage > 1 (actual " + ds.maxInFlight.get()
                            + "), proving parallel Phase-B flush is engaged",
                    ds.maxInFlight.get() > 1);

            // Sanity: data survives round-trip.
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1",
                    Collections.emptyList())) {
                assertEquals(60, s.consume().size());
            }
        }
    }

    @Test
    public void failedPageWriteFailsCheckpointCleanly() throws Exception {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 256);
        config.set(ServerConfiguration.PROPERTY_CHECKPOINT_FLUSH_THREADS, 4);
        config.set(ServerConfiguration.PROPERTY_CHECKPOINT_FLUSH_PARALLELISM, 4);

        InFlightTrackingStorage ds = new InFlightTrackingStorage();
        ds.failOnWriteCount = 3; // third writePage call throws
        String nodeId = "localhost";
        try (DBManager manager = new DBManager(nodeId,
                new MemoryMetadataStorageManager(), ds, new MemoryCommitLogManager(),
                null, null, config, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1",
                    Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, s1 string)",
                    Collections.emptyList());
            for (int i = 0; i < 40; i++) {
                execute(manager,
                        "INSERT INTO tblspace1.t1(k1,s1) values(?,?)",
                        Arrays.asList("k" + i, "payload-" + i + "-padding-padding-padding"));
            }

            // Call TableManager.checkpoint directly so the failure propagates past the
            // per-table try/catch that TableSpaceManager.checkpoint uses to keep going
            // when a table drops mid-checkpoint.
            AbstractTableManager tm = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1");
            try {
                tm.checkpoint(false);
                fail("TableManager.checkpoint must fail when a writePage throws");
            } catch (Exception expected) {
                String message = expected.getMessage() == null ? "" : expected.getMessage();
                Throwable cause = expected.getCause();
                String causeMessage = cause == null || cause.getMessage() == null
                        ? "" : cause.getMessage();
                assertTrue("expected injected-failure cause; got: " + expected,
                        message.contains("injected") || causeMessage.contains("injected"));
            }

            // Record data is still durable via the commit log — the checkpoint marker
            // didn't advance, so a recovery would replay all 40 rows. We verify the
            // in-memory state is still consistent (no orphaned rows, no panics).
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1",
                    Collections.emptyList())) {
                assertEquals(40, s.consume().size());
            }
        }
    }

    /**
     * {@link MemoryDataStorageManager} wrapper that counts {@code writePage} invocations,
     * tracks peak concurrency, optionally sleeps to widen the overlap window, and
     * optionally fails on the N-th invocation.
     */
    private static final class InFlightTrackingStorage extends MemoryDataStorageManager {
        final AtomicInteger writePageCount = new AtomicInteger();
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxInFlight = new AtomicInteger();
        volatile long pageWriteDelayMillis = 0L;
        volatile int failOnWriteCount = -1;
        private final CountDownLatch dummy = new CountDownLatch(0);

        void resetCounters() {
            writePageCount.set(0);
            inFlight.set(0);
            maxInFlight.set(0);
        }

        @Override
        public void writePage(String tableSpace, String uuid, long pageId,
                Collection<Record> newPage) throws herddb.storage.DataStorageManagerException {
            int hits = writePageCount.incrementAndGet();
            int current = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(current, Math::max);
            try {
                if (failOnWriteCount > 0 && hits == failOnWriteCount) {
                    throw new RuntimeException("injected writePage failure (#" + hits + ")");
                }
                if (pageWriteDelayMillis > 0) {
                    try {
                        dummy.await(pageWriteDelayMillis, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                super.writePage(tableSpace, uuid, pageId, newPage);
            } finally {
                inFlight.decrementAndGet();
            }
        }
    }
}
