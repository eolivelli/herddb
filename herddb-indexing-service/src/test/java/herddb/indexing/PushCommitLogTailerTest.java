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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.log.CommitLogTailing;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for {@link PushCommitLogTailer}: in-order dispatch, the bounded
 * buffer's blocking back-pressure, idempotent skipping of stale or
 * out-of-order (re-pushed) entries, and lifecycle including the drop of
 * still-buffered entries on {@code close()}.
 */
public class PushCommitLogTailerTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(30);

    /** A throwaway entry — the tailer never interprets the payload. */
    private static LogEntry anEntry() {
        return LogEntryFactory.noop();
    }

    private static void awaitSize(List<?> list, int target, long timeoutMs) throws InterruptedException {
        awaitCondition(() -> list.size() >= target, timeoutMs);
    }

    private static void awaitCondition(BooleanSupplier condition, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }

    @Test
    public void entriesAreDispatchedInPushOrder() throws Exception {
        List<LogSequenceNumber> consumed = Collections.synchronizedList(new ArrayList<>());
        PushCommitLogTailer tailer = new PushCommitLogTailer(128, LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> consumed.add(lsn));
        Thread t = new Thread(tailer, "test-push-tailer");
        t.start();
        try {
            for (int i = 1; i <= 50; i++) {
                tailer.push(new LogSequenceNumber(1, i), anEntry());
            }
            awaitSize(consumed, 50, 5000);
        } finally {
            tailer.close();
            t.join(5000);
        }
        assertEquals(50, consumed.size());
        for (int i = 0; i < 50; i++) {
            assertEquals(1, consumed.get(i).ledgerId);
            assertEquals(i + 1, consumed.get(i).offset);
        }
        assertEquals(50L, tailer.getEntriesProcessed());
        assertEquals(new LogSequenceNumber(1, 50), tailer.getWatermark());
    }

    @Test
    public void pushBlocksWhenBufferIsFullAndResumesWhenDrained() throws Exception {
        // Capacity 2: with the consumer gated, the tailer thread parks holding
        // one entry, two more fill the buffer, and the next push must block.
        CountDownLatch release = new CountDownLatch(1);
        List<LogSequenceNumber> consumed = Collections.synchronizedList(new ArrayList<>());
        PushCommitLogTailer tailer = new PushCommitLogTailer(2, LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> {
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    consumed.add(lsn);
                });
        Thread tailerThread = new Thread(tailer, "test-push-tailer");
        tailerThread.start();

        final int total = 10;
        AtomicReference<Throwable> pushFailure = new AtomicReference<>();
        Thread pusher = new Thread(() -> {
            try {
                for (int i = 1; i <= total; i++) {
                    tailer.push(new LogSequenceNumber(1, i), anEntry());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                pushFailure.set(e);
            } catch (RuntimeException e) {
                pushFailure.set(e);
            }
        }, "test-pusher");
        pusher.start();
        try {
            // Wait until the buffer is full and the pusher is parked on it.
            awaitCondition(() -> tailer.getBufferedCount() == tailer.getBufferCapacity(), 5000);
            assertEquals("buffer must be full", tailer.getBufferCapacity(),
                    tailer.getBufferedCount());
            assertTrue("pusher must still be blocked on a full buffer", pusher.isAlive());
            assertTrue("no entry can be dispatched while the consumer is gated",
                    consumed.isEmpty());

            // Release the consumer: the buffer drains and the pusher completes.
            release.countDown();
            pusher.join(5000);
            assertFalse("pusher must finish once the buffer drains", pusher.isAlive());
            awaitSize(consumed, total, 5000);
            assertEquals(total, consumed.size());
        } finally {
            release.countDown();
            tailer.close();
            tailerThread.join(5000);
            pusher.join(5000);
        }
        assertNull("push must not fail", pushFailure.get());
    }

    @Test
    public void entriesAtOrBeforeWatermarkAreSkipped() throws Exception {
        // Restart semantics: a tailer resuming at a durable watermark must
        // drop re-pushed entries that have already been applied.
        LogSequenceNumber watermark = new LogSequenceNumber(5, 10);
        List<LogSequenceNumber> consumed = Collections.synchronizedList(new ArrayList<>());
        PushCommitLogTailer tailer = new PushCommitLogTailer(128, watermark,
                (lsn, entry) -> consumed.add(lsn));
        Thread t = new Thread(tailer, "test-push-tailer");
        t.start();
        try {
            tailer.push(new LogSequenceNumber(5, 8), anEntry());   // stale
            tailer.push(new LogSequenceNumber(5, 10), anEntry());  // == watermark
            tailer.push(new LogSequenceNumber(5, 11), anEntry());  // new
            tailer.push(new LogSequenceNumber(6, 1), anEntry());   // new
            awaitSize(consumed, 2, 5000);
            // Give any erroneously-accepted stale entry time to slip through.
            Thread.sleep(300);
        } finally {
            tailer.close();
            t.join(5000);
        }
        assertEquals(2, consumed.size());
        assertEquals(new LogSequenceNumber(5, 11), consumed.get(0));
        assertEquals(new LogSequenceNumber(6, 1), consumed.get(1));
        assertEquals(2L, tailer.getEntriesProcessed());
        assertEquals(new LogSequenceNumber(6, 1), tailer.getWatermark());
    }

    @Test
    public void outOfOrderPushAfterAdvanceIsSkipped() throws Exception {
        // A client that violates the strictly-increasing-LSN contract by
        // pushing a regressing LSN gets that entry silently skipped (it is at
        // or before the already-advanced watermark) — the watermark and the
        // processed count, which the PushEntries response relies on, do not
        // move backwards.
        List<LogSequenceNumber> consumed = Collections.synchronizedList(new ArrayList<>());
        PushCommitLogTailer tailer = new PushCommitLogTailer(128, LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> consumed.add(lsn));
        Thread t = new Thread(tailer, "test-push-tailer");
        t.start();
        try {
            tailer.push(new LogSequenceNumber(1, 1), anEntry());
            tailer.push(new LogSequenceNumber(1, 2), anEntry());
            tailer.push(new LogSequenceNumber(1, 3), anEntry());
            awaitSize(consumed, 3, 5000);
            assertEquals(new LogSequenceNumber(1, 3), tailer.getWatermark());

            // Regressing LSN — must be skipped.
            tailer.push(new LogSequenceNumber(1, 2), anEntry());
            Thread.sleep(300);
        } finally {
            tailer.close();
            t.join(5000);
        }
        assertEquals("the out-of-order entry must not be dispatched", 3, consumed.size());
        assertEquals(3L, tailer.getEntriesProcessed());
        assertEquals(new LogSequenceNumber(1, 3), tailer.getWatermark());
    }

    @Test
    public void closeWithBufferedEntriesDropsThem() throws Exception {
        // close() drops entries still in the buffer (the engine does not drain
        // on shutdown). Only the entry already in-flight in the consumer when
        // close() is called is dispatched.
        CountDownLatch gate = new CountDownLatch(1);
        List<LogSequenceNumber> consumed = Collections.synchronizedList(new ArrayList<>());
        PushCommitLogTailer tailer = new PushCommitLogTailer(8, LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> {
                    try {
                        gate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    consumed.add(lsn);
                });
        Thread t = new Thread(tailer, "test-push-tailer");
        t.start();
        try {
            // Entry 1 is taken and parks the tailer in the gated consumer;
            // entries 2 and 3 sit undispatched in the buffer.
            tailer.push(new LogSequenceNumber(1, 1), anEntry());
            tailer.push(new LogSequenceNumber(1, 2), anEntry());
            tailer.push(new LogSequenceNumber(1, 3), anEntry());
            awaitCondition(() -> tailer.getBufferedCount() == 2, 5000);
            assertEquals("two entries must be buffered before close()", 2,
                    tailer.getBufferedCount());

            tailer.close();
        } finally {
            gate.countDown();
            t.join(5000);
        }
        // Only entry 1 (already in-flight at close) was dispatched; 2 and 3 dropped.
        assertEquals(1L, tailer.getEntriesProcessed());
        assertEquals(1, consumed.size());
        assertEquals("dropped entries are left undispatched in the buffer",
                2, tailer.getBufferedCount());
    }

    @Test
    public void closeStopsTheTailerThread() throws Exception {
        PushCommitLogTailer tailer = new PushCommitLogTailer(8, LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> { });
        Thread t = new Thread(tailer, "test-push-tailer");
        t.start();
        assertTrue(tailer.isRunning());
        tailer.close();
        t.join(5000);
        assertFalse("tailer thread must exit after close()", t.isAlive());
        assertFalse(tailer.isRunning());
    }

    @Test
    public void pushAfterCloseIsRejected() throws Exception {
        PushCommitLogTailer tailer = new PushCommitLogTailer(8, LogSequenceNumber.START_OF_TIME,
                (lsn, entry) -> { });
        tailer.close();
        try {
            tailer.push(new LogSequenceNumber(1, 1), anEntry());
            fail("push after close must be rejected");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("closed"));
        }
    }

    @Test
    public void constructorRejectsInvalidArguments() {
        CommitLogTailing.EntryConsumer noop = (lsn, entry) -> { };
        try {
            new PushCommitLogTailer(0, LogSequenceNumber.START_OF_TIME, noop);
            fail("zero capacity must be rejected");
        } catch (IllegalArgumentException expected) {
            // expected
        }
        try {
            new PushCommitLogTailer(8, LogSequenceNumber.START_OF_TIME, null);
            fail("null consumer must be rejected");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }
}
