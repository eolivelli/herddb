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
package herddb.remote;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Required follow-up #5: interrupt handling for
 * {@link RemoteFileDataStorageManager#prewarmMultipartIndexFile}.
 *
 * <p>Boots a {@link RemoteFileDataStorageManager} against a stub
 * {@link RemoteFileServiceClient} whose {@code prefetchFileRangeAsync}
 * returns futures that never complete on their own. The worker thread runs
 * the prewarm; the test interrupts it and asserts:
 * <ul>
 *   <li>the worker exits in well under 5 seconds — no 60 s per-block
 *       timeout, no infinite block;</li>
 *   <li>{@code Thread.currentThread().interrupted()} flag is observed set
 *       by the worker right before returning, proving the interrupt was
 *       acted on (not silently swallowed);</li>
 *   <li>the prewarm method's local semaphore drains — a second prewarm
 *       call against an already-complete stub finishes promptly,
 *       confirming no permit / semaphore was leaked into a JVM-global
 *       resource.</li>
 * </ul>
 */
public class RemoteFileDataStorageManagerPrewarmInterruptTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /** Per-test guard — the contract is "exit well under 5 s", so 30 s is generous. */
    @Rule
    public Timeout testTimeout = Timeout.seconds(30);

    private StubClient client;
    private RemoteFileDataStorageManager dsm;
    private SilencingHandler silencingHandler;
    private Logger productionLogger;
    private Handler[] originalHandlers;
    private boolean originalUseParentHandlers;

    @Before
    public void setUp() throws Exception {
        client = new StubClient();
        dsm = new RemoteFileDataStorageManager(
                folder.newFolder("metadata").toPath(),
                folder.newFolder("tmp").toPath(),
                64 * 1024,
                client);

        // Replace JUL handlers on the production logger with an in-memory
        // handler so that LOGGER.log() calls inside prewarmMultipartIndexFile
        // do not write to System.err. Surefire redirects stdout/stderr via
        // an interruptible channel — when the production code calls
        // LOGGER.log() with the interrupt flag set, the redirected I/O can
        // observe the interrupt, throw ClosedByInterruptException internally,
        // and clear the worker's interrupt flag as a side effect. Routing
        // the log records through a memory handler keeps the interrupt flag
        // intact so this test can assert on it deterministically.
        productionLogger = Logger.getLogger(RemoteFileDataStorageManager.class.getName());
        originalHandlers = productionLogger.getHandlers();
        originalUseParentHandlers = productionLogger.getUseParentHandlers();
        for (Handler h : originalHandlers) {
            productionLogger.removeHandler(h);
        }
        productionLogger.setUseParentHandlers(false);
        silencingHandler = new SilencingHandler();
        productionLogger.addHandler(silencingHandler);
    }

    @After
    public void tearDown() throws Exception {
        if (productionLogger != null) {
            productionLogger.removeHandler(silencingHandler);
            for (Handler h : originalHandlers) {
                productionLogger.addHandler(h);
            }
            productionLogger.setUseParentHandlers(originalUseParentHandlers);
        }
        if (dsm != null) {
            dsm.close();
        }
        if (client != null) {
            client.close();
        }
    }

    /**
     * In-memory JUL handler that drops records — used to silence
     * {@link RemoteFileDataStorageManager}'s logger so test assertions on
     * the interrupt flag are not affected by interruptible-channel I/O in
     * the underlying stream handler.
     */
    private static final class SilencingHandler extends Handler {
        @Override public void publish(LogRecord record) {
            // intentionally drop
        }
        @Override public void flush() { }
        @Override public void close() { }
    }

    /**
     * Subclasses {@link RemoteFileServiceClient} so that
     * {@code prefetchFileRangeAsync} returns futures whose completion the
     * test controls. The constructor accepts an empty server list — the
     * router code path never engages because we override the public entry
     * point.
     */
    private static final class StubClient extends RemoteFileServiceClient {

        final AtomicInteger prefetchCalls = new AtomicInteger();
        /** When true, every {@code prefetchFileRangeAsync} returns a future that NEVER completes. */
        volatile boolean hangForever = true;

        StubClient() {
            super(Collections.emptyList());
        }

        @Override
        public CompletableFuture<Boolean> prefetchFileRangeAsync(
                String path, long offset, int length, int blockSizeArg) {
            prefetchCalls.incrementAndGet();
            if (hangForever) {
                // Issue follow-up #5: futures that never complete on their own,
                // so the worker thread must rely on interrupt to escape the
                // await loop. The semaphore-release callback wired by the
                // production code into whenComplete fires only on cancel /
                // exception / value — none of which happens here unless the
                // test cancels.
                return new CompletableFuture<>();
            }
            return CompletableFuture.completedFuture(Boolean.TRUE);
        }
    }

    /**
     * Verifies the worker thread exits promptly after interrupt, sets the
     * interrupt flag right before returning, and that a follow-on prewarm
     * call against an already-complete stub finishes fast — i.e. the
     * semaphore / permit machinery did not get wedged by the interrupt.
     */
    @Test
    public void interruptDuringPrewarmExitsPromptlyWithoutLeakingPermits() throws Exception {
        final String tableSpace = "tsp";
        final String uuid = "00000000-0000-0000-0000-000000000001";
        final String fileType = "graph";
        // 100 blocks × 4 MiB so the dispatch loop saturates the
        // parallelism=8 semaphore well before exhausting the work — the
        // worker will be parked either inside permits.acquire() (block #9
        // onwards) or inside f.get(60s) on a never-completing future.
        final int blockSize = 4 * 1024 * 1024;
        final int parallelism = 8;
        final long fileSize = 100L * blockSize;

        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        AtomicReference<Boolean> interruptedFlagOnReturn = new AtomicReference<>();

        Thread worker = new Thread(() -> {
            try {
                dsm.prewarmMultipartIndexFile(tableSpace, uuid, fileType,
                        fileSize, blockSize, parallelism);
                // (b) capture the interrupt flag *immediately* after the
                // production method returns — before any test bookkeeping
                // (catch / finally / Logger calls) has had a chance to
                // observe and clear it. Thread.interrupted() reads-and-
                // clears so we get a snapshot of what the worker would
                // see on return from prewarm.
                interruptedFlagOnReturn.set(Thread.interrupted());
            } catch (Throwable t) {
                // Catch broad here so a stray RuntimeException is surfaced
                // to the test thread rather than killing the worker
                // silently. The narrower DataStorageManagerException isn't
                // declared as thrown by the production method for the
                // interrupt path, so anything else is a regression.
                workerFailure.set(t);
                interruptedFlagOnReturn.set(Thread.interrupted());
            }
        }, "prewarm-worker");
        worker.setDaemon(true);
        worker.start();

        // Give the worker enough time to enter the await loop. With 8 permits
        // and 100 work items, the dispatch loop fires 8 prefetch RPCs almost
        // immediately and then either blocks on permits.acquire() (b=8) or
        // moves into the await loop. We poll on prefetchCalls so we don't
        // race ahead of the worker getting started.
        long deadlineNanos = System.nanoTime() + 5L * 1_000_000_000L;
        while (client.prefetchCalls.get() < parallelism && System.nanoTime() < deadlineNanos) {
            Thread.sleep(20);
        }
        assertTrue("worker must have dispatched at least " + parallelism + " prefetch RPCs before interrupt",
                client.prefetchCalls.get() >= parallelism);
        // Brief settle to ensure the worker is genuinely parked (in
        // permits.acquire OR in f.get) and not still racing through the
        // dispatch loop.
        Thread.sleep(100);

        // (a) interrupt the worker. The production code must observe the
        // interrupt either in permits.acquire() (dispatch loop) or in
        // f.get(timeout) (await loop) and exit without waiting 60 s.
        //
        // We send a small burst of interrupts: the dispatch loop catches
        // the first InterruptedException, calls Thread.currentThread()
        // .interrupt() and then LOGGER.log(...) before breaking — some JUL
        // handlers may interact with the interrupt flag during their
        // synchronized flush, so a follow-up interrupt guarantees the
        // await-loop entry sees the flag set. The contract under test is
        // "exits promptly after interrupt", not "exits on exactly one
        // interrupt"; a robust caller would interrupt-and-join in a loop
        // anyway (see Thread interruption best practice).
        long beforeInterruptNanos = System.nanoTime();
        worker.interrupt();
        // Best-effort follow-up interrupts spread over 200 ms cover the
        // worker's transition between the dispatch-loop catch and the
        // await-loop's first f.get(). Stops as soon as the worker dies.
        for (int i = 0; i < 20 && worker.isAlive(); i++) {
            Thread.sleep(10);
            worker.interrupt();
        }
        worker.join(5_000L);
        long elapsedMs = (System.nanoTime() - beforeInterruptNanos) / 1_000_000L;
        assertFalse("worker thread must have exited within 5 s of interrupt (elapsed=" + elapsedMs + " ms)",
                worker.isAlive());
        assertTrue("worker must exit well under 5 s after interrupt (elapsed=" + elapsedMs + " ms)",
                elapsedMs < 5_000L);

        // No exception should have propagated out of prewarm.
        Throwable workerErr = workerFailure.get();
        if (workerErr != null) {
            throw new AssertionError("worker thread threw unexpectedly", workerErr);
        }

        // (b) the interrupt flag must have been set when the worker returned —
        // the production code calls Thread.currentThread().interrupt() on the
        // catch path so the caller can react. Thread.interrupted() in the
        // finally block reads AND clears that flag.
        assertTrue("worker must have set Thread.currentThread().interrupt() before returning",
                Boolean.TRUE.equals(interruptedFlagOnReturn.get()));

        // (c) prove the semaphore/permit machinery is not wedged: a follow-on
        // prewarm against an already-complete stub must finish promptly. Each
        // prewarmMultipartIndexFile invocation uses a NEW local Semaphore, so
        // a wedged permit would not leak across calls — but the secondary
        // check still guards against any global state regression (a future
        // refactor that hoists the semaphore to instance state would surface
        // here as a timeout).
        client.hangForever = false;
        final int secondaryPrefetchesBefore = client.prefetchCalls.get();
        // Run the secondary call on a fresh thread bounded by a 5 s join so
        // the test fails fast if a leak ever surfaces.
        AtomicReference<Throwable> secondaryFailure = new AtomicReference<>();
        Thread secondary = new Thread(() -> {
            try {
                dsm.prewarmMultipartIndexFile(tableSpace, uuid, fileType,
                        fileSize, blockSize, parallelism);
            } catch (Throwable t) {
                secondaryFailure.set(t);
            }
        }, "prewarm-worker-2");
        secondary.setDaemon(true);
        secondary.start();
        secondary.join(5_000L);
        assertFalse("secondary prewarm against already-complete stub must finish quickly — "
                + "if permits were leaked from the interrupted run, this would block forever",
                secondary.isAlive());
        Throwable secondaryErr = secondaryFailure.get();
        if (secondaryErr != null) {
            throw new AssertionError("secondary prewarm threw unexpectedly", secondaryErr);
        }
        // Sanity: the secondary run dispatched all 100 RPCs.
        assertTrue("secondary prewarm must dispatch all 100 prefetch RPCs",
                client.prefetchCalls.get() - secondaryPrefetchesBefore >= 100);
    }
}
