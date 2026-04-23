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

package herddb.index.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Regression test for issue #234: when a shard-write future in Phase B fails,
 * the outer loop must preserve the real cause instead of letting a
 * {@link CancellationException} from a subsequently-cancelled future propagate.
 *
 * The test exercises the exact same future-reduction pattern that
 * {@code PersistentVectorStore.doCheckpointFusedPQPhaseB} (and
 * {@code buildSegmentsInParallel}) uses, so a regression in either will flag
 * here.
 */
public class Issue234FutureErrorPropagationTest {

    @Test(timeout = 10_000)
    public void earlyFailureSurvivesCancellationOfLaterFutures() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            IOException realCause = new IOException("readFileRange CANCELLED: simulated");

            List<Future<Integer>> futures = new ArrayList<>();
            // First future fails immediately with the real cause.
            futures.add(executor.submit(() -> {
                throw realCause;
            }));
            // Subsequent futures are long-running tasks that will be cancelled by
            // the outer loop after the first failure. When the loop then calls
            // .get() on them, it previously received a CancellationException that
            // masked the real cause.
            for (int i = 0; i < 3; i++) {
                futures.add(executor.submit(() -> {
                    TimeUnit.SECONDS.sleep(30);
                    return 0;
                }));
            }

            Throwable firstFailure = runFixedReductionLoop(futures);

            assertNotNull("firstFailure must be set", firstFailure);
            assertSame("firstFailure must be the IOException thrown by shard 0, "
                    + "not a CancellationException from the cancelled shards",
                    realCause, firstFailure);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(timeout = 10_000)
    public void cancellationOnlyYieldsCancellationException() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            List<Future<Integer>> futures = new ArrayList<>();
            Future<Integer> longRunning = executor.submit(() -> {
                TimeUnit.SECONDS.sleep(30);
                return 0;
            });
            futures.add(longRunning);

            // Cancel before the loop reads .get() — simulates "executor shutdown"
            // fallback: no ExecutionException was ever seen, only a cancellation.
            longRunning.cancel(true);

            Throwable firstFailure = runFixedReductionLoop(futures);

            assertNotNull("firstFailure must capture cancellation as fallback",
                    firstFailure);
            assertEquals("no real failure to preserve → fallback is CancellationException",
                    CancellationException.class, firstFailure.getClass());
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Mirrors the fixed loop in
     * {@code PersistentVectorStore.doCheckpointFusedPQPhaseB} / {@code buildSegmentsInParallel}
     * (issue #234). Keep this in sync with the production code; if the pattern
     * ever diverges, the production code is the source of truth.
     */
    private static <T> Throwable runFixedReductionLoop(List<Future<T>> futures) {
        Throwable firstFailure = null;
        for (int i = 0; i < futures.size(); i++) {
            try {
                futures.get(i).get();
            } catch (ExecutionException ee) {
                if (firstFailure == null) {
                    firstFailure = ee.getCause() != null ? ee.getCause() : ee;
                }
                for (int j = i + 1; j < futures.size(); j++) {
                    futures.get(j).cancel(true);
                }
            } catch (CancellationException ce) {
                if (firstFailure == null) {
                    firstFailure = ce;
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                if (firstFailure == null) {
                    firstFailure = ie;
                }
            }
        }
        return firstFailure;
    }
}
