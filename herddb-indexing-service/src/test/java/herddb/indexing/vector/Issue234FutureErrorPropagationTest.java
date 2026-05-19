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

package herddb.indexing.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Regression test for issue #234: when a shard-write future fails, the
 * shared {@link PersistentVectorStore#awaitAllOrFirstFailure} helper must
 * preserve the real cause instead of letting a {@link CancellationException}
 * from a subsequently-cancelled future propagate.
 *
 * <p>Both {@code doCheckpointFusedPQPhaseB} and {@code buildSegmentsInParallel}
 * delegate to the same helper, so a regression in either flags here.
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
            // the helper after the first failure. When the helper then calls
            // .get() on them it observes a CancellationException that would
            // previously mask the real cause.
            for (int i = 0; i < 3; i++) {
                futures.add(executor.submit(() -> {
                    TimeUnit.SECONDS.sleep(30);
                    return 0;
                }));
            }

            Throwable firstFailure = PersistentVectorStore.awaitAllOrFirstFailure(
                    futures, (i, v) -> { /* no-op */ });

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

            // Cancel before the helper reads .get() — simulates "executor shutdown"
            // fallback: no ExecutionException is ever seen, only a cancellation.
            longRunning.cancel(true);

            Throwable firstFailure = PersistentVectorStore.awaitAllOrFirstFailure(
                    futures, (i, v) -> { /* no-op */ });

            assertNotNull("firstFailure must capture cancellation as fallback",
                    firstFailure);
            assertEquals("no real failure to preserve → fallback is CancellationException",
                    CancellationException.class, firstFailure.getClass());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(timeout = 10_000)
    public void onSuccessReceivesIndexAndValue() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            List<Future<String>> futures = new ArrayList<>();
            futures.add(executor.submit(() -> "a"));
            futures.add(executor.submit(() -> "b"));
            futures.add(executor.submit(() -> "c"));

            List<String> collected = new ArrayList<>();
            Throwable firstFailure = PersistentVectorStore.awaitAllOrFirstFailure(
                    futures, (i, v) -> collected.add(i + "=" + v));

            assertEquals(null, firstFailure);
            assertEquals(List.of("0=a", "1=b", "2=c"), collected);
        } finally {
            executor.shutdownNow();
        }
    }
}
