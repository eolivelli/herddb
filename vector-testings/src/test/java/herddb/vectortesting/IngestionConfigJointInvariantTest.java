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
package herddb.vectortesting;

import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Reviewer follow-up #3: the joint invariant
 * {@code batch-size <= effective transaction-size <= ingest-max-ops}
 * must hold under concurrent admin POSTs. Two setters racing on different
 * fields (e.g. {@code setBatchSize} vs {@code setTransactionSize}) must not
 * both pass their individual checks against the old value of the other
 * field and then commit writes that violate the invariant.
 */
class IngestionConfigJointInvariantTest {

    @Test
    void concurrentSettersPreserveJointInvariant() throws Exception {
        Config cfg = new Config();
        cfg.batchSize = 100;
        cfg.transactionSize = 500;
        cfg.ingestMaxOpsPerSecond = 100_000;
        cfg.ingestThreads = 1;
        BenchRuntime runtime = new BenchRuntime(cfg);

        int rounds = 200;
        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(3);
        try {
            // Hammer setBatchSize within [50, 500]
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < rounds; i++) {
                    int v = 50 + (i % 10) * 50;
                    try {
                        runtime.setBatchSize(v);
                    } catch (IllegalArgumentException ignored) {
                        // expected occasionally — invariant violation against the
                        // current other-field value; the call leaves config untouched.
                    }
                }
                done.countDown();
            });
            // Hammer setTransactionSize within [200, 2000]
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < rounds; i++) {
                    int v = 200 + (i % 10) * 200;
                    try {
                        runtime.setTransactionSize(v);
                    } catch (IllegalArgumentException ignored) {
                        // expected occasionally — see above
                    }
                }
                done.countDown();
            });
            // Hammer setIngestMaxOps within [1000, 200_000]
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < rounds; i++) {
                    int v = 1000 + (i % 10) * 20_000;
                    try {
                        runtime.setIngestMaxOps(v);
                    } catch (IllegalArgumentException ignored) {
                        // expected occasionally — see above
                    }
                }
                done.countDown();
            });

            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS), "stress workers did not finish in time");
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }

        // After the storm settles, the joint invariant must hold.
        int batch = cfg.batchSize;
        int txn = cfg.effectiveTransactionSize();
        int rate = cfg.ingestMaxOpsPerSecond;
        assertTrue(batch >= 1, "batch-size invariant: " + batch);
        assertTrue(txn >= batch,
                "joint invariant batch-size <= transaction-size violated: batch="
                        + batch + " txn=" + txn);
        assertTrue(rate == 0 || txn <= rate,
                "joint invariant transaction-size <= ingest-max-ops violated: txn="
                        + txn + " rate=" + rate);
    }
}
