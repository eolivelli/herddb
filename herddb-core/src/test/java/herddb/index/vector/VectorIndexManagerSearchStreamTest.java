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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.log.LogSequenceNumber;
import herddb.model.StatementExecutionException;
import herddb.utils.Bytes;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Unit tests for {@link VectorIndexManager.ExpandingSearchIterator} that exercise the
 * expansion / dedupe / exhaustion logic in isolation, without requiring a full
 * {@link VectorIndexManager} or a running DBManager.
 */
public class VectorIndexManagerSearchStreamTest {

    private static final float[] QUERY = {1.0f, 0.0f, 0.0f};

    /**
     * Test stub of {@link RemoteVectorIndexService}: returns a deterministic
     * sorted list of {@code (pk, score)} pairs, truncated to the requested
     * {@code topK}. Records every search call so tests can assert the exact
     * expansion schedule.
     */
    private static final class StubService implements RemoteVectorIndexService {

        private final List<Map.Entry<Bytes, Float>> population;
        private final List<Integer> requestedLimits =
                Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger callCount = new AtomicInteger();
        private RuntimeException throwOnNext;

        StubService(int populationSize) {
            this.population = new ArrayList<>(populationSize);
            for (int i = 0; i < populationSize; i++) {
                // Decreasing scores so the order is stable and deterministic:
                // pk=0 has the highest score, pk=i has score (1.0 - i*1e-5).
                float score = 1.0f - (i * 1.0e-5f);
                population.add(new AbstractMap.SimpleImmutableEntry<>(
                        Bytes.from_int(i), score));
            }
        }

        void throwOnNextSearch(RuntimeException e) {
            this.throwOnNext = e;
        }

        int callCount() {
            return callCount.get();
        }

        List<Integer> requestedLimits() {
            synchronized (requestedLimits) {
                return new ArrayList<>(requestedLimits);
            }
        }

        @Override
        public List<Map.Entry<Bytes, Float>> search(String tablespace, String table, String index,
                                                      float[] vector, int topK) {
            callCount.incrementAndGet();
            requestedLimits.add(topK);
            RuntimeException toThrow = this.throwOnNext;
            if (toThrow != null) {
                this.throwOnNext = null;
                throw toThrow;
            }
            int end = Math.min(population.size(), topK);
            return new ArrayList<>(population.subList(0, end));
        }

        @Override
        public IndexStatusInfo getIndexStatus(String tablespace, String table, String index) {
            return new IndexStatusInfo(population.size(), 1, 0, 0, "stub");
        }

        @Override
        public boolean waitForCatchUp(String tablespace, LogSequenceNumber sequenceNumber,
                                       long timeoutMs) {
            return true;
        }

        @Override
        public java.util.Optional<LogSequenceNumber> getMinProcessedLsn(String tablespace) {
            return java.util.Optional.empty();
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static VectorIndexManager.SearchIterator newIterator(
            StubService service, int targetK, float factor, int minInitial, int maxExpansions) {
        return VectorIndexManager.newSearchIteratorForTest(service,
                "tblspace1", "t1", "vidx", QUERY,
                targetK, factor, minInitial, maxExpansions, null, null);
    }

    @Test
    public void initialBudgetSufficientEmitsEverythingInOneRpc() throws Exception {
        StubService stub = new StubService(100);
        try (VectorIndexManager.SearchIterator it = newIterator(stub, 10, 1.5f, 16, 6)) {
            int consumed = 0;
            while (it.hasNext() && consumed < 10) {
                it.next();
                consumed++;
            }
            assertEquals("should have consumed exactly targetK rows", 10, consumed);
        }
        assertEquals("only one RPC expected for abundant population", 1, stub.callCount());
        // factor=1.5, targetK=10 -> ceil(15); floor=16 wins
        assertEquals("initial budget must honour minInitial floor",
                Integer.valueOf(16), stub.requestedLimits().get(0));
    }

    @Test
    public void initialBudgetComputesFactorWhenAboveFloor() throws Exception {
        StubService stub = new StubService(1000);
        try (VectorIndexManager.SearchIterator it = newIterator(stub, 100, 1.5f, 16, 6)) {
            it.hasNext();
        }
        // factor=1.5, targetK=100 -> ceil(150); above floor
        assertEquals("initial budget must be ceil(targetK * factor) when > minInitial",
                Integer.valueOf(150), stub.requestedLimits().get(0));
    }

    @Test
    public void deduplicationAcrossExpansions() throws Exception {
        StubService stub = new StubService(200);
        Set<Bytes> distinct = new HashSet<>();
        // Drain the iterator entirely; each expansion returns a superset of
        // the previous, so without dedupe we'd see duplicates.
        try (VectorIndexManager.SearchIterator it = newIterator(stub, 16, 1.0f, 16, 6)) {
            while (it.hasNext()) {
                Map.Entry<Bytes, Float> e = it.next();
                assertTrue("each PK must be emitted at most once",
                        distinct.add(e.getKey()));
            }
        }
        // Expansion schedule: 16, 32, 64, 128, 256 — the last one is clamped
        // to the 200-entry population, so we should see exactly 200 unique
        // entries. 5 RPCs: 16+32+64+128 = 240 <= 200? No, 16+32+64+128 = 240
        // which exceeds population at the fourth call (128<200, fifth=256>=200
        // triggers exhaustion). Six RPCs total in the worst case.
        assertEquals("all population emitted exactly once", 200, distinct.size());
        assertTrue("multiple expansions expected", stub.callCount() >= 2);
    }

    @Test
    public void exhaustionStopsBeforeMaxExpansions() throws Exception {
        StubService stub = new StubService(5);
        int consumed = 0;
        try (VectorIndexManager.SearchIterator it = newIterator(stub, 20, 1.5f, 16, 6)) {
            while (it.hasNext()) {
                it.next();
                consumed++;
            }
        }
        assertEquals("iterator returns the full small population", 5, consumed);
        // Initial budget = max(16, ceil(30)) = 30, service returns 5 < 30 on
        // the first call, so exhaustion is detected immediately.
        assertEquals("exhaustion should short-circuit after a single RPC",
                1, stub.callCount());
    }

    @Test
    public void expansionDoublingSchedule() throws Exception {
        // Use factor=1.0, minInitial=10, targetK=10 -> initial=10.
        // Population=1000 so every request is filled exactly.
        StubService stub = new StubService(1000);
        // Force all 6 expansions by draining as aggressively as possible:
        // we will drain the buffer after every next(), forcing a refill on
        // each iteration past the initial batch.
        int drained = 0;
        try (VectorIndexManager.SearchIterator it = newIterator(stub, 10, 1.0f, 10, 6)) {
            while (it.hasNext() && drained < 1000) {
                it.next();
                drained++;
            }
        }
        // Budgets: 10, 20, 40, 80, 160, 320, 640 -> total unique rows emitted
        // = 640 (then next expansion would be 1280, but maxExpansions=6 means
        // at most 7 RPCs total: initial + 6 expansions). Actually index state
        // shows maxExpansions=6 -> we keep going while expansionsUsed <= 6, so
        // 7 RPCs. 10+10+20+40+80+160+320 = 640 distinct rows.
        // Check budget schedule ordering and monotone doubling.
        List<Integer> limits = stub.requestedLimits();
        assertTrue("at least 2 RPCs expected", limits.size() >= 2);
        for (int i = 1; i < limits.size(); i++) {
            assertEquals("each expansion must double the previous budget",
                    limits.get(i - 1) * 2, (int) limits.get(i));
        }
        // Hard ceiling: iterator must NOT exceed maxExpansions+1 RPCs.
        assertTrue("at most 1 + maxExpansions = 7 RPCs", stub.callCount() <= 7);
    }

    @Test
    public void closeIsIdempotentAndStopsHasNext() throws Exception {
        StubService stub = new StubService(50);
        VectorIndexManager.SearchIterator it = newIterator(stub, 10, 1.5f, 16, 6);
        assertTrue(it.hasNext());
        it.next();
        it.close();
        // hasNext after close must be false.
        assertFalse(it.hasNext());
        try {
            it.next();
            fail("next() after close must throw NoSuchElementException");
        } catch (NoSuchElementException expected) {
            // expected
        }
        // double close is a no-op.
        it.close();
    }

    @Test
    public void rpcErrorIsWrappedInStatementExecutionException() {
        StubService stub = new StubService(50);
        stub.throwOnNextSearch(new RuntimeException("boom"));
        try (VectorIndexManager.SearchIterator it = newIterator(stub, 10, 1.5f, 16, 6)) {
            it.hasNext();
            fail("expected StatementExecutionException");
        } catch (StatementExecutionException expected) {
            assertTrue(expected.getMessage().contains("boom"));
        }
    }

    @Test
    public void emptyIndexReturnsNoResults() throws Exception {
        StubService stub = new StubService(0);
        try (VectorIndexManager.SearchIterator it = newIterator(stub, 10, 1.5f, 16, 6)) {
            assertFalse("empty index should yield hasNext=false", it.hasNext());
        }
        assertEquals("one RPC establishes exhaustion on an empty index",
                1, stub.callCount());
    }

    @Test
    public void predicateFilterSimulation() throws Exception {
        // Simulates the real caller pattern: pull entries one at a time until
        // 5 of them satisfy a "pk%10 == 0" predicate. The iterator must keep
        // expanding until it finds enough.
        StubService stub = new StubService(200);
        int matched = 0;
        List<Bytes> hits = new ArrayList<>();
        try (VectorIndexManager.SearchIterator it = newIterator(stub, 5, 1.5f, 16, 6)) {
            while (it.hasNext() && matched < 5) {
                Map.Entry<Bytes, Float> e = it.next();
                int pk = e.getKey().to_int();
                if (pk % 10 == 0) {
                    hits.add(e.getKey());
                    matched++;
                }
            }
        }
        assertEquals("predicate must ultimately match 5 rows", 5, matched);
        assertEquals("distinct hits",
                new HashSet<>(hits).size(), hits.size());
        // The initial budget is max(16, ceil(7.5)) = 16. That already contains
        // pks {0, 10} = 2 matches, not 5 → expansion must happen.
        assertTrue("expansion expected when predicate is selective",
                stub.callCount() >= 2);
    }

    @Test
    public void invalidArgumentsAreRejected() {
        StubService stub = new StubService(10);
        try {
            newIterator(stub, 0, 1.5f, 16, 6);
            fail();
        } catch (IllegalArgumentException expected) {
            // ok
        }
        try {
            newIterator(stub, 10, 0.5f, 16, 6);
            fail();
        } catch (IllegalArgumentException expected) {
            // ok
        }
        try {
            newIterator(stub, 10, 1.5f, 16, -1);
            fail();
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

}
