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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.index.vector.BulkPrefetchReaderSupplier;
import herddb.mem.MemoryDataStorageManager;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the bulk-prefetch wiring introduced by issue #619:
 * {@link PersistentVectorStore#bulkPrefetchSegmentIntoCache} only triggers when
 * the segment's {@code onDiskReaderSupplier} implements
 * {@link BulkPrefetchReaderSupplier}; when it does, the prefetch is invoked
 * exactly once per segment with the configured budget; when the prefetch fails
 * with an {@link IOException} the failure is swallowed so the BFS fallback path
 * can run; and when the supplier does NOT implement the mixin (e.g. local-disk
 * suppliers in unit tests) the bulk-prefetch path is skipped.
 *
 * <p>Uses a hand-rolled {@link ReaderSupplier} so the test does not require a
 * running remote file server — the contract under test is "does the dispatch
 * pick the bulk path when available", not "does the network read succeed".
 */
public class Issue619BulkWarmupTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /** A budget chosen large enough that warmupBytesPerSegment > 0 always trips the prefetch. */
    private static final long BUDGET_BYTES = 4L * 1024 * 1024;

    /**
     * Minimal {@link ReaderSupplier} that records every bulk-prefetch call and
     * lets the test simulate success / failure paths.
     */
    private static final class RecordingBulkPrefetchSupplier
            implements ReaderSupplier, BulkPrefetchReaderSupplier {

        final AtomicInteger calls = new AtomicInteger();
        final AtomicLong lastBudget = new AtomicLong(-1);
        final AtomicLong lastStartOffset = new AtomicLong(-1);
        volatile IOException failWith;

        @Override
        public long bulkPrefetchIntoCache(long startOffset, long maxBytes) throws IOException {
            calls.incrementAndGet();
            lastStartOffset.set(startOffset);
            lastBudget.set(maxBytes);
            if (failWith != null) {
                throw failWith;
            }
            return maxBytes; // pretend we covered the full budget
        }

        @Override
        public RandomAccessReader get() throws IOException {
            throw new IOException("test supplier does not vend readers");
        }

        @Override
        public void close() {
        }
    }

    /**
     * A {@link ReaderSupplier} that does NOT implement
     * {@link BulkPrefetchReaderSupplier} — used to verify the local-disk
     * fallthrough.
     */
    private static final class PlainSupplier implements ReaderSupplier {
        final AtomicInteger getCalls = new AtomicInteger();

        @Override
        public RandomAccessReader get() throws IOException {
            getCalls.incrementAndGet();
            throw new IOException("test supplier does not vend readers");
        }

        @Override
        public void close() {
        }
    }

    private PersistentVectorStore newStore(Path tmpDir) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(
                "vidx619", "vectable", "tstblspace", "vec", "uuid-619", tmpDir,
                new MemoryDataStorageManager(), mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
    }

    /**
     * When the supplier implements {@link BulkPrefetchReaderSupplier}, the
     * prefetch is invoked exactly once with {@code (startOffset=0, maxBytes=budget)}.
     */
    @Test
    public void bulkPrefetchInvokedOnceWhenSupplierSupportsIt() throws Exception {
        try (PersistentVectorStore store = newStore(tmpFolder.newFolder().toPath())) {
            store.setWarmupBytesPerSegment(BUDGET_BYTES);
            // Construct a synthetic segment carrying just enough state for the
            // dispatch helper to reach the supplier check. We don't go through
            // warmUpSegment() proper because that path also requires a real
            // OnDiskGraphIndex — outside the unit-test scope.
            VectorSegment seg = new VectorSegment(0);
            seg.graphFileSize = BUDGET_BYTES * 2L;
            RecordingBulkPrefetchSupplier supplier = new RecordingBulkPrefetchSupplier();
            seg.onDiskReaderSupplier = supplier;

            store.bulkPrefetchSegmentIntoCache(seg, BUDGET_BYTES);

            assertEquals("prefetch invoked exactly once", 1, supplier.calls.get());
            assertEquals("prefetch startOffset == 0", 0L, supplier.lastStartOffset.get());
            assertEquals("prefetch budget == warmupBytesPerSegment",
                    BUDGET_BYTES, supplier.lastBudget.get());
        }
    }

    /**
     * When the supplier does NOT implement {@link BulkPrefetchReaderSupplier}
     * the helper must short-circuit — no prefetch attempt, no reader.get()
     * call, no exception. Local-disk suppliers in unit tests rely on this.
     */
    @Test
    public void bulkPrefetchSkippedForNonRemoteSupplier() throws Exception {
        try (PersistentVectorStore store = newStore(tmpFolder.newFolder().toPath())) {
            store.setWarmupBytesPerSegment(BUDGET_BYTES);
            VectorSegment seg = new VectorSegment(0);
            seg.graphFileSize = BUDGET_BYTES * 2L;
            PlainSupplier supplier = new PlainSupplier();
            seg.onDiskReaderSupplier = supplier;

            store.bulkPrefetchSegmentIntoCache(seg, BUDGET_BYTES);

            assertEquals("plain supplier must not be invoked via get()",
                    0, supplier.getCalls.get());
        }
    }

    /**
     * An {@link IOException} from the supplier must be swallowed (best-effort
     * contract); the caller continues to the BFS fallback path. A
     * {@link RuntimeException} from the supplier is also swallowed — see the
     * narrow catch in {@code bulkPrefetchSegmentIntoCache}.
     */
    @Test
    public void ioExceptionFromSupplierIsSwallowed() throws Exception {
        try (PersistentVectorStore store = newStore(tmpFolder.newFolder().toPath())) {
            store.setWarmupBytesPerSegment(BUDGET_BYTES);
            VectorSegment seg = new VectorSegment(0);
            seg.graphFileSize = BUDGET_BYTES * 2L;
            RecordingBulkPrefetchSupplier supplier = new RecordingBulkPrefetchSupplier();
            supplier.failWith = new IOException("simulated S3 fault");
            seg.onDiskReaderSupplier = supplier;

            // Must not throw — the warmup contract is best-effort.
            store.bulkPrefetchSegmentIntoCache(seg, BUDGET_BYTES);
            assertEquals(1, supplier.calls.get());
        }
    }

    /**
     * A zero (or negative) budget is a no-op: the helper does not even check
     * the supplier type, so it never calls {@code bulkPrefetchIntoCache}.
     */
    @Test
    public void zeroBudgetIsNoOp() throws Exception {
        try (PersistentVectorStore store = newStore(tmpFolder.newFolder().toPath())) {
            VectorSegment seg = new VectorSegment(0);
            seg.graphFileSize = BUDGET_BYTES * 2L;
            RecordingBulkPrefetchSupplier supplier = new RecordingBulkPrefetchSupplier();
            seg.onDiskReaderSupplier = supplier;

            store.bulkPrefetchSegmentIntoCache(seg, 0L);
            store.bulkPrefetchSegmentIntoCache(seg, -1L);

            assertEquals("prefetch must not run with non-positive budget", 0, supplier.calls.get());
        }
    }

    /**
     * The cached supplier reference is unchanged across the call — sanity
     * check that the helper does not mutate the segment.
     */
    @Test
    public void supplierReferenceIsNotMutated() throws Exception {
        try (PersistentVectorStore store = newStore(tmpFolder.newFolder().toPath())) {
            store.setWarmupBytesPerSegment(BUDGET_BYTES);
            VectorSegment seg = new VectorSegment(0);
            seg.graphFileSize = BUDGET_BYTES * 2L;
            RecordingBulkPrefetchSupplier supplier = new RecordingBulkPrefetchSupplier();
            seg.onDiskReaderSupplier = supplier;

            store.bulkPrefetchSegmentIntoCache(seg, BUDGET_BYTES);

            assertSame("supplier reference must not be mutated",
                    supplier, seg.onDiskReaderSupplier);
        }
    }

    /**
     * Multiple invocations call the supplier each time — the helper is
     * stateless and just dispatches. The caller (warmUpSegment) is responsible
     * for the per-segment "warmedUp" idempotency guard.
     */
    @Test
    public void repeatedInvocationCallsSupplierEachTime() throws Exception {
        try (PersistentVectorStore store = newStore(tmpFolder.newFolder().toPath())) {
            store.setWarmupBytesPerSegment(BUDGET_BYTES);
            VectorSegment seg = new VectorSegment(0);
            seg.graphFileSize = BUDGET_BYTES * 2L;
            RecordingBulkPrefetchSupplier supplier = new RecordingBulkPrefetchSupplier();
            seg.onDiskReaderSupplier = supplier;

            store.bulkPrefetchSegmentIntoCache(seg, BUDGET_BYTES);
            store.bulkPrefetchSegmentIntoCache(seg, BUDGET_BYTES);
            store.bulkPrefetchSegmentIntoCache(seg, BUDGET_BYTES);

            assertEquals(3, supplier.calls.get());
        }
    }

    /**
     * A null {@code onDiskReaderSupplier} (which can happen for segments that
     * have not been registered yet — defensive guard) must not throw.
     */
    @Test
    public void nullReaderSupplierIsNoOp() throws Exception {
        try (PersistentVectorStore store = newStore(tmpFolder.newFolder().toPath())) {
            store.setWarmupBytesPerSegment(BUDGET_BYTES);
            VectorSegment seg = new VectorSegment(0);
            seg.graphFileSize = BUDGET_BYTES * 2L;
            seg.onDiskReaderSupplier = null;

            // Must not throw and must not log at SEVERE.
            store.bulkPrefetchSegmentIntoCache(seg, BUDGET_BYTES);
            assertTrue("null supplier path is a clean no-op", true);
        }
    }
}
