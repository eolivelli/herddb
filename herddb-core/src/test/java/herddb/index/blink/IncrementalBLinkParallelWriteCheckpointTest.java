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

package herddb.index.blink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.MemoryManager;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Exercises the parallel BLink node-write path added for issue #396 in
 * {@link IncrementalBLinkKeyToPageIndex}. The legacy
 * {@link BLinkKeyToPageIndex} already used
 * {@code DataStorageManager.writeIndexPageAsync} during checkpoint (issue
 * #202) but the incremental implementation kept writing each node-page
 * synchronously, which on remote storage made Phase C duration grow linearly
 * with the page count.
 *
 * <p>Both tests wrap a {@link MemoryDataStorageManager} with a spy that
 * dispatches each async write onto a real executor so the futures actually
 * run off-thread, and check (1) that DML uses the sync path and checkpoint
 * uses the async path on both the snapshot and delta legs of the incremental
 * format, and (2) that an injected async-write failure surfaces a
 * {@link DataStorageManagerException} rather than persisting a half-baked
 * manifest.</p>
 */
public class IncrementalBLinkParallelWriteCheckpointTest {

    private static IncrementalBLinkKeyToPageIndex newIndex(DataStorageManager ds) {
        MemoryManager mem = new MemoryManager(
                5 * (1L << 20), 0, 10 * (128L << 10), (128L << 10));
        IncrementalBLinkKeyToPageIndex idx =
                new IncrementalBLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
        idx.start(LogSequenceNumber.START_OF_TIME, true);
        return idx;
    }

    @Test
    public void checkpointDispatchesEachNodeViaWriteIndexPageAsync() throws Exception {
        // Populate enough keys to produce many dirty BLink leaves and inner nodes,
        // then run the snapshot leg of the incremental format. After mutating
        // a smaller batch run a second checkpoint that produces a delta. Both
        // legs must dispatch node writes through writeIndexPageAsync.
        CountingAsyncStorage ds = new CountingAsyncStorage();
        try (IncrementalBLinkKeyToPageIndex idx = newIndex(ds)) {
            for (int i = 0; i < 2000; i++) {
                idx.put(Bytes.from_int(i), (long) i);
            }
            assertEquals("Population (DML splits) should go through sync path only",
                    0, ds.asyncCalls.get());

            // Snapshot leg.
            int asyncBefore = ds.asyncCalls.get();
            idx.checkpoint(new LogSequenceNumber(1, 1), false);
            int asyncAfterSnapshot = ds.asyncCalls.get();
            assertNotNull("manifest must be persisted after first checkpoint",
                    idx.getCurrentManifest());
            assertTrue("expected writeIndexPageAsync calls during snapshot checkpoint, got "
                            + (asyncAfterSnapshot - asyncBefore),
                    asyncAfterSnapshot > asyncBefore);

            // Mutate enough to dirty several leaves but not so many that it
            // triggers a full snapshot rewrite.
            for (int i = 2000; i < 3000; i++) {
                idx.put(Bytes.from_int(i), (long) i);
            }
            assertEquals("Post-checkpoint DML must continue to use sync path",
                    asyncAfterSnapshot, ds.asyncCalls.get());

            // Delta leg.
            idx.checkpoint(new LogSequenceNumber(1, 2), false);
            assertTrue("expected writeIndexPageAsync calls during delta checkpoint, got "
                            + (ds.asyncCalls.get() - asyncAfterSnapshot),
                    ds.asyncCalls.get() > asyncAfterSnapshot);
        } finally {
            ds.shutdown();
        }
    }

    @Test
    public void failingAsyncWriteFailsCheckpointCleanly() throws Exception {
        // Inject a failure in one writeIndexPageAsync call; the checkpoint must
        // surface a DataStorageManagerException rather than silently completing
        // with a half-published manifest.
        CountingAsyncStorage ds = new CountingAsyncStorage();
        ds.failNthAsyncWrite(3);
        try (IncrementalBLinkKeyToPageIndex idx = newIndex(ds)) {
            for (int i = 0; i < 2000; i++) {
                idx.put(Bytes.from_int(i), (long) i);
            }
            try {
                idx.checkpoint(new LogSequenceNumber(1, 1), false);
                fail("checkpoint must fail when an async index write fails");
            } catch (DataStorageManagerException expected) {
                String chain = chainMessages(expected);
                assertTrue("unexpected cause chain: " + chain,
                        chain.contains("injected"));
            }
            // The previous failed checkpoint must NOT have published a manifest:
            // the join in awaitBatch happens before indexCheckpoint persists the
            // IndexStatus, so currentManifest stays null on the failure path.
            assertEquals("failed checkpoint must not advance manifest state",
                    null, idx.getCurrentManifest());
        } finally {
            ds.shutdown();
        }
    }

    private static String chainMessages(Throwable t) {
        StringBuilder b = new StringBuilder();
        Throwable cur = t;
        while (cur != null) {
            if (b.length() > 0) {
                b.append(" <- ");
            }
            b.append(cur.getClass().getSimpleName()).append(": ").append(cur.getMessage());
            cur = cur.getCause();
        }
        return b.toString();
    }

    /**
     * {@link MemoryDataStorageManager} subclass that:
     * <ul>
     *   <li>Dispatches {@code writeIndexPageAsync} onto a real executor so
     *       the futures actually complete off-thread (the default
     *       implementation runs synchronously).</li>
     *   <li>Counts async and sync writes so the test can assert the correct
     *       path was taken.</li>
     *   <li>Can inject a failure on the N-th async write for the error path.</li>
     * </ul>
     */
    private static final class CountingAsyncStorage extends MemoryDataStorageManager {
        final AtomicInteger asyncCalls = new AtomicInteger();
        private final AtomicInteger asyncSeq = new AtomicInteger();
        volatile int failAt = -1;
        private final ExecutorService exec = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "incremental-blink-parallel-test-writer");
            t.setDaemon(true);
            return t;
        });

        void failNthAsyncWrite(int n) {
            this.failAt = n;
        }

        void shutdown() throws InterruptedException {
            exec.shutdown();
            exec.awaitTermination(10, TimeUnit.SECONDS);
        }

        @Override
        public CompletableFuture<Void> writeIndexPageAsync(String tableSpace, String indexName,
                long pageId, DataWriter writer) {
            asyncCalls.incrementAndGet();
            int seq = asyncSeq.incrementAndGet();
            CompletableFuture<Void> future = new CompletableFuture<>();
            exec.submit(() -> {
                if (failAt > 0 && seq == failAt) {
                    future.completeExceptionally(new RuntimeException(
                            "injected failure on async write #" + seq));
                    return;
                }
                try {
                    // Delegate the actual byte storage to the synchronous base impl.
                    super.writeIndexPage(tableSpace, indexName, pageId, writer);
                    future.complete(null);
                } catch (RuntimeException ex) {
                    future.completeExceptionally(ex);
                }
            });
            return future;
        }
    }
}
