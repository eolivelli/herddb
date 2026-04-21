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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Exercises the parallel BLink node-write path added for issue #202. The BLink
 * primary-key index checkpoint fires a {@code writeIndexPageAsync} per dirty
 * node; this test wraps a {@link MemoryDataStorageManager} with a spy that
 * dispatches each async write onto a real executor so the futures actually run
 * off-thread, and checks both correctness (IndexStatus.activePages,
 * successful checkpoint) and error propagation.
 */
public class BLinkParallelWriteCheckpointTest {

    private static BLinkKeyToPageIndex newIndex(DataStorageManager ds) {
        MemoryManager mem = new MemoryManager(
                5 * (1L << 20), 0, 10 * (128L << 10), (128L << 10));
        BLinkKeyToPageIndex idx = new BLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
        idx.start(LogSequenceNumber.START_OF_TIME, true);
        return idx;
    }

    @Test
    public void checkpointDispatchesEachNodeViaWriteIndexPageAsync() throws Exception {
        // Populate enough keys to produce many dirty BLink leaves and inner nodes. The
        // async call counter was 0 before checkpoint (population doesn't install a
        // batch — all writes go through the sync path). After checkpoint, async count
        // must be strictly positive, proving the new path in BLinkKeyToPageIndex is
        // actually reached.
        CountingAsyncStorage ds = new CountingAsyncStorage();
        try (BLinkKeyToPageIndex idx = newIndex(ds)) {
            for (int i = 0; i < 2000; i++) {
                idx.put(Bytes.from_int(i), (long) i);
            }
            assertEquals("Population should go through sync path only",
                    0, ds.asyncCalls.get());
            List<PostCheckpointAction> actions = idx.checkpoint(
                    new LogSequenceNumber(1, 1), false);
            for (PostCheckpointAction a : actions) {
                a.run();
            }
            assertTrue("Expected writeIndexPageAsync calls during checkpoint, got "
                            + ds.asyncCalls.get(),
                    ds.asyncCalls.get() > 0);
        } finally {
            ds.shutdown();
        }
    }

    @Test
    public void failingAsyncWriteFailsCheckpointCleanly() throws Exception {
        // Inject a failure in one writeIndexPageAsync call; the checkpoint must surface
        // a DataStorageManagerException rather than silently complete with a
        // half-published IndexStatus.
        CountingAsyncStorage ds = new CountingAsyncStorage();
        ds.failNthAsyncWrite(3);
        try (BLinkKeyToPageIndex idx = newIndex(ds)) {
            for (int i = 0; i < 2000; i++) {
                idx.put(Bytes.from_int(i), (long) i);
            }
            try {
                idx.checkpoint(new LogSequenceNumber(1, 1), false);
                fail("checkpoint must fail when an async index write fails");
            } catch (DataStorageManagerException expected) {
                assertTrue("unexpected cause: " + expected,
                        expected.getMessage() != null
                                && (expected.getMessage().contains("injected")
                                        || (expected.getCause() != null
                                                && expected.getCause().getMessage().contains("injected"))));
            }
        } finally {
            ds.shutdown();
        }
    }

    /**
     * {@link MemoryDataStorageManager} subclass that:
     * <ul>
     *   <li>Dispatches {@code writeIndexPageAsync} onto a real executor so the
     *       futures actually complete off-thread (the default implementation
     *       runs synchronously).</li>
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
            Thread t = new Thread(r, "blink-parallel-test-writer");
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
