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
package herddb.indexing.segment;

import static org.junit.Assert.assertTrue;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

/**
 * Verifies that the dispatch-executor thread created by
 * {@link SegmentAssignmentWatcher}'s public constructor is a
 * {@link FastThreadLocalThread} so Netty's recyclable-object fast-path is
 * available on watcher threads (issue #470).
 *
 * <p>A no-op {@link SegmentRegistryClient} is constructed with a supplier that
 * returns {@code null}; because the watcher's constructor does not invoke any
 * ZooKeeper operations, this is safe. We access the private {@code dispatchExecutor}
 * field reflectively, submit a one-shot task, and capture the running thread.
 */
public class SegmentAssignmentWatcherFastThreadLocalTest {

    private SegmentAssignmentWatcher watcher;

    @After
    public void tearDown() {
        if (watcher != null) {
            watcher.close();
        }
    }

    @Test
    public void dispatchExecutorUsesFastThreadLocal() throws Exception {
        // A SegmentRegistryClient whose ZK supplier returns null — safe as long
        // as no ZK operations are triggered (and the watcher constructor is ZK-free).
        SegmentRegistryClient noopRegistry =
                new SegmentRegistryClient(() -> null, "/herd-test-470");

        watcher = new SegmentAssignmentWatcher(
                noopRegistry,
                /* instanceId */ 0,
                /* listener — no-op */ new SegmentAssignmentListener() {},
                /* refreshIntervalMs */ 60_000L);

        ScheduledExecutorService exec =
                readField(watcher, "dispatchExecutor", ScheduledExecutorService.class);

        CompletableFuture<Thread> future = new CompletableFuture<>();
        exec.execute(() -> future.complete(Thread.currentThread()));
        Thread t = future.get(10, TimeUnit.SECONDS);

        assertTrue("dispatchExecutor thread must be FastThreadLocalThread, got: " + t.getClass(),
                t instanceof FastThreadLocalThread);
    }

    // -----------------------------------------------------------------------

    private static <T> T readField(Object target, String name, Class<T> type) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return type.cast(f.get(target));
    }
}
