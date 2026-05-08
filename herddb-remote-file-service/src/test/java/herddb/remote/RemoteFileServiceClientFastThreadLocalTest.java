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

import static org.junit.Assert.assertTrue;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

/**
 * Verifies that the retry-scheduler thread created by {@link RemoteFileServiceClient}
 * is a {@link FastThreadLocalThread} so Netty's recyclable-object fast-path is
 * available during retry logic (issue #470).
 *
 * <p>The client is constructed in cold-start mode (empty server list) so no
 * real network connections are opened. We access the private {@code retryScheduler}
 * field reflectively, submit a task, and capture the thread that runs it.
 */
public class RemoteFileServiceClientFastThreadLocalTest {

    private RemoteFileServiceClient client;

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void retrySchedulerUsesFastThreadLocal() throws Exception {
        // Empty server list — cold-start ZK-discovery mode; no network I/O.
        client = new RemoteFileServiceClient(Collections.emptyList());

        ScheduledExecutorService scheduler =
                readField(client, "retryScheduler", ScheduledExecutorService.class);

        CompletableFuture<Thread> future = new CompletableFuture<>();
        scheduler.execute(() -> future.complete(Thread.currentThread()));
        Thread t = future.get(10, TimeUnit.SECONDS);

        assertTrue("retryScheduler thread must be FastThreadLocalThread, got: " + t.getClass(),
                t instanceof FastThreadLocalThread);
    }

    // -----------------------------------------------------------------------

    private static <T> T readField(Object target, String name, Class<T> type) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return type.cast(f.get(target));
    }
}
