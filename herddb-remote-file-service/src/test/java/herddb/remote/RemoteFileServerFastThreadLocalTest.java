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
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that every executor pool created by {@link RemoteFileServer} uses
 * {@link FastThreadLocalThread} so Netty's recyclable-object fast-path is
 * available on all file-server threads (issue #470).
 */
public class RemoteFileServerFastThreadLocalTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void metadataExecutorUsesFastThreadLocal() throws Exception {
        server = new RemoteFileServer("localhost", 0,
                folder.newFolder("data-meta").toPath(), 1, new Properties());
        server.start();

        ThreadPoolExecutor exec = readField(server, "metadataExecutor", ThreadPoolExecutor.class);
        Thread t = runOnExecutor(exec);
        assertTrue("metadataExecutor thread must be FastThreadLocalThread, got: " + t.getClass(),
                t instanceof FastThreadLocalThread);
    }

    @Test
    public void readExecutorUsesFastThreadLocal() throws Exception {
        server = new RemoteFileServer("localhost", 0,
                folder.newFolder("data-read").toPath(), 1, new Properties());
        server.start();

        ThreadPoolExecutor exec = readField(server, "readExecutor", ThreadPoolExecutor.class);
        Thread t = runOnExecutor(exec);
        assertTrue("readExecutor thread must be FastThreadLocalThread, got: " + t.getClass(),
                t instanceof FastThreadLocalThread);
    }

    @Test
    public void writeExecutorUsesFastThreadLocal() throws Exception {
        server = new RemoteFileServer("localhost", 0,
                folder.newFolder("data-write").toPath(), 1, new Properties());
        server.start();

        ThreadPoolExecutor exec = readField(server, "writeExecutor", ThreadPoolExecutor.class);
        Thread t = runOnExecutor(exec);
        assertTrue("writeExecutor thread must be FastThreadLocalThread, got: " + t.getClass(),
                t instanceof FastThreadLocalThread);
    }

    // -----------------------------------------------------------------------

    private static Thread runOnExecutor(ThreadPoolExecutor exec) throws Exception {
        CompletableFuture<Thread> future = new CompletableFuture<>();
        exec.execute(() -> future.complete(Thread.currentThread()));
        return future.get(10, TimeUnit.SECONDS);
    }

    private static <T> T readField(Object target, String name, Class<T> type) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return type.cast(f.get(target));
    }
}
