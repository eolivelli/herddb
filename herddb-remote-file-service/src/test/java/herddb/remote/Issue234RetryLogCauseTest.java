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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #234: the retryAsync log must carry the underlying
 * Throwable (with its full cause chain) so the root cause of a CANCELLED
 * gRPC response can be diagnosed from the log alone.
 */
public class Issue234RetryLogCauseTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void retryLogAttachesThrowableWithCauseChain() throws Exception {
        RemoteFileServer server = new RemoteFileServer(0, folder.newFolder("data").toPath());
        server.start();

        // Short per-call timeout and a couple of retries so the test stays fast.
        Map<String, Object> config = new HashMap<>();
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 2L);
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 2);

        List<LogRecord> captured = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger(RemoteFileServiceClient.class.getName());
        Handler captor = new Handler() {
            @Override
            public void publish(LogRecord record) {
                captured.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        captor.setLevel(Level.ALL);
        logger.addHandler(captor);
        Level prevLevel = logger.getLevel();
        logger.setLevel(Level.ALL);

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), config)) {
            // Stop the server so the next client call fails with a transport error
            // (UNAVAILABLE). This triggers the retry path in retryAsync.
            server.stop();

            try {
                client.readFile("ts/none/data/1.page");
            } catch (RuntimeException expected) {
                // We expect the call to fail after retries are exhausted.
            }
        } finally {
            logger.removeHandler(captor);
            logger.setLevel(prevLevel);
            server.stop();
        }

        // Find the "retry N/M" log records and verify at least one carries the
        // Throwable (not just a message string). Without the fix these records
        // would have a null `thrown` field, losing the cause chain.
        List<LogRecord> retries = new java.util.ArrayList<>();
        for (LogRecord r : captured) {
            String msg = r.getMessage();
            if (msg != null && msg.contains("retry") && msg.contains("readFile")) {
                retries.add(r);
            }
        }
        assertTrue("expected at least one retry log record, captured=" + captured.size(),
                !retries.isEmpty());
        LogRecord retry = retries.get(0);
        assertNotNull("retry log must attach the Throwable (setThrown)", retry.getThrown());
        assertEquals(Level.INFO, retry.getLevel());
        // Give the scheduler a moment so no retry handlers keep running
        TimeUnit.MILLISECONDS.sleep(100);
    }
}
