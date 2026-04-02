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

package herddb.indexing;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.TableSpace;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that IndexingServiceEngine polls for tablespace availability instead of failing immediately.
 *
 * @author enrico.olivelli
 */
public class IndexingServiceTablespaceWaitTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Engine must wait until the tablespace is registered, then start successfully.
     */
    @Test
    public void testStartWaitsForTablespace() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        MemoryMetadataStorageManager metadataManager = new MemoryMetadataStorageManager();
        metadataManager.start();
        // Do NOT register the tablespace yet

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_POLL_INTERVAL_MS, "200");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(metadataManager);
        IndexingServer server = new IndexingServer("localhost", 0, engine, config);

        // Register the tablespace in a background thread after a short delay
        Thread registrar = new Thread(() -> {
            try {
                Thread.sleep(500);
                metadataManager.registerTableSpace(TableSpace.builder()
                        .name(TableSpace.DEFAULT).uuid("test-uuid")
                        .leader("local").replica("local").build());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        registrar.setDaemon(true);
        registrar.start();

        try {
            server.start();
            engine.start();
            // If we reach here the engine resolved the tablespace successfully
        } finally {
            engine.close();
            server.close();
        }
        registrar.join(5_000);
    }

    /**
     * Engine must stop polling and propagate InterruptedException when interrupted during the wait.
     */
    @Test
    public void testStartInterruptedWhileWaiting() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        MemoryMetadataStorageManager metadataManager = new MemoryMetadataStorageManager();
        metadataManager.start();
        // Do NOT register the tablespace — engine will poll forever unless interrupted

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_POLL_INTERVAL_MS, "200");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(metadataManager);
        IndexingServer server = new IndexingServer("localhost", 0, engine, config);
        server.start();

        AtomicReference<Throwable> caught = new AtomicReference<>();
        Thread startThread = new Thread(() -> {
            try {
                engine.start();
            } catch (Throwable t) {
                caught.set(t);
            }
        });
        startThread.start();

        // Give the engine time to enter the polling loop, then interrupt it
        Thread.sleep(300);
        startThread.interrupt();
        startThread.join(5_000);

        server.close();
        engine.close();

        Throwable t = caught.get();
        if (t == null) {
            fail("Expected an exception due to interruption, but engine.start() returned normally");
        }
        boolean isInterrupted = (t instanceof InterruptedException)
                || (t.getCause() instanceof InterruptedException);
        assertTrue("Expected InterruptedException, got: " + t, isInterrupted);
    }
}
