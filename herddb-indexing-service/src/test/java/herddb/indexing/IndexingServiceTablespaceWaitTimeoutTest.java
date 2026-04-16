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

import static org.junit.Assert.fail;
import herddb.mem.MemoryMetadataStorageManager;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that {@link IndexingServiceEngine.start()} times out with a configurable
 * timeout when the tablespace is never available.
 *
 * @author enrico.olivelli
 */
public class IndexingServiceTablespaceWaitTimeoutTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test(timeout = 10000)
    public void testTablespaceWaitTimeout() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        // Create a metadata storage manager that never returns the tablespace
        MemoryMetadataStorageManager metadataManager = new MemoryMetadataStorageManager();
        metadataManager.start();
        // Intentionally do NOT register the tablespace

        // Configure with a very short timeout for the test (100ms)
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_POLL_INTERVAL_MS, "50");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_TIMEOUT_MS, "100");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(metadataManager);
        IndexingServer server = new IndexingServer("localhost", 0, engine, config);
        server.start();

        try {
            engine.start();
            fail("Expected RuntimeException due to tablespace wait timeout");
        } catch (RuntimeException e) {
            // Expected: timeout while waiting for tablespace
            if (!e.getMessage().contains("Timed out") || !e.getMessage().contains("tablespace")) {
                throw e;
            }
        } finally {
            engine.close();
            server.close();
        }
    }
}
