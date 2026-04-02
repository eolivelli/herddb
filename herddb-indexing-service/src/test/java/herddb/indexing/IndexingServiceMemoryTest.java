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

import static org.junit.Assert.*;

import herddb.mem.MemoryMetadataStorageManager;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for MemoryManager and DataStorageManager integration with IndexingServiceEngine.
 *
 * @author enrico.olivelli
 */
public class IndexingServiceMemoryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static MemoryMetadataStorageManager createTestMetadata() throws Exception {
        MemoryMetadataStorageManager m = new MemoryMetadataStorageManager();
        m.start();
        m.ensureDefaultTableSpace("local", "local", 0, 1);
        return m;
    }

    @Test
    public void testEngineStartsWithMemoryStorage() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());
        IndexingServer server = new IndexingServer("localhost", 0, engine, config);
        try {
            server.start();
            engine.start();

            assertNotNull("MemoryManager should be set after server start",
                    engine.getMemoryManager());
            assertNotNull("DataStorageManager should be set after server start",
                    engine.getDataStorageManager());
        } finally {
            engine.close();
            server.close();
        }
    }

    @Test
    public void testMemoryManagerDefaults() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        IndexingServerConfiguration config = new IndexingServerConfiguration();
        IndexingServer server = new IndexingServer("localhost", 0,
                new IndexingServiceEngine(logDir, dataDir, config), config);

        // Test buildMemoryManager with defaults (auto memory)
        var memoryManager = server.buildMemoryManager();
        assertNotNull(memoryManager);
        assertTrue("maxDataUsedMemory should be > 0", memoryManager.getMaxDataUsedMemory() > 0);
        assertEquals("default page size should be 1 MB",
                IndexingServerConfiguration.PROPERTY_MEMORY_PAGE_SIZE_DEFAULT,
                memoryManager.getMaxLogicalPageSize());
    }

    @Test
    public void testMemoryManagerExplicitLimit() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        long explicitLimit = 64 * 1024 * 1024L; // 64 MB
        props.setProperty(IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_LIMIT,
                String.valueOf(explicitLimit));
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServer server = new IndexingServer("localhost", 0,
                new IndexingServiceEngine(logDir, dataDir, config), config);

        var memoryManager = server.buildMemoryManager();
        assertEquals("maxDataUsedMemory should match explicit limit",
                explicitLimit, memoryManager.getMaxDataUsedMemory());
    }

    @Test
    public void testBuildDataStorageManagerFile() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        IndexingServerConfiguration config = new IndexingServerConfiguration();
        IndexingServer server = new IndexingServer("localhost", 0,
                new IndexingServiceEngine(logDir, dataDir, config), config);

        var dsm = server.buildDataStorageManager(dataDir);
        assertNotNull(dsm);
        assertEquals("herddb.file.FileDataStorageManager", dsm.getClass().getName());
    }

    @Test
    public void testBuildDataStorageManagerMemory() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServer server = new IndexingServer("localhost", 0,
                new IndexingServiceEngine(logDir, dataDir, config), config);

        var dsm = server.buildDataStorageManager(dataDir);
        assertNotNull(dsm);
        assertEquals("herddb.mem.MemoryDataStorageManager", dsm.getClass().getName());
    }

    @Test
    public void testCloseEngineClosesDataStorageManager() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());
        IndexingServer server = new IndexingServer("localhost", 0, engine, config);

        server.start();
        engine.start();

        assertNotNull(engine.getDataStorageManager());

        // Close should not throw
        engine.close();
        server.close();

        // Double close should also not throw
        engine.close();
    }

    @Test
    public void testEngineWithoutMemoryManagerAndDSM() throws Exception {
        // Test backward compatibility: engine works without MemoryManager/DSM
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        IndexingServerConfiguration config = new IndexingServerConfiguration();
        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());

        assertNull("MemoryManager should be null before start", engine.getMemoryManager());
        assertNull("DataStorageManager should be null before start", engine.getDataStorageManager());

        engine.start();

        // Still null because we didn't go through IndexingServer
        assertNull(engine.getMemoryManager());
        assertNull(engine.getDataStorageManager());

        engine.close();
    }
}
