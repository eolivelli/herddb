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

import herddb.mem.MemoryMetadataStorageManager;
import herddb.metadata.MetadataStorageManager;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Test helper that starts an IndexingService in-process (engine + gRPC server)
 * on a random port. Provides the gRPC address for creating clients.
 * <p>
 * Usage:
 * <pre>
 * try (EmbeddedIndexingService eis = new EmbeddedIndexingService(logDir, dataDir)) {
 *     eis.start();
 *     IndexingServiceClient client = new IndexingServiceClient(
 *             Arrays.asList("localhost:" + eis.getPort()));
 *     // ... use client ...
 * }
 * </pre>
 *
 * @author enrico.olivelli
 */
public class EmbeddedIndexingService implements AutoCloseable {

    private final Path logDirectory;
    private final Path dataDirectory;
    private final IndexingServerConfiguration config;
    private MetadataStorageManager metadataStorageManager;
    private IndexingServiceEngine engine;
    private IndexingServer server;

    public EmbeddedIndexingService(Path logDirectory, Path dataDirectory) {
        this(logDirectory, dataDirectory, defaultTestConfig());
    }

    public EmbeddedIndexingService(Path logDirectory, Path dataDirectory, IndexingServerConfiguration config) {
        this.logDirectory = logDirectory;
        this.dataDirectory = dataDirectory;
        this.config = config;
    }

    public EmbeddedIndexingService(Path logDirectory, Path dataDirectory, int instanceId, int numInstances) {
        this(logDirectory, dataDirectory, defaultTestConfig(instanceId, numInstances));
    }

    private static IndexingServerConfiguration defaultTestConfig() {
        return defaultTestConfig(0, 1);
    }

    private static IndexingServerConfiguration defaultTestConfig(int instanceId, int numInstances) {
        java.util.Properties props = new java.util.Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID, String.valueOf(instanceId));
        props.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, String.valueOf(numInstances));
        return new IndexingServerConfiguration(props);
    }

    public void setMetadataStorageManager(MetadataStorageManager metadataStorageManager) {
        this.metadataStorageManager = metadataStorageManager;
    }

    public void start() throws Exception {
        engine = new IndexingServiceEngine(logDirectory, dataDirectory, config);

        if (metadataStorageManager == null) {
            // Tests use an in-memory metadata manager with a default tablespace
            MemoryMetadataStorageManager memMeta = new MemoryMetadataStorageManager();
            memMeta.start();
            memMeta.ensureDefaultTableSpace("local", "local", 0, 1);
            metadataStorageManager = memMeta;
        }
        engine.setMetadataStorageManager(metadataStorageManager);

        // Start server first so it wires MemoryManager and DataStorageManager
        // onto the engine before the engine starts and configures its VectorStoreFactory
        server = new IndexingServer("localhost", 0, engine, config);
        server.start();
        engine.start();
    }

    public int getPort() {
        return server.getPort();
    }

    public String getAddress() {
        return "localhost:" + getPort();
    }

    public IndexingServiceEngine getEngine() {
        return engine;
    }

    public IndexingServiceClient createClient() {
        return new IndexingServiceClient(Arrays.asList(getAddress()));
    }

    @Override
    public void close() throws Exception {
        if (server != null) {
            server.stop();
        }
        if (engine != null) {
            engine.close();
        }
    }
}
