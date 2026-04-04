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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.metadata.MetadataStorageManager;
import herddb.model.TableSpace;
import herddb.model.TableSpaceReplicaState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that IndexingServer registers/unregisters itself in the MetadataStorageManager on start/stop.
 */
public class IndexingServerRegistrationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testRegistrationOnStartAndUnregistrationOnStop() throws Exception {
        TrackingMetadataStorageManager tracker = new TrackingMetadataStorageManager();

        java.nio.file.Path logDir = folder.newFolder("log").toPath();
        java.nio.file.Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        // Use a real MemoryMetadataStorageManager for the engine (it needs schema info)
        MemoryMetadataStorageManager engineMeta = new MemoryMetadataStorageManager();
        engineMeta.start();
        engineMeta.ensureDefaultTableSpace("local", "local", 0, 1);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(engineMeta);

        IndexingServer server = new IndexingServer("localhost", 0, engine, config);
        server.setMetadataStorageManager(tracker);
        try {
            server.start();

            assertEquals("One indexing service should be registered after start", 1, tracker.registeredIndexingServices.size());
            String registered = tracker.registeredIndexingServices.get(0);
            assertTrue("Registered address should contain the port",
                    registered.startsWith("localhost:") && !registered.equals("localhost:0"));

            server.stop();

            assertTrue("Indexing services should be empty after stop", tracker.registeredIndexingServices.isEmpty());
        } finally {
            try {
                server.close();
            } catch (Exception ignored) {
            }
            engine.close();
            engineMeta.close();
        }
    }

    /**
     * Simple test double that tracks register/unregister calls.
     */
    private static class TrackingMetadataStorageManager extends MetadataStorageManager {
        final List<String> registeredIndexingServices = new ArrayList<>();

        @Override
        public void registerIndexingService(String serviceId, String address) {
            registeredIndexingServices.add(address);
        }

        @Override
        public void unregisterIndexingService(String serviceId) {
            registeredIndexingServices.remove(serviceId);
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
        }

        @Override
        public boolean ensureDefaultTableSpace(String a, String b, long c, int d) {
            return true;
        }

        @Override
        public Collection<String> listTableSpaces() {
            return Collections.emptyList();
        }

        @Override
        public TableSpace describeTableSpace(String name) {
            return null;
        }

        @Override
        public void registerTableSpace(TableSpace ts) {
        }

        @Override
        public void dropTableSpace(String name, TableSpace prev) {
        }

        @Override
        public boolean updateTableSpace(TableSpace ts, TableSpace prev) {
            return true;
        }

        @Override
        public void updateTableSpaceReplicaState(TableSpaceReplicaState state) {
        }

        @Override
        public List<TableSpaceReplicaState> getTableSpaceReplicaState(String uuid) {
            return Collections.emptyList();
        }
    }
}
