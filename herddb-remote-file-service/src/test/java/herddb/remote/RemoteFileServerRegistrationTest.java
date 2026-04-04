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
import static org.junit.Assert.assertTrue;
import herddb.metadata.MetadataStorageManager;
import herddb.model.TableSpace;
import herddb.model.TableSpaceReplicaState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that RemoteFileServer registers/unregisters itself in the MetadataStorageManager on start/stop.
 */
public class RemoteFileServerRegistrationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testRegistrationOnStartAndUnregistrationOnStop() throws Exception {
        TrackingMetadataStorageManager tracker = new TrackingMetadataStorageManager();

        java.nio.file.Path dataDir = folder.newFolder("data").toPath();

        RemoteFileServer server = new RemoteFileServer("localhost", 0, dataDir);
        server.setMetadataStorageManager(tracker);
        try {
            server.start();

            assertEquals("One file server should be registered after start", 1, tracker.registeredFileServers.size());
            String registered = tracker.registeredFileServers.get(0);
            assertTrue("Registered address should contain the port",
                    registered.startsWith("localhost:") && !registered.equals("localhost:0"));

            server.stop();

            assertTrue("File servers should be empty after stop", tracker.registeredFileServers.isEmpty());
        } finally {
            try {
                server.close();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Simple test double that tracks register/unregister calls.
     */
    private static class TrackingMetadataStorageManager extends MetadataStorageManager {
        final List<String> registeredFileServers = new ArrayList<>();

        @Override
        public void registerFileServer(String serviceId, String address) {
            registeredFileServers.add(address);
        }

        @Override
        public void unregisterFileServer(String serviceId) {
            registeredFileServers.remove(serviceId);
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
