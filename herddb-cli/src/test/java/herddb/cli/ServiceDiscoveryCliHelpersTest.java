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
package herddb.cli;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.metadata.IndexingServiceInstanceDescriptor;
import herddb.metadata.MetadataStorageManager;
import herddb.model.DDLException;
import herddb.model.TableSpace;
import herddb.model.TableSpaceReplicaState;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Issue #350: unit-tests for the new CLI helpers that list/describe/remove
 * file-server and indexing-service registrations. Drives the helpers with an
 * in-memory {@link MetadataStorageManager} subclass and captures
 * {@link System#out}.
 */
public class ServiceDiscoveryCliHelpersTest {

    private PrintStream originalOut;
    private ByteArrayOutputStream capturedOut;
    private InMemoryMetadataStorageManager metadata;

    @Before
    public void before() {
        originalOut = System.out;
        capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut));
        metadata = new InMemoryMetadataStorageManager();
    }

    @After
    public void after() {
        System.setOut(originalOut);
    }

    private String output() {
        System.out.flush();
        return capturedOut.toString();
    }

    @Test
    public void testPrintFileServersEmpty() throws Exception {
        HerdDBCLI.printFileServers(metadata);
        String out = output();
        assertTrue(out, out.contains("No file-server registrations to show"));
    }

    @Test
    public void testPrintFileServers() throws Exception {
        metadata.registerFileServer("fs-0", "10.0.0.1:9846");
        metadata.registerFileServer("fs-1", "10.0.0.2:9846");
        HerdDBCLI.printFileServers(metadata);
        String out = output();
        assertTrue(out, out.contains("File servers"));
        assertTrue(out, out.contains("10.0.0.1:9846"));
        assertTrue(out, out.contains("10.0.0.2:9846"));
    }

    @Test
    public void testDescribeFileServerFound() throws Exception {
        metadata.registerFileServer("fs-0", "10.0.0.1:9846");
        HerdDBCLI.describeFileServer(metadata, "fs-0");
        String out = output();
        assertTrue(out, out.contains("fs-0"));
        assertTrue(out, out.contains("10.0.0.1:9846"));
    }

    @Test
    public void testDescribeFileServerMissing() throws Exception {
        HerdDBCLI.describeFileServer(metadata, "missing");
        String out = output();
        assertTrue(out, out.contains("not found"));
    }

    @Test
    public void testRemoveFileServerSucceeds() throws Exception {
        metadata.registerFileServer("fs-0", "10.0.0.1:9846");
        HerdDBCLI.removeFileServer(metadata, "fs-0");
        String out = output();
        assertTrue(out, out.contains("Removed file-server registration"));
        assertTrue(out, out.contains("fs-0"));
        assertTrue(out, out.contains("10.0.0.1:9846"));
        // Was actually removed
        assertTrue(metadata.listFileServers().isEmpty());
    }

    @Test
    public void testRemoveFileServerMissing() throws Exception {
        HerdDBCLI.removeFileServer(metadata, "missing");
        String out = output();
        assertTrue(out, out.contains("nothing to remove"));
        assertFalse(out, out.contains("Removed file-server registration"));
    }

    @Test
    public void testPrintIndexingServiceInstancesEmpty() throws Exception {
        HerdDBCLI.printIndexingServiceInstances(metadata);
        String out = output();
        assertTrue(out, out.contains("No indexing-service registrations to show"));
    }

    @Test
    public void testPrintIndexingServiceInstances() throws Exception {
        metadata.registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.primary("svc-0", "10.0.0.10:9850", 0));
        metadata.registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.shadow("svc-1", "10.0.0.11:9850", 0));
        HerdDBCLI.printIndexingServiceInstances(metadata);
        String out = output();
        assertTrue(out, out.contains("svc-0"));
        assertTrue(out, out.contains("10.0.0.10:9850"));
        assertTrue(out, out.contains("primary"));
        assertTrue(out, out.contains("svc-1"));
        assertTrue(out, out.contains("10.0.0.11:9850"));
        assertTrue(out, out.contains("shadow"));
        assertTrue(out, out.contains("ShadowOf:"));
    }

    @Test
    public void testDescribeIndexingServiceInstanceFound() throws Exception {
        metadata.registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.primary("svc-0", "10.0.0.10:9850", 7));
        HerdDBCLI.describeIndexingServiceInstance(metadata, "svc-0");
        String out = output();
        assertTrue(out, out.contains("svc-0"));
        assertTrue(out, out.contains("10.0.0.10:9850"));
        assertTrue(out, out.contains("primary"));
        assertTrue(out, out.contains("InstanceId: 7"));
    }

    @Test
    public void testDescribeIndexingServiceInstanceMissing() throws Exception {
        HerdDBCLI.describeIndexingServiceInstance(metadata, "missing");
        String out = output();
        assertTrue(out, out.contains("not found"));
    }

    @Test
    public void testRemoveIndexingServiceInstanceSucceeds() throws Exception {
        metadata.registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.primary("svc-0", "10.0.0.10:9850", 0));
        HerdDBCLI.removeIndexingServiceInstance(metadata, "svc-0");
        String out = output();
        assertTrue(out, out.contains("Removed indexing-service registration"));
        assertTrue(out, out.contains("svc-0"));
        assertTrue(out, out.contains("primary"));
        assertTrue(metadata.listIndexingServiceInstances().isEmpty());
    }

    @Test
    public void testRemoveIndexingServiceInstanceMissing() throws Exception {
        HerdDBCLI.removeIndexingServiceInstance(metadata, "missing");
        String out = output();
        assertTrue(out, out.contains("nothing to remove"));
    }

    @Test
    public void testNullManagerEmitsClusterModeMessage() throws Exception {
        HerdDBCLI.printFileServers(null);
        String out = output();
        assertTrue(out, out.contains("not managing a cluster"));
    }

    /**
     * Minimal MetadataStorageManager that backs the file-server and
     * indexing-service registrations with in-memory maps. Tablespace-related
     * abstract methods are stubbed; other defaults from the abstract base
     * are inherited as no-ops.
     */
    private static final class InMemoryMetadataStorageManager extends MetadataStorageManager {

        private final Map<String, String> fileServers = new HashMap<>();
        private final Map<String, IndexingServiceInstanceDescriptor> indexingServices = new HashMap<>();

        @Override
        public void start() {
        }

        @Override
        public boolean ensureDefaultTableSpace(String localNodeId, String initialReplicaList,
                long maxLeaderInactivityTime, int replicationFactor) {
            return false;
        }

        @Override
        public void close() {
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
        public void registerTableSpace(TableSpace tableSpace) throws DDLException {
        }

        @Override
        public void dropTableSpace(String name, TableSpace previous) throws DDLException {
        }

        @Override
        public boolean updateTableSpace(TableSpace tableSpace, TableSpace previous) throws DDLException {
            return false;
        }

        @Override
        public void updateTableSpaceReplicaState(TableSpaceReplicaState state) {
        }

        @Override
        public List<TableSpaceReplicaState> getTableSpaceReplicaState(String tableSpaceUuid) {
            return Collections.emptyList();
        }

        // ----- file-server registry (issue #350) -----
        @Override
        public void registerFileServer(String serviceId, String address) {
            fileServers.put(serviceId, address);
        }

        @Override
        public void unregisterFileServer(String serviceId) {
            fileServers.remove(serviceId);
        }

        @Override
        public List<String> listFileServers() {
            return new ArrayList<>(fileServers.values());
        }

        @Override
        public String getFileServerRegistration(String serviceId) {
            return fileServers.get(serviceId);
        }

        @Override
        public void removeFileServerRegistration(String serviceId) {
            fileServers.remove(serviceId);
        }

        // ----- indexing-service registry (issue #350) -----
        @Override
        public void registerIndexingServiceInstance(IndexingServiceInstanceDescriptor descriptor) {
            indexingServices.put(descriptor.getServiceId(), descriptor);
        }

        @Override
        public void unregisterIndexingServiceInstance(String serviceId) {
            indexingServices.remove(serviceId);
        }

        @Override
        public List<IndexingServiceInstanceDescriptor> listIndexingServiceInstances() {
            return new ArrayList<>(indexingServices.values());
        }

        @Override
        public IndexingServiceInstanceDescriptor getIndexingServiceInstanceRegistration(String serviceId) {
            return indexingServices.get(serviceId);
        }

        @Override
        public void removeIndexingServiceInstanceRegistration(String serviceId) {
            indexingServices.remove(serviceId);
        }
    }
}
