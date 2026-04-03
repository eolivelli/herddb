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

package herddb.server;

import static org.junit.Assert.*;

import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.metadata.ServiceDiscoveryListener;
import herddb.utils.ZKTestEnv;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ServerDiscoveryWiringTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private ZKTestEnv testEnv;

    @Before
    public void before() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void after() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Test
    public void testIndexingServiceRegistrationAndDiscovery() throws Exception {
        try (ZookeeperMetadataStorageManager manager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager.start();

            // Register an indexing service
            manager.registerIndexingService("svc1", "localhost:9850");

            // Verify discovery
            List<String> services = manager.listIndexingServices();
            assertEquals(1, services.size());
            assertEquals("localhost:9850", services.get(0));

            // Unregister
            manager.unregisterIndexingService("svc1");
            assertTrue(manager.listIndexingServices().isEmpty());
        }
    }

    @Test
    public void testFileServerRegistrationAndDiscovery() throws Exception {
        try (ZookeeperMetadataStorageManager manager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager.start();

            manager.registerFileServer("fs1", "localhost:9846");

            List<String> servers = manager.listFileServers();
            assertEquals(1, servers.size());
            assertEquals("localhost:9846", servers.get(0));

            manager.unregisterFileServer("fs1");
            assertTrue(manager.listFileServers().isEmpty());
        }
    }

    @Test
    public void testServiceDiscoveryListenerNotification() throws Exception {
        try (ZookeeperMetadataStorageManager manager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager.start();

            CountDownLatch latch = new CountDownLatch(1);
            List<String> receivedAddresses = new CopyOnWriteArrayList<>();

            manager.addServiceDiscoveryListener(new ServiceDiscoveryListener() {
                @Override
                public void onIndexingServicesChanged(List<String> currentAddresses) {
                    receivedAddresses.addAll(currentAddresses);
                    latch.countDown();
                }

                @Override
                public void onFileServersChanged(List<String> currentAddresses) {
                }
            });

            // Install the watch by listing first
            manager.listIndexingServices();

            // Register — this triggers the watcher
            manager.registerIndexingService("svc1", "localhost:9850");

            assertTrue("Listener should be notified within 10s", latch.await(10, TimeUnit.SECONDS));
            assertTrue(receivedAddresses.contains("localhost:9850"));
        }
    }
}
