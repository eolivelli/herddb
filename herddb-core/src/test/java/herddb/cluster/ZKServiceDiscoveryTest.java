/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

public class ZKServiceDiscoveryTest {

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
    public void testRegisterAndListIndexingServices() throws Exception {
        try (ZookeeperMetadataStorageManager manager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager.start();

            assertTrue(manager.listIndexingServices().isEmpty());

            manager.registerIndexingService("svc1", "localhost:9850");
            List<String> services = manager.listIndexingServices();
            assertEquals(1, services.size());
            assertEquals("localhost:9850", services.get(0));

            manager.registerIndexingService("svc2", "localhost:9851");
            services = manager.listIndexingServices();
            assertEquals(2, services.size());
            assertTrue(services.contains("localhost:9850"));
            assertTrue(services.contains("localhost:9851"));

            manager.unregisterIndexingService("svc1");
            services = manager.listIndexingServices();
            assertEquals(1, services.size());
            assertEquals("localhost:9851", services.get(0));
        }
    }

    @Test
    public void testRegisterAndListFileServers() throws Exception {
        try (ZookeeperMetadataStorageManager manager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager.start();

            assertTrue(manager.listFileServers().isEmpty());

            manager.registerFileServer("fs1", "localhost:9846");
            List<String> servers = manager.listFileServers();
            assertEquals(1, servers.size());
            assertEquals("localhost:9846", servers.get(0));

            manager.unregisterFileServer("fs1");
            assertTrue(manager.listFileServers().isEmpty());
        }
    }

    @Test
    public void testServiceDiscoveryListener() throws Exception {
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

            // First call installs the watch
            manager.listIndexingServices();

            // This should trigger the watcher
            manager.registerIndexingService("svc1", "localhost:9850");

            assertTrue("Listener should be notified", latch.await(10, TimeUnit.SECONDS));
            assertTrue(receivedAddresses.contains("localhost:9850"));
        }
    }

    @Test
    public void testEphemeralNodeDisappearsOnClose() throws Exception {
        // Manager 1 registers a service
        ZookeeperMetadataStorageManager manager1 = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath());
        manager1.start();
        manager1.registerIndexingService("svc1", "localhost:9850");

        // Manager 2 sees it
        try (ZookeeperMetadataStorageManager manager2 = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager2.start();
            assertEquals(1, manager2.listIndexingServices().size());

            // Close manager1 - ephemeral node should disappear
            manager1.close();

            // Wait for ZK to detect session close
            Thread.sleep(2000);

            List<String> services = manager2.listIndexingServices();
            assertTrue("Ephemeral node should be gone after close", services.isEmpty());
        }
    }
}
