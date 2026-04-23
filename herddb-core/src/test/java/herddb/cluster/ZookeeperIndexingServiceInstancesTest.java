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

package herddb.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.core.ClusterTest;
import herddb.metadata.IndexingServiceCheckpointState;
import herddb.metadata.IndexingServiceInstanceDescriptor;
import herddb.metadata.ServiceDiscoveryListener;
import herddb.utils.ZKTestEnv;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the ZK schema for role-aware indexing-service registration and
 * per-primary checkpoint-state publication — the foundation for shadow
 * replicas.
 */
@Category(ClusterTest.class)
public class ZookeeperIndexingServiceInstancesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    private ZookeeperMetadataStorageManager newManager() throws Exception {
        ZookeeperMetadataStorageManager mgr = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath());
        mgr.start();
        return mgr;
    }

    @Test
    public void registerAndListPrimaryAndShadow() throws Exception {
        try (ZookeeperMetadataStorageManager mgr = newManager()) {
            mgr.registerIndexingServiceInstance(
                    IndexingServiceInstanceDescriptor.primary("p0", "host-p0:9850", 0));
            mgr.registerIndexingServiceInstance(
                    IndexingServiceInstanceDescriptor.shadow("s0a", "host-s0a:9850", 0));
            mgr.registerIndexingServiceInstance(
                    IndexingServiceInstanceDescriptor.shadow("s0b", "host-s0b:9850", 0));

            List<IndexingServiceInstanceDescriptor> instances = mgr.listIndexingServiceInstances();
            assertEquals(3, instances.size());

            int primaries = 0;
            int shadows = 0;
            for (IndexingServiceInstanceDescriptor d : instances) {
                if (d.isShadow()) {
                    assertEquals(0, d.getShadowOf());
                    shadows++;
                } else {
                    assertEquals(0, d.getInstanceId());
                    primaries++;
                }
            }
            assertEquals(1, primaries);
            assertEquals(2, shadows);
        }
    }

    @Test
    public void legacyListIndexingServicesReturnsAddresses() throws Exception {
        try (ZookeeperMetadataStorageManager mgr = newManager()) {
            mgr.registerIndexingServiceInstance(
                    IndexingServiceInstanceDescriptor.primary("p0", "host-p0:9850", 0));
            mgr.registerIndexingServiceInstance(
                    IndexingServiceInstanceDescriptor.shadow("s0", "host-s0:9850", 0));

            List<String> addresses = mgr.listIndexingServices();
            assertEquals(2, addresses.size());
            assertTrue(addresses.contains("host-p0:9850"));
            assertTrue(addresses.contains("host-s0:9850"));
        }
    }

    @Test
    public void ephemeralCleanupOnClose() throws Exception {
        try (ZookeeperMetadataStorageManager writer = newManager()) {
            writer.registerIndexingServiceInstance(
                    IndexingServiceInstanceDescriptor.primary("p0", "host-p0:9850", 0));
        }
        // Re-open; the ephemeral znode should be gone because the previous
        // session was closed.
        try (ZookeeperMetadataStorageManager reader = newManager()) {
            // Poll briefly — ephemeral deletion is observed via the session
            // close; ZK may lag by ticks.
            long deadline = System.currentTimeMillis() + 10_000L;
            List<IndexingServiceInstanceDescriptor> instances;
            do {
                instances = reader.listIndexingServiceInstances();
                if (instances.isEmpty()) {
                    break;
                }
                Thread.sleep(50);
            } while (System.currentTimeMillis() < deadline);
            assertEquals(0, instances.size());
        }
    }

    @Test
    public void publishGetAndWatchCheckpointState() throws Exception {
        try (ZookeeperMetadataStorageManager mgr = newManager()) {
            // No state yet
            assertNull(mgr.getIndexingServiceCheckpointState(0));

            IndexingServiceCheckpointState s1 = new IndexingServiceCheckpointState(
                    0, 10L, 100L, 3, 1_700_000_000_000L);
            mgr.publishIndexingServiceCheckpointState(s1);

            IndexingServiceCheckpointState read = mgr.getIndexingServiceCheckpointState(0);
            assertNotNull(read);
            assertEquals(10L, read.getLedgerId());
            assertEquals(100L, read.getOffset());
            assertEquals(3, read.getSegmentCount());

            // Install a watcher; should fire when we publish a new state.
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<IndexingServiceCheckpointState> seen = new AtomicReference<>();
            mgr.watchIndexingServiceCheckpointState(0, state -> {
                seen.set(state);
                latch.countDown();
            });

            IndexingServiceCheckpointState s2 = new IndexingServiceCheckpointState(
                    0, 10L, 200L, 4, 1_700_000_001_000L);
            mgr.publishIndexingServiceCheckpointState(s2);

            assertTrue("watcher did not fire within 5s", latch.await(5, TimeUnit.SECONDS));
            assertEquals(200L, seen.get().getOffset());
            assertEquals(4, seen.get().getSegmentCount());
        }
    }

    @Test
    public void watchFiresOnInitialPublish() throws Exception {
        try (ZookeeperMetadataStorageManager mgr = newManager()) {
            // No state published yet — watchpath installs exists() watch.
            CountDownLatch latch = new CountDownLatch(1);
            mgr.watchIndexingServiceCheckpointState(7, state -> latch.countDown());
            mgr.publishIndexingServiceCheckpointState(
                    new IndexingServiceCheckpointState(7, 1L, 2L, 0, 0L));
            assertTrue("watcher did not fire on first publish", latch.await(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void descriptorListenerFiresOnRegistration() throws Exception {
        try (ZookeeperMetadataStorageManager mgr = newManager()) {
            AtomicReference<List<IndexingServiceInstanceDescriptor>> seen = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            mgr.addServiceDiscoveryListener(new ServiceDiscoveryListener() {
                @Override
                public void onIndexingServiceInstancesChanged(
                        List<IndexingServiceInstanceDescriptor> currentInstances) {
                    // Copy list to avoid mutation races.
                    seen.set(new ArrayList<>(currentInstances));
                    if (currentInstances.size() >= 2) {
                        latch.countDown();
                    }
                }

                @Override
                public void onFileServersChanged(List<String> currentAddresses) {
                }
            });

            // Arm the watch by listing once first.
            mgr.listIndexingServiceInstances();

            mgr.registerIndexingServiceInstance(
                    IndexingServiceInstanceDescriptor.primary("p0", "host-p0:9850", 0));
            mgr.registerIndexingServiceInstance(
                    IndexingServiceInstanceDescriptor.shadow("s0", "host-s0:9850", 0));

            assertTrue("descriptor listener did not observe both registrations",
                    latch.await(5, TimeUnit.SECONDS));
            List<IndexingServiceInstanceDescriptor> last = seen.get();
            boolean hasPrimary = last.stream().anyMatch(d -> !d.isShadow() && "p0".equals(d.getServiceId()));
            boolean hasShadow = last.stream().anyMatch(d -> d.isShadow() && "s0".equals(d.getServiceId()));
            assertTrue(hasPrimary);
            assertTrue(hasShadow);
        }
    }
}
