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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.core.ClusterTest;
import herddb.metadata.IndexingServiceInstanceDescriptor;
import herddb.utils.ZKTestEnv;
import java.util.List;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #350: indexing-service registrations are PERSISTENT and survive ZK
 * session expiries / manager close. Operator can remove stale entries via
 * {@link ZookeeperMetadataStorageManager#removeIndexingServiceInstanceRegistration(String)}.
 */
@Category(ClusterTest.class)
public class ZKIndexingServicePersistentRegistrationTest {

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
    public void testRegisterCreatesPersistentZnode() throws Exception {
        try (ZookeeperMetadataStorageManager manager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager.start();
            manager.registerIndexingServiceInstance(
                    IndexingServiceInstanceDescriptor.primary("svc1", "localhost:9850", 0));

            ZooKeeper zk = manager.getZooKeeper();
            Stat stat = new Stat();
            byte[] data = zk.getData(
                    manager.getBasePath() + "/indexingServices/instances/svc1", false, stat);
            assertNotNull(data);
            assertEquals("Registration must be PERSISTENT (ephemeralOwner == 0)",
                    0L, stat.getEphemeralOwner());
        }
    }

    @Test
    public void testRegistrationSurvivesManagerClose() throws Exception {
        ZookeeperMetadataStorageManager manager1 = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath());
        manager1.start();
        manager1.registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.primary("svc1", "localhost:9850", 0));
        manager1.registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.shadow("svc2", "localhost:9851", 0));
        manager1.close();

        // Plenty of time for ZK to notice a closed session; if the znode were
        // ephemeral it would be gone after this sleep.
        Thread.sleep(2000);

        try (ZookeeperMetadataStorageManager manager2 = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager2.start();
            List<IndexingServiceInstanceDescriptor> services =
                    manager2.listIndexingServiceInstances();
            assertEquals(2, services.size());

            IndexingServiceInstanceDescriptor primary = manager2.getIndexingServiceInstanceRegistration("svc1");
            assertNotNull(primary);
            assertEquals("localhost:9850", primary.getAddress());
            assertEquals(IndexingServiceInstanceDescriptor.ROLE_PRIMARY, primary.getRole());
            assertEquals(0, primary.getInstanceId());

            IndexingServiceInstanceDescriptor shadow = manager2.getIndexingServiceInstanceRegistration("svc2");
            assertNotNull(shadow);
            assertEquals("localhost:9851", shadow.getAddress());
            assertEquals(IndexingServiceInstanceDescriptor.ROLE_SHADOW, shadow.getRole());
            assertEquals(0, shadow.getShadowOf());

            assertNull(manager2.getIndexingServiceInstanceRegistration("missing"));

            manager2.removeIndexingServiceInstanceRegistration("svc1");
            assertEquals(1, manager2.listIndexingServiceInstances().size());

            manager2.removeIndexingServiceInstanceRegistration("svc2");
            assertTrue(manager2.listIndexingServiceInstances().isEmpty());

            // Idempotent
            manager2.removeIndexingServiceInstanceRegistration("svc1");
        }
    }

    @Test
    public void testReRegisterUpdatesPayload() throws Exception {
        try (ZookeeperMetadataStorageManager manager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager.start();
            manager.registerIndexingServiceInstance(
                    IndexingServiceInstanceDescriptor.primary("svc1", "old-host:9850", 0));
            manager.registerIndexingServiceInstance(
                    IndexingServiceInstanceDescriptor.primary("svc1", "new-host:9850", 0));

            IndexingServiceInstanceDescriptor d = manager.getIndexingServiceInstanceRegistration("svc1");
            assertNotNull(d);
            assertEquals("new-host:9850", d.getAddress());
            assertEquals(1, manager.listIndexingServiceInstances().size());
        }
    }

    @Test
    public void testRemoveIsIdempotent() throws Exception {
        try (ZookeeperMetadataStorageManager manager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager.start();
            manager.removeIndexingServiceInstanceRegistration("never-existed");
            manager.registerIndexingServiceInstance(
                    IndexingServiceInstanceDescriptor.primary("svc1", "localhost:9850", 0));
            manager.removeIndexingServiceInstanceRegistration("svc1");
            manager.removeIndexingServiceInstanceRegistration("svc1");
            assertTrue(manager.listIndexingServiceInstances().isEmpty());
        }
    }
}
