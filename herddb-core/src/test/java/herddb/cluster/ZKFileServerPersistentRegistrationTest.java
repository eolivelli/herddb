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
 * Issue #350: file-server registrations are PERSISTENT and survive ZK
 * session expiries / manager close. Operator can remove stale entries via
 * {@link ZookeeperMetadataStorageManager#removeFileServerRegistration(String)}.
 */
@Category(ClusterTest.class)
public class ZKFileServerPersistentRegistrationTest {

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

    /** registerFileServer creates a PERSISTENT znode (ephemeralOwner == 0). */
    @Test
    public void testRegisterCreatesPersistentZnode() throws Exception {
        try (ZookeeperMetadataStorageManager manager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager.start();
            manager.registerFileServer("fs1", "localhost:9846");

            ZooKeeper zk = manager.getZooKeeper();
            Stat stat = new Stat();
            byte[] data = zk.getData(manager.getBasePath() + "/fileServers/fs1", false, stat);
            assertNotNull(data);
            assertEquals("Registration must be PERSISTENT (ephemeralOwner == 0)",
                    0L, stat.getEphemeralOwner());
        }
    }

    /** Registration survives the registering manager closing its session. */
    @Test
    public void testRegistrationSurvivesManagerClose() throws Exception {
        ZookeeperMetadataStorageManager manager1 = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath());
        manager1.start();
        manager1.registerFileServer("fs1", "localhost:9846");
        manager1.close();

        // Plenty of time for ZK to notice a closed session; if the znode were
        // ephemeral it would be gone after this sleep.
        Thread.sleep(2000);

        try (ZookeeperMetadataStorageManager manager2 = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager2.start();
            List<String> servers = manager2.listFileServers();
            assertEquals(1, servers.size());
            assertEquals("localhost:9846", servers.get(0));

            assertEquals("localhost:9846", manager2.getFileServerRegistration("fs1"));
            assertNull(manager2.getFileServerRegistration("missing"));

            manager2.removeFileServerRegistration("fs1");
            assertTrue(manager2.listFileServers().isEmpty());
            // Idempotent
            manager2.removeFileServerRegistration("fs1");
        }
    }

    /** Re-registering with a different address from a fresh session updates the payload. */
    @Test
    public void testReRegisterUpdatesPayload() throws Exception {
        try (ZookeeperMetadataStorageManager manager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager.start();
            manager.registerFileServer("fs1", "old-host:9846");
            assertEquals("old-host:9846", manager.getFileServerRegistration("fs1"));

            // Same id, different address — must update via setData on the
            // existing PERSISTENT znode.
            manager.registerFileServer("fs1", "new-host:9846");
            assertEquals("new-host:9846", manager.getFileServerRegistration("fs1"));
            assertEquals(1, manager.listFileServers().size());
        }
    }

    /** removeFileServerRegistration is idempotent and tolerates an unknown id. */
    @Test
    public void testRemoveIsIdempotent() throws Exception {
        try (ZookeeperMetadataStorageManager manager = new ZookeeperMetadataStorageManager(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath())) {
            manager.start();
            manager.removeFileServerRegistration("never-existed");
            manager.registerFileServer("fs1", "localhost:9846");
            manager.removeFileServerRegistration("fs1");
            manager.removeFileServerRegistration("fs1");
            assertTrue(manager.listFileServers().isEmpty());
        }
    }
}
