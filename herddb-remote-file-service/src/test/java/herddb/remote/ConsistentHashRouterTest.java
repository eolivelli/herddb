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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Tests for ConsistentHashRouter.
 *
 * @author enrico.olivelli
 */
public class ConsistentHashRouterTest {

    @Test
    public void testRoutesToValidServer() {
        List<String> servers = Arrays.asList("server1:9001", "server2:9002", "server3:9003");
        ConsistentHashRouter router = new ConsistentHashRouter(servers);

        for (int i = 0; i < 100; i++) {
            String server = router.getServer("path/to/file/" + i);
            assertNotNull(server);
            assertTrue("Server not in list: " + server, servers.contains(server));
        }
    }

    @Test
    public void testConsistency() {
        List<String> servers = Arrays.asList("server1:9001", "server2:9002");
        ConsistentHashRouter router = new ConsistentHashRouter(servers);

        // Same path should always go to same server
        String first = router.getServer("ts1/uuid1/data/42.page");
        for (int i = 0; i < 10; i++) {
            assertEquals(first, router.getServer("ts1/uuid1/data/42.page"));
        }
    }

    @Test
    public void testDistribution() {
        List<String> servers = Arrays.asList("server1:9001", "server2:9002");
        ConsistentHashRouter router = new ConsistentHashRouter(servers);

        Map<String, Integer> counts = new HashMap<>();
        for (String s : servers) {
            counts.put(s, 0);
        }

        int total = 1000;
        for (int i = 0; i < total; i++) {
            String server = router.getServer("ts1/uuid" + i + "/data/" + i + ".page");
            counts.merge(server, 1, Integer::sum);
        }

        // Each server should get at least 20% of files (very loose bound for distribution)
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            int count = entry.getValue();
            assertTrue("Server " + entry.getKey() + " got too few: " + count,
                    count > total * 0.2);
        }
    }

    @Test
    public void testSingleServer() {
        ConsistentHashRouter router = new ConsistentHashRouter(Arrays.asList("server1:9001"));
        // All paths must go to the single server
        for (int i = 0; i < 50; i++) {
            assertEquals("server1:9001", router.getServer("path/" + i));
        }
    }
}
