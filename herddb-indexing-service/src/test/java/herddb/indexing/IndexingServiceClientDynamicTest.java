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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import herddb.log.LogSequenceNumber;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IndexingServiceClientDynamicTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private EmbeddedIndexingService service1;
    private EmbeddedIndexingService service2;
    private IndexingServiceClient client;

    @Before
    public void setUp() throws Exception {
        Path logDir1 = folder.newFolder("log1").toPath();
        Path dataDir1 = folder.newFolder("data1").toPath();
        service1 = new EmbeddedIndexingService(logDir1, dataDir1);
        service1.start();

        Path logDir2 = folder.newFolder("log2").toPath();
        Path dataDir2 = folder.newFolder("data2").toPath();
        service2 = new EmbeddedIndexingService(logDir2, dataDir2);
        service2.start();
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (service1 != null) {
            service1.close();
        }
        if (service2 != null) {
            service2.close();
        }
    }

    @Test
    public void testDynamicServerUpdates() throws Exception {
        String addr1 = service1.getAddress();
        String addr2 = service2.getAddress();

        // Start with only server1
        client = new IndexingServiceClient(Collections.singletonList(addr1));
        assertEquals(1, client.getServers().size());
        assertEquals(addr1, client.getServers().get(0));

        // Add server2
        client.updateServers(Arrays.asList(addr1, addr2));
        assertEquals(2, client.getServers().size());

        // Remove server1, keep only server2
        client.updateServers(Collections.singletonList(addr2));
        assertEquals(1, client.getServers().size());
        assertEquals(addr2, client.getServers().get(0));

        // Verify search still works after update (empty index returns empty results)
        client.search("herd", "testtable", "testindex", new float[]{1.0f, 2.0f, 3.0f}, 10);
    }

    @Test
    public void testUpdateServersEmptyListKeepsCurrent() throws Exception {
        String addr1 = service1.getAddress();

        client = new IndexingServiceClient(Collections.singletonList(addr1));
        assertEquals(1, client.getServers().size());

        // Empty list should be ignored
        client.updateServers(Collections.emptyList());

        // Server list should be unchanged
        assertEquals(1, client.getServers().size());
        assertEquals(addr1, client.getServers().get(0));

        // Search should still work
        client.search("herd", "testtable", "testindex", new float[]{1.0f, 2.0f, 3.0f}, 10);
    }

    @Test
    public void testSearchThrowsWithEmptyServerList() {
        client = new IndexingServiceClient(Collections.emptyList());

        try {
            client.search("herd", "testtable", "testindex", new float[]{1.0f, 2.0f, 3.0f}, 10);
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            assertEquals("No indexing service instances available", e.getMessage());
        }
    }

    @Test
    public void testGetIndexStatusThrowsWithEmptyServerList() {
        client = new IndexingServiceClient(Collections.emptyList());

        try {
            client.getIndexStatus("herd", "testtable", "testindex");
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            assertEquals("No indexing service instances available", e.getMessage());
        }
    }

    @Test
    public void testWaitForCatchUpReturnsFalseWithEmptyServerList() throws Exception {
        client = new IndexingServiceClient(Collections.emptyList());

        boolean result = client.waitForCatchUp("herd", new LogSequenceNumber(1, 0), 5000);
        assertFalse("waitForCatchUp should return false when no servers are available", result);
    }
}
