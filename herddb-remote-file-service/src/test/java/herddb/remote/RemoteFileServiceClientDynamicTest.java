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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for dynamic server list updates in RemoteFileServiceClient.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceClientDynamicTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server1;
    private RemoteFileServer server2;

    @Before
    public void setUp() throws Exception {
        Path dataDir1 = folder.newFolder("data1").toPath();
        Path dataDir2 = folder.newFolder("data2").toPath();
        server1 = new RemoteFileServer(0, dataDir1);
        server1.start();
        server2 = new RemoteFileServer(0, dataDir2);
        server2.start();
    }

    @After
    public void tearDown() throws Exception {
        if (server1 != null) {
            server1.stop();
        }
        if (server2 != null) {
            server2.stop();
        }
    }

    @Test
    public void testUpdateServersAddsNewServer() throws Exception {
        String addr1 = "localhost:" + server1.getPort();
        String addr2 = "localhost:" + server2.getPort();

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Collections.singletonList(addr1))) {

            // Write a file using server1 only
            client.writeFile("test/file1.dat", "hello".getBytes());
            assertNotNull(client.readFile("test/file1.dat"));

            // Update to include both servers
            client.updateServers(Arrays.asList(addr1, addr2));

            // Verify distribution across both servers
            Set<String> serversUsed = new HashSet<>();
            for (int i = 0; i < 20; i++) {
                serversUsed.add(client.getServerForPath("test/file" + i + ".dat"));
            }
            assertEquals("Files should distribute across both servers", 2, serversUsed.size());
        }
    }

    @Test
    public void testUpdateServersRemovesServer() throws Exception {
        String addr1 = "localhost:" + server1.getPort();
        String addr2 = "localhost:" + server2.getPort();

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Arrays.asList(addr1, addr2))) {

            // Update to only server2
            client.updateServers(Collections.singletonList(addr2));

            // All paths should now route to server2
            for (int i = 0; i < 10; i++) {
                assertEquals(addr2, client.getServerForPath("test/file" + i + ".dat"));
            }
        }
    }

    @Test
    public void testUpdateServersEmptyListKeepsOld() throws Exception {
        String addr1 = "localhost:" + server1.getPort();

        try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                Collections.singletonList(addr1))) {

            // Empty list should be ignored
            client.updateServers(Collections.emptyList());

            // Should still work
            client.writeFile("test/file.dat", "hello".getBytes());
            assertNotNull(client.readFile("test/file.dat"));
        }
    }
}
