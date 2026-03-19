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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Multi-server tests: 2 RemoteFileServer instances, verifying distribution and recovery.
 *
 * @author enrico.olivelli
 */
public class MultiServerClientTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server1;
    private RemoteFileServer server2;
    private RemoteFileServiceClient client;

    @Before
    public void setUp() throws Exception {
        server1 = new RemoteFileServer(0, folder.newFolder("data1").toPath());
        server1.start();
        server2 = new RemoteFileServer(0, folder.newFolder("data2").toPath());
        server2.start();

        client = new RemoteFileServiceClient(Arrays.asList(
                "localhost:" + server1.getPort(),
                "localhost:" + server2.getPort()
        ));
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        server1.stop();
        server2.stop();
    }

    @Test
    public void testWriteAndReadBack() {
        int total = 50;
        for (int i = 0; i < total; i++) {
            byte[] content = ("content-" + i).getBytes(StandardCharsets.UTF_8);
            client.writeFile("ts1/uuid" + i + "/data/" + i + ".page", content);
        }

        for (int i = 0; i < total; i++) {
            byte[] read = client.readFile("ts1/uuid" + i + "/data/" + i + ".page");
            assertNotNull("Expected to read file " + i, read);
            assertArrayEquals(("content-" + i).getBytes(StandardCharsets.UTF_8), read);
        }
    }

    @Test
    public void testDistributionAcrossServers() {
        int total = 100;
        Map<String, Integer> distribution = new HashMap<>();
        distribution.put("localhost:" + server1.getPort(), 0);
        distribution.put("localhost:" + server2.getPort(), 0);

        for (int i = 0; i < total; i++) {
            String path = "ts1/uuid" + i + "/data/" + i + ".page";
            String server = client.getServerForPath(path);
            distribution.merge(server, 1, Integer::sum);
        }

        // Both servers should receive files
        for (Map.Entry<String, Integer> entry : distribution.entrySet()) {
            assertTrue("Server " + entry.getKey() + " got no files",
                    entry.getValue() > 0);
        }
    }

    @Test
    public void testDeleteAndVerifyGone() {
        String path = "ts1/uuid99/data/1.page";
        client.writeFile(path, "data".getBytes(StandardCharsets.UTF_8));
        assertNotNull(client.readFile(path));

        assertTrue(client.deleteFile(path));
        assertNull(client.readFile(path));
        assertFalse(client.deleteFile(path));
    }

    @Test
    public void testListAcrossServers() {
        int total = 40;
        for (int i = 0; i < total; i++) {
            client.writeFile("common/uuid" + i + "/data/" + i + ".page",
                    ("v" + i).getBytes(StandardCharsets.UTF_8));
        }

        List<String> all = client.listFiles("common/");
        assertTrue("Expected all " + total + " files, got " + all.size(), all.size() == total);
    }

    @Test
    public void testDeleteByPrefix() {
        for (int i = 0; i < 10; i++) {
            client.writeFile("del/uuid" + i + "/data/1.page", "x".getBytes(StandardCharsets.UTF_8));
        }
        client.writeFile("keep/uuid0/data/1.page", "y".getBytes(StandardCharsets.UTF_8));

        int deleted = client.deleteByPrefix("del/");
        assertTrue("Expected 10 deletions, got " + deleted, deleted == 10);

        // The keep file should still be there
        assertNotNull(client.readFile("keep/uuid0/data/1.page"));

        // del files should be gone
        List<String> remaining = client.listFiles("del/");
        assertTrue("Expected no remaining del files, got " + remaining.size(), remaining.isEmpty());
    }
}
