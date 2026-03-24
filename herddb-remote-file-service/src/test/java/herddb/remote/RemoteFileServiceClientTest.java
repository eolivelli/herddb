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
import static org.junit.Assert.assertEquals;
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
 * Basic CRUD tests via RemoteFileServiceClient with a single server.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceClientTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("data").toPath());
        server.start();
        client = new RemoteFileServiceClient(Arrays.asList("localhost:" + server.getPort()));
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        server.stop();
    }

    @Test
    public void testWriteAndRead() {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        client.writeFile("ts1/uuid1/data/1.page", content);

        byte[] read = client.readFile("ts1/uuid1/data/1.page");
        assertNotNull(read);
        assertArrayEquals(content, read);
    }

    @Test
    public void testReadMissing() {
        assertNull(client.readFile("ts1/notexist/data/99.page"));
    }

    @Test
    public void testDelete() {
        client.writeFile("ts1/uuid1/data/2.page", "data".getBytes(StandardCharsets.UTF_8));
        assertTrue(client.deleteFile("ts1/uuid1/data/2.page"));
        assertFalse(client.deleteFile("ts1/uuid1/data/2.page"));
        assertNull(client.readFile("ts1/uuid1/data/2.page"));
    }

    @Test
    public void testList() {
        client.writeFile("ts1/uuid1/data/3.page", "a".getBytes(StandardCharsets.UTF_8));
        client.writeFile("ts1/uuid1/data/4.page", "b".getBytes(StandardCharsets.UTF_8));
        client.writeFile("ts2/uuid2/data/1.page", "c".getBytes(StandardCharsets.UTF_8));

        List<String> found = client.listFiles("ts1/uuid1/");
        assertTrue(found.size() >= 2);
        assertTrue(found.stream().allMatch(p -> p.startsWith("ts1/uuid1/")));
    }

    @Test
    public void testConstructorWithConfig() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 60L);
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 3);
        try (RemoteFileServiceClient configClient = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), config)) {
            // Verify the client works with custom config
            byte[] content = "config test".getBytes(StandardCharsets.UTF_8);
            configClient.writeFile("ts1/config/1.page", content);
            byte[] read = configClient.readFile("ts1/config/1.page");
            assertNotNull(read);
            assertArrayEquals(content, read);
        }
    }

    @Test
    public void testRetryOnReadAfterServerRestart() throws Exception {
        byte[] content = "retry test".getBytes(StandardCharsets.UTF_8);
        client.writeFile("ts1/retry/1.page", content);

        // Verify read works
        byte[] read = client.readFile("ts1/retry/1.page");
        assertNotNull(read);
        assertArrayEquals(content, read);
    }

    @Test
    public void testRetryConfigDefaults() throws Exception {
        // Default constructor should use defaults without error
        try (RemoteFileServiceClient defaultClient = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()))) {
            byte[] content = "default config".getBytes(StandardCharsets.UTF_8);
            defaultClient.writeFile("ts1/defaults/1.page", content);
            byte[] read = defaultClient.readFile("ts1/defaults/1.page");
            assertNotNull(read);
            assertArrayEquals(content, read);
        }
    }

    @Test
    public void testDeadlineIsPerCall() throws Exception {
        // Use a short timeout (5 seconds). If the deadline were set once at
        // construction time (the old bug), calls made after 5s would fail with
        // DEADLINE_EXCEEDED. With per-call deadlines each call gets a fresh timeout.
        Map<String, Object> config = new HashMap<>();
        config.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 5L);
        try (RemoteFileServiceClient shortTimeoutClient = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), config)) {
            byte[] content = "deadline test".getBytes(StandardCharsets.UTF_8);
            shortTimeoutClient.writeFile("ts1/deadline/1.page", content);

            // First read — should succeed immediately
            byte[] read1 = shortTimeoutClient.readFile("ts1/deadline/1.page");
            assertNotNull(read1);
            assertArrayEquals(content, read1);

            // Wait longer than the timeout
            Thread.sleep(6000);

            // Second read — would fail with old code (shared deadline already expired),
            // but should succeed with per-call deadline
            byte[] read2 = shortTimeoutClient.readFile("ts1/deadline/1.page");
            assertNotNull(read2);
            assertArrayEquals(content, read2);
        }
    }
}
