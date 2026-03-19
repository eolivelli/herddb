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
import java.util.List;
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
}
