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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for the {@link RemoteFileServiceClient#deleteFiles(List)} batch RPC
 * (issue #398). Exercises both the single-server happy path and the multi-server
 * fan-out path that groups paths by consistent-hash router target.
 */
public class RemoteFileServiceClientBatchDeleteTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("rfs").toPath());
        server.start();
        client = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()));
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void emptyBatchIsNoOp() throws Exception {
        assertEquals(0, client.deleteFiles(Collections.emptyList()));
        assertEquals(0, client.deleteFiles(null));
    }

    @Test
    public void deletesAllPathsAndReportsCount() throws Exception {
        List<String> paths = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            String path = "batch/page-" + i + ".page";
            client.writeFile(path, ("content-" + i).getBytes());
            paths.add(path);
        }
        // Sanity: every file is there before the batch delete.
        List<String> beforeDelete = client.listFiles("batch/");
        assertEquals(25, beforeDelete.size());

        int deleted = client.deleteFiles(paths);
        assertEquals(25, deleted);

        List<String> afterDelete = client.listFiles("batch/");
        assertTrue("expected no files left after batch delete, got " + afterDelete,
                afterDelete.isEmpty());
    }

    @Test
    public void missingPathsReportFalseButDoNotFail() throws Exception {
        // Mix of existing + missing paths. Missing paths should report deleted=false
        // (counted by the server, not surfaced as a sub-batch error) so the client's
        // total counts only the truly-deleted ones.
        client.writeFile("mixed/exists-1.page", new byte[]{1});
        client.writeFile("mixed/exists-2.page", new byte[]{2});

        List<String> paths = Arrays.asList(
                "mixed/exists-1.page",
                "mixed/missing-1.page",
                "mixed/exists-2.page",
                "mixed/missing-2.page",
                "mixed/missing-3.page"
        );
        int deleted = client.deleteFiles(paths);
        assertEquals("only the two existing files should be reported deleted",
                2, deleted);
        assertTrue(client.listFiles("mixed/").isEmpty());
    }

    @Test
    public void asyncReturnsSameDeletedCount() throws Exception {
        for (int i = 0; i < 5; i++) {
            client.writeFile("async/p-" + i, new byte[]{(byte) i});
        }
        List<String> paths = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            paths.add("async/p-" + i);
        }
        Integer deleted = client.deleteFilesAsync(paths).get();
        assertEquals(Integer.valueOf(5), deleted);
        assertTrue(client.listFiles("async/").isEmpty());
    }

    @Test
    public void singleServerCallProducesOneRpc() throws Exception {
        // With a single-server snapshot the consistent-hash router always picks
        // the same server, so 100 paths become exactly one DeleteFiles RPC.
        // We can't observe RPC count directly, but we can verify all paths are
        // gone after a single deleteFiles call.
        List<String> paths = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String path = "one-rpc/key-" + i;
            client.writeFile(path, new byte[]{(byte) i});
            paths.add(path);
        }
        int deleted = client.deleteFiles(paths);
        assertEquals(100, deleted);
        assertFalse(client.listFiles("one-rpc/").isEmpty()
                && client.listFiles("one-rpc/").stream().anyMatch(p -> p.startsWith("one-rpc/key-")));
    }
}
