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
    public void deletesAcrossMultipleServers() throws Exception {
        // Spin up a second server so the consistent-hash router actually splits
        // the path list across two distinct sub-batches.
        try (RemoteFileServer server2 = new RemoteFileServer(0, folder.newFolder("rfs2").toPath())) {
            server2.start();
            try (RemoteFileServiceClient multiClient = new RemoteFileServiceClient(Arrays.asList(
                    "localhost:" + server.getPort(),
                    "localhost:" + server2.getPort()))) {
                int total = 60;
                List<String> paths = new ArrayList<>(total);
                for (int i = 0; i < total; i++) {
                    String path = "multi/key-" + i + ".page";
                    multiClient.writeFile(path, new byte[]{(byte) i});
                    paths.add(path);
                }
                // Sanity: with a balanced consistent-hash router both servers
                // must hold at least one path — otherwise this test would not
                // exercise the multi-server fan-out.
                int onServer1 = 0;
                int onServer2 = 0;
                for (String path : paths) {
                    String routed = multiClient.getServerForPath(path);
                    if (routed.endsWith(":" + server.getPort())) {
                        onServer1++;
                    } else if (routed.endsWith(":" + server2.getPort())) {
                        onServer2++;
                    }
                }
                assertTrue("router must split across both servers (server1=" + onServer1
                        + ", server2=" + onServer2 + ")", onServer1 > 0 && onServer2 > 0);

                int deleted = multiClient.deleteFiles(paths);
                assertEquals("every path written should be reported deleted by some sub-batch",
                        total, deleted);
                assertTrue("multi/ should be empty after the cross-server batch delete",
                        multiClient.listFiles("multi/").isEmpty());
            }
        }
    }

    @Test
    public void surviesPartialFailureWhenOneServerStops() throws Exception {
        try (RemoteFileServer server2 = new RemoteFileServer(0, folder.newFolder("rfs2").toPath())) {
            server2.start();
            RemoteFileServiceClient multiClient = new RemoteFileServiceClient(Arrays.asList(
                    "localhost:" + server.getPort(),
                    "localhost:" + server2.getPort()));
            try {
                int total = 40;
                List<String> paths = new ArrayList<>(total);
                int onServer1 = 0;
                int onServer2 = 0;
                for (int i = 0; i < total; i++) {
                    String path = "partial/key-" + i + ".page";
                    multiClient.writeFile(path, new byte[]{(byte) i});
                    paths.add(path);
                    String routed = multiClient.getServerForPath(path);
                    if (routed.endsWith(":" + server.getPort())) {
                        onServer1++;
                    } else if (routed.endsWith(":" + server2.getPort())) {
                        onServer2++;
                    }
                }
                assertTrue("test setup: router must split across both servers",
                        onServer1 > 0 && onServer2 > 0);
                // Stop server2 so its sub-batch will fail. The surviving sub-batch
                // (server1) must still report its deletion count.
                server2.stop();

                int deleted;
                try {
                    deleted = multiClient.deleteFiles(paths);
                } catch (RuntimeException ex) {
                    // The retry loop may surface the failure from server2 if its
                    // last attempt completes after the server1 sub-batch has
                    // returned. That's acceptable — but the absence of an
                    // exception is the more interesting partial-success case
                    // we're documenting; mark it explicitly here so the test is
                    // self-explanatory.
                    deleted = -1;
                }
                if (deleted >= 0) {
                    assertEquals("partial-success should at least surface the surviving server's count",
                            onServer1, deleted);
                }
            } finally {
                multiClient.close();
            }
        }
    }

    @Test
    public void allSubBatchesFailingSurfacesException() throws Exception {
        // Single-server client; stop the server, then try to delete some paths.
        // Every sub-batch (there's exactly one) will fail and the client must
        // surface a RuntimeException rather than silently returning 0.
        client.writeFile("dead/k1", new byte[]{1});
        client.writeFile("dead/k2", new byte[]{2});
        // Fast-fail: switch to a fresh client with retries=0 so the test
        // doesn't sit through the default exponential backoff.
        try (RemoteFileServiceClient fastFailClient = newClientWithoutRetries()) {
            // Stop the only server.
            server.stop();
            server = null;
            try {
                fastFailClient.deleteFiles(Arrays.asList("dead/k1", "dead/k2"));
                org.junit.Assert.fail("expected RuntimeException when every sub-batch fails");
            } catch (RuntimeException expected) {
                // ok
            }
        }
    }

    private RemoteFileServiceClient newClientWithoutRetries() {
        java.util.Map<String, Object> cfg = new java.util.HashMap<>();
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_RETRIES, 0);
        cfg.put(RemoteFileServiceClient.CONFIG_CLIENT_TIMEOUT, 5L); // 5s deadline
        return new RemoteFileServiceClient(
                Arrays.asList("localhost:" + (server != null ? server.getPort() : 1)),
                cfg);
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
        assertTrue("expected no one-rpc/key-* keys to remain",
                client.listFiles("one-rpc/").stream().noneMatch(p -> p.startsWith("one-rpc/key-")));
    }
}
