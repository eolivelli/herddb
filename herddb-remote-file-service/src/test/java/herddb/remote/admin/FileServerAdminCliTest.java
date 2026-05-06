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

package herddb.remote.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.proto.PduCodec;
import herddb.remote.RemoteFileServer;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration tests for the fileserver-admin CLI and admin service
 * (issue #336, ported to the native Netty wire protocol in issue #425).
 * <p>
 * Uses a {@link RemoteFileServer} in local-storage mode (no S3 needed) to verify:
 * <ul>
 *   <li>{@code server-info} returns JVM and block-cache stats</li>
 *   <li>{@code resize-cache} correctly rejects when disk cache is absent</li>
 *   <li>admin client works end-to-end</li>
 *   <li>JSON output contains expected keys</li>
 * </ul>
 *
 * @author enrico.olivelli
 */
public class FileServerAdminCliTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private RemoteFileServer fileServer;
    private String serverAddress;

    @Before
    public void setUp() throws Exception {
        Path dataDir = tmp.newFolder("data").toPath();
        Properties config = new Properties();
        // Local mode — no S3, no disk cache, but block cache enabled
        config.setProperty("storage.mode", "local");
        config.setProperty("block.cache.enabled", "true");
        config.setProperty("block.cache.max.bytes", String.valueOf(16 * 1024 * 1024)); // 16 MB

        fileServer = new RemoteFileServer("127.0.0.1", 0, dataDir, 2, config);
        fileServer.start();
        serverAddress = "127.0.0.1:" + fileServer.getPort();
    }

    @After
    public void tearDown() throws Exception {
        if (fileServer != null) {
            fileServer.stop();
        }
    }

    // ---------------------------------------------------------------
    // Admin client tests
    // ---------------------------------------------------------------

    @Test
    public void testGetServerInfoViaClient() throws Exception {
        try (FileServerAdminClient client = new FileServerAdminClient(serverAddress, 10)) {
            PduCodec.GetServerInfoResponse.Info resp = client.getServerInfo();
            assertNotNull(resp);
            assertEquals("local", resp.storageMode);
            assertTrue("jvm_heap_max_bytes must be > 0", resp.jvmHeapMaxBytes > 0);
            assertTrue("jvm_heap_used_bytes must be > 0", resp.jvmHeapUsedBytes > 0);
            // Block cache should be populated
            assertTrue("block_cache_max_bytes must be > 0", resp.blockCacheMaxBytes > 0);
            // Disk cache absent in local mode — all fields zero
            assertEquals("disk_cache_max_bytes must be 0 in local mode",
                    0L, resp.diskCacheMaxBytes);
        }
    }

    @Test
    public void testResizeDiskCacheFailsInLocalMode() {
        // In local mode there is no disk cache; resize must surface as
        // an IOException carrying the server's TYPE_ERROR message.
        try (FileServerAdminClient client = new FileServerAdminClient(serverAddress, 10)) {
            try {
                client.resizeDiskCache(1024 * 1024 * 512L);
                throw new AssertionError("Expected IOException");
            } catch (java.io.IOException expected) {
                assertTrue("error message must mention disk cache",
                        expected.getMessage().toLowerCase().contains("disk cache"));
            }
        }
    }

    // ---------------------------------------------------------------
    // CLI plain-text output tests
    // ---------------------------------------------------------------

    @Test
    public void testServerInfoCliPlainText() {
        ByteArrayOutputStream outBuf = new ByteArrayOutputStream();
        ByteArrayOutputStream errBuf = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBuf, true, StandardCharsets.UTF_8);
        PrintStream err = new PrintStream(errBuf, true, StandardCharsets.UTF_8);

        FileServerAdminCli cli = new FileServerAdminCli(out, err);
        int rc = cli.run(new String[]{"server-info", "--server", serverAddress});

        assertEquals("CLI should exit 0", 0, rc);
        String output = outBuf.toString(StandardCharsets.UTF_8);
        assertTrue("Output should contain 'Server info'", output.contains("Server info:"));
        assertTrue("Output should contain 'storage_mode'", output.contains("storage_mode"));
        assertTrue("Output should contain 'local'", output.contains("local"));
        assertTrue("Output should contain 'Block cache'", output.contains("Block cache"));
    }

    @Test
    public void testServerInfoCliJson() {
        ByteArrayOutputStream outBuf = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBuf, true, StandardCharsets.UTF_8);
        PrintStream err = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);

        FileServerAdminCli cli = new FileServerAdminCli(out, err);
        int rc = cli.run(new String[]{"server-info", "--server", serverAddress, "--json"});

        assertEquals("CLI should exit 0", 0, rc);
        String json = outBuf.toString(StandardCharsets.UTF_8).trim();
        assertTrue("JSON must start with {", json.startsWith("{"));
        assertTrue("JSON must contain storage_mode", json.contains("\"storage_mode\""));
        assertTrue("JSON must contain block_cache_max_bytes", json.contains("\"block_cache_max_bytes\""));
        assertTrue("JSON must contain disk_cache_max_bytes", json.contains("\"disk_cache_max_bytes\""));
    }

    @Test
    public void testResizeCacheCliFailsInLocalMode() {
        ByteArrayOutputStream outBuf = new ByteArrayOutputStream();
        ByteArrayOutputStream errBuf = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBuf, true, StandardCharsets.UTF_8);
        PrintStream err = new PrintStream(errBuf, true, StandardCharsets.UTF_8);

        FileServerAdminCli cli = new FileServerAdminCli(out, err);
        int rc = cli.run(new String[]{
                "resize-cache", "--server", serverAddress, "--max-bytes", "536870912"
        });

        // server returns an ERROR PDU → CLI catches the IOException and returns 1
        assertEquals("CLI should exit 1 on failed precondition", 1, rc);
        String errOutput = errBuf.toString(StandardCharsets.UTF_8);
        assertTrue("Error output should mention 'ERROR'", errOutput.contains("ERROR"));
    }

    @Test
    public void testCliNoArgsReturns2() {
        FileServerAdminCli cli = new FileServerAdminCli(
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));
        int rc = cli.run(new String[0]);
        assertEquals(2, rc);
    }

    @Test
    public void testCliUnknownCommandReturns2() {
        FileServerAdminCli cli = new FileServerAdminCli(
                new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));
        int rc = cli.run(new String[]{"no-such-command"});
        assertEquals(2, rc);
    }

    // ---------------------------------------------------------------
    // JSON serialiser unit test
    // ---------------------------------------------------------------

    @Test
    public void testToJsonBasic() {
        java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("str", "hello");
        m.put("num", 42L);
        m.put("bool", true);
        m.put("nil", null);
        String json = FileServerAdminCli.toJson(m);
        assertEquals("{\"str\":\"hello\",\"num\":42,\"bool\":true,\"nil\":null}", json);
    }
}
