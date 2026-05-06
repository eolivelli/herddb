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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that Prometheus metrics are recorded by the file-server data
 * dispatcher and exposed via the metrics HTTP endpoint. Issue #425 ported
 * these from raw gRPC stubs to the {@link RemoteFileServiceClient} public
 * API; the metric names ({@code rfs_*}) are unchanged.
 */
public class RemoteFileServiceMetricsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;

    @Before
    public void setUp() throws Exception {
        Properties config = new Properties();
        config.setProperty("http.enable", "true");
        config.setProperty("http.port", "0");
        server = new RemoteFileServer("localhost", 0,
                folder.newFolder("data").toPath(), 2, config);
        server.start();
        client = new RemoteFileServiceClient(List.of("localhost:" + server.getPort()));
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
    public void testMetricsEndpointAvailable() throws Exception {
        int httpPort = server.getHttpPort();
        assertNotEquals(-1, httpPort);
        String metrics = fetchMetrics(httpPort);
        assertTrue(metrics.contains("# TYPE"));
    }

    @Test
    public void testWriteMetricsRecorded() throws Exception {
        client.writeFile("metrics/test/1.page", "metrics test data".getBytes(StandardCharsets.UTF_8));

        String metrics = fetchMetrics(server.getHttpPort());
        assertTrue("write_requests counter should be present",
                metrics.contains("rfs_write_requests"));
        assertTrue("written_bytes counter should be present",
                metrics.contains("rfs_written_bytes"));
        assertTrue("write_latency should be present",
                metrics.contains("rfs_write_latency"));
    }

    @Test
    public void testReadMetricsRecorded() throws Exception {
        client.writeFile("metrics/read/1.page", "read metrics".getBytes(StandardCharsets.UTF_8));
        // Successful read
        assertTrue(client.readFile("metrics/read/1.page") != null);
        // Missing file read
        org.junit.Assert.assertNull(client.readFile("metrics/read/missing.page"));

        String metrics = fetchMetrics(server.getHttpPort());
        assertTrue("read_requests counter should be present",
                metrics.contains("rfs_read_requests"));
        assertTrue("read_not_found counter should be present",
                metrics.contains("rfs_read_not_found"));
        assertTrue("read_latency should be present",
                metrics.contains("rfs_read_latency"));
    }

    @Test
    public void testDeleteMetricsRecorded() throws Exception {
        client.writeFile("metrics/del/1.page", "x".getBytes());
        client.deleteFile("metrics/del/1.page");

        String metrics = fetchMetrics(server.getHttpPort());
        assertTrue("delete_requests counter should be present",
                metrics.contains("rfs_delete_requests"));
        assertTrue("delete_latency should be present",
                metrics.contains("rfs_delete_latency"));
    }

    @Test
    public void testListMetricsRecorded() throws Exception {
        client.writeFile("metrics/list/1.page", "a".getBytes());
        client.listFiles("metrics/list/");

        String metrics = fetchMetrics(server.getHttpPort());
        assertTrue("list_requests counter should be present",
                metrics.contains("rfs_list_requests"));
        assertTrue("list_latency should be present",
                metrics.contains("rfs_list_latency"));
    }

    @Test
    public void testDeleteByPrefixMetricsRecorded() throws Exception {
        client.writeFile("metrics/pfx/1.page", "a".getBytes());
        client.deleteByPrefix("metrics/pfx/");

        String metrics = fetchMetrics(server.getHttpPort());
        assertTrue("deletebyprefix_requests counter should be present",
                metrics.contains("rfs_deletebyprefix_requests"));
        assertTrue("deletebyprefix_latency should be present",
                metrics.contains("rfs_deletebyprefix_latency"));
    }

    @Test
    public void testAllOperationsCountersIncrement() throws Exception {
        for (int i = 0; i < 3; i++) {
            client.writeFile("metrics/count/" + i + ".page", ("data" + i).getBytes());
        }
        for (int i = 0; i < 3; i++) {
            client.readFile("metrics/count/" + i + ".page");
        }

        String metrics = fetchMetrics(server.getHttpPort());
        assertTrue("write_requests should have value",
                hasPositiveCounterValue(metrics, "rfs_write_requests"));
        assertTrue("read_requests should have value",
                hasPositiveCounterValue(metrics, "rfs_read_requests"));
    }

    private String fetchMetrics(int httpPort) throws Exception {
        URL url = new URL("http://localhost:" + httpPort + "/metrics");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        assertEquals(200, conn.getResponseCode());
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }

    private boolean hasPositiveCounterValue(String metrics, String counterName) {
        for (String line : metrics.split("\n")) {
            if (line.startsWith(counterName + " ")) {
                String value = line.substring(counterName.length()).trim();
                try {
                    return Double.parseDouble(value) > 0;
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        }
        return false;
    }
}
