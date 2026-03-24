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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import com.google.protobuf.ByteString;
import herddb.remote.proto.DeleteByPrefixRequest;
import herddb.remote.proto.DeleteFileRequest;
import herddb.remote.proto.ListFilesRequest;
import herddb.remote.proto.ReadFileRequest;
import herddb.remote.proto.ReadFileResponse;
import herddb.remote.proto.RemoteFileServiceGrpc;
import herddb.remote.proto.WriteFileRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that Prometheus metrics are recorded and exposed via the HTTP endpoint.
 */
public class RemoteFileServiceMetricsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private ManagedChannel channel;
    private RemoteFileServiceGrpc.RemoteFileServiceBlockingStub stub;

    @Before
    public void setUp() throws Exception {
        Properties config = new Properties();
        config.setProperty("http.enable", "true");
        config.setProperty("http.port", "0");
        server = new RemoteFileServer("localhost", 0,
                folder.newFolder("data").toPath(), 2, config);
        server.start();

        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        stub = RemoteFileServiceGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() throws Exception {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        server.stop();
    }

    @Test
    public void testMetricsEndpointAvailable() throws Exception {
        int httpPort = server.getHttpPort();
        assertNotEquals(-1, httpPort);

        String metrics = fetchMetrics(httpPort);
        // The endpoint should return valid Prometheus text
        assertTrue(metrics.contains("# TYPE"));
    }

    @Test
    public void testWriteMetricsRecorded() throws Exception {
        byte[] data = "metrics test data".getBytes(StandardCharsets.UTF_8);
        stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("metrics/test/1.page")
                .setContent(ByteString.copyFrom(data))
                .build());

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
        // Write a file first
        byte[] data = "read metrics".getBytes(StandardCharsets.UTF_8);
        stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("metrics/read/1.page")
                .setContent(ByteString.copyFrom(data))
                .build());

        // Successful read
        ReadFileResponse resp = stub.readFile(ReadFileRequest.newBuilder()
                .setPath("metrics/read/1.page")
                .build());
        assertTrue(resp.getFound());

        // Missing file read
        ReadFileResponse missing = stub.readFile(ReadFileRequest.newBuilder()
                .setPath("metrics/read/missing.page")
                .build());
        assertFalse(missing.getFound());

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
        stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("metrics/del/1.page")
                .setContent(ByteString.copyFromUtf8("x"))
                .build());
        stub.deleteFile(DeleteFileRequest.newBuilder()
                .setPath("metrics/del/1.page")
                .build());

        String metrics = fetchMetrics(server.getHttpPort());
        assertTrue("delete_requests counter should be present",
                metrics.contains("rfs_delete_requests"));
        assertTrue("delete_latency should be present",
                metrics.contains("rfs_delete_latency"));
    }

    @Test
    public void testListMetricsRecorded() throws Exception {
        stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("metrics/list/1.page")
                .setContent(ByteString.copyFromUtf8("a"))
                .build());
        stub.listFiles(ListFilesRequest.newBuilder()
                .setPrefix("metrics/list/")
                .build()).forEachRemaining(e -> {});

        String metrics = fetchMetrics(server.getHttpPort());
        assertTrue("list_requests counter should be present",
                metrics.contains("rfs_list_requests"));
        assertTrue("list_latency should be present",
                metrics.contains("rfs_list_latency"));
    }

    @Test
    public void testDeleteByPrefixMetricsRecorded() throws Exception {
        stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("metrics/pfx/1.page")
                .setContent(ByteString.copyFromUtf8("a"))
                .build());
        stub.deleteByPrefix(DeleteByPrefixRequest.newBuilder()
                .setPrefix("metrics/pfx/")
                .build());

        String metrics = fetchMetrics(server.getHttpPort());
        assertTrue("deletebyprefix_requests counter should be present",
                metrics.contains("rfs_deletebyprefix_requests"));
        assertTrue("deletebyprefix_latency should be present",
                metrics.contains("rfs_deletebyprefix_latency"));
    }

    @Test
    public void testAllOperationsCountersIncrement() throws Exception {
        // Perform multiple writes
        for (int i = 0; i < 3; i++) {
            stub.writeFile(WriteFileRequest.newBuilder()
                    .setPath("metrics/count/" + i + ".page")
                    .setContent(ByteString.copyFromUtf8("data" + i))
                    .build());
        }

        // Read them
        for (int i = 0; i < 3; i++) {
            stub.readFile(ReadFileRequest.newBuilder()
                    .setPath("metrics/count/" + i + ".page")
                    .build());
        }

        String metrics = fetchMetrics(server.getHttpPort());

        // Verify counters have values > 0
        // The counter line looks like: rfs_write_requests 3.0
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
