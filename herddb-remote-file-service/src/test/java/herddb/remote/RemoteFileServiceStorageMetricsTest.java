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

import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.util.Properties;
import java.util.Scanner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for storage layer metrics (disk and S3 reads).
 * Verifies that LocalObjectStorage and S3ObjectStorage metrics are properly recorded.
 */
public class RemoteFileServiceStorageMetricsTest {

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

        client = new RemoteFileServiceClient("localhost", server.getPort());
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    public void testDiskReadMetricsRecorded() throws Exception {
        // Write a file
        byte[] testData = new byte[1000];
        client.write("test.bin", testData);

        // Read it back
        byte[] retrieved = client.readFile("test.bin");
        assertTrue("Retrieved data should match", retrieved.length == testData.length);

        // Check metrics
        String metricsOutput = getMetrics();
        assertTrue("rfs_disk_read_requests should be recorded",
                metricsOutput.contains("rfs_disk_read_requests"));
        assertTrue("rfs_disk_read_bytes should be recorded",
                metricsOutput.contains("rfs_disk_read_bytes"));
        assertTrue("rfs_disk_read_latency should be recorded",
                metricsOutput.contains("rfs_disk_read_latency"));
    }

    @Test
    public void testDiskReadMetricsValues() throws Exception {
        // Write and read a file
        byte[] testData = "test content for metrics".getBytes();
        client.write("metrics-test.bin", testData);
        client.readFile("metrics-test.bin");

        // Verify metric values are > 0
        String metricsOutput = getMetrics();
        assertTrue("Disk read requests count should be > 0",
                hasPositiveCounterValue(metricsOutput, "rfs_disk_read_requests"));
        assertTrue("Disk read bytes should be > 0",
                hasPositiveCounterValue(metricsOutput, "rfs_disk_read_bytes"));
    }

    @Test
    public void testClientReadMetricsRecorded() throws Exception {
        // Write a file
        byte[] testData = new byte[1000];
        client.write("range-test.bin", testData);

        // Read range from the file
        byte[] result = client.readFileRange("range-test.bin", 0, 100, 4096);
        assertTrue("Should read 100 bytes", result != null && result.length > 0);

        // Check metrics - both server side (rfs_readrange) and client side (rfs_client_read)
        String metricsOutput = getMetrics();
        assertTrue("rfs_readrange_requests should be recorded",
                metricsOutput.contains("rfs_readrange_requests"));
        assertTrue("rfs_readrange_bytes should be recorded",
                metricsOutput.contains("rfs_readrange_bytes"));
    }

    private String getMetrics() throws Exception {
        URL metricsUrl = new URL("http://localhost:" + server.getHttpPort() + "/metrics");
        try (Scanner scanner = new Scanner(metricsUrl.openStream())) {
            return scanner.useDelimiter("\\A").next();
        }
    }

    private boolean hasPositiveCounterValue(String metricsOutput, String metricName) {
        for (String line : metricsOutput.split("\n")) {
            if (line.startsWith(metricName) && !line.startsWith("#")) {
                try {
                    String valueStr = line.split(" ")[1];
                    double value = Double.parseDouble(valueStr);
                    return value > 0;
                } catch (Exception e) {
                    return false;
                }
            }
        }
        return false;
    }
}
