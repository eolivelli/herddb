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

package herddb.server;

import static org.junit.Assert.assertTrue;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.curator.test.TestingServer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BookKeeperMainWrapperTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testBootAndMetrics() throws Exception {
        try (TestingServer zk = new TestingServer(-1, folder.newFolder("zk"))) {
            int bookiePort;
            try (ServerSocket ss = new ServerSocket(0)) {
                bookiePort = ss.getLocalPort();
            }
            int httpPort;
            try (ServerSocket ss = new ServerSocket(0)) {
                httpPort = ss.getLocalPort();
            }

            Properties config = new Properties();
            config.put("zkServers", "localhost:" + zk.getPort());
            config.put("zkLedgersRootPath", "/ledgers");
            config.put("bookiePort", String.valueOf(bookiePort));
            config.put("ledgerDirNames", folder.newFolder("bk-ledgers").getAbsolutePath());
            config.put("journalDirName", folder.newFolder("bk-journal").getAbsolutePath());
            config.put("httpServerPort", String.valueOf(httpPort));

            BookKeeperMainWrapper wrapper = new BookKeeperMainWrapper(config);
            Thread runner = new Thread(() -> {
                try {
                    wrapper.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            runner.setDaemon(true);
            runner.start();

            try {
                // Wait for metrics HTTP endpoint to become available
                String metricsBody = null;
                for (int i = 0; i < 60; i++) {
                    try {
                        URL metricsUrl = new URL("http://localhost:" + httpPort + "/metrics");
                        HttpURLConnection conn = (HttpURLConnection) metricsUrl.openConnection();
                        conn.setRequestMethod("GET");
                        conn.setConnectTimeout(1000);
                        conn.setReadTimeout(1000);
                        if (conn.getResponseCode() == 200) {
                            StringBuilder body = new StringBuilder();
                            try (BufferedReader reader = new BufferedReader(
                                    new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                                String line;
                                while ((line = reader.readLine()) != null) {
                                    body.append(line).append("\n");
                                }
                            }
                            metricsBody = body.toString();
                            break;
                        }
                    } catch (Exception e) {
                        Thread.sleep(500);
                    }
                }

                assertTrue("Metrics endpoint did not become available in time", metricsBody != null);
                // JVM metrics should be present
                assertTrue("Metrics should contain JVM info, got: " + metricsBody,
                        metricsBody.contains("jvm_"));
            } finally {
                wrapper.close();
            }
        }
    }
}
