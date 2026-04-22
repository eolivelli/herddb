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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ZooKeeperMainWrapperTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testAdminServerRuok() throws Exception {
        // This exercises the AdminServer configuration used by the Helm
        // chart's ZooKeeper readiness/liveness probe (issue #211): with
        // admin.enableServer=true + admin.serverPort=<port>, GET
        // /commands/ruok must return HTTP 200 and a JSON body that
        // identifies the command as "ruok".
        int clientPort;
        try (ServerSocket ss = new ServerSocket(0)) {
            clientPort = ss.getLocalPort();
        }
        int httpPort;
        try (ServerSocket ss = new ServerSocket(0)) {
            httpPort = ss.getLocalPort();
        }
        int adminPort;
        try (ServerSocket ss = new ServerSocket(0)) {
            adminPort = ss.getLocalPort();
        }

        Properties config = new Properties();
        config.put("tickTime", "2000");
        config.put("dataDir", folder.newFolder("zk-data-admin").getAbsolutePath());
        config.put("clientPort", String.valueOf(clientPort));
        config.put("http.port", String.valueOf(httpPort));
        // Same settings the Helm chart writes into zoo.cfg.
        config.put("admin.enableServer", "true");
        config.put("admin.serverPort", String.valueOf(adminPort));
        config.put("admin.serverAddress", "0.0.0.0");

        ZooKeeperMainWrapper wrapper = new ZooKeeperMainWrapper(config);
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
            // Wait for ZK client port to accept connections.
            boolean ready = false;
            for (int i = 0; i < 60; i++) {
                try (java.net.Socket s = new java.net.Socket("localhost", clientPort)) {
                    ready = true;
                    break;
                } catch (Exception e) {
                    Thread.sleep(500);
                }
            }
            assertTrue("ZooKeeper did not start in time", ready);

            // /commands/ruok must respond 200 once AdminServer is up.
            String body = null;
            int responseCode = -1;
            for (int i = 0; i < 60; i++) {
                try {
                    URL ruokUrl = new URL("http://localhost:" + adminPort + "/commands/ruok");
                    HttpURLConnection conn = (HttpURLConnection) ruokUrl.openConnection();
                    conn.setRequestMethod("GET");
                    conn.setConnectTimeout(2000);
                    conn.setReadTimeout(2000);
                    responseCode = conn.getResponseCode();
                    if (responseCode == 200) {
                        StringBuilder sb = new StringBuilder();
                        try (BufferedReader reader = new BufferedReader(
                                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                sb.append(line).append('\n');
                            }
                        }
                        body = sb.toString();
                        break;
                    }
                } catch (Exception e) {
                    Thread.sleep(500);
                }
            }
            assertTrue("Expected HTTP 200 from /commands/ruok, got " + responseCode, responseCode == 200);
            assertTrue("Expected /commands/ruok body to mention the ruok command, got: " + body,
                    body != null && body.contains("ruok"));
        } finally {
            wrapper.close();
        }
    }

    @Test
    public void testBootAndMetrics() throws Exception {
        int clientPort;
        try (ServerSocket ss = new ServerSocket(0)) {
            clientPort = ss.getLocalPort();
        }
        int httpPort;
        try (ServerSocket ss = new ServerSocket(0)) {
            httpPort = ss.getLocalPort();
        }

        Properties config = new Properties();
        config.put("tickTime", "2000");
        config.put("dataDir", folder.newFolder("zk-data").getAbsolutePath());
        config.put("clientPort", String.valueOf(clientPort));
        config.put("http.port", String.valueOf(httpPort));
        // ZK 3.6+ enables AdminServer by default on port 8080. Keep this
        // test focused on /metrics by disabling it — the AdminServer path
        // is covered by testAdminServerRuok.
        config.put("admin.enableServer", "false");
        // These properties should be stripped and not cause a ClassNotFoundException
        config.put("metricsProvider.className", "org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider");
        config.put("metricsProvider.httpPort", "7070");
        config.put("metricsProvider.exportJvmInfo", "true");

        ZooKeeperMainWrapper wrapper = new ZooKeeperMainWrapper(config);
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
            // Wait for ZK to start accepting connections
            boolean ready = false;
            for (int i = 0; i < 60; i++) {
                try {
                    java.net.Socket s = new java.net.Socket("localhost", clientPort);
                    s.close();
                    ready = true;
                    break;
                } catch (Exception e) {
                    Thread.sleep(500);
                }
            }
            assertTrue("ZooKeeper did not start in time", ready);

            // Verify metrics endpoint
            URL metricsUrl = new URL("http://localhost:" + httpPort + "/metrics");
            HttpURLConnection conn = (HttpURLConnection) metricsUrl.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);

            int responseCode = conn.getResponseCode();
            assertTrue("Expected HTTP 200, got " + responseCode, responseCode == 200);

            StringBuilder body = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    body.append(line).append("\n");
                }
            }

            String metricsBody = body.toString();
            // JVM metrics should be present
            assertTrue("Metrics should contain JVM info, got: " + metricsBody,
                    metricsBody.contains("jvm_"));
        } finally {
            wrapper.close();
        }
    }
}
