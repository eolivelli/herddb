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

package herddb.indexing;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.bookkeeper.stats.prometheus.PrometheusServlet;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * gRPC server for the IndexingService.
 * Exposes vector index Search and GetIndexStatus RPCs, plus a Prometheus metrics endpoint.
 *
 * @author enrico.olivelli
 */
public class IndexingServer implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(IndexingServer.class.getName());

    private final String host;
    private final int port;
    private final Properties config;
    private final IndexingServiceEngine engine;

    private Server server;
    private PrometheusMetricsProvider statsProvider;
    private org.eclipse.jetty.server.Server httpServer;

    public IndexingServer(String host, int port, IndexingServiceEngine engine, Properties config) {
        this.host = host;
        this.port = port;
        this.engine = engine;
        this.config = config;
    }

    public IndexingServer(int port, IndexingServiceEngine engine) {
        this("0.0.0.0", port, engine, new Properties());
    }

    public void start() throws IOException {
        // Initialize metrics
        statsProvider = new PrometheusMetricsProvider();
        PropertiesConfiguration statsConfig = new PropertiesConfiguration();
        statsConfig.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, false);
        statsProvider.start(statsConfig);
        StatsLogger statsLogger = statsProvider.getStatsLogger("");

        engine.setStatsLogger(statsLogger);
        IndexingServiceImpl serviceImpl = new IndexingServiceImpl(engine, statsLogger);
        server = ServerBuilder.forPort(port)
                .addService(serviceImpl)
                .build()
                .start();

        // Start HTTP server for metrics
        boolean httpEnabled = Boolean.parseBoolean(config.getProperty("http.enable", "false"));
        if (httpEnabled) {
            int httpPort = Integer.parseInt(config.getProperty("http.port", "9851"));
            String httpHost = config.getProperty("http.host", "0.0.0.0");
            try {
                httpServer = new org.eclipse.jetty.server.Server();
                ServerConnector connector = new ServerConnector(httpServer);
                connector.setPort(httpPort);
                connector.setHost(httpHost);
                httpServer.addConnector(connector);
                ServletContextHandler context = new ServletContextHandler(ServletContextHandler.GZIP);
                context.setContextPath("/");
                context.addServlet(new ServletHolder(new PrometheusServlet(statsProvider)), "/metrics");
                httpServer.setHandler(context);
                httpServer.start();
                LOGGER.log(Level.INFO, "Metrics HTTP server started on {0}:{1}",
                        new Object[]{httpHost, httpPort});
            } catch (Exception e) {
                throw new IOException("Failed to start metrics HTTP server", e);
            }
        }

        LOGGER.log(Level.INFO, "IndexingServer started on port {0}", getPort());
    }

    public int getPort() {
        return server != null ? server.getPort() : port;
    }

    public String getHost() {
        return host;
    }

    public String getAddress() {
        return host + ":" + getPort();
    }

    public int getHttpPort() {
        if (httpServer != null) {
            return ((ServerConnector) httpServer.getConnectors()[0]).getLocalPort();
        }
        return -1;
    }

    public void stop() throws InterruptedException {
        if (httpServer != null) {
            try {
                httpServer.stop();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error stopping metrics HTTP server", e);
            }
        }
        if (statsProvider != null) {
            statsProvider.stop();
        }
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            LOGGER.log(Level.INFO, "IndexingServer stopped");
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}
