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
package herddb.vectortesting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Embedded Jetty server exposing a JSON admin API for live inspection and
 * tuning of a running {@link VectorBench}. Controlled by system property
 * {@code vectorbench.admin.port} (default 8080; 0 or negative disables).
 *
 * <p>Endpoints (see {@code VECTOR_BENCH.md} for full docs):
 * <ul>
 *   <li>{@code GET /ingestion/config}</li>
 *   <li>{@code POST /ingestion/config/ingest-max-ops}</li>
 *   <li>{@code GET /ingestion/config/ingest-threads}</li>
 *   <li>{@code POST /ingestion/config/ingest-threads}</li>
 *   <li>{@code GET /query/config}</li>
 *   <li>{@code POST /query/config/query-max-ops}</li>
 *   <li>{@code GET /query/config/query-threads}</li>
 *   <li>{@code POST /query/config/query-threads}</li>
 *   <li>{@code GET /status}</li>
 * </ul>
 */
public class AdminApiServer {

    public static final String PORT_SYSTEM_PROPERTY = "vectorbench.admin.port";
    public static final int DEFAULT_PORT = 8080;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BenchRuntime runtime;
    private final int port;
    private Server server;

    public AdminApiServer(BenchRuntime runtime, int port) {
        this.runtime = runtime;
        this.port = port;
    }

    /** Reads the port from the system property; returns {@code 0} to disable. */
    public static int readPortFromSystemProperty() {
        String raw = System.getProperty(PORT_SYSTEM_PROPERTY);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_PORT;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid " + PORT_SYSTEM_PROPERTY + "=" + raw);
        }
    }

    /**
     * Starts the server. Returns the bound port (useful when {@code 0} was
     * passed to pick an ephemeral port in tests). Throws if start fails.
     */
    public int start() throws Exception {
        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        server.addConnector(connector);

        ServletContextHandler ctx = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        ctx.setContextPath("/");
        ctx.addServlet(new ServletHolder(new AdminServlet(runtime)), "/*");
        server.setHandler(ctx);

        server.start();
        return connector.getLocalPort();
    }

    public void stop() {
        if (server == null) {
            return;
        }
        try {
            server.stop();
        } catch (Exception e) {
            // Stopping is best-effort — swallow so we don't mask the primary
            // failure during benchmark shutdown.
            System.err.println("AdminApiServer stop failed: " + e.getMessage());
        }
    }

    /** Single servlet dispatching by method + path. */
    static final class AdminServlet extends HttpServlet {

        private static final long serialVersionUID = 1L;

        // The servlet is never actually serialized (we only register it in an
        // embedded Jetty in-process), but HttpServlet implements Serializable
        // so SpotBugs flags non-transient non-Serializable fields.
        private final transient BenchRuntime runtime;

        AdminServlet(BenchRuntime runtime) {
            this.runtime = runtime;
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            String path = pathOf(req);
            switch (path) {
                case "/ingestion/config" -> writeJson(resp, HttpServletResponse.SC_OK, ingestionConfig());
                case "/ingestion/config/ingest-threads" ->
                        writeJson(resp, HttpServletResponse.SC_OK,
                                Map.of("ingest-threads", runtime.config().ingestThreads));
                case "/query/config" -> writeJson(resp, HttpServletResponse.SC_OK, queryConfig());
                case "/query/config/query-threads" ->
                        writeJson(resp, HttpServletResponse.SC_OK,
                                Map.of("query-threads", runtime.config().queryThreads));
                case "/status" -> writeJson(resp, HttpServletResponse.SC_OK, runtime.getStatusSupplier().get());
                default -> writeError(resp, HttpServletResponse.SC_NOT_FOUND, "no such endpoint: GET " + path);
            }
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            String path = pathOf(req);
            try {
                switch (path) {
                    case "/ingestion/config/ingest-max-ops" -> {
                        int value = readIntValue(req);
                        runtime.setIngestMaxOps(value);
                        writeJson(resp, HttpServletResponse.SC_OK, ingestionConfig());
                    }
                    case "/ingestion/config/ingest-threads" -> {
                        int value = readIntValue(req);
                        runtime.setIngestThreads(value);
                        writeJson(resp, HttpServletResponse.SC_OK, ingestionConfig());
                    }
                    case "/query/config/query-max-ops" -> {
                        int value = readIntValue(req);
                        runtime.setQueryMaxOps(value);
                        writeJson(resp, HttpServletResponse.SC_OK, queryConfig());
                    }
                    case "/query/config/query-threads" -> {
                        int value = readIntValue(req);
                        runtime.setQueryThreads(value);
                        writeJson(resp, HttpServletResponse.SC_OK, queryConfig());
                    }
                    case "/query/config/top-k" -> {
                        int value = readIntValue(req);
                        if (value <= 0) {
                            throw new IllegalArgumentException("top-k must be > 0, got " + value);
                        }
                        runtime.setTopK(value);
                        writeJson(resp, HttpServletResponse.SC_OK, queryConfig());
                    }
                    default -> writeError(resp, HttpServletResponse.SC_NOT_FOUND, "no such endpoint: POST " + path);
                }
            } catch (IllegalArgumentException e) {
                writeError(resp, HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                writeError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                        "request interrupted: " + e.getMessage());
            }
        }

        private Map<String, Object> ingestionConfig() {
            Config c = runtime.config();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ingest-max-ops", c.ingestMaxOpsPerSecond);
            m.put("ingest-threads", c.ingestThreads);
            m.put("batch-size", c.batchSize);
            m.put("rows", c.numRows);
            m.put("resume-from", c.resumeFrom);
            m.put("ingest-commit-retries", c.ingestCommitRetries);
            return m;
        }

        private Map<String, Object> queryConfig() {
            Config c = runtime.config();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("query-max-ops", c.queryMaxOpsPerSecond);
            m.put("query-threads", c.queryThreads);
            m.put("queries", c.queryCount);
            m.put("top-k", c.topK);
            return m;
        }

        private static String pathOf(HttpServletRequest req) {
            String p = req.getPathInfo();
            if (p == null || p.isEmpty()) {
                p = req.getServletPath();
            }
            if (p == null) {
                p = "/";
            }
            if (p.length() > 1 && p.endsWith("/")) {
                p = p.substring(0, p.length() - 1);
            }
            return p;
        }

        /**
         * Accepts either JSON {@code {"value": 1234}} or a plain numeric body
         * so simple {@code curl} invocations work without quoting gymnastics.
         */
        private static int readIntValue(HttpServletRequest req) throws IOException {
            String body = new String(req.getInputStream().readAllBytes(),
                    req.getCharacterEncoding() != null ? req.getCharacterEncoding() : "UTF-8").trim();
            if (body.isEmpty()) {
                throw new IllegalArgumentException("empty request body");
            }
            if (body.startsWith("{")) {
                JsonNode node = MAPPER.readTree(body);
                JsonNode value = node.get("value");
                if (value == null || !value.canConvertToInt()) {
                    throw new IllegalArgumentException("expected JSON field 'value' of type int");
                }
                return value.asInt();
            }
            try {
                return Integer.parseInt(body);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("body is neither JSON {value:int} nor a bare integer: " + body);
            }
        }

        private static void writeJson(HttpServletResponse resp, int status, Object payload) throws IOException {
            resp.setStatus(status);
            resp.setContentType("application/json; charset=utf-8");
            MAPPER.writeValue(resp.getOutputStream(), payload);
        }

        private static void writeError(HttpServletResponse resp, int status, String message) throws IOException {
            writeJson(resp, status, Map.of("error", message));
        }
    }
}
