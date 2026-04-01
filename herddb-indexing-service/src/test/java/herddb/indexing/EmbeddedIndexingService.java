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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Properties;

/**
 * Test helper that starts an IndexingService in-process (engine + gRPC server)
 * on a random port. Provides the gRPC address for creating clients.
 * <p>
 * Usage:
 * <pre>
 * try (EmbeddedIndexingService eis = new EmbeddedIndexingService(logDir, dataDir)) {
 *     eis.start();
 *     IndexingServiceClient client = new IndexingServiceClient(
 *             Arrays.asList("localhost:" + eis.getPort()));
 *     // ... use client ...
 * }
 * </pre>
 *
 * @author enrico.olivelli
 */
public class EmbeddedIndexingService implements AutoCloseable {

    private final Path logDirectory;
    private final Path dataDirectory;
    private final Properties config;
    private IndexingServiceEngine engine;
    private IndexingServer server;

    public EmbeddedIndexingService(Path logDirectory, Path dataDirectory) {
        this(logDirectory, dataDirectory, new Properties());
    }

    public EmbeddedIndexingService(Path logDirectory, Path dataDirectory, Properties config) {
        this.logDirectory = logDirectory;
        this.dataDirectory = dataDirectory;
        this.config = config;
    }

    public void start() throws Exception {
        engine = new IndexingServiceEngine(logDirectory, dataDirectory, config);
        engine.start();

        server = new IndexingServer("localhost", 0, engine, config);
        server.start();
    }

    public int getPort() {
        return server.getPort();
    }

    public String getAddress() {
        return "localhost:" + getPort();
    }

    public IndexingServiceEngine getEngine() {
        return engine;
    }

    public IndexingServiceClient createClient() {
        return new IndexingServiceClient(Arrays.asList(getAddress()));
    }

    @Override
    public void close() throws Exception {
        if (server != null) {
            server.stop();
        }
        if (engine != null) {
            engine.close();
        }
    }
}
