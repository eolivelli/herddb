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

package org.herddb.ui.api.v2;

import herddb.server.Server;
import herddb.server.ServerConfiguration;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

/**
 * JUnit 4 rule that boots a real local-mode {@link Server} (no ZooKeeper, no
 * BookKeeper) into a temporary folder and tears it down after the test.
 *
 * <p>Use this rule from any test that needs a running HerdDB instance to
 * exercise the v2 REST resources end-to-end. Network ports auto-discover so
 * concurrent test classes do not collide.
 */
public final class EmbeddedHerdDbServerRule extends ExternalResource {

    private final TemporaryFolder tempFolder = new TemporaryFolder();
    private final String mode;
    private Path baseDir;
    private Server server;

    /**
     * Default flavour: starts the server in {@code local} mode (no
     * network listener, in-memory storage). Suitable for tests that only
     * exercise the SQL surface of the v2 REST endpoints.
     */
    public EmbeddedHerdDbServerRule() {
        this(ServerConfiguration.PROPERTY_MODE_LOCAL);
    }

    /**
     * Explicit-mode constructor. Pass
     * {@link ServerConfiguration#PROPERTY_MODE_STANDALONE} when the test
     * needs the file-backed storage (and therefore the B-Link PK index).
     */
    public EmbeddedHerdDbServerRule(String mode) {
        this.mode = mode;
    }

    @Override
    protected void before() throws Throwable {
        tempFolder.create();
        baseDir = tempFolder.getRoot().toPath();
        Files.createDirectories(baseDir);

        ServerConfiguration config = new ServerConfiguration(baseDir)
                .set(ServerConfiguration.PROPERTY_PORT,
                        ServerConfiguration.PROPERTY_PORT_AUTODISCOVERY)
                .set(ServerConfiguration.PROPERTY_MODE, mode);

        server = new Server(config);
        server.start();
        server.waitForStandaloneBoot();
    }

    @Override
    protected void after() {
        if (server != null) {
            try {
                server.close();
            } catch (Exception e) { // Server.close() throws checked Exception
                // Best-effort cleanup; the test has already run, so we just
                // log to stderr to avoid masking the original failure.
                System.err.println("Failed to close embedded HerdDB Server: " + e);
            }
            server = null;
        }
        tempFolder.delete();
    }

    public Server getServer() {
        if (server == null) {
            throw new IllegalStateException(
                    "EmbeddedHerdDbServerRule has not been started yet");
        }
        return server;
    }
}
