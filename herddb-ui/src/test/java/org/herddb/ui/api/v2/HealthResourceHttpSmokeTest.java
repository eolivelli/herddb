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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.test.jetty.JettyTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.herddb.ui.dto.HealthDTO;
import org.herddb.ui.internal.ServerLocator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * HTTP smoke test that exercises the full JAX-RS + Jackson + Jetty stack
 * used in production. Boots an embedded HerdDB {@link Server} in local
 * mode and registers the v2 resource classes with an HK2 binder that
 * supplies a fixed {@link ServerLocator} (bypassing the production
 * {@code ServletContext} lookup so the test does not have to plug into a
 * real servlet context).
 *
 * <p>{@link JerseyTest} calls {@link #configure()} from its constructor
 * (before {@code @Before}/{@code setUp} runs), so the {@link Server} is
 * started in a {@link BeforeClass} hook and exposed through a static field
 * that {@link #configure()} can safely read.
 */
public class HealthResourceHttpSmokeTest extends JerseyTest {

    private static volatile Server embeddedServer;
    private static volatile Path baseDir;

    @BeforeClass
    public static void startEmbeddedServer() throws Exception {
        baseDir = Files.createTempDirectory("herddb-ui-smoke");
        ServerConfiguration config = new ServerConfiguration(baseDir)
                .set(ServerConfiguration.PROPERTY_PORT,
                        ServerConfiguration.PROPERTY_PORT_AUTODISCOVERY)
                .set(ServerConfiguration.PROPERTY_MODE,
                        ServerConfiguration.PROPERTY_MODE_LOCAL);
        embeddedServer = new Server(config);
        embeddedServer.start();
        embeddedServer.waitForStandaloneBoot();

        // Seed a user table so the table-detail smoke test can verify a
        // non-system table is reachable via the HTTP endpoint.
        DBManager manager = embeddedServer.getManager();
        manager.executeSimpleStatement(
                TableSpace.DEFAULT,
                "CREATE TABLE smoke_t (id INTEGER PRIMARY KEY, val STRING)",
                Collections.emptyList(), -1, false,
                TransactionContext.NO_TRANSACTION, null);
    }

    @AfterClass
    public static void stopEmbeddedServer() {
        if (embeddedServer != null) {
            try {
                embeddedServer.close();
            } catch (Exception e) { // Server.close() throws checked Exception
                System.err.println("Failed to close embedded HerdDB Server: " + e);
            }
            embeddedServer = null;
        }
        if (baseDir != null) {
            deleteRecursively(baseDir);
            baseDir = null;
        }
    }

    @Override
    protected TestContainerFactory getTestContainerFactory() {
        return new JettyTestContainerFactory();
    }

    @Override
    protected Application configure() {
        enable(TestProperties.LOG_TRAFFIC);
        Server server = embeddedServer;
        if (server == null) {
            throw new IllegalStateException(
                    "@BeforeClass must run before configure(); embeddedServer is null");
        }
        ServerLocator locator = ServerLocator.of(server);
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(JacksonFeature.class);
        resourceConfig.register(HealthResource.class);
        resourceConfig.register(TablespacesResource.class);
        resourceConfig.register(ServerInfoResource.class);
        resourceConfig.register(TablesResource.class);
        resourceConfig.register(TableResource.class);
        resourceConfig.register(IndexingServicesResource.class);
        resourceConfig.register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(locator).to(ServerLocator.class);
            }
        });
        return resourceConfig;
    }

    @Test
    public void healthEndpointReturnsOk() {
        WebTarget target = target("health");
        Response response = target.request(MediaType.APPLICATION_JSON).get();
        try {
            assertEquals(200, response.getStatus());
            HealthDTO health = response.readEntity(HealthDTO.class);
            assertNotNull(health);
            assertEquals("ok", health.getStatus());
            assertEquals(embeddedServer.getNodeId(), health.getNodeId());
        } finally {
            response.close();
        }
    }

    @Test
    public void tablespacesEndpointReturnsJsonArray() {
        WebTarget target = target("tablespaces");
        Response response = target.request(MediaType.APPLICATION_JSON).get();
        try {
            assertEquals(200, response.getStatus());
            String json = response.readEntity(String.class);
            assertNotNull(json);
            // Cheap shape check: the body must be a JSON array containing
            // at least the default tablespace ("herd").
            assertEquals('[', json.charAt(0));
            assertEquals(']', json.charAt(json.length() - 1));
            assertTrue(
                    "response body must contain the default tablespace name, got: " + json,
                    json.contains("\"name\":\"herd\""));
        } finally {
            response.close();
        }
    }

    @Test
    public void serverInfoEndpointReturnsNodeIdAndMode() {
        WebTarget target = target("server-info");
        Response response = target.request(MediaType.APPLICATION_JSON).get();
        try {
            assertEquals(200, response.getStatus());
            String json = response.readEntity(String.class);
            assertNotNull(json);
            assertTrue(
                    "response body must contain the node id, got: " + json,
                    json.contains("\"nodeId\":\"" + embeddedServer.getNodeId() + "\""));
            assertTrue(
                    "response body must contain mode local, got: " + json,
                    json.contains("\"mode\":\"local\""));
            assertTrue(
                    "response body must contain default tablespace herd, got: " + json,
                    json.contains("\"defaultTablespace\":\"herd\""));
        } finally {
            response.close();
        }
    }

    /**
     * Exercises the full Jetty+Jersey routing for the table-detail endpoint
     * ({@code GET /tablespaces/{ts}/tables/{name}}) which was previously
     * returning 404 because {@code IndexesResource} (now {@link TableResource})
     * shadowed {@code TablesResource}'s sub-path handler without providing a
     * {@code @GET} at the class root.
     */
    @Test
    public void tableDetailEndpointReturnsMetadata() {
        // System table "syscolumns" always exists; user table "smoke_t" was
        // seeded in @BeforeClass — verify both are reachable.
        for (String tableName : new String[]{"syscolumns", "smoke_t"}) {
            WebTarget target = target("tablespaces/herd/tables/" + tableName);
            Response response = target.request(MediaType.APPLICATION_JSON).get();
            try {
                assertEquals(
                        "GET tablespaces/herd/tables/" + tableName + " must return 200",
                        200, response.getStatus());
                String json = response.readEntity(String.class);
                assertNotNull(json);
                assertTrue(
                        "response for " + tableName + " must contain its name, got: " + json,
                        json.toLowerCase(java.util.Locale.ROOT)
                                .contains("\"name\":\"" + tableName.toLowerCase(java.util.Locale.ROOT) + "\""));
                assertTrue(
                        "response for " + tableName + " must contain a columns array, got: " + json,
                        json.contains("\"columns\":["));
            } finally {
                response.close();
            }
        }
    }

    private static void deleteRecursively(Path dir) {
        try {
            if (!Files.exists(dir)) {
                return;
            }
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignore) {
                            // best-effort cleanup; the OS temp folder will
                            // be reaped eventually
                        }
                    });
        } catch (IOException ignore) {
            // best-effort cleanup
        }
    }
}
