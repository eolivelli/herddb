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
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.herddb.ui.dto.HealthDTO;
import org.herddb.ui.internal.ServerLocator;

/**
 * Liveness probe for the Web UI v2 backend. Returns "ok" plus the running
 * node id, mirroring what {@code Server.getNodeId()} reports.
 *
 * <p>Mounted at {@code GET /api/v2/health}.
 *
 * <p>The {@link ServerLocator} is injected via HK2 — see
 * {@link ApplicationConfigV2} for how it is bound from the {@link
 * javax.servlet.ServletContext} attribute populated by {@code ServerMain}.
 * Direct-call unit tests can simply call the constructor with
 * {@link ServerLocator#of(Server)}.
 */
@Path("health")
public class HealthResource {

    private final ServerLocator locator;

    @Inject
    public HealthResource(ServerLocator locator) {
        if (locator == null) {
            throw new IllegalArgumentException("locator cannot be null");
        }
        this.locator = locator;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public HealthDTO get() {
        Server server = locator.getServer();
        return new HealthDTO("ok", server.getNodeId(), System.currentTimeMillis());
    }
}
