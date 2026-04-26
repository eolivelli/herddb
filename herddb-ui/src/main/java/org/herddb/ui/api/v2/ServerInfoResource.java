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

import herddb.model.TableSpace;
import herddb.server.Server;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.herddb.ui.dto.ServerInfoDTO;
import org.herddb.ui.internal.ServerLocator;

/**
 * Server-level metadata endpoint used by the SPA header to render the node
 * id, the deployment mode (local / standalone / cluster / shared-storage),
 * the JDBC URL, and the default tablespace.
 *
 * <p>Mounted at {@code GET /api/v2/server-info}.
 */
@Path("server-info")
public class ServerInfoResource {

    private final ServerLocator locator;

    @Inject
    public ServerInfoResource(ServerLocator locator) {
        if (locator == null) {
            throw new IllegalArgumentException("locator cannot be null");
        }
        this.locator = locator;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public ServerInfoDTO get() {
        Server server = locator.getServer();
        return new ServerInfoDTO(
                server.getNodeId(),
                server.getManager().getMode(),
                server.getJdbcUrl(),
                TableSpace.DEFAULT);
    }
}
