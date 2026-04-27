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
import herddb.model.TableSpace;
import org.herddb.ui.dto.ServerInfoDTO;
import org.herddb.ui.internal.ServerLocator;
import org.junit.Rule;
import org.junit.Test;

/**
 * Direct-call test of {@link ServerInfoResource} against a real embedded
 * {@code Server}.
 */
public class ServerInfoResourceTest {

    @Rule
    public final EmbeddedHerdDbServerRule serverRule = new EmbeddedHerdDbServerRule();

    @Test
    public void returnsModeAndNodeId() {
        ServerInfoResource resource = new ServerInfoResource(
                ServerLocator.of(serverRule.getServer()));

        ServerInfoDTO info = resource.get();

        assertNotNull("ServerInfoDTO must not be null", info);
        assertEquals(serverRule.getServer().getNodeId(), info.getNodeId());
        // EmbeddedHerdDbServerRule starts the server in local mode.
        assertEquals("local", info.getMode());
        assertEquals(TableSpace.DEFAULT, info.getDefaultTablespace());
    }
}
