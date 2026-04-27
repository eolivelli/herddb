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
import org.herddb.ui.dto.HealthDTO;
import org.herddb.ui.internal.ServerLocator;
import org.junit.Rule;
import org.junit.Test;

/**
 * Direct-call test of {@link HealthResource} against a real embedded
 * {@code Server}. We call the resource's method directly (no JAX-RS / no
 * HTTP) — the corresponding HTTP smoke test lives in
 * {@link HealthResourceHttpSmokeTest}.
 */
public class HealthResourceTest {

    @Rule
    public final EmbeddedHerdDbServerRule serverRule = new EmbeddedHerdDbServerRule();

    @Test
    public void returnsOkAndNodeId() {
        HealthResource resource = new HealthResource(ServerLocator.of(serverRule.getServer()));

        long before = System.currentTimeMillis();
        HealthDTO health = resource.get();
        long after = System.currentTimeMillis();

        assertNotNull("HealthDTO must not be null", health);
        assertEquals("ok", health.getStatus());
        assertEquals(serverRule.getServer().getNodeId(), health.getNodeId());
        assertTrue(
                "timestamp must be in the test window",
                health.getTimestamp() >= before && health.getTimestamp() <= after);
    }
}
