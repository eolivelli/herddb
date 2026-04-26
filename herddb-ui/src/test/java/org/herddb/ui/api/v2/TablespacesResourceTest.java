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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.model.TableSpace;
import java.util.List;
import org.herddb.ui.dto.TablespaceDTO;
import org.herddb.ui.internal.QueryService;
import org.junit.Rule;
import org.junit.Test;

/**
 * Direct-call test of {@link TablespacesResource} against a real embedded
 * {@code Server}. Verifies that the resource returns at least the default
 * tablespace ({@value herddb.model.TableSpace#DEFAULT}) and that DTO fields
 * are populated.
 */
public class TablespacesResourceTest {

    @Rule
    public final EmbeddedHerdDbServerRule serverRule = new EmbeddedHerdDbServerRule();

    @Test
    public void listsDefaultTablespace() {
        TablespacesResource resource = new TablespacesResource(
                new QueryService(serverRule.getServer()));

        List<TablespaceDTO> tablespaces = resource.list();

        assertNotNull("tablespace list must not be null", tablespaces);
        assertFalse(
                "tablespace list must contain at least the default tablespace",
                tablespaces.isEmpty());

        TablespaceDTO defaultTs = tablespaces.stream()
                .filter(t -> TableSpace.DEFAULT.equals(t.getName()))
                .findFirst()
                .orElse(null);
        assertNotNull(
                "must include tablespace " + TableSpace.DEFAULT + ", got " + tablespaces,
                defaultTs);

        assertNotNull("uuid must be populated", defaultTs.getUuid());
        assertFalse("uuid must not be blank", defaultTs.getUuid().isEmpty());
        assertNotNull("leader must be populated", defaultTs.getLeader());
        assertFalse("leader must not be blank", defaultTs.getLeader().isEmpty());
        assertNotNull("replicas list must not be null", defaultTs.getReplicas());
        assertTrue(
                "expectedReplicaCount must be >= 1, got " + defaultTs.getExpectedReplicaCount(),
                defaultTs.getExpectedReplicaCount() >= 1);
    }
}
