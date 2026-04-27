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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import herddb.model.TableSpace;
import java.util.List;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import org.herddb.ui.dto.IndexStatusDTO;
import org.herddb.ui.dto.LogStatusDTO;
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

    @Test
    public void getReturnsSingleTablespaceByName() {
        TablespacesResource resource = new TablespacesResource(
                new QueryService(serverRule.getServer()));

        TablespaceDTO ts = resource.get(TableSpace.DEFAULT);

        assertNotNull("get(name) must not return null", ts);
        assertEquals(TableSpace.DEFAULT, ts.getName());
    }

    @Test
    public void getReturns404ForUnknownName() {
        TablespacesResource resource = new TablespacesResource(
                new QueryService(serverRule.getServer()));

        assertThrows(NotFoundException.class, () -> resource.get("does-not-exist"));
    }

    @Test
    public void getRejectsBlankName() {
        TablespacesResource resource = new TablespacesResource(
                new QueryService(serverRule.getServer()));

        WebApplicationException ex = assertThrows(
                WebApplicationException.class,
                () -> resource.get(""));
        assertEquals(400, ex.getResponse().getStatus());
    }

    @Test
    public void getLogStatusReturnsDataForDefaultTablespace() {
        TablespacesResource resource = new TablespacesResource(
                new QueryService(serverRule.getServer()));

        LogStatusDTO dto = resource.getLogStatus(TableSpace.DEFAULT);

        assertNotNull("log-status DTO must not be null", dto);
        assertNotNull("tablespace field must be populated", dto.getTablespace());
        assertFalse("tablespace field must not be blank", dto.getTablespace().isEmpty());
        assertNotNull("status must be populated", dto.getStatus());
        // ledger and offset are present even in local/in-memory mode.
        // In the initial "start of time" state, ledger and offset may both be
        // -1 (LogSequenceNumber.START_OF_TIME), so we only assert non-null.
        assertNotNull("ledger must not be null", dto.getLedger());
        assertNotNull("offset must not be null", dto.getOffset());
        // checkpoint fields may be null if no checkpoint has been taken yet —
        // that is valid in local/in-memory mode.
    }

    @Test
    public void getLogStatusThrowsForUnknownTablespace() {
        TablespacesResource resource = new TablespacesResource(
                new QueryService(serverRule.getServer()));

        // In direct-call tests (no Jersey), querying syslogstatus in a
        // non-existent tablespace propagates as an unchecked RuntimeException
        // (DataScannerException wraps the underlying NotLeaderException or
        // similar).  We assert that *something* is thrown without tying the
        // test to the exact type — Jersey would map it to a 5xx response in
        // production.
        assertThrows(RuntimeException.class,
                () -> resource.getLogStatus("does_not_exist_ts"));
    }

    @Test
    public void getLogStatusRejectsBlankName() {
        TablespacesResource resource = new TablespacesResource(
                new QueryService(serverRule.getServer()));

        WebApplicationException ex = assertThrows(
                WebApplicationException.class,
                () -> resource.getLogStatus(""));
        assertEquals(400, ex.getResponse().getStatus());
    }

    @Test
    public void listIndexesReturnsEmptyListWhenNoUserIndexes() {
        // A freshly started server has no user-created indexes; sysindexstatus
        // only reports indexes owned by user tables, so the list must be empty.
        TablespacesResource resource = new TablespacesResource(
                new QueryService(serverRule.getServer()));

        List<IndexStatusDTO> indexes = resource.listIndexes(TableSpace.DEFAULT);

        assertNotNull("index list must not be null", indexes);
        assertTrue(
                "expected no user indexes on a fresh server, got " + indexes,
                indexes.isEmpty());
    }
}
