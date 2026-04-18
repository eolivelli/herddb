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

package herddb.sql;

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.DataAccessor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SyslogstatusTest {

    @Test
    public void testSyslogstatusColumnsPresent() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager(nodeId,
                new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(),
                new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1,
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            try (DataScanner scan = scan(manager,
                    "SELECT * FROM tblspace1.syslogstatus", Collections.emptyList())) {
                List<DataAccessor> records = scan.consume();
                assertEquals(1, records.size());
                Map<String, Object> row = records.get(0).toMap();

                // Existing columns still present
                assertNotNull(row.get("tablespace_uuid"));
                assertEquals(nodeId, row.get("nodeid").toString());
                assertEquals("tblspace1", row.get("tablespace_name").toString());
                assertNotNull(row.get("ledger"));
                assertNotNull(row.get("offset"));
                assertEquals("leader", row.get("status").toString());

                // New checkpoint columns are null until the first checkpoint completes.
                assertNull("checkpoint_ledger must be null before any checkpoint",
                        row.get("checkpoint_ledger"));
                assertNull("checkpoint_offset must be null before any checkpoint",
                        row.get("checkpoint_offset"));
                assertNull("checkpoint_timestamp must be null before any checkpoint",
                        row.get("checkpoint_timestamp"));
                assertNull("checkpoint_duration_ms must be null before any checkpoint",
                        row.get("checkpoint_duration_ms"));
                assertNull("dirty_ledgers_count must be null before any checkpoint",
                        row.get("dirty_ledgers_count"));
            }
        }
    }

    @Test
    public void testSyslogstatusAfterCheckpoint() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager(nodeId,
                new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(),
                new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1,
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager,
                    "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)",
                    Collections.emptyList());
            execute(manager,
                    "INSERT INTO tblspace1.t1(k1, n1) VALUES('a', 1)",
                    Collections.emptyList());

            long beforeCheckpointMs = System.currentTimeMillis();
            manager.checkpoint();

            try (DataScanner scan = scan(manager,
                    "SELECT * FROM tblspace1.syslogstatus", Collections.emptyList())) {
                List<DataAccessor> records = scan.consume();
                assertEquals(1, records.size());
                Map<String, Object> row = records.get(0).toMap();

                Object cpLedger = row.get("checkpoint_ledger");
                Object cpOffset = row.get("checkpoint_offset");
                Object cpTs = row.get("checkpoint_timestamp");
                Object cpDuration = row.get("checkpoint_duration_ms");
                Object dirtyLedgers = row.get("dirty_ledgers_count");

                assertNotNull("checkpoint_ledger should be populated after checkpoint", cpLedger);
                assertNotNull("checkpoint_offset should be populated after checkpoint", cpOffset);
                assertNotNull("checkpoint_timestamp should be populated after checkpoint", cpTs);
                assertNotNull("checkpoint_duration_ms should be populated after checkpoint", cpDuration);
                assertNotNull("dirty_ledgers_count should be populated after checkpoint", dirtyLedgers);

                // checkpoint_offset must be <= current log offset (checkpoint is behind or equal to write head)
                long ledger = ((Number) row.get("ledger")).longValue();
                long offset = ((Number) row.get("offset")).longValue();
                long cpLedgerVal = ((Number) cpLedger).longValue();
                long cpOffsetVal = ((Number) cpOffset).longValue();
                assertTrue("checkpoint_ledger must not exceed current ledger",
                        cpLedgerVal <= ledger);
                if (cpLedgerVal == ledger) {
                    assertTrue("checkpoint_offset must not exceed current offset on same ledger",
                            cpOffsetVal <= offset);
                }

                // duration is non-negative
                long durationMs = ((Number) cpDuration).longValue();
                assertTrue("checkpoint_duration_ms should be >= 0", durationMs >= 0);

                // timestamp falls in a sensible window around the checkpoint call
                long cpTsMs = ((java.sql.Timestamp) cpTs).getTime();
                assertTrue("checkpoint_timestamp must be >= before-checkpoint time",
                        cpTsMs >= beforeCheckpointMs);
                assertTrue("checkpoint_timestamp must not be in the future",
                        cpTsMs <= System.currentTimeMillis() + 1000);

                int dirtyLedgersVal = ((Number) dirtyLedgers).intValue();
                assertTrue("dirty_ledgers_count must be >= 0", dirtyLedgersVal >= 0);
            }
        }
    }
}
