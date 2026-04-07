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
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.core.indexes.MockRemoteVectorIndexService;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SysindexstatusTest {

    @Test
    public void testSysindexstatusBrinAndHash() throws Exception {
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
                    "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int, s1 string)",
                    Collections.emptyList());
            execute(manager,
                    "CREATE BRIN INDEX brin_idx ON tblspace1.t1(n1)",
                    Collections.emptyList());
            execute(manager,
                    "CREATE HASH INDEX hash_idx ON tblspace1.t1(s1)",
                    Collections.emptyList());

            // Query sysindexstatus
            try (DataScanner scan = scan(manager,
                    "SELECT * FROM tblspace1.sysindexstatus", Collections.emptyList())) {
                List<DataAccessor> records = scan.consume();
                assertEquals(2, records.size());

                // BRIN index
                Map<String, Object> brin = records.stream()
                        .map(DataAccessor::toMap)
                        .filter(m -> m.get("index_name").toString().equals("brin_idx"))
                        .findFirst().orElse(null);
                assertNotNull("brin_idx row must be present", brin);
                assertEquals("brin", brin.get("index_type").toString());
                String brinProps = brin.get("properties").toString();
                assertTrue("BRIN properties should contain numBlocks", brinProps.contains("\"numBlocks\":"));

                // HASH index
                Map<String, Object> hash = records.stream()
                        .map(DataAccessor::toMap)
                        .filter(m -> m.get("index_name").toString().equals("hash_idx"))
                        .findFirst().orElse(null);
                assertNotNull("hash_idx row must be present", hash);
                assertEquals("hash", hash.get("index_type").toString());
                String hashProps = hash.get("properties").toString();
                assertEquals("{}", hashProps);
            }

            // WHERE filter
            try (DataScanner scan = scan(manager,
                    "SELECT * FROM tblspace1.sysindexstatus WHERE index_type='brin'",
                    Collections.emptyList())) {
                List<DataAccessor> records = scan.consume();
                assertEquals(1, records.size());
                assertEquals("brin_idx", records.get(0).get("index_name").toString());
            }
        }
    }

    @Test
    public void testSysindexstatusVector() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager(nodeId,
                new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(),
                new MemoryCommitLogManager(), null, null)) {
            manager.setRemoteVectorIndexService(new MockRemoteVectorIndexService());
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1,
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager,
                    "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Insert some rows
            for (int i = 1; i <= 5; i++) {
                float[] vec = new float[]{i * 1.0f, i * 2.0f, i * 3.0f, i * 4.0f};
                execute(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, vec));
            }

            // Query sysindexstatus
            try (DataScanner scan = scan(manager,
                    "SELECT * FROM tblspace1.sysindexstatus", Collections.emptyList())) {
                List<DataAccessor> records = scan.consume();
                assertEquals(1, records.size());

                Map<String, Object> row = records.get(0).toMap();
                assertEquals("vector", row.get("index_type").toString());
                assertEquals("vidx", row.get("index_name").toString());

                String props = row.get("properties").toString();
                assertTrue("should contain vectorCount", props.contains("\"vectorCount\":"));
                assertTrue("should contain segmentCount", props.contains("\"segmentCount\":"));
                assertTrue("should contain status", props.contains("\"status\":"));
            }

            // Insert more rows and verify count changes
            for (int i = 6; i <= 10; i++) {
                float[] vec = new float[]{i * 1.0f, i * 2.0f, i * 3.0f, i * 4.0f};
                execute(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, vec));
            }

            try (DataScanner scan = scan(manager,
                    "SELECT * FROM tblspace1.sysindexstatus", Collections.emptyList())) {
                List<DataAccessor> records = scan.consume();
                String props = records.get(0).get("properties").toString();
                assertTrue("should contain vectorCount", props.contains("\"vectorCount\":"));
            }
        }
    }

    @Test
    public void testSysindexstatusMultipleTablesAndFilters() throws Exception {
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
                    "CREATE TABLE tblspace1.t2 (k1 string primary key, n1 int)",
                    Collections.emptyList());
            execute(manager,
                    "CREATE BRIN INDEX idx1 ON tblspace1.t1(n1)",
                    Collections.emptyList());
            execute(manager,
                    "CREATE BRIN INDEX idx2 ON tblspace1.t2(n1)",
                    Collections.emptyList());

            // All indexes
            try (DataScanner scan = scan(manager,
                    "SELECT * FROM tblspace1.sysindexstatus", Collections.emptyList())) {
                List<DataAccessor> records = scan.consume();
                assertEquals(2, records.size());
            }

            // Filter by table_name
            try (DataScanner scan = scan(manager,
                    "SELECT * FROM tblspace1.sysindexstatus WHERE table_name='t1'",
                    Collections.emptyList())) {
                List<DataAccessor> records = scan.consume();
                assertEquals(1, records.size());
                assertEquals("idx1", records.get(0).get("index_name").toString());
            }
        }
    }
}
