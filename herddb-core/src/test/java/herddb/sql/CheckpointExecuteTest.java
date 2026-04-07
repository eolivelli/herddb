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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.DBManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementExecutionException;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class CheckpointExecuteTest {

    private DBManager buildManager() throws Exception {
        DBManager manager = new DBManager("localhost",
                new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(),
                new MemoryCommitLogManager(), null, null);
        manager.start();
        assertTrue(manager.waitForBootOfLocalTablespaces(10000));
        return manager;
    }

    @Test
    public void checkpointDefaultTablespace() throws Exception {
        try (DBManager manager = buildManager()) {
            execute(manager, "EXECUTE CHECKPOINT 'herd'", Collections.emptyList());
        }
    }

    @Test
    public void checkpointCustomTablespace() throws Exception {
        try (DBManager manager = buildManager()) {
            execute(manager, "CREATE TABLESPACE 'tblspace1'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "EXECUTE CHECKPOINT 'tblspace1'", Collections.emptyList());
        }
    }

    @Test
    public void checkpointWithData() throws Exception {
        try (DBManager manager = buildManager()) {
            execute(manager, "CREATE TABLE herd.t1 (id int primary key, v varchar)", Collections.emptyList());
            execute(manager, "INSERT INTO herd.t1 VALUES (1, 'hello')", Collections.emptyList());
            execute(manager, "EXECUTE CHECKPOINT 'herd'", Collections.emptyList());
            try (DataScanner scan = scan(manager, "SELECT * FROM herd.t1", Collections.emptyList())) {
                List rows = scan.consume();
                assertEquals(1, rows.size());
            }
        }
    }

    @Test
    public void checkpointRequiresTablespaceParam() throws Exception {
        try (DBManager manager = buildManager()) {
            try {
                execute(manager, "EXECUTE CHECKPOINT", Collections.emptyList());
                fail("Expected StatementExecutionException");
            } catch (StatementExecutionException e) {
                assertTrue(e.getMessage().contains("CHECKPOINT requires 1 or 2 parameters"));
            }
        }
    }
}
