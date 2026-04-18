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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.DBManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.StatementExecutionException;
import herddb.model.commands.WaitForIndexesStatement;
import java.util.Collections;
import org.junit.Test;

/**
 * Parse + execute tests for the new {@code EXECUTE WAITFORINDEXES} SQL procedure.
 * With no external tailers configured, the default implementation of
 * {@link herddb.core.AbstractIndexManager#waitForTailersCatchUp} returns true,
 * so the procedure completes immediately.
 */
public class WaitForIndexesExecuteTest {

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
    public void waitForIndexesOnDefaultTablespaceWithNoIndexes() throws Exception {
        try (DBManager manager = buildManager()) {
            execute(manager, "CREATE TABLE herd.t1 (id int primary key, v varchar)", Collections.emptyList());
            execute(manager, "INSERT INTO herd.t1 VALUES (1, 'hello')", Collections.emptyList());
            execute(manager, "EXECUTE WAITFORINDEXES 'herd', 60", Collections.emptyList());
        }
    }

    @Test
    public void waitForIndexesOnEmptyTablespace() throws Exception {
        // No writes yet: log has no written LSN, so the procedure returns immediately.
        try (DBManager manager = buildManager()) {
            execute(manager, "EXECUTE WAITFORINDEXES 'herd', 60", Collections.emptyList());
        }
    }

    @Test
    public void requiresTimeoutArgument() throws Exception {
        try (DBManager manager = buildManager()) {
            try {
                execute(manager, "EXECUTE WAITFORINDEXES 'herd'", Collections.emptyList());
                fail("Expected StatementExecutionException");
            } catch (StatementExecutionException e) {
                assertTrue(e.getMessage(),
                        e.getMessage().contains("WAITFORINDEXES requires two parameters"));
            }
        }
    }

    @Test
    public void rejectsZeroTimeout() throws Exception {
        try (DBManager manager = buildManager()) {
            try {
                execute(manager, "EXECUTE WAITFORINDEXES 'herd', 0", Collections.emptyList());
                fail("Expected StatementExecutionException");
            } catch (StatementExecutionException e) {
                assertTrue(e.getMessage(),
                        e.getMessage().contains("strictly positive"));
            }
        }
    }

    @Test
    public void rejectsNegativeTimeout() throws Exception {
        try (DBManager manager = buildManager()) {
            try {
                execute(manager, "EXECUTE WAITFORINDEXES 'herd', -5", Collections.emptyList());
                fail("Expected StatementExecutionException");
            } catch (StatementExecutionException e) {
                assertTrue(e.getMessage(),
                        e.getMessage().contains("strictly positive"));
            }
        }
    }

    @Test
    public void statementConstructorRejectsNonPositiveTimeout() {
        try {
            new WaitForIndexesStatement("herd", 0L);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("strictly positive"));
        }
    }
}
