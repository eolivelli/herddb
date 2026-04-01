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

package herddb.indexing;

import static org.junit.Assert.*;

import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Table;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class TransactionBufferTest {

    private TransactionBuffer buffer;

    @Before
    public void setUp() {
        buffer = new TransactionBuffer();
    }

    private static Table buildTestTable(String name) {
        return Table.builder()
                .tablespace("default")
                .name(name)
                .column("id", ColumnTypes.LONG)
                .primaryKey("id")
                .build();
    }

    @Test
    public void testBeginAndCommit() {
        long txId = 1L;
        buffer.beginTransaction(txId);
        assertTrue(buffer.hasTransaction(txId));

        Table table = buildTestTable("t1");
        LogEntry entry1 = LogEntryFactory.createTable(table, null);
        LogEntry entry2 = LogEntryFactory.alterTable(table, null);
        LogSequenceNumber lsn1 = new LogSequenceNumber(1, 1);
        LogSequenceNumber lsn2 = new LogSequenceNumber(1, 2);

        buffer.addEntry(txId, lsn1, entry1);
        buffer.addEntry(txId, lsn2, entry2);

        List<TransactionBuffer.BufferedLogEntry> committed = buffer.commitTransaction(txId);
        assertEquals(2, committed.size());
        assertSame(entry1, committed.get(0).getEntry());
        assertSame(entry2, committed.get(1).getEntry());
        assertEquals(lsn1, committed.get(0).getLsn());
        assertEquals(lsn2, committed.get(1).getLsn());

        // After commit the transaction should no longer be tracked
        assertFalse(buffer.hasTransaction(txId));
    }

    @Test
    public void testRollback() {
        long txId = 2L;
        buffer.beginTransaction(txId);

        Table table = buildTestTable("t1");
        LogEntry entry = LogEntryFactory.createTable(table, null);
        buffer.addEntry(txId, new LogSequenceNumber(1, 1), entry);
        assertTrue(buffer.hasTransaction(txId));

        buffer.rollbackTransaction(txId);
        assertFalse(buffer.hasTransaction(txId));

        // Committing after rollback should return empty
        List<TransactionBuffer.BufferedLogEntry> result = buffer.commitTransaction(txId);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testMultipleTransactions() {
        long txId1 = 10L;
        long txId2 = 20L;
        buffer.beginTransaction(txId1);
        buffer.beginTransaction(txId2);

        Table table = buildTestTable("t1");
        LogEntry entry1 = LogEntryFactory.createTable(table, null);
        LogEntry entry2 = LogEntryFactory.alterTable(table, null);

        buffer.addEntry(txId1, new LogSequenceNumber(1, 1), entry1);
        buffer.addEntry(txId2, new LogSequenceNumber(1, 2), entry2);

        // Rollback tx1
        buffer.rollbackTransaction(txId1);
        assertFalse(buffer.hasTransaction(txId1));
        assertTrue(buffer.hasTransaction(txId2));

        // Commit tx2
        List<TransactionBuffer.BufferedLogEntry> committed = buffer.commitTransaction(txId2);
        assertEquals(1, committed.size());
        assertSame(entry2, committed.get(0).getEntry());
        assertFalse(buffer.hasTransaction(txId2));
    }

    @Test
    public void testCommitUnknownTransaction() {
        List<TransactionBuffer.BufferedLogEntry> result = buffer.commitTransaction(999L);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testAutoBeginOnAdd() {
        // addEntry with a txId that was never begun should auto-create the transaction
        long txId = 42L;
        assertFalse(buffer.hasTransaction(txId));

        Table table = buildTestTable("t1");
        LogEntry entry = LogEntryFactory.createTable(table, null);
        buffer.addEntry(txId, new LogSequenceNumber(1, 1), entry);

        assertTrue(buffer.hasTransaction(txId));
        List<TransactionBuffer.BufferedLogEntry> committed = buffer.commitTransaction(txId);
        assertEquals(1, committed.size());
        assertSame(entry, committed.get(0).getEntry());
    }
}
