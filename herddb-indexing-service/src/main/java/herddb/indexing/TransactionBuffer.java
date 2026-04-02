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

import herddb.log.LogEntry;
import herddb.log.LogSequenceNumber;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Buffers log entries per transaction until COMMIT or ROLLBACK.
 *
 * @author enrico.olivelli
 */
public class TransactionBuffer {

    /**
     * A buffered log entry with its LSN.
     */
    public static class BufferedLogEntry {
        private final LogSequenceNumber lsn;
        private final LogEntry entry;

        public BufferedLogEntry(LogSequenceNumber lsn, LogEntry entry) {
            this.lsn = lsn;
            this.entry = entry;
        }

        public LogSequenceNumber getLsn() {
            return lsn;
        }

        public LogEntry getEntry() {
            return entry;
        }
    }

    private final Map<Long, List<BufferedLogEntry>> pendingTransactions = new HashMap<>();

    /**
     * Begins tracking a new transaction.
     */
    public void beginTransaction(long txId) {
        pendingTransactions.put(txId, new ArrayList<>());
    }

    /**
     * Adds an entry to the buffer for the given transaction.
     * If the transaction has not been explicitly begun, it is implicitly started.
     */
    public void addEntry(long txId, LogSequenceNumber lsn, LogEntry entry) {
        pendingTransactions.computeIfAbsent(txId, k -> new ArrayList<>())
                .add(new BufferedLogEntry(lsn, entry));
    }

    /**
     * Commits the transaction, returning all buffered entries in order.
     * The transaction is removed from the buffer.
     *
     * @return the buffered entries, or an empty list if the transaction was not tracked
     */
    public List<BufferedLogEntry> commitTransaction(long txId) {
        List<BufferedLogEntry> entries = pendingTransactions.remove(txId);
        return entries != null ? entries : Collections.emptyList();
    }

    /**
     * Rolls back the transaction, discarding all buffered entries.
     */
    public void rollbackTransaction(long txId) {
        pendingTransactions.remove(txId);
    }

    /**
     * Checks whether a transaction is currently being tracked.
     */
    public boolean hasTransaction(long txId) {
        return pendingTransactions.containsKey(txId);
    }
}
