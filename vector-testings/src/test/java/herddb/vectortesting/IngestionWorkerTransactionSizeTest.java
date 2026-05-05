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
package herddb.vectortesting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import herddb.jdbc.PreparedStatementAsync;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Issue #401: behavioural tests for the split between batch-size (per
 * {@code executeBatch} flush) and transaction-size (per {@code commit}).
 *
 * <p>Each test drives {@code IngestionWorker.runIngestLoop} directly with a
 * stubbed {@link PreparedStatement}/{@link Connection} so we can assert the
 * exact sequence of executeBatch/commit calls and their sizes. Reuses the
 * same fake-PS/fake-Conn pattern as {@code IngestionWorkerTest}.
 */
class IngestionWorkerTransactionSizeTest {

    /**
     * Records the size of each sub-batch flush and commit in chronological order.
     */
    private static final class CallTrace {
        final List<Integer> flushSizes = new ArrayList<>();
        final List<Integer> commitSizes = new ArrayList<>();
        /** Total rows added to the PS since the last flush — what the next flush will report. */
        int rowsInPs = 0;
        /** Cumulative rows added to the PS since the last commit. */
        int rowsSinceCommit = 0;
    }

    /**
     * Drives {@code runIngestLoop} on a worker with the given {@link Config}
     * and {@code numRows} rows enqueued. Returns the call trace.
     */
    private static CallTrace driveLoop(Config cfg, int numRows) throws Exception {
        return driveLoop(cfg, numRows, null);
    }

    private static CallTrace driveLoop(Config cfg, int numRows, Runnable beforePoisonPill)
            throws Exception {
        CallTrace trace = new CallTrace();
        LinkedBlockingQueue<float[]> queue = new LinkedBlockingQueue<>();
        for (int i = 0; i < numRows; i++) {
            queue.put(new float[]{(float) i, (float) i, (float) i});
        }
        if (beforePoisonPill != null) {
            beforePoisonPill.run();
        }
        queue.put(new float[0]); // poison pill

        AtomicBoolean done = new AtomicBoolean(true);

        IngestionWorker worker = new IngestionWorker(
                cfg,
                queue,
                done,
                new AtomicLong(0),
                new MetricsCollector(),
                new AtomicReference<>(""),
                System.nanoTime(),
                new AtomicLong(0),
                new AtomicLong(0),
                new AtomicLong(0));
        worker.backoffBaseMillis = 0L;

        FakePs ps = new FakePs(trace);
        FakeConn conn = new FakeConn(trace);
        worker.runIngestLoop(conn, ps);
        return trace;
    }

    // ---- (1) default behaviour: transaction-size unset == batch-size ----

    @Test
    void defaultBehaviorOneFlushPerCommit() throws Exception {
        Config cfg = new Config();
        cfg.batchSize = 5;
        cfg.transactionSize = 0; // default: track batchSize
        CallTrace trace = driveLoop(cfg, 20);

        // 20 / 5 = 4 commits, each preceded by exactly one flush of size 5.
        assertEquals(List.of(5, 5, 5, 5), trace.flushSizes);
        assertEquals(List.of(5, 5, 5, 5), trace.commitSizes);
    }

    // ---- (2) transaction-size = N × batch-size ----

    @Test
    void transactionSizeMultipleOfBatchSize() throws Exception {
        Config cfg = new Config();
        cfg.batchSize = 250;
        cfg.transactionSize = 1000; // 4 sub-batches per commit
        CallTrace trace = driveLoop(cfg, 2000);

        // 2 commits, each preceded by 4 flushes of 250.
        assertEquals(List.of(250, 250, 250, 250, 250, 250, 250, 250), trace.flushSizes);
        assertEquals(List.of(1000, 1000), trace.commitSizes);
    }

    // ---- (3) transaction-size NOT a multiple of batch-size ----

    @Test
    void transactionSizeNotMultipleOfBatchSizeYieldsRemainder() throws Exception {
        Config cfg = new Config();
        cfg.batchSize = 300;
        cfg.transactionSize = 1000; // 300, 300, 300, 100 per transaction
        CallTrace trace = driveLoop(cfg, 1000);

        // One transaction: three full sub-batches of 300, then a remainder of 100.
        assertEquals(List.of(300, 300, 300, 100), trace.flushSizes);
        // The transaction was committed once, with 1000 rows in total.
        assertEquals(List.of(1000), trace.commitSizes);
    }

    // ---- (4) end-of-input drain: transaction-size > total rows ----

    @Test
    void endOfInputDrainCommitsRemainderTransaction() throws Exception {
        Config cfg = new Config();
        cfg.batchSize = 250;
        cfg.transactionSize = 750; // 1500 rows -> 750 + 750 commits
        CallTrace trace = driveLoop(cfg, 1500);

        // Two full transactions of 750 each; each transaction = 3 flushes of 250.
        assertEquals(List.of(250, 250, 250, 250, 250, 250), trace.flushSizes);
        assertEquals(List.of(750, 750), trace.commitSizes);
    }

    @Test
    void endOfInputDrainCommitsPartialTransaction() throws Exception {
        Config cfg = new Config();
        cfg.batchSize = 250;
        cfg.transactionSize = 1000; // 1500 rows -> 1000 + 500 (partial)
        CallTrace trace = driveLoop(cfg, 1500);

        // Tx 1: 4 flushes of 250 -> commit 1000.
        // Tx 2 (partial drain): 2 flushes of 250 -> commit 500.
        assertEquals(List.of(250, 250, 250, 250, 250, 250), trace.flushSizes);
        assertEquals(List.of(1000, 500), trace.commitSizes);
    }

    // ---- (5) batch-size == 1 with transaction-size > 1 ----

    @Test
    void batchSizeOneWithLargerTransactionSize() throws Exception {
        Config cfg = new Config();
        cfg.batchSize = 1;
        cfg.transactionSize = 4;
        CallTrace trace = driveLoop(cfg, 8);

        // 4 flushes of 1 per transaction × 2 transactions.
        assertEquals(List.of(1, 1, 1, 1, 1, 1, 1, 1), trace.flushSizes);
        assertEquals(List.of(4, 4), trace.commitSizes);
    }

    // ---- (6) partial-success on a sub-batch (not the final one) ----

    @Test
    void partialSuccessOnSubBatchAbortsTransactionAndPropagates() {
        Config cfg = new Config();
        cfg.batchSize = 100;
        cfg.transactionSize = 400; // 4 flushes per transaction
        cfg.ingestCommitRetries = 0;

        // Use a custom driver: PS reports 1 row dropped on the 2nd flush.
        CallTrace trace = new CallTrace();
        LinkedBlockingQueue<float[]> queue = new LinkedBlockingQueue<>();
        for (int i = 0; i < 400; i++) {
            queue.add(new float[]{(float) i});
        }
        queue.add(new float[0]); // poison pill

        IngestionWorker worker = new IngestionWorker(
                cfg, queue, new AtomicBoolean(true), new AtomicLong(0),
                new MetricsCollector(), new AtomicReference<>(""), System.nanoTime(),
                new AtomicLong(0), new AtomicLong(0), new AtomicLong(0));
        worker.backoffBaseMillis = 0L;

        FakePs ps = new FakePs(trace) {
            int flushCount = 0;

            @Override
            public CompletableFuture<int[]> executeBatchAsync() {
                flushCount++;
                trace.flushSizes.add(trace.rowsInPs);
                int reported = trace.rowsInPs;
                if (flushCount == 2) {
                    reported = trace.rowsInPs - 1; // drop one row
                }
                int[] counts = new int[trace.rowsInPs];
                int filled = 0;
                for (int i = 0; i < trace.rowsInPs && filled < reported; i++) {
                    counts[i] = 1;
                    filled++;
                }
                trace.rowsInPs = 0;
                return CompletableFuture.completedFuture(counts);
            }
        };
        FakeConn conn = new FakeConn(trace);

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> {
                    try {
                        worker.runIngestLoop(conn, ps);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        // The transaction must NOT have been committed.
        assertEquals(List.of(), trace.commitSizes);
        // The 2nd flush was where the partial-success was detected.
        assertEquals(2, trace.flushSizes.size());
        // The cause is BatchPartialSuccessException.
        Throwable cause = ex.getCause();
        while (cause != null && !(cause instanceof IngestionWorker.BatchPartialSuccessException)) {
            cause = cause.getCause();
        }
        if (cause == null) {
            throw new AssertionError("Expected BatchPartialSuccessException in cause chain; got: " + ex);
        }
    }

    // ---- (7) live decrease of batch-size mid-transaction ----

    @Test
    void liveBatchSizeDecreaseTakesEffectAtNextFlushBoundary() throws Exception {
        Config cfg = new Config();
        cfg.batchSize = 200;
        cfg.transactionSize = 800;
        cfg.ingestCommitRetries = 0;

        // First transaction runs at batch=200; we'll lower batch-size to 100
        // before the second transaction starts. Drive 1600 rows total so we
        // see two transactions with two distinct flush patterns.
        CallTrace trace = new CallTrace();
        LinkedBlockingQueue<float[]> queue = new LinkedBlockingQueue<>();
        for (int i = 0; i < 800; i++) {
            queue.add(new float[]{(float) i});
        }
        // After the first 800 rows are enqueued, lower batch-size before the
        // second batch of 800 rows. We do that by inserting a "marker" flush
        // — simpler: enqueue all 1600 then expect the worker to use batch=200
        // for both transactions (since the change is queued ahead of time).
        for (int i = 800; i < 1600; i++) {
            queue.add(new float[]{(float) i});
        }
        queue.add(new float[0]); // poison pill

        // Use a fake PS that lowers batch-size after the 4th flush completes
        // (i.e. between transaction 1's last flush and transaction 2's first flush).
        FakePs ps = new FakePs(trace) {
            int flushCount = 0;

            @Override
            public CompletableFuture<int[]> executeBatchAsync() {
                CompletableFuture<int[]> f = super.executeBatchAsync();
                flushCount++;
                if (flushCount == 4) {
                    // First transaction (4 flushes of 200) just committed; lower batch-size.
                    cfg.batchSize = 100;
                }
                return f;
            }
        };
        FakeConn conn = new FakeConn(trace);

        IngestionWorker worker = new IngestionWorker(
                cfg, queue, new AtomicBoolean(true), new AtomicLong(0),
                new MetricsCollector(), new AtomicReference<>(""), System.nanoTime(),
                new AtomicLong(0), new AtomicLong(0), new AtomicLong(0));
        worker.backoffBaseMillis = 0L;

        worker.runIngestLoop(conn, ps);

        // Transaction 1: 4 flushes of 200 (batch was 200), commit 800.
        // Transaction 2: 8 flushes of 100 (batch lowered to 100), commit 800.
        assertEquals(List.of(200, 200, 200, 200, 100, 100, 100, 100, 100, 100, 100, 100),
                trace.flushSizes);
        assertEquals(List.of(800, 800), trace.commitSizes);
    }

    // ---- (8) live increase of transaction-size mid-run ----

    @Test
    void liveTransactionSizeIncreaseTakesEffectAtNextTransactionBoundary() throws Exception {
        Config cfg = new Config();
        cfg.batchSize = 100;
        cfg.transactionSize = 200; // start small
        cfg.ingestCommitRetries = 0;

        CallTrace trace = new CallTrace();
        LinkedBlockingQueue<float[]> queue = new LinkedBlockingQueue<>();
        // Enough rows to exercise: one transaction at 200 rows, then 800 more
        // rows after we increase transaction-size to 800.
        for (int i = 0; i < 1000; i++) {
            queue.add(new float[]{(float) i});
        }
        queue.add(new float[0]);

        // After the first commit of 200, raise transaction-size to 800.
        FakeConn conn = new FakeConn(trace) {
            int commitCount = 0;

            @Override
            public void commit() throws SQLException {
                super.commit();
                commitCount++;
                if (commitCount == 1) {
                    cfg.transactionSize = 800;
                }
            }
        };
        FakePs ps = new FakePs(trace);

        IngestionWorker worker = new IngestionWorker(
                cfg, queue, new AtomicBoolean(true), new AtomicLong(0),
                new MetricsCollector(), new AtomicReference<>(""), System.nanoTime(),
                new AtomicLong(0), new AtomicLong(0), new AtomicLong(0));
        worker.backoffBaseMillis = 0L;

        worker.runIngestLoop(conn, ps);

        // Tx 1 at txn=200: 2 flushes of 100, commit 200.
        // Tx 2 at txn=800: 8 flushes of 100, commit 800.
        assertEquals(List.of(100, 100, 100, 100, 100, 100, 100, 100, 100, 100),
                trace.flushSizes);
        assertEquals(List.of(200, 800), trace.commitSizes);
    }

    // ---- helpers ----

    private static class FakePs implements PreparedStatement, PreparedStatementAsync {
        protected final CallTrace trace;
        protected final ConcurrentLinkedQueue<Integer> pendingPerCall = new ConcurrentLinkedQueue<>();

        FakePs(CallTrace trace) {
            this.trace = trace;
        }

        @Override
        public CompletableFuture<int[]> executeBatchAsync() {
            int n = trace.rowsInPs;
            trace.flushSizes.add(n);
            int[] counts = new int[n];
            for (int i = 0; i < n; i++) {
                counts[i] = 1;
            }
            trace.rowsInPs = 0;
            return CompletableFuture.completedFuture(counts);
        }

        @Override

        public CompletableFuture<Long> executeLargeUpdateAsync() {

            throw new UnsupportedOperationException();

        }
        @Override

        public CompletableFuture<Integer> executeUpdateAsync() {

            throw new UnsupportedOperationException();

        }
        @Override

        public <T> T unwrap(Class<T> iface) {

            return (T) this;

        }
        @Override

        public boolean isWrapperFor(Class<?> iface) {

            return iface == PreparedStatementAsync.class;

        }
        @Override
        public void addBatch() {
            trace.rowsInPs++;
            trace.rowsSinceCommit++;
        }

        @Override

        public void clearBatch() {

            trace.rowsInPs = 0;

        }
        @Override

        public void setLong(int p, long x) {

        }
        @Override

        public void setObject(int p, Object x) {

        }
        @Override

        public int[] executeBatch() {

            return new int[0];

        }
        // ---- minimal stubs for the rest of PreparedStatement ----
        @Override
        public java.sql.ResultSet executeQuery() {
            throw new UnsupportedOperationException();
        }
        @Override
        public int executeUpdate() {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setNull(int p, int t) {
        }
        @Override
        public void setBoolean(int p, boolean x) {
        }
        @Override
        public void setByte(int p, byte x) {
        }
        @Override
        public void setShort(int p, short x) {
        }
        @Override
        public void setInt(int p, int x) {
        }
        @Override
        public void setFloat(int p, float x) {
        }
        @Override
        public void setDouble(int p, double x) {
        }
        @Override
        public void setBigDecimal(int p, java.math.BigDecimal x) {
        }
        @Override
        public void setString(int p, String x) {
        }
        @Override
        public void setBytes(int p, byte[] x) {
        }
        @Override
        public void setDate(int p, java.sql.Date x) {
        }
        @Override
        public void setTime(int p, java.sql.Time x) {
        }
        @Override
        public void setTimestamp(int p, java.sql.Timestamp x) {
        }
        @Override
        public void setAsciiStream(int p, java.io.InputStream x, int l) {
        }
        @Override
        public void setUnicodeStream(int p, java.io.InputStream x, int l) {
        }
        @Override
        public void setBinaryStream(int p, java.io.InputStream x, int l) {
        }
        @Override
        public void clearParameters() {
        }
        @Override
        public void setObject(int p, Object x, int t) {
        }
        @Override
        public boolean execute() {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setCharacterStream(int p, java.io.Reader r, int l) {
        }
        @Override
        public void setRef(int p, java.sql.Ref x) {
        }
        @Override
        public void setBlob(int p, java.sql.Blob x) {
        }
        @Override
        public void setClob(int p, java.sql.Clob x) {
        }
        @Override
        public void setArray(int p, java.sql.Array x) {
        }
        @Override
        public java.sql.ResultSetMetaData getMetaData() {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setDate(int p, java.sql.Date x, java.util.Calendar c) {
        }
        @Override
        public void setTime(int p, java.sql.Time x, java.util.Calendar c) {
        }
        @Override
        public void setTimestamp(int p, java.sql.Timestamp x, java.util.Calendar c) {
        }
        @Override
        public void setNull(int p, int t, String n) {
        }
        @Override
        public void setURL(int p, java.net.URL x) {
        }
        @Override
        public java.sql.ParameterMetaData getParameterMetaData() {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setRowId(int p, java.sql.RowId x) {
        }
        @Override
        public void setNString(int p, String x) {
        }
        @Override
        public void setNCharacterStream(int p, java.io.Reader r, long l) {
        }
        @Override
        public void setNClob(int p, java.sql.NClob x) {
        }
        @Override
        public void setClob(int p, java.io.Reader r, long l) {
        }
        @Override
        public void setBlob(int p, java.io.InputStream s, long l) {
        }
        @Override
        public void setNClob(int p, java.io.Reader r, long l) {
        }
        @Override
        public void setSQLXML(int p, java.sql.SQLXML x) {
        }
        @Override
        public void setObject(int p, Object x, int t, int s) {
        }
        @Override
        public void setAsciiStream(int p, java.io.InputStream x, long l) {
        }
        @Override
        public void setBinaryStream(int p, java.io.InputStream x, long l) {
        }
        @Override
        public void setCharacterStream(int p, java.io.Reader r, long l) {
        }
        @Override
        public void setAsciiStream(int p, java.io.InputStream x) {
        }
        @Override
        public void setBinaryStream(int p, java.io.InputStream x) {
        }
        @Override
        public void setCharacterStream(int p, java.io.Reader r) {
        }
        @Override
        public void setNCharacterStream(int p, java.io.Reader r) {
        }
        @Override
        public void setClob(int p, java.io.Reader r) {
        }
        @Override
        public void setBlob(int p, java.io.InputStream s) {
        }
        @Override
        public void setNClob(int p, java.io.Reader r) {
        }
        @Override
        public java.sql.ResultSet executeQuery(String s) {
            throw new UnsupportedOperationException();
        }
        @Override
        public int executeUpdate(String s) {
            throw new UnsupportedOperationException();
        }
        @Override
        public void close() {
        }
        @Override
        public int getMaxFieldSize() {
            return 0;
        }
        @Override
        public void setMaxFieldSize(int m) {
        }
        @Override
        public int getMaxRows() {
            return 0;
        }
        @Override
        public void setMaxRows(int m) {
        }
        @Override
        public void setEscapeProcessing(boolean e) {
        }
        @Override
        public int getQueryTimeout() {
            return 0;
        }
        @Override
        public void setQueryTimeout(int s) {
        }
        @Override
        public void cancel() {
        }
        @Override
        public java.sql.SQLWarning getWarnings() {
            return null;
        }
        @Override
        public void clearWarnings() {
        }
        @Override
        public void setCursorName(String n) {
        }
        @Override
        public boolean execute(String s) {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.ResultSet getResultSet() {
            throw new UnsupportedOperationException();
        }
        @Override
        public int getUpdateCount() {
            return 0;
        }
        @Override
        public boolean getMoreResults() {
            return false;
        }
        @Override
        public void setFetchDirection(int d) {
        }
        @Override
        public int getFetchDirection() {
            return 0;
        }
        @Override
        public void setFetchSize(int r) {
        }
        @Override
        public int getFetchSize() {
            return 0;
        }
        @Override
        public int getResultSetConcurrency() {
            return 0;
        }
        @Override
        public int getResultSetType() {
            return 0;
        }
        @Override
        public void addBatch(String s) {
        }
        @Override
        public java.sql.Connection getConnection() {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean getMoreResults(int c) {
            return false;
        }
        @Override
        public java.sql.ResultSet getGeneratedKeys() {
            throw new UnsupportedOperationException();
        }
        @Override
        public int executeUpdate(String s, int a) {
            throw new UnsupportedOperationException();
        }
        @Override
        public int executeUpdate(String s, int[] c) {
            throw new UnsupportedOperationException();
        }
        @Override
        public int executeUpdate(String s, String[] c) {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean execute(String s, int a) {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean execute(String s, int[] c) {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean execute(String s, String[] c) {
            throw new UnsupportedOperationException();
        }
        @Override
        public int getResultSetHoldability() {
            return 0;
        }
        @Override
        public boolean isClosed() {
            return false;
        }
        @Override
        public void setPoolable(boolean p) {
        }
        @Override
        public boolean isPoolable() {
            return false;
        }
        @Override
        public void closeOnCompletion() {
        }
        @Override
        public boolean isCloseOnCompletion() {
            return false;
        }
    }

    private static class FakeConn implements Connection {
        private final CallTrace trace;
        private int rollbackCount = 0;

        FakeConn(CallTrace trace) {
            this.trace = trace;
        }

        @Override
        public void commit() throws SQLException {
            // The committed transaction's row count is rowsSinceCommit.
            trace.commitSizes.add(trace.rowsSinceCommit);
            trace.rowsSinceCommit = 0;
        }

        @Override
        public void rollback() {
            rollbackCount++;
            trace.rowsSinceCommit = 0;
            trace.rowsInPs = 0;
        }

        @Override

        public void setAutoCommit(boolean a) {

        }
        @Override

        public boolean getAutoCommit() {

            return false;

        }
        // ---- minimal stubs ----
        @Override
        public java.sql.Statement createStatement() {
            throw new UnsupportedOperationException();
        }
        @Override
        public PreparedStatement prepareStatement(String s) {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.CallableStatement prepareCall(String s) {
            throw new UnsupportedOperationException();
        }
        @Override
        public String nativeSQL(String s) {
            return s;
        }
        @Override
        public void close() {
        }
        @Override
        public boolean isClosed() {
            return false;
        }
        @Override
        public java.sql.DatabaseMetaData getMetaData() {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setReadOnly(boolean r) {
        }
        @Override
        public boolean isReadOnly() {
            return false;
        }
        @Override
        public void setCatalog(String c) {
        }
        @Override
        public String getCatalog() {
            return null;
        }
        @Override
        public void setTransactionIsolation(int l) {
        }
        @Override
        public int getTransactionIsolation() {
            return 0;
        }
        @Override
        public java.sql.SQLWarning getWarnings() {
            return null;
        }
        @Override
        public void clearWarnings() {
        }
        @Override
        public java.sql.Statement createStatement(int t, int c) {
            throw new UnsupportedOperationException();
        }
        @Override
        public PreparedStatement prepareStatement(String s, int t, int c) {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.CallableStatement prepareCall(String s, int t, int c) {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.util.Map<String, Class<?>> getTypeMap() {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setTypeMap(java.util.Map<String, Class<?>> m) {
        }
        @Override
        public void setHoldability(int h) {
        }
        @Override
        public int getHoldability() {
            return 0;
        }
        @Override
        public java.sql.Savepoint setSavepoint() {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.Savepoint setSavepoint(String n) {
            throw new UnsupportedOperationException();
        }
        @Override
        public void rollback(java.sql.Savepoint s) {
        }
        @Override
        public void releaseSavepoint(java.sql.Savepoint s) {
        }
        @Override
        public java.sql.Statement createStatement(int t, int c, int h) {
            throw new UnsupportedOperationException();
        }
        @Override
        public PreparedStatement prepareStatement(String s, int t, int c, int h) {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.CallableStatement prepareCall(String s, int t, int c, int h) {
            throw new UnsupportedOperationException();
        }
        @Override
        public PreparedStatement prepareStatement(String s, int a) {
            throw new UnsupportedOperationException();
        }
        @Override
        public PreparedStatement prepareStatement(String s, int[] c) {
            throw new UnsupportedOperationException();
        }
        @Override
        public PreparedStatement prepareStatement(String s, String[] c) {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.Clob createClob() {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.Blob createBlob() {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.NClob createNClob() {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.SQLXML createSQLXML() {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean isValid(int t) {
            return true;
        }
        @Override
        public void setClientInfo(String n, String v) {
        }
        @Override
        public void setClientInfo(java.util.Properties p) {
        }
        @Override
        public String getClientInfo(String n) {
            return null;
        }
        @Override
        public java.util.Properties getClientInfo() {
            return new java.util.Properties();
        }
        @Override
        public java.sql.Array createArrayOf(String t, Object[] e) {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.Struct createStruct(String t, Object[] a) {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setSchema(String s) {
        }
        @Override
        public String getSchema() {
            return null;
        }
        @Override
        public void abort(java.util.concurrent.Executor e) {
        }
        @Override
        public void setNetworkTimeout(java.util.concurrent.Executor e, int m) {
        }
        @Override
        public int getNetworkTimeout() {
            return 0;
        }
        @Override
        public <T> T unwrap(Class<T> iface) {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }
}
