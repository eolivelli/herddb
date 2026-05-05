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
import static org.junit.jupiter.api.Assertions.assertTrue;
import herddb.jdbc.PreparedStatementAsync;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for issue #220 (now updated for issue #402): the
 * {@code --ingest-max-ops} cap must be a true global cap across all ingest
 * workers — and after #402 it must <em>also</em> not serialise N threads
 * behind a shared lock.
 *
 * <p>The new design splits the global rate evenly across N per-thread
 * limiters owned by an {@link IngestRateLimiterGroup}. Two properties
 * matter, and this test covers both:
 *
 * <ul>
 *   <li><b>Global cap.</b> Sum of per-thread acquires equals the configured
 *       global rate. With N threads each acquiring at {@code rate / N}, the
 *       aggregate elapsed time matches what a single shared limiter at
 *       {@code rate} would produce — but without the inter-thread blocking.</li>
 *   <li><b>No serialisation.</b> N threads each acquire {@code N × P} permits
 *       concurrently in roughly the same wall-clock time it would take a
 *       single thread to acquire {@code P} permits, because each thread has
 *       its own limiter. (Bug 1 of #402: a shared limiter blew this up by
 *       a factor of N.)</li>
 * </ul>
 */
class IngestionWorkerRateLimitTest {

    @Test
    void perThreadGroupBoundsAggregateThroughput() throws Exception {
        final double rate = 500.0; // permits/s, global
        final int threads = 4;
        final int acquisitionsPerThread = 250;
        final int totalAcquisitions = threads * acquisitionsPerThread;

        IngestRateLimiterGroup group = new IngestRateLimiterGroup(threads, rate);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int i = 0; i < threads; i++) {
                final int idx = i;
                pool.submit(() -> {
                    try {
                        group.attachThread(idx, Thread.currentThread());
                        start.await();
                        for (int j = 0; j < acquisitionsPerThread; j++) {
                            group.acquire(idx, 1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }

            long t0 = System.nanoTime();
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS), "workers did not finish in time");
            double elapsedSecs = (System.nanoTime() - t0) / 1e9;

            // Per-thread rate is rate / threads. With acquisitionsPerThread acquires
            // per worker, each worker takes ~ (acquisitionsPerThread × threads) / rate
            // = totalAcquisitions / rate seconds — same as a shared limiter, but the
            // workers run in parallel without inter-thread blocking.
            double expectedMinSecs = (totalAcquisitions / rate) * 0.85;
            assertTrue(elapsedSecs >= expectedMinSecs,
                    "per-thread group failed to bound aggregate throughput: elapsed="
                            + elapsedSecs + "s, expected >= " + expectedMinSecs + "s");
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Reviewer follow-up #1: pre-commit acquire must pace <em>every</em>
     * transaction in steady state, not just the first. An earlier draft
     * accidentally stored "wanted" in the per-call return and skipped the
     * acquire on every subsequent transaction (toAcquire = wanted - wanted = 0),
     * silently disabling rate limiting once steady state was reached.
     *
     * <p>Drives {@link IngestionWorker#runIngestLoop} through 4 transactions
     * with a real {@link IngestRateLimiterGroup} configured at 200 permits/s,
     * transaction-size 50: each transaction must take ~ 250 ms of pacing,
     * so total wall-clock should be ≥ ~750 ms (3 × 250 ms; first txn's
     * pre-acquire returns instantly because the limiter has no prior
     * reservation).
     */
    @Test
    void runIngestLoopRateLimitsEverySteadyStateTransaction() throws Exception {
        Config cfg = new Config();
        cfg.batchSize = 50;
        cfg.transactionSize = 50;
        cfg.ingestCommitRetries = 0;
        cfg.ingestMaxOpsPerSecond = 200; // 200/s -> 250 ms per txn of 50 rows

        IngestRateLimiterGroup group = new IngestRateLimiterGroup(1, 200.0);

        LinkedBlockingQueue<float[]> queue = new LinkedBlockingQueue<>();
        // 4 transactions × 50 rows = 200 rows.
        for (int i = 0; i < 200; i++) {
            queue.add(new float[]{(float) i});
        }
        queue.add(new float[0]); // poison pill

        IngestionWorker worker = new IngestionWorker(
                cfg, queue, new AtomicBoolean(true), new AtomicLong(0),
                new MetricsCollector(), new AtomicReference<>(""), System.nanoTime(),
                new AtomicLong(0), new AtomicLong(0), new AtomicLong(0),
                group, 0);
        worker.backoffBaseMillis = 0L;
        group.attachThread(0, Thread.currentThread());

        StubPs ps = new StubPs();
        StubConn conn = new StubConn();

        long t0 = System.nanoTime();
        worker.runIngestLoop(conn, ps);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

        // Expected lower bound: 3 × 250 ms = 750 ms (first txn's pre-acquire is
        // instantaneous, the next 3 must each pace 250 ms). Allow 600 ms to
        // tolerate jitter on slow CI; a bug that skips the acquire would
        // produce ~ 0–50 ms total, an order of magnitude below the bound.
        assertTrue(elapsedMs >= 600L,
                "rate limiter should pace every steady-state transaction; elapsed=" + elapsedMs + "ms");
        // Sanity: 4 commits happened.
        assertEquals(4, conn.commitCount, "expected 4 commits; got " + conn.commitCount);
    }

    /**
     * Issue #402, bug 1 regression: with the per-thread group, N concurrent
     * workers acquiring P permits each finish in approximately the time it
     * takes one worker to acquire P × N permits at the per-thread rate —
     * <em>not</em> N times longer (which is what a shared limiter caused).
     */
    @Test
    void perThreadGroupDoesNotSerialiseNThreads() throws Exception {
        final double globalRate = 1000.0;
        final int threads = 8;
        final int permitsPerThread = 200;

        IngestRateLimiterGroup group = new IngestRateLimiterGroup(threads, globalRate);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int i = 0; i < threads; i++) {
                final int idx = i;
                pool.submit(() -> {
                    try {
                        group.attachThread(idx, Thread.currentThread());
                        start.await();
                        for (int j = 0; j < permitsPerThread; j++) {
                            group.acquire(idx, 1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }

            long t0 = System.nanoTime();
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS), "workers did not finish in time");
            double elapsedSecs = (System.nanoTime() - t0) / 1e9;

            // Per-thread rate = globalRate / threads; each worker takes
            // permitsPerThread / (globalRate / threads) = permitsPerThread * threads / globalRate.
            // Total expected ≈ 200 × 8 / 1000 = 1.6 s.
            // A SHARED limiter would force 8× that = 12.8 s. We assert well under that bound.
            double sharedLimiterUpperBound = (double) permitsPerThread * threads * threads / globalRate * 0.6;
            assertTrue(elapsedSecs < sharedLimiterUpperBound,
                    "per-thread group should not serialise " + threads + " threads; elapsed="
                            + elapsedSecs + "s, would-be-shared-bound=" + sharedLimiterUpperBound + "s");
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** Minimal PreparedStatement stub: succeeds on every executeBatchAsync. */
    private static class StubPs implements PreparedStatement, PreparedStatementAsync {
        int rowsInPs = 0;

        @Override
        public CompletableFuture<int[]> executeBatchAsync() {
            int[] counts = new int[rowsInPs];
            for (int i = 0; i < rowsInPs; i++) {
                counts[i] = 1;
            }
            rowsInPs = 0;
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

            rowsInPs++;

        }
        @Override

        public void clearBatch() {

            rowsInPs = 0;

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
        // ---- minimal stubs ----
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
        public Connection getConnection() {
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

    /** Minimal Connection stub: counts commits. */
    private static class StubConn implements Connection {
        int commitCount = 0;

        @Override

        public void commit() {

            commitCount++;

        }
        @Override

        public void rollback() {

        }
        @Override
        public void setAutoCommit(boolean a) {
        }
        @Override
        public boolean getAutoCommit() {
            return false;
        }
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
