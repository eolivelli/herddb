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
import static org.junit.jupiter.api.Assertions.assertTrue;
import herddb.jdbc.PreparedStatementAsync;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

/**
 * Tests for IngestionWorker batch flushing with executeBatchAsync.
 *
 * The tests use minimal test doubles to verify that flushBatch correctly
 * calls executeBatchAsync().get() and then commit().
 */
class IngestionWorkerTest {

    /**
     * Test successful batch flush: executeBatchAsync().get() succeeds and commit succeeds.
     */
    @Test
    void testFlushBatchSuccess() throws Exception {
        FakePreparedStatement ps = new FakePreparedStatement();
        FakeConnection conn = new FakeConnection();

        IngestionWorker worker = createTestWorker();
        long startNanos = System.nanoTime();
        long latencyNanos = worker.testFlushBatch(ps, conn, startNanos);

        assertTrue(ps.batchExecuted, "Batch should have been executed");
        assertTrue(conn.committed, "Connection should have been committed");
        assertTrue(latencyNanos > 0, "Latency should be positive");
    }

    /**
     * Test batch flush with ExecutionException from async batch.
     */
    @Test
    void testFlushBatchExecutionException() {
        FakePreparedStatement ps = new FakePreparedStatement();
        ps.failMode = FailMode.EXECUTION_EXCEPTION;
        FakeConnection conn = new FakeConnection();

        IngestionWorker worker = createTestWorker();
        long startNanos = System.nanoTime();

        ExecutionException ex = assertThrows(ExecutionException.class,
                () -> worker.testFlushBatch(ps, conn, startNanos));
        assertTrue(ex.getCause() instanceof RuntimeException);
        assertEquals("Batch execution failed", ex.getCause().getMessage());
    }

    /**
     * Test batch flush with SQLException from commit.
     */
    @Test
    void testFlushBatchCommitFailure() {
        FakePreparedStatement ps = new FakePreparedStatement();
        FakeConnection conn = new FakeConnection();
        conn.failOnCommit = true;

        IngestionWorker worker = createTestWorker();
        long startNanos = System.nanoTime();

        SQLException ex = assertThrows(SQLException.class,
                () -> worker.testFlushBatch(ps, conn, startNanos));
        assertEquals("Commit failed", ex.getMessage());
    }

    /**
     * Test batch flush with InterruptedException from async batch.
     */
    @Test
    void testFlushBatchInterrupted() {
        FakePreparedStatement ps = new FakePreparedStatement();
        ps.failMode = FailMode.INTERRUPTED;
        FakeConnection conn = new FakeConnection();

        IngestionWorker worker = createTestWorker();
        long startNanos = System.nanoTime();

        ExecutionException ex = assertThrows(ExecutionException.class,
                () -> worker.testFlushBatch(ps, conn, startNanos));
        assertTrue(ex.getCause() instanceof InterruptedException);
    }

    /**
     * Test that flushBatch returns correct latency measurement.
     */
    @Test
    void testFlushBatchLatencyMeasurement() throws Exception {
        FakePreparedStatement ps = new FakePreparedStatement();
        FakeConnection conn = new FakeConnection();

        IngestionWorker worker = createTestWorker();
        long startNanos = System.nanoTime();
        long latencyNanos = worker.testFlushBatch(ps, conn, startNanos);
        long endNanos = System.nanoTime();

        // Latency should be between start and end times, and positive
        assertTrue(latencyNanos > 0, "Latency should be positive");
        assertTrue(latencyNanos <= endNanos - startNanos + 1_000_000,
                "Latency should be less than total elapsed time (plus 1ms margin)");
    }

    private IngestionWorker createTestWorker() {
        Config config = new Config();
        return new IngestionWorker(
                config,
                new java.util.concurrent.LinkedBlockingQueue<>(),
                new java.util.concurrent.atomic.AtomicBoolean(false),
                new java.util.concurrent.atomic.AtomicLong(0),
                new MetricsCollector(),
                new java.util.concurrent.atomic.AtomicReference<>(),
                System.nanoTime()
        );
    }

    private enum FailMode {
        NONE, EXECUTION_EXCEPTION, INTERRUPTED
    }

    /**
     * Minimal fake PreparedStatement that implements PreparedStatementAsync.
     */
    private static class FakePreparedStatement implements PreparedStatement, PreparedStatementAsync {
        boolean batchExecuted = false;
        FailMode failMode = FailMode.NONE;

        @Override
        public CompletableFuture<int[]> executeBatchAsync() {
            if (failMode == FailMode.EXECUTION_EXCEPTION) {
                CompletableFuture<int[]> future = new CompletableFuture<>();
                future.completeExceptionally(new RuntimeException("Batch execution failed"));
                return future;
            }
            if (failMode == FailMode.INTERRUPTED) {
                CompletableFuture<int[]> future = new CompletableFuture<>();
                future.completeExceptionally(new InterruptedException("Thread interrupted"));
                return future;
            }
            batchExecuted = true;
            return CompletableFuture.completedFuture(new int[]{1});
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
        public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface == PreparedStatementAsync.class) {
                return (T) this;
            }
            throw new SQLException("Cannot unwrap to " + iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return iface == PreparedStatementAsync.class;
        }

        // Stub implementations - these are called by the test but don't need real implementation
        @Override
        public int[] executeBatch() throws SQLException {
            return new int[]{1};
        }
        @Override
        public void addBatch() throws SQLException {
        }
        @Override
        public void setObject(int parameterIndex, Object x) throws SQLException {
        }
        @Override
        public void setLong(int parameterIndex, long x) throws SQLException {
        }
        @Override
        public java.sql.ResultSet executeQuery() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public int executeUpdate() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setNull(int parameterIndex, int sqlType) throws SQLException {
        }
        @Override
        public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        }
        @Override
        public void setByte(int parameterIndex, byte x) throws SQLException {
        }
        @Override
        public void setShort(int parameterIndex, short x) throws SQLException {
        }
        @Override
        public void setInt(int parameterIndex, int x) throws SQLException {
        }
        @Override
        public void setFloat(int parameterIndex, float x) throws SQLException {
        }
        @Override
        public void setDouble(int parameterIndex, double x) throws SQLException {
        }
        @Override
        public void setBigDecimal(int parameterIndex, java.math.BigDecimal x) throws SQLException {
        }
        @Override
        public void setString(int parameterIndex, String x) throws SQLException {
        }
        @Override
        public void setURL(int parameterIndex, java.net.URL x) throws SQLException {
        }
        @Override
        public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        }
        @Override
        public void setDate(int parameterIndex, java.sql.Date x) throws SQLException {
        }
        @Override
        public void setDate(int parameterIndex, java.sql.Date x, java.util.Calendar cal) throws SQLException {
        }
        @Override
        public void setTime(int parameterIndex, java.sql.Time x) throws SQLException {
        }
        @Override
        public void setTime(int parameterIndex, java.sql.Time x, java.util.Calendar cal) throws SQLException {
        }
        @Override
        public void setTimestamp(int parameterIndex, java.sql.Timestamp x) throws SQLException {
        }
        @Override
        public void setTimestamp(int parameterIndex, java.sql.Timestamp x, java.util.Calendar cal) throws SQLException {
        }
        @Override
        public void setAsciiStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException {
        }
        @Override
        public void setUnicodeStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException {
        }
        @Override
        public void setBinaryStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException {
        }
        @Override
        public void setCharacterStream(int parameterIndex, java.io.Reader reader, int length) throws SQLException {
        }
        @Override
        public void clearParameters() throws SQLException {
        }
        @Override
        public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        }
        @Override
        public boolean execute() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.ResultSet getResultSet() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public int getUpdateCount() throws SQLException {
            return 0;
        }
        @Override
        public boolean getMoreResults() throws SQLException {
            return false;
        }
        @Override
        public void setFetchDirection(int direction) throws SQLException {
        }
        @Override
        public int getFetchDirection() throws SQLException {
            return 0;
        }
        @Override
        public void setFetchSize(int rows) throws SQLException {
        }
        @Override
        public int getFetchSize() throws SQLException {
            return 0;
        }
        @Override
        public int getResultSetConcurrency() throws SQLException {
            return 0;
        }
        @Override
        public int getResultSetType() throws SQLException {
            return 0;
        }
        @Override
        public void addBatch(String sql) throws SQLException {
        }
        @Override
        public void clearBatch() throws SQLException {
        }
        @Override
        public java.sql.Connection getConnection() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean getMoreResults(int current) throws SQLException {
            return false;
        }
        @Override
        public java.sql.ResultSet getGeneratedKeys() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public int executeUpdate(String sql, String[] columnNames) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean execute(String sql, int[] columnIndexes) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean execute(String sql, String[] columnNames) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public int getResultSetHoldability() throws SQLException {
            return 0;
        }
        @Override
        public boolean isClosed() throws SQLException {
            return false;
        }
        @Override
        public void setPoolable(boolean poolable) throws SQLException {
        }
        @Override
        public boolean isPoolable() throws SQLException {
            return false;
        }
        @Override
        public void closeOnCompletion() throws SQLException {
        }
        @Override
        public boolean isCloseOnCompletion() throws SQLException {
            return false;
        }
        @Override
        public java.sql.ResultSetMetaData getMetaData() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        }
        @Override
        public void setRef(int parameterIndex, java.sql.Ref x) throws SQLException {
        }
        @Override
        public void setBlob(int parameterIndex, java.sql.Blob x) throws SQLException {
        }
        @Override
        public void setClob(int parameterIndex, java.sql.Clob x) throws SQLException {
        }
        @Override
        public void setArray(int parameterIndex, java.sql.Array x) throws SQLException {
        }
        @Override
        public java.sql.ParameterMetaData getParameterMetaData() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setRowId(int parameterIndex, java.sql.RowId x) throws SQLException {
        }
        @Override
        public void setNString(int parameterIndex, String value) throws SQLException {
        }
        @Override
        public void setNCharacterStream(int parameterIndex, java.io.Reader value, long length) throws SQLException {
        }
        @Override
        public void setNClob(int parameterIndex, java.sql.NClob value) throws SQLException {
        }
        @Override
        public void setClob(int parameterIndex, java.io.Reader reader, long length) throws SQLException {
        }
        @Override
        public void setBlob(int parameterIndex, java.io.InputStream inputStream, long length) throws SQLException {
        }
        @Override
        public void setNClob(int parameterIndex, java.io.Reader reader, long length) throws SQLException {
        }
        @Override
        public void setSQLXML(int parameterIndex, java.sql.SQLXML xmlObject) throws SQLException {
        }
        @Override
        public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        }
        @Override
        public void setAsciiStream(int parameterIndex, java.io.InputStream x, long length) throws SQLException {
        }
        @Override
        public void setBinaryStream(int parameterIndex, java.io.InputStream x, long length) throws SQLException {
        }
        @Override
        public void setCharacterStream(int parameterIndex, java.io.Reader reader, long length) throws SQLException {
        }
        @Override
        public void setAsciiStream(int parameterIndex, java.io.InputStream x) throws SQLException {
        }
        @Override
        public void setBinaryStream(int parameterIndex, java.io.InputStream x) throws SQLException {
        }
        @Override
        public void setCharacterStream(int parameterIndex, java.io.Reader reader) throws SQLException {
        }
        @Override
        public void setNCharacterStream(int parameterIndex, java.io.Reader value) throws SQLException {
        }
        @Override
        public void setClob(int parameterIndex, java.io.Reader reader) throws SQLException {
        }
        @Override
        public void setBlob(int parameterIndex, java.io.InputStream inputStream) throws SQLException {
        }
        @Override
        public void setNClob(int parameterIndex, java.io.Reader reader) throws SQLException {
        }
        @Override
        public void close() throws SQLException {
        }
        @Override
        public java.sql.ResultSet executeQuery(String sql) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public int executeUpdate(String sql) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public int getMaxFieldSize() throws SQLException {
            return 0;
        }
        @Override
        public void setMaxFieldSize(int max) throws SQLException {
        }
        @Override
        public int getMaxRows() throws SQLException {
            return 0;
        }
        @Override
        public void setMaxRows(int max) throws SQLException {
        }
        @Override
        public void setEscapeProcessing(boolean enable) throws SQLException {
        }
        @Override
        public int getQueryTimeout() throws SQLException {
            return 0;
        }
        @Override
        public void setQueryTimeout(int seconds) throws SQLException {
        }
        @Override
        public void cancel() throws SQLException {
        }
        @Override
        public java.sql.SQLWarning getWarnings() throws SQLException {
            return null;
        }
        @Override
        public void clearWarnings() throws SQLException {
        }
        @Override
        public void setCursorName(String name) throws SQLException {
        }
        @Override
        public boolean execute(String sql) throws SQLException {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Minimal fake Connection.
     */
    private static class FakeConnection implements Connection {
        boolean committed = false;
        boolean failOnCommit = false;

        @Override
        public void commit() throws SQLException {
            if (failOnCommit) {
                throw new SQLException("Commit failed");
            }
            committed = true;
        }

        // Stub implementations
        @Override
        public java.sql.Statement createStatement() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.CallableStatement prepareCall(String sql) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public String nativeSQL(String sql) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
        }
        @Override
        public boolean getAutoCommit() throws SQLException {
            return false;
        }
        @Override
        public void rollback() throws SQLException {
        }
        @Override
        public void close() throws SQLException {
        }
        @Override
        public boolean isClosed() throws SQLException {
            return false;
        }
        @Override
        public java.sql.DatabaseMetaData getMetaData() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
        }
        @Override
        public boolean isReadOnly() throws SQLException {
            return false;
        }
        @Override
        public void setCatalog(String catalog) throws SQLException {
        }
        @Override
        public String getCatalog() throws SQLException {
            return null;
        }
        @Override
        public void setTransactionIsolation(int level) throws SQLException {
        }
        @Override
        public int getTransactionIsolation() throws SQLException {
            return 0;
        }
        @Override
        public java.sql.SQLWarning getWarnings() throws SQLException {
            return null;
        }
        @Override
        public void clearWarnings() throws SQLException {
        }
        @Override
        public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {
        }
        @Override
        public void setHoldability(int holdability) throws SQLException {
        }
        @Override
        public int getHoldability() throws SQLException {
            return 0;
        }
        @Override
        public java.sql.Savepoint setSavepoint() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.Savepoint setSavepoint(String name) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public void rollback(java.sql.Savepoint savepoint) throws SQLException {
        }
        @Override
        public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException {
        }
        @Override
        public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.Clob createClob() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.Blob createBlob() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.NClob createNClob() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.SQLXML createSQLXML() throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean isValid(int timeout) throws SQLException {
            return true;
        }
        @Override
        public void setClientInfo(String name, String value) {
        }
        @Override
        public void setClientInfo(java.util.Properties properties) {
        }
        @Override
        public String getClientInfo(String name) throws SQLException {
            return null;
        }
        @Override
        public java.util.Properties getClientInfo() throws SQLException {
            return new java.util.Properties();
        }
        @Override
        public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public java.sql.Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public void setSchema(String schema) throws SQLException {
        }
        @Override
        public String getSchema() throws SQLException {
            return null;
        }
        @Override
        public void abort(java.util.concurrent.Executor executor) throws SQLException {
        }
        @Override
        public void setNetworkTimeout(java.util.concurrent.Executor executor, int milliseconds) throws SQLException {
        }
        @Override
        public int getNetworkTimeout() throws SQLException {
            return 0;
        }
        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }
    }
}
