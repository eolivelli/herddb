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

package herddb.mysql;

import herddb.core.DBManager;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.DDLStatementExecutionResult;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.mysql.protocol.ColumnDef;
import herddb.mysql.protocol.ErrorResult;
import herddb.mysql.protocol.MySQLColumnType;
import herddb.mysql.protocol.MySQLCommandHandler;
import herddb.mysql.protocol.OkResult;
import herddb.mysql.protocol.QueryResult;
import herddb.mysql.protocol.ResultSetResponse;
import herddb.mysql.protocol.ServerStatusFlags;
import herddb.security.UserManager;
import herddb.sql.TranslatedQuery;
import herddb.utils.DataAccessor;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Bridges MySQL protocol commands to HerdDB DBManager.
 */
public class HerdDBMySQLCommandHandler implements MySQLCommandHandler {

    private static final Logger LOGGER = Logger.getLogger(HerdDBMySQLCommandHandler.class.getName());

    private final DBManager manager;
    private final UserManager userManager;
    private final Map<Long, ConnectionState> connections = new ConcurrentHashMap<>();

    private static final Pattern SET_PATTERN = Pattern.compile(
            "^\\s*SET\\s+.*", Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_AT_AT_PATTERN = Pattern.compile(
            "^\\s*SELECT\\s+.*@@.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern SHOW_PATTERN = Pattern.compile(
            "^\\s*SHOW\\s+.*", Pattern.CASE_INSENSITIVE);

    public HerdDBMySQLCommandHandler(DBManager manager, UserManager userManager) {
        this.manager = manager;
        this.userManager = userManager;
    }

    @Override
    public boolean authenticate(String username, byte[] scrambledPassword, byte[] challenge, String database) {
        try {
            String expectedPassword = userManager.getExpectedPassword(username);
            if (expectedPassword == null) {
                LOGGER.log(Level.WARNING, "Authentication failed: unknown user {0}", username);
                return false;
            }
            if (scrambledPassword == null || scrambledPassword.length == 0) {
                if (expectedPassword.isEmpty()) {
                    return true;
                }
                LOGGER.log(Level.WARNING, "Authentication failed for user {0}: no password provided", username);
                return false;
            }
            // mysql_native_password: client sends SHA1(password) XOR SHA1(seed + SHA1(SHA1(password)))
            byte[] expected = computeMySQLNativePasswordScramble(challenge, expectedPassword);
            if (expected.length != scrambledPassword.length) {
                LOGGER.log(Level.WARNING, "Auth scramble length mismatch: expected={0}, got={1}",
                        new Object[]{expected.length, scrambledPassword.length});
                return false;
            }
            for (int i = 0; i < expected.length; i++) {
                if (expected[i] != scrambledPassword[i]) {
                    LOGGER.log(Level.WARNING, "Authentication failed for user {0}: scramble mismatch", username);
                    return false;
                }
            }
            return true;
        } catch (IOException | NoSuchAlgorithmException e) {
            LOGGER.log(Level.SEVERE, "Error during authentication for user " + username, e);
            return false;
        }
    }

    private static byte[] computeMySQLNativePasswordScramble(byte[] challenge, String password) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] passwordBytes = password.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        // SHA1(password)
        byte[] sha1Password = sha1.digest(passwordBytes);

        // SHA1(SHA1(password))
        sha1.reset();
        byte[] doubleSha1 = sha1.digest(sha1Password);

        // SHA1(challenge + SHA1(SHA1(password)))
        sha1.reset();
        sha1.update(challenge);
        byte[] challengeHash = sha1.digest(doubleSha1);

        // XOR SHA1(password) with SHA1(challenge + SHA1(SHA1(password)))
        byte[] result = new byte[sha1Password.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) (sha1Password[i] ^ challengeHash[i]);
        }
        return result;
    }

    @Override
    public CompletableFuture<QueryResult> executeQuery(long connectionId, String sql) {
        ConnectionState state = connections.computeIfAbsent(connectionId,
                id -> new ConnectionState(TableSpace.DEFAULT));

        String trimmedSql = sql.trim();
        if (trimmedSql.isEmpty()) {
            return CompletableFuture.completedFuture(
                    new OkResult(0, 0, getServerStatus(state)));
        }

        // Handle SET commands — just return OK
        if (SET_PATTERN.matcher(trimmedSql).find()) {
            return CompletableFuture.completedFuture(
                    new OkResult(0, 0, getServerStatus(state)));
        }

        // Handle SELECT @@variable (MySQL system variables)
        if (trimmedSql.contains("@@")) {
            return handleSelectVariable(trimmedSql, state);
        }

        // Handle SHOW commands
        if (SHOW_PATTERN.matcher(trimmedSql).find()) {
            return handleShowCommand(trimmedSql, state);
        }

        // Handle transaction commands
        String upper = trimmedSql.toUpperCase();
        if (upper.equals("BEGIN") || upper.equals("START TRANSACTION")) {
            return handleBegin(state);
        }
        if (upper.equals("COMMIT")) {
            return handleCommit(state);
        }
        if (upper.equals("ROLLBACK")) {
            return handleRollback(state);
        }

        // Regular SQL — delegate to DBManager
        return executeSql(state, trimmedSql);
    }

    @Override
    public void useDatabase(long connectionId, String database) {
        ConnectionState state = connections.computeIfAbsent(connectionId,
                id -> new ConnectionState(TableSpace.DEFAULT));
        state.tableSpace = database;
    }

    @Override
    public void connectionClosed(long connectionId) {
        ConnectionState state = connections.remove(connectionId);
        if (state != null && state.transactionId != 0) {
            try {
                RollbackTransactionStatement rollback = new RollbackTransactionStatement(
                        state.tableSpace, state.transactionId);
                manager.executeStatement(rollback,
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        new TransactionContext(state.transactionId));
            } catch (StatementExecutionException e) {
                LOGGER.log(Level.WARNING, "Error rolling back transaction on connection close", e);
            }
        }
    }

    private CompletableFuture<QueryResult> executeSql(ConnectionState state, String sql) {
        try {
            TranslatedQuery translated = manager.getPlanner().translate(
                    state.tableSpace, sql, Collections.emptyList(),
                    true, true, false, -1);

            TransactionContext txContext = state.transactionId != 0
                    ? new TransactionContext(state.transactionId)
                    : TransactionContext.NO_TRANSACTION;

            return manager.executePlanAsync(translated.plan, translated.context, txContext)
                    .thenApply(result -> convertResult(result, state));
        } catch (StatementExecutionException e) {
            LOGGER.log(Level.FINE, "SQL execution error for: " + sql, e);
            return CompletableFuture.completedFuture(
                    new ErrorResult(1064, "42000", e.getMessage()));
        }
    }

    private QueryResult convertResult(StatementExecutionResult result, ConnectionState state) {
        // Update transaction ID from result
        if (result.transactionId > 0 && state.transactionId == 0) {
            // auto-started transaction, track it
        }

        if (result instanceof ScanResult) {
            return convertScanResult((ScanResult) result, state);
        } else if (result instanceof DMLStatementExecutionResult) {
            DMLStatementExecutionResult dml = (DMLStatementExecutionResult) result;
            return new OkResult(dml.getUpdateCount(), 0, getServerStatus(state));
        } else if (result instanceof DDLStatementExecutionResult) {
            return new OkResult(0, 0, getServerStatus(state));
        } else if (result instanceof TransactionResult) {
            TransactionResult txResult = (TransactionResult) result;
            state.transactionId = txResult.getTransactionId();
            return new OkResult(0, 0, getServerStatus(state));
        } else {
            return new OkResult(0, 0, getServerStatus(state));
        }
    }

    private QueryResult convertScanResult(ScanResult scanResult, ConnectionState state) {
        try (DataScanner scanner = scanResult.dataScanner) {
            Column[] schema = scanner.getSchema();
            String[] fieldNames = scanner.getFieldNames();

            List<ColumnDef> columns = new ArrayList<>();
            for (int i = 0; i < schema.length; i++) {
                columns.add(new ColumnDef(
                        fieldNames[i],
                        state.tableSpace,
                        "", // table name not easily available
                        mapColumnType(schema[i].type),
                        getColumnLength(schema[i].type)));
            }

            List<Object[]> rows = new ArrayList<>();
            while (scanner.hasNext()) {
                DataAccessor accessor = scanner.next();
                Object[] row = new Object[fieldNames.length];
                for (int i = 0; i < fieldNames.length; i++) {
                    Object value = accessor.get(i);
                    row[i] = convertValue(value);
                }
                rows.add(row);
            }

            return new ResultSetResponse(columns, rows);
        } catch (DataScannerException e) {
            return new ErrorResult(1105, "HY000", "Error scanning results: " + e.getMessage());
        }
    }

    private static Object convertValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            return bytesToHex((byte[]) value);
        }
        return value.toString();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }

    static MySQLColumnType mapColumnType(int herddbType) {
        switch (herddbType) {
            case ColumnTypes.STRING:
            case ColumnTypes.NOTNULL_STRING:
                return MySQLColumnType.MYSQL_TYPE_VAR_STRING;
            case ColumnTypes.LONG:
            case ColumnTypes.NOTNULL_LONG:
                return MySQLColumnType.MYSQL_TYPE_LONGLONG;
            case ColumnTypes.INTEGER:
            case ColumnTypes.NOTNULL_INTEGER:
                return MySQLColumnType.MYSQL_TYPE_LONG;
            case ColumnTypes.DOUBLE:
            case ColumnTypes.NOTNULL_DOUBLE:
                return MySQLColumnType.MYSQL_TYPE_DOUBLE;
            case ColumnTypes.BOOLEAN:
            case ColumnTypes.NOTNULL_BOOLEAN:
                return MySQLColumnType.MYSQL_TYPE_TINY;
            case ColumnTypes.TIMESTAMP:
            case ColumnTypes.NOTNULL_TIMESTAMP:
                return MySQLColumnType.MYSQL_TYPE_DATETIME;
            case ColumnTypes.BYTEARRAY:
            case ColumnTypes.NOTNULL_BYTEARRAY:
            case ColumnTypes.FLOATARRAY:
            case ColumnTypes.NOTNULL_FLOATARRAY:
                return MySQLColumnType.MYSQL_TYPE_BLOB;
            default:
                return MySQLColumnType.MYSQL_TYPE_VAR_STRING;
        }
    }

    private static int getColumnLength(int herddbType) {
        switch (herddbType) {
            case ColumnTypes.INTEGER:
            case ColumnTypes.NOTNULL_INTEGER:
                return 11;
            case ColumnTypes.LONG:
            case ColumnTypes.NOTNULL_LONG:
                return 20;
            case ColumnTypes.DOUBLE:
            case ColumnTypes.NOTNULL_DOUBLE:
                return 22;
            case ColumnTypes.BOOLEAN:
            case ColumnTypes.NOTNULL_BOOLEAN:
                return 1;
            case ColumnTypes.TIMESTAMP:
            case ColumnTypes.NOTNULL_TIMESTAMP:
                return 19;
            default:
                return 255;
        }
    }

    private CompletableFuture<QueryResult> handleBegin(ConnectionState state) {
        try {
            BeginTransactionStatement stmt = new BeginTransactionStatement(state.tableSpace);
            StatementExecutionResult result = manager.executeStatement(stmt,
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            state.transactionId = result.transactionId;
            return CompletableFuture.completedFuture(
                    new OkResult(0, 0, getServerStatus(state)));
        } catch (StatementExecutionException e) {
            return CompletableFuture.completedFuture(
                    new ErrorResult(1105, "HY000", "BEGIN failed: " + e.getMessage()));
        }
    }

    private CompletableFuture<QueryResult> handleCommit(ConnectionState state) {
        if (state.transactionId == 0) {
            return CompletableFuture.completedFuture(
                    new OkResult(0, 0, getServerStatus(state)));
        }
        try {
            CommitTransactionStatement stmt = new CommitTransactionStatement(
                    state.tableSpace, state.transactionId);
            manager.executeStatement(stmt,
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    new TransactionContext(state.transactionId));
            state.transactionId = 0;
            return CompletableFuture.completedFuture(
                    new OkResult(0, 0, getServerStatus(state)));
        } catch (StatementExecutionException e) {
            return CompletableFuture.completedFuture(
                    new ErrorResult(1105, "HY000", "COMMIT failed: " + e.getMessage()));
        }
    }

    private CompletableFuture<QueryResult> handleRollback(ConnectionState state) {
        if (state.transactionId == 0) {
            return CompletableFuture.completedFuture(
                    new OkResult(0, 0, getServerStatus(state)));
        }
        try {
            RollbackTransactionStatement stmt = new RollbackTransactionStatement(
                    state.tableSpace, state.transactionId);
            manager.executeStatement(stmt,
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    new TransactionContext(state.transactionId));
            state.transactionId = 0;
            return CompletableFuture.completedFuture(
                    new OkResult(0, 0, getServerStatus(state)));
        } catch (StatementExecutionException e) {
            return CompletableFuture.completedFuture(
                    new ErrorResult(1105, "HY000", "ROLLBACK failed: " + e.getMessage()));
        }
    }

    private static final Map<String, String> SYSTEM_VARIABLES = new java.util.LinkedHashMap<>();
    static {
        SYSTEM_VARIABLES.put("auto_increment_increment", "1");
        SYSTEM_VARIABLES.put("character_set_client", "utf8mb4");
        SYSTEM_VARIABLES.put("character_set_connection", "utf8mb4");
        SYSTEM_VARIABLES.put("character_set_results", "utf8mb4");
        SYSTEM_VARIABLES.put("character_set_server", "utf8mb4");
        SYSTEM_VARIABLES.put("collation_server", "utf8mb4_general_ci");
        SYSTEM_VARIABLES.put("collation_connection", "utf8mb4_general_ci");
        SYSTEM_VARIABLES.put("init_connect", "");
        SYSTEM_VARIABLES.put("interactive_timeout", "28800");
        SYSTEM_VARIABLES.put("license", "Apache-2.0");
        SYSTEM_VARIABLES.put("lower_case_table_names", "0");
        SYSTEM_VARIABLES.put("max_allowed_packet", "16777216");
        SYSTEM_VARIABLES.put("net_write_timeout", "60");
        SYSTEM_VARIABLES.put("performance_schema", "0");
        SYSTEM_VARIABLES.put("query_cache_size", "0");
        SYSTEM_VARIABLES.put("query_cache_type", "OFF");
        SYSTEM_VARIABLES.put("sql_mode", "STRICT_TRANS_TABLES");
        SYSTEM_VARIABLES.put("system_time_zone", "UTC");
        SYSTEM_VARIABLES.put("time_zone", "SYSTEM");
        SYSTEM_VARIABLES.put("transaction_isolation", "REPEATABLE-READ");
        SYSTEM_VARIABLES.put("tx_isolation", "REPEATABLE-READ");
        SYSTEM_VARIABLES.put("wait_timeout", "28800");
        SYSTEM_VARIABLES.put("version_comment", "HerdDB");
        SYSTEM_VARIABLES.put("version", "5.7.99-HerdDB");
    }

    private static final Pattern AT_VAR_PATTERN = Pattern.compile(
            "@@(?:session\\.|global\\.)?(\\w+)");

    private CompletableFuture<QueryResult> handleSelectVariable(String sql, ConnectionState state) {
        // Parse all @@variable references from the SELECT
        Matcher m = AT_VAR_PATTERN.matcher(sql);
        List<String> varNames = new ArrayList<>();
        while (m.find()) {
            varNames.add(m.group(1).toLowerCase());
        }

        if (varNames.isEmpty()) {
            return CompletableFuture.completedFuture(
                    new OkResult(0, 0, getServerStatus(state)));
        }

        // Look for AS aliases
        // Parse "@@session.var AS alias" patterns
        Pattern aliasPattern = Pattern.compile(
                "@@(?:session\\.|global\\.)?(\\w+)(?:\\s+AS\\s+(\\w+))?", Pattern.CASE_INSENSITIVE);
        Matcher am = aliasPattern.matcher(sql);

        List<ColumnDef> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        while (am.find()) {
            String varName = am.group(1).toLowerCase();
            String alias = am.group(2) != null ? am.group(2) : "@@" + varName;
            String value;
            if ("autocommit".equals(varName)) {
                value = state.transactionId == 0 ? "1" : "0";
            } else {
                value = SYSTEM_VARIABLES.getOrDefault(varName, "");
            }
            columns.add(new ColumnDef(alias, "", "", MySQLColumnType.MYSQL_TYPE_VAR_STRING, 255));
            values.add(value);
        }

        List<Object[]> rows = Collections.singletonList(values.toArray());
        return CompletableFuture.completedFuture(new ResultSetResponse(columns, rows));
    }

    private CompletableFuture<QueryResult> handleShowCommand(String sql, ConnectionState state) {
        // Return empty result set for SHOW commands
        List<ColumnDef> columns = Collections.singletonList(
                new ColumnDef("Value", "", "", MySQLColumnType.MYSQL_TYPE_VAR_STRING, 255));
        return CompletableFuture.completedFuture(new ResultSetResponse(columns, Collections.emptyList()));
    }

    private int getServerStatus(ConnectionState state) {
        int status = ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT;
        if (state.transactionId != 0) {
            status |= ServerStatusFlags.SERVER_STATUS_IN_TRANS;
            status &= ~ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT;
        }
        return status;
    }

    static class ConnectionState {
        String tableSpace;
        long transactionId;

        ConnectionState(String tableSpace) {
            this.tableSpace = tableSpace;
        }
    }
}
