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

package org.herddb.ui.internal;

import herddb.core.DBManager;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.server.Server;
import herddb.utils.DataAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Read-only gateway around {@link DBManager#executeSimpleQuery}. The Web UI
 * v2 backend uses this class instead of JDBC: queries run inside the server
 * process against system tables and are guarded so only {@code SELECT}
 * statements can be issued.
 *
 * <p>Returned rows are normalised to {@code List<Map<String, Object>>} where
 * each map preserves the original column order via {@link LinkedHashMap}.
 * Column keys are lower-cased to match the convention used by the {@code sys*}
 * table managers.
 */
public class QueryService {

    private static final int DEFAULT_MAX_ROWS = 10_000;

    private final Server server;

    public QueryService(Server server) {
        if (server == null) {
            throw new IllegalArgumentException("server cannot be null");
        }
        this.server = server;
    }

    public Server getServer() {
        return server;
    }

    /**
     * Executes a read-only {@code SELECT} statement against a system table in
     * the given tablespace.
     *
     * @param tablespace the target tablespace (must not be {@code null} or
     *                   blank)
     * @param query      the SQL text; only {@code SELECT} statements are
     *                   permitted
     * @return the result set, with one map per row keyed by column name
     * @throws DataScannerException     if the underlying scan fails
     * @throws IllegalArgumentException if the query is not a {@code SELECT}
     */
    public List<Map<String, Object>> selectRows(String tablespace, String query) throws DataScannerException {
        return selectRows(tablespace, query, DEFAULT_MAX_ROWS);
    }

    /**
     * Variant of {@link #selectRows(String, String)} that bounds the row
     * count.
     *
     * @param tablespace the target tablespace
     * @param query      the SQL text; only {@code SELECT} statements are
     *                   permitted
     * @param maxRows    upper bound on the number of returned rows; must be
     *                   positive
     * @return the result set
     * @throws DataScannerException if the underlying scan fails
     */
    public List<Map<String, Object>> selectRows(String tablespace, String query, int maxRows)
            throws DataScannerException {
        validateTablespace(tablespace);
        validateSelectOnly(query);
        if (maxRows <= 0) {
            throw new IllegalArgumentException("maxRows must be positive (got " + maxRows + ")");
        }

        DBManager manager = server.getManager();
        try (DataScanner scanner = manager.executeSimpleQuery(
                tablespace,
                query,
                Collections.emptyList(),
                maxRows,
                false,
                TransactionContext.NO_TRANSACTION,
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT())) {
            return materialise(scanner);
        }
    }

    private static void validateTablespace(String tablespace) {
        if (tablespace == null || tablespace.trim().isEmpty()) {
            throw new IllegalArgumentException("tablespace cannot be null or blank");
        }
    }

    private static void validateSelectOnly(String query) {
        if (query == null) {
            throw new IllegalArgumentException("query cannot be null");
        }
        String trimmed = query.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("query cannot be blank");
        }
        // Strip a leading SQL comment if present (cheap defensive parse).
        String stripped = trimmed;
        while (stripped.startsWith("--")) {
            int newline = stripped.indexOf('\n');
            if (newline < 0) {
                throw new IllegalArgumentException("query cannot be only a comment");
            }
            stripped = stripped.substring(newline + 1).trim();
        }
        String upper = stripped.toUpperCase(Locale.ROOT);
        if (!upper.startsWith("SELECT")) {
            throw new IllegalArgumentException(
                    "QueryService only allows SELECT statements; got: "
                            + truncateForError(query));
        }
    }

    private static String truncateForError(String query) {
        String oneLine = query.replace('\n', ' ').trim();
        return oneLine.length() <= 80 ? oneLine : oneLine.substring(0, 77) + "...";
    }

    private static List<Map<String, Object>> materialise(DataScanner scanner) throws DataScannerException {
        String[] fieldNames = scanner.getFieldNames();
        List<DataAccessor> rows = scanner.consume();
        List<Map<String, Object>> out = new ArrayList<>(rows.size());
        for (DataAccessor row : rows) {
            Map<String, Object> map = new LinkedHashMap<>(fieldNames.length);
            for (String field : fieldNames) {
                map.put(field.toLowerCase(Locale.ROOT), row.get(field));
            }
            out.add(map);
        }
        return out;
    }
}
