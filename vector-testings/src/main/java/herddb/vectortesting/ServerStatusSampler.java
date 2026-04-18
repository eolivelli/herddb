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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Snapshots server-side state — commit-log head, last checkpoint, table dirty state, vector
 * index progress — by querying {@code syslogstatus}, {@code systablestats} and
 * {@code sysindexstatus}. Used by {@link VectorBench} during ingestion to emit periodic
 * {@code [status]} lines that make checkpoint stalls and index-tail lag visible in the run log.
 *
 * <p>Robust to older servers missing the new checkpoint columns: the {@code SELECT *} query
 * picks up whatever is available, and absent columns simply produce {@code null} fields.</p>
 */
final class ServerStatusSampler {

    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);

    private static final Pattern VECTOR_COUNT = Pattern.compile("\"vectorCount\"\\s*:\\s*(\\d+)");
    private static final Pattern SEGMENT_COUNT = Pattern.compile("\"segmentCount\"\\s*:\\s*(\\d+)");
    private static final Pattern LAST_LSN_LEDGER = Pattern.compile("\"lastLsnLedger\"\\s*:\\s*(-?\\d+)");
    private static final Pattern LAST_LSN_OFFSET = Pattern.compile("\"lastLsnOffset\"\\s*:\\s*(-?\\d+)");
    private static final Pattern INDEX_STATUS = Pattern.compile("\"status\"\\s*:\\s*\"([^\"]*)\"");

    private final Config config;
    private final String tableSpace;

    ServerStatusSampler(Config config) {
        this.config = config;
        this.tableSpace = "herd";
    }

    /**
     * Runs one sample round-trip and returns a nested field map suitable for
     * {@link BenchOutput#status(String, double, LinkedHashMap)}. Never throws — errors are
     * surfaced as an {@code error=<message>} field so the status thread keeps trying on the
     * next tick.
     */
    LinkedHashMap<String, Object> sample() {
        LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        try (Connection conn = java.sql.DriverManager.getConnection(
                config.effectiveJdbcUrl(), config.username, config.password)) {
            conn.setAutoCommit(true);
            fillLogStatus(conn, fields);
            fillTableStats(conn, fields);
            fillIndexStatus(conn, fields);
        } catch (SQLException e) {
            fields.put("error", e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
        return fields;
    }

    private void fillLogStatus(Connection conn, LinkedHashMap<String, Object> fields) {
        // SELECT * tolerates older servers that don't yet have the checkpoint_* columns.
        String sql = "SELECT * FROM " + tableSpace + ".syslogstatus";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) {
                return;
            }
            LinkedHashMap<String, Object> commitlog = new LinkedHashMap<>();
            putLongIfPresent(rs, "ledger", commitlog, "ledger");
            putLongIfPresent(rs, "offset", commitlog, "offset");
            if (!commitlog.isEmpty()) {
                fields.put("commitlog", commitlog);
            }

            LinkedHashMap<String, Object> checkpoint = new LinkedHashMap<>();
            Long cpLedger = getLongOrNull(rs, "checkpoint_ledger");
            Long cpOffset = getLongOrNull(rs, "checkpoint_offset");
            Long cpDuration = getLongOrNull(rs, "checkpoint_duration_ms");
            Timestamp cpTs = getTimestampOrNull(rs, "checkpoint_timestamp");
            Integer dirtyLedgers = getIntOrNull(rs, "dirty_ledgers_count");
            if (cpLedger != null) {
                checkpoint.put("ledger", cpLedger);
            }
            if (cpOffset != null) {
                checkpoint.put("offset", cpOffset);
            }
            if (dirtyLedgers != null) {
                checkpoint.put("lag_ledgers", dirtyLedgers);
            }
            if (cpDuration != null) {
                checkpoint.put("duration_ms", cpDuration);
            }
            if (cpTs != null) {
                checkpoint.put("ts", TS_FORMAT.format(Instant.ofEpochMilli(cpTs.getTime())));
            }
            if (!checkpoint.isEmpty()) {
                fields.put("checkpoint", checkpoint);
            }
        } catch (SQLException e) {
            fields.put("syslogstatus_error", e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    private void fillTableStats(Connection conn, LinkedHashMap<String, Object> fields) {
        String sql = "SELECT tablesize, dirtypages, dirtyrecords, buffersmemory, keysmemory "
                + "FROM " + tableSpace + ".systablestats WHERE table_name=?";
        try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, config.tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return;
                }
                LinkedHashMap<String, Object> table = new LinkedHashMap<>();
                table.put("rows", rs.getLong("tablesize"));
                table.put("dirty_pages", rs.getLong("dirtypages"));
                table.put("dirty_records", rs.getLong("dirtyrecords"));
                table.put("buffers_mb", rs.getLong("buffersmemory") / (1024L * 1024L));
                table.put("keys_mb", rs.getLong("keysmemory") / (1024L * 1024L));
                fields.put("table", table);
            }
        } catch (SQLException e) {
            fields.put("systablestats_error", e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    private void fillIndexStatus(Connection conn, LinkedHashMap<String, Object> fields) {
        String sql = "SELECT properties, index_type FROM " + tableSpace
                + ".sysindexstatus WHERE table_name=? AND index_type='vector'";
        try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, config.tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return;
                }
                String props = rs.getString("properties");
                LinkedHashMap<String, Object> index = new LinkedHashMap<>();
                extractLong(VECTOR_COUNT, props, v -> index.put("vectors", v));
                extractLong(SEGMENT_COUNT, props, v -> index.put("segments", v));
                extractLong(LAST_LSN_LEDGER, props, v -> index.put("lsn_ledger", v));
                extractLong(LAST_LSN_OFFSET, props, v -> index.put("lsn_offset", v));
                Matcher m = INDEX_STATUS.matcher(props == null ? "" : props);
                if (m.find()) {
                    index.put("status", m.group(1));
                }
                if (!index.isEmpty()) {
                    fields.put("index", index);
                }
            }
        } catch (SQLException e) {
            fields.put("sysindexstatus_error", e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    private static void extractLong(Pattern p, String haystack, java.util.function.LongConsumer sink) {
        if (haystack == null) {
            return;
        }
        Matcher m = p.matcher(haystack);
        if (m.find()) {
            try {
                sink.accept(Long.parseLong(m.group(1)));
            } catch (NumberFormatException ignore) {
                // Malformed properties payload — skip this field rather than abort the whole sample.
            }
        }
    }

    private static void putLongIfPresent(ResultSet rs, String column,
                                         LinkedHashMap<String, Object> target, String key) throws SQLException {
        Long v = getLongOrNull(rs, column);
        if (v != null) {
            target.put(key, v);
        }
    }

    private static Long getLongOrNull(ResultSet rs, String column) {
        try {
            long v = rs.getLong(column);
            return rs.wasNull() ? null : v;
        } catch (SQLException e) {
            // Column doesn't exist on an older server — treat as null.
            return null;
        }
    }

    private static Integer getIntOrNull(ResultSet rs, String column) {
        try {
            int v = rs.getInt(column);
            return rs.wasNull() ? null : v;
        } catch (SQLException e) {
            return null;
        }
    }

    private static Timestamp getTimestampOrNull(ResultSet rs, String column) {
        try {
            Timestamp v = rs.getTimestamp(column);
            return rs.wasNull() ? null : v;
        } catch (SQLException e) {
            return null;
        }
    }
}
