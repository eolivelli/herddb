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

package herddb.server;

import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Per-request-type statistics exposed under the {@code requests} scope of the
 * server's root {@link StatsLogger}.
 *
 * <p>Each request type has a single {@link OpStatsLogger} that produces both a
 * latency distribution (with {@code quantile} label) and a count series (suffix
 * {@code _count}); the latter, combined with Prometheus {@code rate()},
 * provides the request rate broken down by type. Each {@link OpStatsLogger}
 * also exposes a {@code success} label so failed requests can be tracked
 * separately from successful ones.</p>
 *
 * <p>The metric names produced by this class are:
 * <ul>
 *   <li>{@code requests_execute_statement}</li>
 *   <li>{@code requests_execute_statements}</li>
 *   <li>{@code requests_prepare_statement}</li>
 *   <li>{@code requests_open_scanner}</li>
 *   <li>{@code requests_fetch_scanner_data}</li>
 *   <li>{@code requests_close_scanner}</li>
 *   <li>{@code requests_tx_begin}</li>
 *   <li>{@code requests_tx_commit}</li>
 *   <li>{@code requests_tx_rollback}</li>
 *   <li>{@code requests_request_tablespace_dump}</li>
 * </ul>
 *
 * <p>Latency values are stored in milliseconds (the BookKeeper Prometheus
 * provider converts the registered {@code TimeUnit.NANOSECONDS} value to ms
 * before exporting), so dashboard panels should use the {@code ms} format.</p>
 */
public class RequestStats {

    private static final String SCOPE = "requests";

    private final OpStatsLogger executeStatement;
    private final OpStatsLogger executeStatements;
    private final OpStatsLogger prepareStatement;
    private final OpStatsLogger openScanner;
    private final OpStatsLogger fetchScannerData;
    private final OpStatsLogger closeScanner;
    private final OpStatsLogger txBegin;
    private final OpStatsLogger txCommit;
    private final OpStatsLogger txRollback;
    private final OpStatsLogger requestTablespaceDump;

    public RequestStats(StatsLogger rootStatsLogger) {
        StatsLogger scope = rootStatsLogger.scope(SCOPE);
        this.executeStatement = scope.getOpStatsLogger("execute_statement");
        this.executeStatements = scope.getOpStatsLogger("execute_statements");
        this.prepareStatement = scope.getOpStatsLogger("prepare_statement");
        this.openScanner = scope.getOpStatsLogger("open_scanner");
        this.fetchScannerData = scope.getOpStatsLogger("fetch_scanner_data");
        this.closeScanner = scope.getOpStatsLogger("close_scanner");
        this.txBegin = scope.getOpStatsLogger("tx_begin");
        this.txCommit = scope.getOpStatsLogger("tx_commit");
        this.txRollback = scope.getOpStatsLogger("tx_rollback");
        this.requestTablespaceDump = scope.getOpStatsLogger("request_tablespace_dump");
    }

    public OpStatsLogger executeStatement() {
        return executeStatement;
    }

    public OpStatsLogger executeStatements() {
        return executeStatements;
    }

    public OpStatsLogger prepareStatement() {
        return prepareStatement;
    }

    public OpStatsLogger openScanner() {
        return openScanner;
    }

    public OpStatsLogger fetchScannerData() {
        return fetchScannerData;
    }

    public OpStatsLogger closeScanner() {
        return closeScanner;
    }

    public OpStatsLogger txBegin() {
        return txBegin;
    }

    public OpStatsLogger txCommit() {
        return txCommit;
    }

    public OpStatsLogger txRollback() {
        return txRollback;
    }

    public OpStatsLogger requestTablespaceDump() {
        return requestTablespaceDump;
    }
}
