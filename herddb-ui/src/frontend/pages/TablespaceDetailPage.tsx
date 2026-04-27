/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { Link, useParams } from 'react-router-dom';
import {
    HerdDbApi,
    type IndexStatusDTO,
    type LogStatusDTO,
    type TableSummaryDTO,
} from '../api/client';
import { useAsync } from '../components/AsyncResource';

const fmt = new Intl.DateTimeFormat('en-US', {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
});

function formatTimestamp(epochMs: number | null | undefined): string {
    if (epochMs == null || epochMs <= 0) return '—';
    return fmt.format(new Date(epochMs));
}

function formatLong(v: number | null | undefined): string {
    return v == null ? '—' : String(v);
}

interface TablespaceDetailPageProps {
    logStatusLoader?: (tablespace: string) => Promise<LogStatusDTO>;
    tablesLoader?: (tablespace: string) => Promise<TableSummaryDTO[]>;
    indexesLoader?: (tablespace: string) => Promise<IndexStatusDTO[]>;
}

export function TablespaceDetailPage({
    logStatusLoader = HerdDbApi.getLogStatus,
    tablesLoader = HerdDbApi.listTables,
    indexesLoader = HerdDbApi.listIndexes,
}: TablespaceDetailPageProps = {}) {
    const { tablespace = '' } = useParams();

    const logStatus = useAsync(
        () => logStatusLoader(tablespace),
        [tablespace, logStatusLoader],
    );
    const tables = useAsync(
        () => tablesLoader(tablespace),
        [tablespace, tablesLoader],
    );
    const indexes = useAsync(
        () => indexesLoader(tablespace),
        [tablespace, indexesLoader],
    );

    const anyLoading = logStatus.loading || tables.loading || indexes.loading;
    const anyError = logStatus.error ?? tables.error ?? indexes.error;

    return (
        <section className="herd-page">
            <p>
                <Link to="/tablespaces">← Back to tablespaces</Link>
            </p>
            <h1>Tablespace — {tablespace}</h1>

            {anyLoading && <p>Loading…</p>}
            {anyError && (
                <p className="herd-error" role="alert">
                    {anyError}
                </p>
            )}

            {!anyLoading && !anyError && (
                <>
                    {/* ── Checkpoint / log status ────────────────────────── */}
                    <h2>Checkpoint &amp; log status</h2>
                    {logStatus.data ? (
                        <table className="herd-table">
                            <tbody>
                                <tr>
                                    <th scope="row">Status</th>
                                    <td>{logStatus.data.status ?? '—'}</td>
                                </tr>
                                <tr>
                                    <th scope="row">Write-head (ledger / offset)</th>
                                    <td>
                                        {formatLong(logStatus.data.ledger)} /{' '}
                                        {formatLong(logStatus.data.offset)}
                                    </td>
                                </tr>
                                <tr>
                                    <th scope="row">
                                        Last checkpoint (ledger / offset)
                                    </th>
                                    <td>
                                        {logStatus.data.checkpointLedger != null
                                            ? `${logStatus.data.checkpointLedger} / ${logStatus.data.checkpointOffset}`
                                            : '—'}
                                    </td>
                                </tr>
                                <tr>
                                    <th scope="row">Checkpoint timestamp</th>
                                    <td>
                                        {formatTimestamp(
                                            logStatus.data.checkpointTimestamp,
                                        )}
                                    </td>
                                </tr>
                                <tr>
                                    <th scope="row">Checkpoint duration</th>
                                    <td>
                                        {logStatus.data.checkpointDurationMs != null
                                            ? `${logStatus.data.checkpointDurationMs} ms`
                                            : '—'}
                                    </td>
                                </tr>
                                <tr>
                                    <th scope="row">Dirty ledger count</th>
                                    <td>
                                        {formatLong(
                                            logStatus.data.dirtyLedgersCount,
                                        )}
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    ) : (
                        <p className="herd-page__hint">
                            Log status unavailable.
                        </p>
                    )}

                    {/* ── Tables ──────────────────────────────────────────── */}
                    <h2>Tables</h2>
                    {tables.data && tables.data.length === 0 ? (
                        <p className="herd-page__hint">No tables found.</p>
                    ) : (
                        <table className="herd-table">
                            <thead>
                                <tr>
                                    <th scope="col">Name</th>
                                    <th scope="col">Rows</th>
                                    <th scope="col">System</th>
                                </tr>
                            </thead>
                            <tbody>
                                {(tables.data ?? []).map((t) => (
                                    <tr key={t.uuid}>
                                        <td>
                                            <Link
                                                to={`/tablespaces/${encodeURIComponent(tablespace)}/tables/${encodeURIComponent(t.name)}`}
                                            >
                                                {t.name}
                                            </Link>
                                        </td>
                                        <td>{t.tableSize}</td>
                                        <td>{t.systemTable ? 'yes' : 'no'}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    )}

                    {/* ── Indexes ─────────────────────────────────────────── */}
                    <h2>Indexes</h2>
                    {indexes.data && indexes.data.length === 0 ? (
                        <p className="herd-page__hint">No indexes found.</p>
                    ) : (
                        <table className="herd-table">
                            <thead>
                                <tr>
                                    <th scope="col">Name</th>
                                    <th scope="col">Table</th>
                                    <th scope="col">Type</th>
                                </tr>
                            </thead>
                            <tbody>
                                {(indexes.data ?? []).map((idx) => (
                                    <tr key={idx.indexUuid}>
                                        <td>
                                            <Link
                                                to={`/tablespaces/${encodeURIComponent(tablespace)}/tables/${encodeURIComponent(idx.tableName)}/indexes/${encodeURIComponent(idx.indexName)}`}
                                            >
                                                <code>{idx.indexName}</code>
                                            </Link>
                                        </td>
                                        <td>
                                            <Link
                                                to={`/tablespaces/${encodeURIComponent(tablespace)}/tables/${encodeURIComponent(idx.tableName)}`}
                                            >
                                                {idx.tableName}
                                            </Link>
                                        </td>
                                        <td>{idx.indexType}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    )}
                </>
            )}
        </section>
    );
}
