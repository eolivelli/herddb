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
import { HerdDbApi, type TableDetailDTO } from '../api/client';
import { useAsync } from '../components/AsyncResource';

const formatter = new Intl.NumberFormat('en-US');

function formatNumber(value: number | undefined | null): string {
    if (value === undefined || value === null) {
        return '—';
    }
    return formatter.format(value);
}

interface TableDetailPageProps {
    /** Optional override used by tests to inject a mock loader. */
    loader?: (tablespace: string, name: string) => Promise<TableDetailDTO>;
}

export function TableDetailPage({
    loader = HerdDbApi.getTable,
}: TableDetailPageProps = {}) {
    const { tablespace = '', name = '' } = useParams();
    const { data, loading, error } = useAsync(
        () => loader(tablespace, name),
        [tablespace, name, loader],
    );

    return (
        <section className="herd-page">
            <p>
                <Link
                    to={`/tablespaces/${encodeURIComponent(tablespace)}/tables`}
                >
                    ← Back to tables
                </Link>
            </p>
            <h1>
                {tablespace}.{name}
            </h1>
            {loading && <p>Loading table…</p>}
            {error && (
                <p className="herd-error" role="alert">
                    {error}
                </p>
            )}
            {!loading && !error && data && (
                <>
                    <h2>Summary</h2>
                    <table className="herd-table">
                        <tbody>
                            <tr>
                                <th scope="row">UUID</th>
                                <td>
                                    <code>{data.summary.uuid}</code>
                                </td>
                            </tr>
                            <tr>
                                <th scope="row">Rows</th>
                                <td>{formatNumber(data.summary.tableSize)}</td>
                            </tr>
                            <tr>
                                <th scope="row">Loaded pages</th>
                                <td>
                                    {formatNumber(data.summary.loadedPages)} /{' '}
                                    {formatNumber(
                                        data.summary.loadedPagesCount +
                                            data.summary.unloadedPagesCount,
                                    )}
                                </td>
                            </tr>
                            <tr>
                                <th scope="row">Dirty pages / records</th>
                                <td>
                                    {formatNumber(data.summary.dirtyPages)} /{' '}
                                    {formatNumber(data.summary.dirtyRecords)}
                                </td>
                            </tr>
                            <tr>
                                <th scope="row">Memory (buffers + keys)</th>
                                <td>
                                    {formatNumber(data.summary.buffersMemory)} +{' '}
                                    {formatNumber(data.summary.keysMemory)} bytes
                                </td>
                            </tr>
                            <tr>
                                <th scope="row">Max page size</th>
                                <td>
                                    {formatNumber(data.summary.maxLogicalPageSize)} bytes
                                </td>
                            </tr>
                        </tbody>
                    </table>

                    <h2>Columns</h2>
                    <table className="herd-table">
                        <thead>
                            <tr>
                                <th scope="col">#</th>
                                <th scope="col">Name</th>
                                <th scope="col">Type</th>
                                <th scope="col">Nullable</th>
                                <th scope="col">Default</th>
                                <th scope="col">Auto-increment</th>
                            </tr>
                        </thead>
                        <tbody>
                            {data.columns.map((c) => (
                                <tr key={c.name}>
                                    <td>{c.ordinalPosition}</td>
                                    <td>
                                        <code>{c.name}</code>
                                    </td>
                                    <td>{c.typeName}</td>
                                    <td>{c.nullable ? 'yes' : 'no'}</td>
                                    <td>{c.defaultValue ?? '—'}</td>
                                    <td>{c.autoIncrement ? 'yes' : 'no'}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>

                    <h2>Indexes</h2>
                    {data.indexes.length === 0 ? (
                        <p className="herd-page__hint">No indexes.</p>
                    ) : (
                        <table className="herd-table">
                            <thead>
                                <tr>
                                    <th scope="col">Name</th>
                                    <th scope="col">Type</th>
                                    <th scope="col">Unique</th>
                                    <th scope="col">Columns</th>
                                </tr>
                            </thead>
                            <tbody>
                                {data.indexes.map((idx) => (
                                    <tr key={idx.uuid}>
                                        <td>
                                            <code>{idx.name}</code>
                                        </td>
                                        <td>{idx.type}</td>
                                        <td>{idx.unique ? 'yes' : 'no'}</td>
                                        <td>
                                            {idx.columns.map((c) => (
                                                <code key={c}>{c} </code>
                                            ))}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    )}

                    <h2>Foreign keys</h2>
                    {data.foreignKeys.length === 0 ? (
                        <p className="herd-page__hint">
                            No foreign keys declared on this table.
                        </p>
                    ) : (
                        <table className="herd-table">
                            <thead>
                                <tr>
                                    <th scope="col">Name</th>
                                    <th scope="col">Child columns</th>
                                    <th scope="col">Parent table</th>
                                    <th scope="col">Parent columns</th>
                                    <th scope="col">On delete</th>
                                    <th scope="col">On update</th>
                                </tr>
                            </thead>
                            <tbody>
                                {data.foreignKeys.map((fk) => (
                                    <tr key={fk.name}>
                                        <td>
                                            <code>{fk.name}</code>
                                        </td>
                                        <td>{fk.childColumns.join(', ')}</td>
                                        <td>{fk.parentTable}</td>
                                        <td>{fk.parentColumns.join(', ')}</td>
                                        <td>{fk.onDeleteAction ?? '—'}</td>
                                        <td>{fk.onUpdateAction ?? '—'}</td>
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
