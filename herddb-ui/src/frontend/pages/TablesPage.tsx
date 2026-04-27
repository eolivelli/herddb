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
import { Link } from 'react-router-dom';
import { HerdDbApi, type TableSummaryDTO } from '../api/client';
import { useAsync } from '../components/AsyncResource';
import { useTablespaces } from '../contexts/TablespaceContext';

const formatter = new Intl.NumberFormat('en-US');

function formatNumber(value: number | undefined): string {
    if (value === undefined || value === null) {
        return '—';
    }
    return formatter.format(value);
}

interface TablesPageProps {
    /** Optional override used by tests to inject a mock loader. */
    loader?: (tablespace: string) => Promise<TableSummaryDTO[]>;
}

export function TablesPage({ loader = HerdDbApi.listTables }: TablesPageProps = {}) {
    const { selected } = useTablespaces();
    const { data, loading, error } = useAsync(
        () => loader(selected),
        [selected, loader],
    );

    return (
        <section className="herd-page">
            <h1>Tables in {selected}</h1>
            {loading && <p>Loading tables…</p>}
            {error && (
                <p className="herd-error" role="alert">
                    {error}
                </p>
            )}
            {!loading && !error && data && data.length === 0 && (
                <p className="herd-page__hint">
                    No tables defined in this tablespace yet.
                </p>
            )}
            {!loading && !error && data && data.length > 0 && (
                <table className="herd-table">
                    <thead>
                        <tr>
                            <th scope="col">Name</th>
                            <th scope="col">Rows</th>
                            <th scope="col">Loaded pages</th>
                            <th scope="col">Dirty pages</th>
                            <th scope="col">Buffers (bytes)</th>
                            <th scope="col">Keys (bytes)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {data.map((t) => (
                            <tr key={t.uuid}>
                                <td>
                                    <Link
                                        to={`/tablespaces/${encodeURIComponent(
                                            t.tablespace,
                                        )}/tables/${encodeURIComponent(t.name)}`}
                                    >
                                        {t.name}
                                    </Link>
                                    {t.systemTable && (
                                        <span className="herd-badge"> system </span>
                                    )}
                                </td>
                                <td>{formatNumber(t.tableSize)}</td>
                                <td>
                                    {formatNumber(t.loadedPages)} / {formatNumber(
                                        t.loadedPagesCount + t.unloadedPagesCount,
                                    )}
                                </td>
                                <td>{formatNumber(t.dirtyPages)}</td>
                                <td>{formatNumber(t.buffersMemory)}</td>
                                <td>{formatNumber(t.keysMemory)}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            )}
        </section>
    );
}
