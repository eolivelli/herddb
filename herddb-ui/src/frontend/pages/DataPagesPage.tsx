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
import { HerdDbApi, type PageLayoutDTO } from '../api/client';
import { useAsync } from '../components/AsyncResource';
import { DataPagesView } from '../visualizations/DataPagesView';

const formatter = new Intl.NumberFormat('en-US');
const fmt = (v: number | undefined | null): string =>
    v === undefined || v === null ? '—' : formatter.format(v);

interface DataPagesPageProps {
    /** Optional override used by tests to inject a mock loader. */
    loader?: (tablespace: string, table: string) => Promise<PageLayoutDTO>;
}

export function DataPagesPage({
    loader = HerdDbApi.getDataPages,
}: DataPagesPageProps = {}) {
    const { tablespace = '', name = '' } = useParams();
    const { data, loading, error } = useAsync(
        () => loader(tablespace, name),
        [tablespace, name, loader],
    );

    return (
        <section className="herd-page">
            <p>
                <Link
                    to={`/tablespaces/${encodeURIComponent(tablespace)}/tables/${encodeURIComponent(name)}`}
                >
                    ← Back to {tablespace}.{name}
                </Link>
            </p>
            <h1>Data pages — {tablespace}.{name}</h1>
            {loading && <p>Loading data pages…</p>}
            {error && (
                <p className="herd-error" role="alert">
                    {error}
                </p>
            )}
            {!loading && !error && data && (
                <>
                    <table className="herd-table">
                        <tbody>
                            <tr>
                                <th scope="row">Active pages</th>
                                <td>{fmt(data.totalPages)}</td>
                            </tr>
                            <tr>
                                <th scope="row">Loaded</th>
                                <td>{fmt(data.loadedPages)}</td>
                            </tr>
                            <tr>
                                <th scope="row">Total bytes</th>
                                <td>{fmt(data.totalSizeBytes)}</td>
                            </tr>
                            <tr>
                                <th scope="row">Dirt bytes</th>
                                <td>{fmt(data.totalDirtBytes)}</td>
                            </tr>
                        </tbody>
                    </table>

                    <h2>Layout</h2>
                    <DataPagesView layout={data} />
                </>
            )}
        </section>
    );
}
