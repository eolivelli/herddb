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
import { HerdDbApi, type IndexDetailDTO } from '../api/client';
import { useAsync } from '../components/AsyncResource';
import { BrinIndexView } from '../visualizations/BrinIndexView';

const formatter = new Intl.NumberFormat('en-US');
const fmt = (v: number | undefined | null): string =>
    v === undefined || v === null ? '—' : formatter.format(v);

interface IndexDetailPageProps {
    /** Optional override used by tests to inject a mock loader. */
    loader?: (
        tablespace: string,
        table: string,
        index: string,
    ) => Promise<IndexDetailDTO>;
}

export function IndexDetailPage({
    loader = HerdDbApi.getIndexDetail,
}: IndexDetailPageProps = {}) {
    const {
        tablespace = '',
        name = '',
        index: indexName = '',
    } = useParams();
    const { data, loading, error } = useAsync(
        () => loader(tablespace, name, indexName),
        [tablespace, name, indexName, loader],
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
            <h1>
                Index — {tablespace}.{name}.{indexName}
            </h1>
            {loading && <p>Loading index…</p>}
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
                                <th scope="row">Type</th>
                                <td>{data.summary.type}</td>
                            </tr>
                            <tr>
                                <th scope="row">UUID</th>
                                <td>
                                    <code>{data.summary.uuid}</code>
                                </td>
                            </tr>
                            <tr>
                                <th scope="row">Unique</th>
                                <td>{data.summary.unique ? 'yes' : 'no'}</td>
                            </tr>
                            <tr>
                                <th scope="row">Columns</th>
                                <td>{data.summary.columns.join(', ')}</td>
                            </tr>
                            <tr>
                                <th scope="row">Blocks</th>
                                <td>{fmt(data.blocks.length)}</td>
                            </tr>
                        </tbody>
                    </table>
                    {data.blocks.length > 0 ? (
                        <>
                            <h2>Block layout</h2>
                            <BrinIndexView blocks={data.blocks} />
                        </>
                    ) : (
                        <p className="herd-page__hint">
                            No per-block snapshot available for this index
                            type yet.
                        </p>
                    )}
                </>
            )}
        </section>
    );
}
