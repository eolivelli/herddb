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
import {
    HerdDbApi,
    type IndexingServicesOverviewDTO,
} from '../api/client';
import { useAsync } from '../components/AsyncResource';
import { VectorCountsView } from '../visualizations/VectorCountsView';

const formatter = new Intl.NumberFormat('en-US');
const fmt = (v: number | undefined | null): string =>
    v === undefined || v === null ? '—' : formatter.format(v);

interface IndexingServicesPageProps {
    /** Optional override used by tests to inject a mock loader. */
    loader?: () => Promise<IndexingServicesOverviewDTO>;
}

export function IndexingServicesPage({
    loader = HerdDbApi.listIndexingServices,
}: IndexingServicesPageProps = {}) {
    const { data, loading, error } = useAsync(() => loader(), [loader]);

    return (
        <section className="herd-page">
            <h1>Indexing services</h1>
            {loading && <p>Loading vector index status…</p>}
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
                                <th scope="row">Vector indexes</th>
                                <td>{fmt(data.totalIndexes)}</td>
                            </tr>
                            <tr>
                                <th scope="row">Total vectors</th>
                                <td>{fmt(data.totalVectorCount)}</td>
                            </tr>
                        </tbody>
                    </table>

                    <h2>Per-index summary</h2>
                    <VectorCountsView indexes={data.indexes} />

                    {data.indexes.length > 0 && (
                        <table className="herd-table">
                            <thead>
                                <tr>
                                    <th scope="col">Tablespace</th>
                                    <th scope="col">Table</th>
                                    <th scope="col">Index</th>
                                    <th scope="col">Status</th>
                                    <th scope="col">Vectors</th>
                                    <th scope="col">Segments</th>
                                    <th scope="col">Last LSN</th>
                                </tr>
                            </thead>
                            <tbody>
                                {data.indexes.map((idx) => (
                                    <tr
                                        key={
                                            idx.indexUuid ||
                                            `${idx.tablespace}.${idx.tableName}.${idx.indexName}`
                                        }
                                    >
                                        <td>{idx.tablespace}</td>
                                        <td>{idx.tableName}</td>
                                        <td>
                                            <code>{idx.indexName}</code>
                                        </td>
                                        <td>
                                            {idx.error ? (
                                                <span className="herd-error">
                                                    error
                                                </span>
                                            ) : (
                                                idx.status ?? '—'
                                            )}
                                        </td>
                                        <td>{fmt(idx.vectorCount)}</td>
                                        <td>{fmt(idx.segmentCount)}</td>
                                        <td>
                                            <code>
                                                {idx.lastLsnLedger}:{idx.lastLsnOffset}
                                            </code>
                                        </td>
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
