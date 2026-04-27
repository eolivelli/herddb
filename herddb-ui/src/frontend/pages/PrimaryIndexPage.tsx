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
import { HerdDbApi, type PrimaryIndexDTO } from '../api/client';
import { useAsync } from '../components/AsyncResource';

const formatter = new Intl.NumberFormat('en-US');
const fmt = (v: number | undefined | null): string =>
    v === undefined || v === null ? '—' : formatter.format(v);

interface PrimaryIndexPageProps {
    /** Optional override used by tests to inject a mock loader. */
    loader?: (
        tablespace: string,
        table: string,
    ) => Promise<PrimaryIndexDTO>;
}

export function PrimaryIndexPage({
    loader = HerdDbApi.getPrimaryIndex,
}: PrimaryIndexPageProps = {}) {
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
            <h1>Primary index — {tablespace}.{name}</h1>
            {loading && <p>Loading primary index…</p>}
            {error && (
                <p className="herd-error" role="alert">
                    {error}
                </p>
            )}
            {!loading && !error && data && (
                <table className="herd-table">
                    <tbody>
                        <tr>
                            <th scope="row">Implementation</th>
                            <td>
                                <code>{data.type}</code>
                            </td>
                        </tr>
                        <tr>
                            <th scope="row">Entries</th>
                            <td>{fmt(data.entries)}</td>
                        </tr>
                        <tr>
                            <th scope="row">Loaded nodes</th>
                            <td>{fmt(data.loadedNodes)}</td>
                        </tr>
                        <tr>
                            <th scope="row">Memory used</th>
                            <td>{fmt(data.usedMemoryBytes)} bytes</td>
                        </tr>
                    </tbody>
                </table>
            )}
        </section>
    );
}
