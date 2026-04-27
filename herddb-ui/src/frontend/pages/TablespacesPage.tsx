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
import { useTablespaces } from '../contexts/TablespaceContext';

export function TablespacesPage() {
    const { tablespaces, loading, error } = useTablespaces();

    return (
        <section className="herd-page">
            <h1>Tablespaces</h1>
            {loading && <p>Loading tablespaces…</p>}
            {error && (
                <p className="herd-error" role="alert">
                    {error}
                </p>
            )}
            {!loading && !error && (
                <table className="herd-table">
                    <thead>
                        <tr>
                            <th scope="col">Name</th>
                            <th scope="col">UUID</th>
                            <th scope="col">Leader</th>
                            <th scope="col">Replicas</th>
                            <th scope="col">Expected replica count</th>
                        </tr>
                    </thead>
                    <tbody>
                        {tablespaces.map((ts) => (
                            <tr key={ts.name}>
                                <td>
                                    <Link
                                        to={`/tablespaces/${encodeURIComponent(ts.name ?? '')}`}
                                    >
                                        {ts.name}
                                    </Link>
                                </td>
                                <td>
                                    <code>{ts.uuid}</code>
                                </td>
                                <td>{ts.leader}</td>
                                <td>{ts.replicas.join(', ') || '—'}</td>
                                <td>{ts.expectedReplicaCount}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            )}
        </section>
    );
}
