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
import { useTablespaces } from '../contexts/TablespaceContext';

export function TablespaceSelector() {
    const { selected, setSelected, tablespaces, loading, error } =
        useTablespaces();

    if (loading) {
        return (
            <span className="herd-selector herd-selector--loading">
                Loading tablespaces…
            </span>
        );
    }
    if (error) {
        return (
            <span
                className="herd-selector herd-selector--error"
                role="alert"
                title={error}
            >
                Failed to load tablespaces
            </span>
        );
    }

    return (
        <label className="herd-selector">
            <span className="herd-selector__label">Tablespace</span>
            <select
                aria-label="Tablespace"
                value={selected}
                onChange={(e) => setSelected(e.target.value)}
            >
                {tablespaces.map((ts) => (
                    <option key={ts.name} value={ts.name}>
                        {ts.name}
                    </option>
                ))}
            </select>
        </label>
    );
}
