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
import { useEffect, useState } from 'react';
import { NavLink } from 'react-router-dom';
import { HerdDbApi, type ServerInfoDTO } from '../api/client';
import { TablespaceSelector } from './TablespaceSelector';

interface HeaderProps {
    /** Optional override used by tests to inject a mock loader. */
    serverInfoLoader?: () => Promise<ServerInfoDTO>;
}

export function Header({ serverInfoLoader = HerdDbApi.serverInfo }: HeaderProps) {
    const [serverInfo, setServerInfo] = useState<ServerInfoDTO | null>(null);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        let cancelled = false;
        serverInfoLoader()
            .then((info) => {
                if (!cancelled) {
                    setServerInfo(info);
                }
            })
            .catch((e) => {
                if (!cancelled) {
                    setError(e instanceof Error ? e.message : String(e));
                }
            });
        return () => {
            cancelled = true;
        };
    }, [serverInfoLoader]);

    return (
        <header className="herd-header">
            <div className="herd-header__brand">
                <strong>HerdDB</strong>
                <span className="herd-header__brand-tag">Web UI</span>
            </div>
            <nav className="herd-header__nav">
                <NavLink to="/" end>
                    Dashboard
                </NavLink>
                <NavLink to="/tablespaces">Tablespaces</NavLink>
                <NavLink to="/indexing-services">Indexing Services</NavLink>
            </nav>
            <div className="herd-header__meta">
                <TablespaceSelector />
                <span className="herd-header__node" aria-label="server node">
                    {error
                        ? `Server: error (${error})`
                        : serverInfo
                          ? `Node ${serverInfo.nodeId} · ${serverInfo.mode}`
                          : 'Server: loading…'}
                </span>
                <button
                    className="herd-header__refresh-btn"
                    title="Reload current page"
                    aria-label="Refresh"
                    onClick={() => window.location.reload()}
                >
                    ⟳ Refresh
                </button>
            </div>
        </header>
    );
}
