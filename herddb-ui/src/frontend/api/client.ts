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

/**
 * Wire-shape DTOs returned by the HerdDB v2 REST API.
 * Names mirror the Java DTOs in `org.herddb.ui.dto`.
 */

export interface HealthDTO {
    status: string;
    nodeId: string;
    timestamp: number;
}

export interface ServerInfoDTO {
    nodeId: string;
    mode: string;
    jdbcUrl: string;
    defaultTablespace: string;
}

export interface TablespaceDTO {
    name: string;
    uuid: string;
    leader: string;
    replicas: string[];
    expectedReplicaCount: number;
    maxLeaderInactivityTime: number;
}

export interface TableSummaryDTO {
    tablespace: string;
    name: string;
    uuid: string;
    systemTable: boolean;
    tableSize: number;
    loadedPages: number;
    loadedPagesCount: number;
    unloadedPagesCount: number;
    dirtyPages: number;
    dirtyRecords: number;
    maxLogicalPageSize: number;
    keysMemory: number;
    buffersMemory: number;
    dirtyMemory: number;
}

export interface ColumnDTO {
    name: string;
    ordinalPosition: number;
    nullable: boolean;
    dataType: string;
    typeName: string;
    autoIncrement: boolean;
    defaultValue: string | null;
}

export interface IndexSummaryDTO {
    name: string;
    uuid: string;
    type: string;
    unique: boolean;
    columns: string[];
}

export interface ForeignKeyDTO {
    name: string;
    parentTable: string;
    childColumns: string[];
    parentColumns: string[];
    onDeleteAction: string | null;
    onUpdateAction: string | null;
}

export interface TableDetailDTO {
    summary: TableSummaryDTO;
    columns: ColumnDTO[];
    indexes: IndexSummaryDTO[];
    foreignKeys: ForeignKeyDTO[];
}

/**
 * Builds a fully qualified URL for the v2 REST API. In production the
 * SPA is served at `/ui/` so `apiUrl('/health')` resolves to
 * `/ui/api/v2/health`. In dev the BASE_URL is also `/ui/` thanks to
 * Vite's `base` setting + dev proxy.
 */
export function apiUrl(path: string): string {
    const base = import.meta.env.BASE_URL.replace(/\/$/, '');
    const suffix = path.startsWith('/') ? path : `/${path}`;
    return `${base}/api/v2${suffix}`;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await fetch(apiUrl(path), {
        ...init,
        headers: {
            Accept: 'application/json',
            ...(init?.headers ?? {}),
        },
    });
    if (!response.ok) {
        const body = await response.text();
        throw new Error(
            `HerdDB API ${response.status} ${response.statusText} for ${path}: ${body}`,
        );
    }
    return (await response.json()) as T;
}

export const HerdDbApi = {
    health(): Promise<HealthDTO> {
        return request<HealthDTO>('/health');
    },
    serverInfo(): Promise<ServerInfoDTO> {
        return request<ServerInfoDTO>('/server-info');
    },
    listTablespaces(): Promise<TablespaceDTO[]> {
        return request<TablespaceDTO[]>('/tablespaces');
    },
    getTablespace(name: string): Promise<TablespaceDTO> {
        return request<TablespaceDTO>(`/tablespaces/${encodeURIComponent(name)}`);
    },
    listTables(tablespace: string): Promise<TableSummaryDTO[]> {
        return request<TableSummaryDTO[]>(
            `/tablespaces/${encodeURIComponent(tablespace)}/tables`,
        );
    },
    getTable(tablespace: string, name: string): Promise<TableDetailDTO> {
        return request<TableDetailDTO>(
            `/tablespaces/${encodeURIComponent(tablespace)}/tables/${encodeURIComponent(name)}`,
        );
    },
};
