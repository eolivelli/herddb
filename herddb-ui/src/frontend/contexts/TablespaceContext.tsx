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
    createContext,
    useCallback,
    useContext,
    useEffect,
    useMemo,
    useState,
    type ReactNode,
} from 'react';
import { HerdDbApi, type TablespaceDTO } from '../api/client';

export const DEFAULT_TABLESPACE = 'herd';

interface TablespaceContextValue {
    selected: string;
    setSelected: (name: string) => void;
    tablespaces: TablespaceDTO[];
    loading: boolean;
    error: string | null;
    refresh: () => Promise<void>;
}

const TablespaceContext = createContext<TablespaceContextValue | undefined>(
    undefined,
);

interface TablespaceProviderProps {
    children: ReactNode;
    /** Optional override used by tests to inject a mock loader. */
    loader?: () => Promise<TablespaceDTO[]>;
    /** Optional override of the initial selection. */
    initialSelected?: string;
}

export function TablespaceProvider({
    children,
    loader = HerdDbApi.listTablespaces,
    initialSelected = DEFAULT_TABLESPACE,
}: TablespaceProviderProps) {
    const [tablespaces, setTablespaces] = useState<TablespaceDTO[]>([]);
    const [selected, setSelected] = useState(initialSelected);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    const refresh = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const list = await loader();
            setTablespaces(list);
            // If the previous selection is no longer present, fall back to
            // the default tablespace if possible, otherwise the first row.
            setSelected((current) => {
                if (list.some((ts) => ts.name === current)) {
                    return current;
                }
                if (list.some((ts) => ts.name === DEFAULT_TABLESPACE)) {
                    return DEFAULT_TABLESPACE;
                }
                return list.length > 0 ? list[0].name : current;
            });
        } catch (e) {
            setError(e instanceof Error ? e.message : String(e));
        } finally {
            setLoading(false);
        }
    }, [loader]);

    useEffect(() => {
        void refresh();
    }, [refresh]);

    const value = useMemo<TablespaceContextValue>(
        () => ({ selected, setSelected, tablespaces, loading, error, refresh }),
        [selected, tablespaces, loading, error, refresh],
    );

    return (
        <TablespaceContext.Provider value={value}>
            {children}
        </TablespaceContext.Provider>
    );
}

export function useTablespaces(): TablespaceContextValue {
    const ctx = useContext(TablespaceContext);
    if (!ctx) {
        throw new Error(
            'useTablespaces must be called inside a <TablespaceProvider>',
        );
    }
    return ctx;
}
