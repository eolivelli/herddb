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
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it } from 'vitest';
import type { TableSummaryDTO } from '../api/client';
import { TablespaceProvider } from '../contexts/TablespaceContext';
import { sampleTablespaces } from '../test/fixtures';
import { TablesPage } from './TablesPage';

const sampleTables: TableSummaryDTO[] = [
    {
        tablespace: 'herd',
        name: 'parent_t',
        uuid: 'aaaa',
        systemTable: false,
        tableSize: 2,
        loadedPages: 1,
        loadedPagesCount: 1,
        unloadedPagesCount: 0,
        dirtyPages: 0,
        dirtyRecords: 0,
        maxLogicalPageSize: 1_048_576,
        keysMemory: 64,
        buffersMemory: 256,
        dirtyMemory: 0,
    },
    {
        tablespace: 'herd',
        name: 'child_t',
        uuid: 'bbbb',
        systemTable: false,
        tableSize: 1,
        loadedPages: 1,
        loadedPagesCount: 1,
        unloadedPagesCount: 0,
        dirtyPages: 0,
        dirtyRecords: 0,
        maxLogicalPageSize: 1_048_576,
        keysMemory: 32,
        buffersMemory: 128,
        dirtyMemory: 0,
    },
];

describe('TablesPage', () => {
    it('lists tables for the selected tablespace', async () => {
        render(
            <TablespaceProvider loader={async () => sampleTablespaces}>
                <MemoryRouter>
                    <TablesPage loader={async () => sampleTables} />
                </MemoryRouter>
            </TablespaceProvider>,
        );

        await waitFor(() =>
            expect(screen.getByText(/Tables in herd/i)).toBeInTheDocument(),
        );
        expect(screen.getByRole('link', { name: 'parent_t' })).toHaveAttribute(
            'href',
            '/tablespaces/herd/tables/parent_t',
        );
        expect(screen.getByRole('link', { name: 'child_t' })).toBeInTheDocument();
    });

    it('shows empty state when there are no tables', async () => {
        render(
            <TablespaceProvider loader={async () => sampleTablespaces}>
                <MemoryRouter>
                    <TablesPage loader={async () => []} />
                </MemoryRouter>
            </TablespaceProvider>,
        );

        await waitFor(() =>
            expect(
                screen.getByText(/No tables defined in this tablespace/i),
            ).toBeInTheDocument(),
        );
    });

    it('shows error state when the loader fails', async () => {
        render(
            <TablespaceProvider loader={async () => sampleTablespaces}>
                <MemoryRouter>
                    <TablesPage
                        loader={async () => {
                            throw new Error('boom');
                        }}
                    />
                </MemoryRouter>
            </TablespaceProvider>,
        );

        const alert = await screen.findByRole('alert');
        expect(alert).toHaveTextContent(/boom/);
    });
});
