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
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { describe, expect, it } from 'vitest';
import type { IndexStatusDTO, LogStatusDTO, TableSummaryDTO } from '../api/client';
import { TablespaceDetailPage } from './TablespaceDetailPage';

const logStatus: LogStatusDTO = {
    tablespaceUuid: 'uuid-abc',
    tablespace: 'herd',
    nodeId: 'node1',
    ledger: 5,
    offset: 42,
    status: 'leader',
    checkpointLedger: 4,
    checkpointOffset: 38,
    checkpointTimestamp: 1_700_000_000_000,
    checkpointDurationMs: 120,
    dirtyLedgersCount: 1,
};

const tables: TableSummaryDTO[] = [
    {
        tablespace: 'herd',
        name: 'orders',
        uuid: 'tbl-1',
        systemTable: false,
        tableSize: 100,
        loadedPages: 2,
        loadedPagesCount: 2,
        unloadedPagesCount: 0,
        dirtyPages: 0,
        dirtyRecords: 0,
        maxLogicalPageSize: 1_048_576,
        keysMemory: 64,
        buffersMemory: 256,
        dirtyMemory: 0,
    },
];

const indexes: IndexStatusDTO[] = [
    {
        tablespace: 'herd',
        tableName: 'orders',
        indexName: 'orders_amount_idx',
        indexType: 'BRIN',
        indexUuid: 'idx-uuid-1',
        properties: '{"numBlocks":3}',
    },
];

function renderPage() {
    return render(
        <MemoryRouter initialEntries={['/tablespaces/herd']}>
            <Routes>
                <Route
                    path="/tablespaces/:tablespace"
                    element={
                        <TablespaceDetailPage
                            logStatusLoader={async () => logStatus}
                            tablesLoader={async () => tables}
                            indexesLoader={async () => indexes}
                        />
                    }
                />
            </Routes>
        </MemoryRouter>,
    );
}

describe('TablespaceDetailPage', () => {
    it('renders the tablespace name in the heading', async () => {
        renderPage();
        expect(
            await screen.findByRole('heading', { name: /Tablespace — herd/i }),
        ).toBeInTheDocument();
    });

    it('shows checkpoint and log status rows', async () => {
        renderPage();
        expect(await screen.findByText('leader')).toBeInTheDocument();
        // write-head combined cell
        expect(screen.getByText(/5 \/ 42/)).toBeInTheDocument();
        // checkpoint ledger / offset combined cell
        expect(screen.getByText(/4 \/ 38/)).toBeInTheDocument();
        // duration
        expect(screen.getByText(/120 ms/)).toBeInTheDocument();
    });

    it('lists tables with links to the table detail page', async () => {
        renderPage();
        // There may be multiple "orders" links (one in Tables, one in Indexes
        // as the table-name column), so we collect all and verify at least one
        // points to the table detail page.
        const links = await screen.findAllByRole('link', { name: 'orders' });
        const hrefs = links.map((l) => l.getAttribute('href'));
        expect(hrefs).toContain('/tablespaces/herd/tables/orders');
    });

    it('lists indexes with links to the index detail page', async () => {
        renderPage();
        const link = await screen.findByRole('link', {
            name: 'orders_amount_idx',
        });
        expect(link).toBeInTheDocument();
        expect(link).toHaveAttribute(
            'href',
            '/tablespaces/herd/tables/orders/indexes/orders_amount_idx',
        );
    });

    it('renders an alert on loader error', async () => {
        render(
            <MemoryRouter initialEntries={['/tablespaces/herd']}>
                <Routes>
                    <Route
                        path="/tablespaces/:tablespace"
                        element={
                            <TablespaceDetailPage
                                logStatusLoader={async () => {
                                    throw new Error('connection refused');
                                }}
                                tablesLoader={async () => []}
                                indexesLoader={async () => []}
                            />
                        }
                    />
                </Routes>
            </MemoryRouter>,
        );
        const alert = await screen.findByRole('alert');
        expect(alert).toHaveTextContent(/connection refused/);
    });
});
