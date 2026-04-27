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
import type { PrimaryIndexDTO } from '../api/client';
import { PrimaryIndexPage } from './PrimaryIndexPage';

const snapshot: PrimaryIndexDTO = {
    type: 'blink',
    entries: 1234,
    loadedNodes: 2,
    totalNodes: 3,
    usedMemoryBytes: 65_536,
    truncated: false,
    nodes: [
        {
            nodeId: 1,
            storeId: 10,
            leaf: false,
            keys: 3,
            sizeBytes: 4096,
            loaded: true,
            dirty: false,
        },
        {
            nodeId: 2,
            storeId: 11,
            leaf: true,
            keys: 600,
            sizeBytes: 16_384,
            loaded: true,
            dirty: true,
        },
        {
            nodeId: 3,
            storeId: 12,
            leaf: true,
            keys: 634,
            sizeBytes: 16_500,
            loaded: false,
            dirty: false,
        },
    ],
};

describe('PrimaryIndexPage', () => {
    it('renders summary counters and a heatmap cell per BLink node', async () => {
        render(
            <MemoryRouter
                initialEntries={['/tablespaces/herd/tables/t/primary-index']}
            >
                <Routes>
                    <Route
                        path="/tablespaces/:tablespace/tables/:name/primary-index"
                        element={
                            <PrimaryIndexPage loader={async () => snapshot} />
                        }
                    />
                </Routes>
            </MemoryRouter>,
        );

        expect(
            await screen.findByRole('heading', {
                name: /Primary index — herd\.t/i,
            }),
        ).toBeInTheDocument();

        // Implementation row
        expect(screen.getByText('blink')).toBeInTheDocument();
        // Entries
        expect(screen.getByText('1,234')).toBeInTheDocument();
        // Total nodes (3)
        const cells = await screen.findAllByText('3');
        expect(cells.length).toBeGreaterThan(0);

        // Heatmap rendered with one rect per node
        const svg = await screen.findByTestId('primary-index-heatmap');
        expect(svg.querySelectorAll('rect').length).toBe(snapshot.nodes.length);
    });

    it('shows the truncation hint when the snapshot was clipped', async () => {
        const truncated: PrimaryIndexDTO = {
            ...snapshot,
            truncated: true,
        };
        render(
            <MemoryRouter
                initialEntries={['/tablespaces/herd/tables/t/primary-index']}
            >
                <Routes>
                    <Route
                        path="/tablespaces/:tablespace/tables/:name/primary-index"
                        element={
                            <PrimaryIndexPage loader={async () => truncated} />
                        }
                    />
                </Routes>
            </MemoryRouter>,
        );

        expect(
            await screen.findByRole('status'),
        ).toHaveTextContent(/Tree is too large/i);
    });

    it('falls back gracefully for non-BLink primary indexes (no nodes)', async () => {
        const nonBlink: PrimaryIndexDTO = {
            type: 'ConcurrentMapKeyToPageIndex',
            entries: 42,
            loadedNodes: 0,
            totalNodes: 0,
            usedMemoryBytes: 0,
            truncated: false,
            nodes: [],
        };
        render(
            <MemoryRouter
                initialEntries={['/tablespaces/herd/tables/t/primary-index']}
            >
                <Routes>
                    <Route
                        path="/tablespaces/:tablespace/tables/:name/primary-index"
                        element={
                            <PrimaryIndexPage loader={async () => nonBlink} />
                        }
                    />
                </Routes>
            </MemoryRouter>,
        );

        expect(
            await screen.findByText(/No BLink nodes to display/i),
        ).toBeInTheDocument();
    });
});
