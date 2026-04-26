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
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { describe, expect, it } from 'vitest';
import type { TableDetailDTO } from '../api/client';
import { TableDetailPage } from './TableDetailPage';

const detail: TableDetailDTO = {
    summary: {
        tablespace: 'herd',
        name: 'child_t',
        uuid: 'cccc-dddd',
        systemTable: false,
        tableSize: 42,
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
    columns: [
        {
            name: 'id',
            ordinalPosition: 0,
            nullable: false,
            dataType: 'INTEGER',
            typeName: 'INTEGER',
            autoIncrement: false,
            defaultValue: null,
        },
        {
            name: 'amount',
            ordinalPosition: 1,
            nullable: true,
            dataType: 'LONG',
            typeName: 'LONG',
            autoIncrement: false,
            defaultValue: null,
        },
    ],
    indexes: [
        {
            name: 'child_t_amount_idx',
            uuid: 'idx-1',
            type: 'BRIN',
            unique: false,
            columns: ['amount'],
        },
    ],
    foreignKeys: [
        {
            name: 'fk_child_parent',
            parentTable: 'parent_t',
            childColumns: ['parent_id'],
            parentColumns: ['id'],
            onDeleteAction: 'NO ACTION',
            onUpdateAction: 'NO ACTION',
        },
    ],
};

function renderPage(loader: () => Promise<TableDetailDTO>) {
    return render(
        <MemoryRouter
            initialEntries={['/tablespaces/herd/tables/child_t']}
        >
            <Routes>
                <Route
                    path="/tablespaces/:tablespace/tables/:name"
                    element={<TableDetailPage loader={loader} />}
                />
            </Routes>
        </MemoryRouter>,
    );
}

describe('TableDetailPage', () => {
    it('renders the summary, columns, indexes, and foreign keys', async () => {
        renderPage(async () => detail);

        await waitFor(() =>
            expect(screen.getByText('herd.child_t')).toBeInTheDocument(),
        );

        expect(
            screen.getByRole('heading', { name: 'Summary' }),
        ).toBeInTheDocument();
        expect(
            screen.getByRole('heading', { name: 'Columns' }),
        ).toBeInTheDocument();
        expect(
            screen.getByRole('heading', { name: 'Indexes' }),
        ).toBeInTheDocument();
        expect(
            screen.getByRole('heading', { name: 'Foreign keys' }),
        ).toBeInTheDocument();

        // a couple of cells from each section
        expect(screen.getByText('cccc-dddd')).toBeInTheDocument();
        expect(screen.getByText('child_t_amount_idx')).toBeInTheDocument();
        expect(screen.getByText('fk_child_parent')).toBeInTheDocument();
        expect(screen.getByText('parent_t')).toBeInTheDocument();
    });

    it('renders an alert on loader error', async () => {
        renderPage(async () => {
            throw new Error('not found');
        });

        const alert = await screen.findByRole('alert');
        expect(alert).toHaveTextContent(/not found/);
    });
});
