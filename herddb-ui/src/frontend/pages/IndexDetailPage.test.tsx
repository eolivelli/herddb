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
import type { IndexDetailDTO } from '../api/client';
import { IndexDetailPage } from './IndexDetailPage';

const detail: IndexDetailDTO = {
    summary: {
        name: 't_amount_idx',
        uuid: 'aabb',
        type: 'BRIN',
        unique: false,
        columns: ['amount'],
    },
    blocks: [
        { blockId: 1, pageId: 100, entries: 25, loaded: true, dirty: false },
        { blockId: 2, pageId: 101, entries: 25, loaded: false, dirty: false },
    ],
};

describe('IndexDetailPage', () => {
    it('renders summary, block count and the BRIN visualisation', async () => {
        render(
            <MemoryRouter
                initialEntries={[
                    '/tablespaces/herd/tables/t/indexes/t_amount_idx',
                ]}
            >
                <Routes>
                    <Route
                        path="/tablespaces/:tablespace/tables/:name/indexes/:index"
                        element={
                            <IndexDetailPage loader={async () => detail} />
                        }
                    />
                </Routes>
            </MemoryRouter>,
        );

        expect(
            await screen.findByRole('heading', {
                name: /Index — herd\.t\.t_amount_idx/i,
            }),
        ).toBeInTheDocument();
        expect(screen.getByText('BRIN')).toBeInTheDocument();
        expect(screen.getByText('aabb')).toBeInTheDocument();
        expect(
            screen.getByRole('heading', { name: 'Block layout' }),
        ).toBeInTheDocument();
    });
});
