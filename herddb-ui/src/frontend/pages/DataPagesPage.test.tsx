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
import type { PageLayoutDTO } from '../api/client';
import { DataPagesPage } from './DataPagesPage';

const layout: PageLayoutDTO = {
    totalPages: 3,
    loadedPages: 2,
    totalSizeBytes: 12_345,
    totalDirtBytes: 5,
    pages: [
        { pageId: 1, sizeBytes: 4096, averageRecordSize: 64, dirtBytes: 0, loaded: true },
        { pageId: 2, sizeBytes: 2048, averageRecordSize: 32, dirtBytes: 5, loaded: false },
        { pageId: 3, sizeBytes: 6201, averageRecordSize: 96, dirtBytes: 0, loaded: true },
    ],
};

describe('DataPagesPage', () => {
    it('renders summary counters and a cell per page', async () => {
        render(
            <MemoryRouter
                initialEntries={['/tablespaces/herd/tables/t/data-pages']}
            >
                <Routes>
                    <Route
                        path="/tablespaces/:tablespace/tables/:name/data-pages"
                        element={
                            <DataPagesPage loader={async () => layout} />
                        }
                    />
                </Routes>
            </MemoryRouter>,
        );

        expect(
            await screen.findByRole('heading', {
                name: /Data pages — herd\.t/i,
            }),
        ).toBeInTheDocument();
        expect(screen.getByText('3')).toBeInTheDocument(); // totalPages
        expect(screen.getByText('12,345')).toBeInTheDocument(); // totalSizeBytes
    });
});
