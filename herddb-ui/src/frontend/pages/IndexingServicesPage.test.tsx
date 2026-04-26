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
import { describe, expect, it } from 'vitest';
import type { IndexingServicesOverviewDTO } from '../api/client';
import { IndexingServicesPage } from './IndexingServicesPage';

const overview: IndexingServicesOverviewDTO = {
    totalIndexes: 2,
    totalVectorCount: 12_345,
    indexes: [
        {
            tablespace: 'herd',
            tableName: 'embeddings_t',
            indexName: 'embeddings_idx',
            indexType: 'vector',
            indexUuid: 'aaaa',
            status: 'ready',
            vectorCount: 10_000,
            segmentCount: 4,
            lastLsnLedger: 7,
            lastLsnOffset: 113,
            rawProperties: null,
            error: null,
        },
        {
            tablespace: 'herd',
            tableName: 'docs_t',
            indexName: 'docs_idx',
            indexType: 'vector',
            indexUuid: 'bbbb',
            status: 'indexing',
            vectorCount: 2_345,
            segmentCount: 1,
            lastLsnLedger: 7,
            lastLsnOffset: 80,
            rawProperties: null,
            error: null,
        },
    ],
};

describe('IndexingServicesPage', () => {
    it('renders summary, per-index table and the visualisation', async () => {
        render(<IndexingServicesPage loader={async () => overview} />);

        expect(
            await screen.findByRole('heading', { name: /Indexing services/i }),
        ).toBeInTheDocument();
        expect(screen.getByText('12,345')).toBeInTheDocument(); // total vectors
        expect(screen.getByText('embeddings_idx')).toBeInTheDocument();
        expect(screen.getByText('docs_idx')).toBeInTheDocument();
        expect(screen.getByText('ready')).toBeInTheDocument();
    });

    it('renders the empty state when no vector indexes exist', async () => {
        render(
            <IndexingServicesPage
                loader={async () => ({
                    totalIndexes: 0,
                    totalVectorCount: 0,
                    indexes: [],
                })}
            />,
        );
        expect(
            await screen.findByText(/No vector indexes registered yet/i),
        ).toBeInTheDocument();
    });

    it('renders an alert on loader error', async () => {
        render(
            <IndexingServicesPage
                loader={async () => {
                    throw new Error('boom');
                }}
            />,
        );
        const alert = await screen.findByRole('alert');
        expect(alert).toHaveTextContent(/boom/);
    });
});
