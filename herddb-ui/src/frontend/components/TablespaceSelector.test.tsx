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
import userEvent from '@testing-library/user-event';
import { describe, expect, it } from 'vitest';
import { TablespaceProvider } from '../contexts/TablespaceContext';
import { sampleTablespaces } from '../test/fixtures';
import { TablespaceSelector } from './TablespaceSelector';

describe('TablespaceSelector', () => {
    it("defaults to 'herd' and lists all tablespaces", async () => {
        render(
            <TablespaceProvider loader={async () => sampleTablespaces}>
                <TablespaceSelector />
            </TablespaceProvider>,
        );

        const select = (await screen.findByRole('combobox', {
            name: /tablespace/i,
        })) as HTMLSelectElement;
        expect(select.value).toBe('herd');

        const options = screen.getAllByRole('option');
        expect(options.map((o) => o.textContent)).toEqual([
            'herd',
            'analytics',
        ]);
    });

    it('changes selection when the user picks another tablespace', async () => {
        const user = userEvent.setup();
        render(
            <TablespaceProvider loader={async () => sampleTablespaces}>
                <TablespaceSelector />
            </TablespaceProvider>,
        );

        const select = (await screen.findByRole('combobox', {
            name: /tablespace/i,
        })) as HTMLSelectElement;

        await user.selectOptions(select, 'analytics');
        await waitFor(() => expect(select.value).toBe('analytics'));
    });

    it('falls back to first tablespace when no `herd` exists', async () => {
        render(
            <TablespaceProvider
                loader={async () => [sampleTablespaces[1]]}
                initialSelected="herd"
            >
                <TablespaceSelector />
            </TablespaceProvider>,
        );

        const select = (await screen.findByRole('combobox', {
            name: /tablespace/i,
        })) as HTMLSelectElement;
        expect(select.value).toBe('analytics');
    });

    it('shows an error message when the loader fails', async () => {
        render(
            <TablespaceProvider
                loader={async () => {
                    throw new Error('boom');
                }}
            >
                <TablespaceSelector />
            </TablespaceProvider>,
        );

        const alert = await screen.findByRole('alert');
        expect(alert).toHaveTextContent(/Failed to load/i);
    });
});
