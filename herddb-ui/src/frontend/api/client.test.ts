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
import { afterEach, describe, expect, it, vi } from 'vitest';
import { HerdDbApi, apiUrl } from './client';

afterEach(() => {
    vi.unstubAllGlobals();
});

describe('apiUrl', () => {
    it('joins BASE_URL, /api/v2, and the resource path', () => {
        // import.meta.env.BASE_URL is '/ui/' under the production base.
        expect(apiUrl('/health')).toBe('/ui/api/v2/health');
        expect(apiUrl('health')).toBe('/ui/api/v2/health');
        expect(apiUrl('/tablespaces/herd')).toBe(
            '/ui/api/v2/tablespaces/herd',
        );
    });
});

describe('HerdDbApi', () => {
    it('parses /health JSON', async () => {
        const fetchMock = vi.fn().mockResolvedValue(
            new Response(
                JSON.stringify({
                    status: 'ok',
                    nodeId: 'node-a',
                    timestamp: 1234,
                }),
                { status: 200, headers: { 'Content-Type': 'application/json' } },
            ),
        );
        vi.stubGlobal('fetch', fetchMock);

        const result = await HerdDbApi.health();
        expect(result).toEqual({
            status: 'ok',
            nodeId: 'node-a',
            timestamp: 1234,
        });
        expect(fetchMock).toHaveBeenCalledWith(
            '/ui/api/v2/health',
            expect.objectContaining({
                headers: expect.objectContaining({ Accept: 'application/json' }),
            }),
        );
    });

    it('throws on non-2xx responses', async () => {
        vi.stubGlobal(
            'fetch',
            vi.fn().mockResolvedValue(
                new Response('boom', { status: 500, statusText: 'Server Error' }),
            ),
        );
        await expect(HerdDbApi.health()).rejects.toThrow(
            /HerdDB API 500.*\/health.*boom/,
        );
    });
});
