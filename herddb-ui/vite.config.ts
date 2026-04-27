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
/// <reference types="vitest" />
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'node:path';

// Vite configuration for the HerdDB Web UI v2 SPA.
//
// Production deployment: the SPA is served by Jetty's WebAppContext at
// `/ui` and the JAX-RS application is mounted at `/api/v2/*` inside the
// same context, so all requests resolve at `/ui/...` from the browser's
// origin. We keep `base = '/ui/'` so every asset and every relative API
// URL produced by `apiUrl(...)` lives under `/ui/`.
//
// Development: `npm run dev` runs Vite's dev server at
// http://localhost:5173/ui/ and proxies /ui/api/v2/* to a HerdDB server
// running locally at http://localhost:9845. Override
// `HERDDB_DEV_SERVER_URL` to target a different backend.

const HERDDB_DEV_SERVER_URL =
    process.env.HERDDB_DEV_SERVER_URL || 'http://localhost:9845';

export default defineConfig({
    base: '/ui/',
    plugins: [react()],
    root: path.resolve(__dirname),
    build: {
        outDir: path.resolve(__dirname, 'target/web-build/spa'),
        emptyOutDir: true,
        sourcemap: false,
    },
    server: {
        port: 5173,
        strictPort: true,
        proxy: {
            '/ui/api': {
                target: HERDDB_DEV_SERVER_URL,
                changeOrigin: false,
            },
        },
    },
    test: {
        environment: 'jsdom',
        globals: true,
        setupFiles: ['src/frontend/test/setup.ts'],
        include: ['src/frontend/**/*.test.{ts,tsx}'],
        css: false,
    },
});
