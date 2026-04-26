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
import { Route, Routes } from 'react-router-dom';
import { Header } from './components/Header';
import { TablespaceProvider } from './contexts/TablespaceContext';
import { DashboardPage } from './pages/DashboardPage';
import { PlaceholderPage } from './pages/PlaceholderPage';
import { TablespacesPage } from './pages/TablespacesPage';

export default function App() {
    return (
        <TablespaceProvider>
            <Header />
            <main className="herd-main">
                <Routes>
                    <Route path="/" element={<DashboardPage />} />
                    <Route path="/tablespaces" element={<TablespacesPage />} />
                    <Route
                        path="/tablespaces/:name"
                        element={
                            <PlaceholderPage
                                title="Tablespace detail"
                                phase="phase 3 (tables view)"
                            />
                        }
                    />
                    <Route
                        path="/indexing-services"
                        element={
                            <PlaceholderPage
                                title="Indexing services"
                                phase="phase 5 (indexing services view)"
                            />
                        }
                    />
                    <Route
                        path="*"
                        element={
                            <PlaceholderPage
                                title="Not found"
                                phase="—"
                            />
                        }
                    />
                </Routes>
            </main>
            <footer className="herd-footer">
                HerdDB Web UI · v2 phase 2 skeleton
            </footer>
        </TablespaceProvider>
    );
}
