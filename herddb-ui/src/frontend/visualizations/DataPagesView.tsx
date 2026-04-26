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
import { Group } from '@visx/group';
import { ParentSize } from '@visx/responsive';
import { scaleLinear } from '@visx/scale';
import type { PageLayoutDTO } from '../api/client';

interface DataPagesViewProps {
    layout: PageLayoutDTO;
}

const CELL_PER_ROW = 16;
const CELL_GAP = 2;
const MIN_HEIGHT = 200;

/**
 * Heatmap-style visualisation of a table's active data pages. Each cell
 * is one page; the brightness of the fill encodes the page size, and a
 * solid border distinguishes pages currently resident in the page cache.
 */
export function DataPagesView({ layout }: DataPagesViewProps) {
    if (layout.pages.length === 0) {
        return <p className="herd-page__hint">No active data pages.</p>;
    }

    const maxSize = layout.pages.reduce(
        (acc, p) => Math.max(acc, p.sizeBytes),
        0,
    );
    const fill = scaleLinear<string>({
        domain: [0, Math.max(maxSize, 1)],
        range: ['#0f172a', '#38bdf8'],
    });

    return (
        <div className="herd-viz">
            <ParentSize>
                {({ width }) => {
                    const cellWidth =
                        Math.max(20, (width - CELL_GAP * (CELL_PER_ROW - 1)) / CELL_PER_ROW);
                    const cellHeight = cellWidth * 0.6;
                    const rows = Math.ceil(layout.pages.length / CELL_PER_ROW);
                    const height = Math.max(
                        MIN_HEIGHT,
                        rows * (cellHeight + CELL_GAP),
                    );
                    return (
                        <svg width={width} height={height}>
                            <Group>
                                {layout.pages.map((page, idx) => {
                                    const row = Math.floor(idx / CELL_PER_ROW);
                                    const col = idx % CELL_PER_ROW;
                                    return (
                                        <rect
                                            key={page.pageId}
                                            x={col * (cellWidth + CELL_GAP)}
                                            y={row * (cellHeight + CELL_GAP)}
                                            width={cellWidth}
                                            height={cellHeight}
                                            fill={fill(page.sizeBytes)}
                                            stroke={
                                                page.loaded
                                                    ? '#facc15'
                                                    : '#334155'
                                            }
                                            strokeWidth={page.loaded ? 2 : 1}
                                        >
                                            <title>
                                                {`page #${page.pageId}\n` +
                                                    `size: ${page.sizeBytes} bytes\n` +
                                                    `avg record size: ${page.averageRecordSize}\n` +
                                                    `dirt: ${page.dirtBytes} bytes\n` +
                                                    `loaded: ${page.loaded}`}
                                            </title>
                                        </rect>
                                    );
                                })}
                            </Group>
                        </svg>
                    );
                }}
            </ParentSize>
            <p className="herd-page__hint">
                Each cell is one active data page. Brighter cells hold more
                bytes; cells with a yellow border are currently resident in
                the in-memory page cache. Hover over a cell for details.
            </p>
        </div>
    );
}
