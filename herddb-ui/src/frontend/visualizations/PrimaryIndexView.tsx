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
import type { BLinkNodeDTO } from '../api/client';

interface PrimaryIndexViewProps {
    nodes: BLinkNodeDTO[];
}

const CELL_PER_ROW = 16;
const CELL_GAP = 2;
const MIN_HEIGHT = 200;

/**
 * Heatmap-style visualisation of the BLink primary-key index. Each cell is
 * one node; cell brightness encodes the byte size, a yellow border
 * distinguishes nodes currently resident in memory, a red overlay flags
 * dirty (uncheckpointed) nodes, and inner (non-leaf) nodes are drawn with
 * a dashed outline so the operator can spot the routing layer.
 */
export function PrimaryIndexView({ nodes }: PrimaryIndexViewProps) {
    if (nodes.length === 0) {
        return (
            <p className="herd-page__hint">
                No BLink nodes to display (the primary index may not be a
                B-Link implementation, e.g. the in-memory map used in local
                mode).
            </p>
        );
    }

    const maxSize = nodes.reduce((acc, n) => Math.max(acc, n.sizeBytes), 0);
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
                    const rows = Math.ceil(nodes.length / CELL_PER_ROW);
                    const height = Math.max(
                        MIN_HEIGHT,
                        rows * (cellHeight + CELL_GAP),
                    );
                    return (
                        <svg
                            width={width}
                            height={height}
                            data-testid="primary-index-heatmap"
                        >
                            <Group>
                                {nodes.map((node, idx) => {
                                    const row = Math.floor(idx / CELL_PER_ROW);
                                    const col = idx % CELL_PER_ROW;
                                    return (
                                        <rect
                                            key={node.nodeId}
                                            x={col * (cellWidth + CELL_GAP)}
                                            y={row * (cellHeight + CELL_GAP)}
                                            width={cellWidth}
                                            height={cellHeight}
                                            fill={
                                                node.dirty
                                                    ? '#ef4444'
                                                    : fill(node.sizeBytes)
                                            }
                                            stroke={
                                                node.loaded
                                                    ? '#facc15'
                                                    : '#334155'
                                            }
                                            strokeWidth={node.loaded ? 2 : 1}
                                            strokeDasharray={
                                                node.leaf ? undefined : '3 2'
                                            }
                                        >
                                            <title>
                                                {`node #${node.nodeId}\n` +
                                                    `storeId: ${node.storeId}\n` +
                                                    `leaf: ${node.leaf}\n` +
                                                    `keys: ${node.keys}\n` +
                                                    `size: ${node.sizeBytes} bytes\n` +
                                                    `loaded: ${node.loaded}\n` +
                                                    `dirty: ${node.dirty}`}
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
                Each cell is one BLink node. Brighter cells hold more bytes;
                cells with a yellow border are resident in memory; red cells
                are dirty (uncheckpointed); inner nodes have a dashed border.
                Hover over a cell for details.
            </p>
        </div>
    );
}
