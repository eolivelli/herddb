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
import { Bar } from '@visx/shape';
import type { BrinBlockDTO } from '../api/client';

interface BrinIndexViewProps {
    blocks: BrinBlockDTO[];
}

const HEIGHT = 220;
const PADDING_X = 16;
const PADDING_Y = 24;

/**
 * Bar-chart visualisation of BRIN blocks. Each bar represents one
 * block, ordered by block id; bar height encodes the entry count, and
 * loaded blocks are rendered with a solid yellow border.
 */
export function BrinIndexView({ blocks }: BrinIndexViewProps) {
    if (blocks.length === 0) {
        return <p className="herd-page__hint">No BRIN blocks to display.</p>;
    }

    const sorted = [...blocks].sort((a, b) => a.blockId - b.blockId);
    const maxEntries = sorted.reduce(
        (acc, b) => Math.max(acc, b.entries),
        0,
    );

    return (
        <div className="herd-viz">
            <ParentSize>
                {({ width }) => {
                    const innerWidth = width - PADDING_X * 2;
                    const innerHeight = HEIGHT - PADDING_Y * 2;
                    const yScale = scaleLinear({
                        domain: [0, Math.max(maxEntries, 1)],
                        range: [innerHeight, 0],
                    });
                    const barWidth = Math.max(
                        4,
                        innerWidth / sorted.length - 2,
                    );
                    const step = innerWidth / sorted.length;
                    return (
                        <svg width={width} height={HEIGHT}>
                            <Group left={PADDING_X} top={PADDING_Y}>
                                {sorted.map((block, idx) => {
                                    const barHeight =
                                        innerHeight - yScale(block.entries);
                                    return (
                                        <Bar
                                            key={block.blockId}
                                            x={idx * step}
                                            y={innerHeight - barHeight}
                                            width={barWidth}
                                            height={barHeight}
                                            fill={
                                                block.dirty
                                                    ? '#ef4444'
                                                    : '#38bdf8'
                                            }
                                            stroke={
                                                block.loaded
                                                    ? '#facc15'
                                                    : '#334155'
                                            }
                                            strokeWidth={block.loaded ? 2 : 1}
                                        >
                                            <title>
                                                {`block #${block.blockId}\n` +
                                                    `pageId: ${block.pageId}\n` +
                                                    `entries: ${block.entries}\n` +
                                                    `loaded: ${block.loaded}\n` +
                                                    `dirty: ${block.dirty}`}
                                            </title>
                                        </Bar>
                                    );
                                })}
                            </Group>
                        </svg>
                    );
                }}
            </ParentSize>
            <p className="herd-page__hint">
                One bar per block; height encodes the entry count. Yellow
                border = currently loaded in memory, red fill = dirty.
            </p>
        </div>
    );
}
