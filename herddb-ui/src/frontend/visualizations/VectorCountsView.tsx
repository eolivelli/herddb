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
import type { IndexingServiceIndexDTO } from '../api/client';

interface VectorCountsViewProps {
    indexes: IndexingServiceIndexDTO[];
}

const HEIGHT = 240;
const PADDING_X = 16;
const PADDING_Y = 24;

/**
 * Bar-chart visualisation of vector counts by index, used by the
 * indexing-services overview page.
 */
export function VectorCountsView({ indexes }: VectorCountsViewProps) {
    if (indexes.length === 0) {
        return (
            <p className="herd-page__hint">
                No vector indexes registered yet.
            </p>
        );
    }
    const maxCount = indexes.reduce(
        (acc, i) => Math.max(acc, i.vectorCount),
        0,
    );
    return (
        <div className="herd-viz">
            <ParentSize>
                {({ width }) => {
                    const innerWidth = width - PADDING_X * 2;
                    const innerHeight = HEIGHT - PADDING_Y * 2;
                    const yScale = scaleLinear({
                        domain: [0, Math.max(maxCount, 1)],
                        range: [innerHeight, 0],
                    });
                    const step = innerWidth / indexes.length;
                    const barWidth = Math.max(8, step - 6);
                    return (
                        <svg width={width} height={HEIGHT}>
                            <Group left={PADDING_X} top={PADDING_Y}>
                                {indexes.map((idx, i) => {
                                    const barHeight =
                                        innerHeight - yScale(idx.vectorCount);
                                    return (
                                        <Bar
                                            key={
                                                idx.indexUuid ||
                                                `${idx.tablespace}.${idx.tableName}.${idx.indexName}`
                                            }
                                            x={i * step}
                                            y={innerHeight - barHeight}
                                            width={barWidth}
                                            height={barHeight}
                                            fill={
                                                idx.error
                                                    ? '#ef4444'
                                                    : idx.status === 'ready'
                                                      ? '#4ade80'
                                                      : '#38bdf8'
                                            }
                                            stroke="#334155"
                                        >
                                            <title>
                                                {`${idx.tablespace}.${idx.tableName}.${idx.indexName}\n` +
                                                    `vectors: ${idx.vectorCount}\n` +
                                                    `segments: ${idx.segmentCount}\n` +
                                                    `status: ${idx.status ?? '—'}\n` +
                                                    (idx.error
                                                        ? `error: ${idx.error}`
                                                        : '')}
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
                Bar height = vector count. Green = ready, blue = in
                progress, red = unreachable / error.
            </p>
        </div>
    );
}
