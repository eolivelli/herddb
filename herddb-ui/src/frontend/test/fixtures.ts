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
import type { ServerInfoDTO, TablespaceDTO } from '../api/client';

export const sampleTablespaces: TablespaceDTO[] = [
    {
        name: 'herd',
        uuid: '11111111-1111-1111-1111-111111111111',
        leader: 'node-a',
        replicas: ['node-a'],
        expectedReplicaCount: 1,
        maxLeaderInactivityTime: 0,
    },
    {
        name: 'analytics',
        uuid: '22222222-2222-2222-2222-222222222222',
        leader: 'node-a',
        replicas: ['node-a', 'node-b'],
        expectedReplicaCount: 2,
        maxLeaderInactivityTime: 60_000,
    },
];

export const sampleServerInfo: ServerInfoDTO = {
    nodeId: 'node-a',
    mode: 'local',
    jdbcUrl: 'jdbc:herddb:server:localhost:7000',
    defaultTablespace: 'herd',
};
