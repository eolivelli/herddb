/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package herddb.model.commands;

import herddb.model.DDLStatement;

/**
 * {@code EXECUTE INDEXING_SERVICE_REBALANCE 'tableSpace', N}.
 *
 * <p>Writes an {@link herddb.log.LogEntryType#INDEXING_SERVICE_REBALANCE}
 * entry whose payload carries the new {@code numInstances} and a snapshot
 * of the current schema. Every indexing-service replica that observes the
 * entry updates its effective {@code numInstances} on the spot — so from
 * that LSN onward EVERY existing vector index spreads new writes across
 * the new owner set. Freshly-added replicas (whose BookKeeper history has
 * been trimmed) also use the schema snapshot to bootstrap.
 *
 * <p>The call returns as soon as the entry is durable; there is no
 * acknowledgement or barrier, since log ordering already gives every
 * indexing-service replica a deterministic point at which the change takes
 * effect.
 *
 * @author enrico.olivelli
 */
public class IndexingServiceRebalanceStatement extends DDLStatement {

    private final int numInstances;

    public IndexingServiceRebalanceStatement(String tableSpace, int numInstances) {
        super(tableSpace);
        this.numInstances = numInstances;
    }

    public int getNumInstances() {
        return numInstances;
    }
}
