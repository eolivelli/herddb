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

package herddb.index;

import herddb.log.LogSequenceNumber;

/**
 * Default fallback snapshot returned by the interface-level
 * {@code prepareCheckpoint} when an implementation has not opted into the
 * fuzzy two-phase checkpoint protocol. Carries only the LSN and the
 * {@code pin} flag so that the matching {@code persistCheckpoint} can call
 * the legacy single-phase {@code checkpoint(...)} unchanged.
 *
 * @author enrico.olivelli
 */
public final class SinglePhaseCheckpointSnapshot implements KeyToPageCheckpointSnapshot {

    private final LogSequenceNumber sequenceNumber;
    final boolean pin;

    public SinglePhaseCheckpointSnapshot(LogSequenceNumber sequenceNumber, boolean pin) {
        this.sequenceNumber = sequenceNumber;
        this.pin = pin;
    }

    @Override
    public LogSequenceNumber sequenceNumber() {
        return sequenceNumber;
    }
}
