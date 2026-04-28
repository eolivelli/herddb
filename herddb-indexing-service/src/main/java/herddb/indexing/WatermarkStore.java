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

package herddb.indexing;

import herddb.log.LogSequenceNumber;
import java.io.IOException;

/**
 * Persists the durable indexing-service checkpoint state — the last processed
 * {@link LogSequenceNumber} (the "watermark") and the engine's effective
 * {@code numInstances} at the time of the checkpoint — so that the
 * indexing service can resume correctly after a restart.
 *
 * <p>Persisting {@code numInstances} alongside the watermark makes recovery
 * robust against BookKeeper history trimming: even if the ledger that
 * carried the most recent {@code INDEXING_SERVICE_REBALANCE} entry is gone
 * by the time the engine restarts, the engine re-acquires the correct
 * routing value from the watermark file (it was captured at the same
 * checkpoint LSN that the watermark refers to).
 *
 * <p><b>Save contract</b>: implementations must be called ONLY after a
 * successful {@code indexCheckpoint} (i.e. after all index pages and
 * IndexStatus markers have been durably persisted for the LSN being
 * saved). Saving an LSN that is not yet covered by a completed checkpoint
 * would mean, on restart with a wiped disk, that the service would skip
 * replaying entries it never actually persisted.
 *
 * @author enrico.olivelli
 */
public interface WatermarkStore {

    /**
     * Loads the last saved watermark snapshot.
     *
     * @return the saved snapshot, or {@link WatermarkSnapshot#START_OF_TIME}
     *         if no watermark exists
     */
    WatermarkSnapshot load() throws IOException;

    /**
     * Saves the snapshot atomically. Must be called only after the matching
     * checkpoint has been fully published.
     */
    void save(WatermarkSnapshot snapshot) throws IOException;
}
