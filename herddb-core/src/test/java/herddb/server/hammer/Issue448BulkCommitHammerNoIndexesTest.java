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

package herddb.server.hammer;

import org.junit.Test;

/**
 * Issue #448 hammer — multi-op transactions, no secondary indexes.
 * Two variants: with and without a periodic checkpoint thread.
 */
public class Issue448BulkCommitHammerNoIndexesTest extends Issue448BulkCommitHammerSuite {

    /**
     * No periodic checkpoint: every page-eviction unload during a commit
     * must therefore go through the new commit-time
     * {@code CheckpointFlushBatch} path — there is no concurrent Phase B
     * /Phase C to interfere.
     */
    @Test(timeout = 180_000)
    public void hammerNoCheckpoint() throws Exception {
        performHammer(0, false, false);
    }

    /**
     * Periodic checkpoint at 2 s — exercises the interleaving between
     * Phase B / Phase C and the new commit-batched unload dispatch (the
     * issue #157 / #431 / #448 invariants).
     */
    @Test(timeout = 240_000)
    public void hammerWithCheckpoint() throws Exception {
        performHammer(2_000, false, false);
    }
}
