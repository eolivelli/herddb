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
 * Issue #448 hammer — multi-op transactions plus a UNIQUE secondary index
 * on (n1, id). The {@code id} component avoids collisions and lock
 * timeouts (same trick as
 * {@link DirectMultipleConcurrentUpdatesSuite}); n1 still drives the index
 * write fan-out so we exercise the unique-constraint check inside the
 * commit's apply loop.
 */
public class Issue448BulkCommitHammerWithUniqueIndexesTest extends Issue448BulkCommitHammerSuite {

    @Test(timeout = 180_000)
    public void hammerNoCheckpoint() throws Exception {
        performHammer(0, true, true);
    }

    /**
     * Timeout raised to 300 s (from 240 s) to give extra headroom on CI
     * runners where the checkpoint thread adds significant commit stalls
     * (issue #456).
     */
    @Test(timeout = 300_000)
    public void hammerWithCheckpoint() throws Exception {
        performHammer(2_000, true, true);
    }
}
