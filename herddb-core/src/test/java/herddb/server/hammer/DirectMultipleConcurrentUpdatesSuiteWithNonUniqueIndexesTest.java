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

public class DirectMultipleConcurrentUpdatesSuiteWithNonUniqueIndexesTest extends DirectMultipleConcurrentUpdatesSuite {

    // No-checkpoint variants: 180 s JUnit ceiling, 120 s inner per-future limit
    // (raised from 90 s per issue #456; inner is strictly smaller so TimeoutException
    // fires before JUnit interrupt).
    @Test(timeout = 180_000)
    public void testWithIndexes() throws Exception {
        performTest(false, 0, true, false);
    }

    @Test(timeout = 180_000)
    public void testWithTransactionsAndIndexes() throws Exception {
        performTest(true, 0, true, false);
    }

    // Checkpoint variants: 300 s JUnit ceiling (raised from 240 s per issue #456),
    // 120 s inner per-future limit (raised from 90 s in the suite per issue #456).
    // (issue #417 — cuts 300 s+ CI hangs; dumpOnFailure rule captures thread dump).
    @Test(timeout = 300_000)
    public void testWithCheckpointsAndIndexes() throws Exception {
        performTest(false, 2000, true, false);
    }

    @Test(timeout = 300_000)
    public void testWithTransactionsWithCheckpointsAndIndexes() throws Exception {
        performTest(true, 2000, true, false);
    }

}
