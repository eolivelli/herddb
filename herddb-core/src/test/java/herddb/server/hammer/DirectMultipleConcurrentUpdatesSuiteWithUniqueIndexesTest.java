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


public class DirectMultipleConcurrentUpdatesSuiteWithUniqueIndexesTest extends DirectMultipleConcurrentUpdatesSuite {

    // No-checkpoint variants: 180 s JUnit ceiling, 90 s inner per-future limit
    // (inner is strictly smaller so TimeoutException fires before JUnit interrupt).
    @Test(timeout = 180_000)
    public void testWithUniqueIndexes() throws Exception {
        performTest(false, 0, true, true);
    }

    @Test(timeout = 180_000)
    public void testWithTransactionsAndUniqueIndexes() throws Exception {
        performTest(true, 0, true, true);
    }

    // Checkpoint variants: 240 s JUnit ceiling, 90 s inner per-future limit
    // (issue #417 — cuts 300 s+ CI hangs; dumpOnFailure rule captures thread dump).
    @Test(timeout = 240_000)
    public void testWithCheckpointsAndUniqueIndexes() throws Exception {
        performTest(false, 2000, true, true);
    }

    @Test(timeout = 240_000)
    public void testWithTransactionsWithCheckpointsAndUniqueIndexes() throws Exception {
        performTest(true, 2000, true, true);
    }

}
