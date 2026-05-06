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

package herddb.remote.hammer;

import org.junit.Test;

/**
 * Issue #411 — lazy-DSM ({@code RemoteFileDataStorageManager} +
 * {@code LazyValueCache}) variant of
 * {@code DirectMultipleConcurrentUpdatesSuiteWithNonUniqueIndexesTest}.
 * Same workload shape (with a non-unique secondary index on {@code n1}),
 * same 4 variants, routed through the byte-range read path.
 */
public class LazyValueCacheConcurrentUpdatesSuiteWithNonUniqueIndexesTest
        extends LazyValueCacheConcurrentUpdatesSuite {

    @Test
    public void test() throws Exception {
        performTest(false, 0, true, false);
    }

    @Test
    public void testWithTransactions() throws Exception {
        performTest(true, 0, true, false);
    }

    @Test
    public void testWithCheckpoints() throws Exception {
        performTest(false, 2000, true, false);
    }

    @Test
    public void testWithTransactionsWithCheckpoints() throws Exception {
        performTest(true, 2000, true, false);
    }

}
