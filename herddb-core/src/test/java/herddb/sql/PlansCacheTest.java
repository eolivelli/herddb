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

package herddb.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import org.junit.Test;

public class PlansCacheTest {

    @Test
    public void testDeterministic() {
        String k1 = PlansCache.buildCacheKey(true, "herd", "SELECT 1", false, 100);
        String k2 = PlansCache.buildCacheKey(true, "herd", "SELECT 1", false, 100);
        assertEquals(k1, k2);
    }

    @Test
    public void testCompactFormat() {
        String key = PlansCache.buildCacheKey(true, "ts", "SELECT 1", false, -1);
        assertEquals("1|ts|SELECT 1|0|-1", key);

        String key2 = PlansCache.buildCacheKey(false, "ts", "SELECT 1", true, 0);
        assertEquals("0|ts|SELECT 1|1|0", key2);
    }

    @Test
    public void testDifferentScan() {
        String k1 = PlansCache.buildCacheKey(true, "herd", "SELECT 1", false, -1);
        String k2 = PlansCache.buildCacheKey(false, "herd", "SELECT 1", false, -1);
        assertNotEquals(k1, k2);
    }

    @Test
    public void testDifferentTableSpace() {
        String k1 = PlansCache.buildCacheKey(true, "ts1", "SELECT 1", false, -1);
        String k2 = PlansCache.buildCacheKey(true, "ts2", "SELECT 1", false, -1);
        assertNotEquals(k1, k2);
    }

    @Test
    public void testDifferentQuery() {
        String k1 = PlansCache.buildCacheKey(true, "herd", "SELECT 1", false, -1);
        String k2 = PlansCache.buildCacheKey(true, "herd", "SELECT 2", false, -1);
        assertNotEquals(k1, k2);
    }

    @Test
    public void testDifferentReturnValues() {
        String k1 = PlansCache.buildCacheKey(true, "herd", "SELECT 1", true, -1);
        String k2 = PlansCache.buildCacheKey(true, "herd", "SELECT 1", false, -1);
        assertNotEquals(k1, k2);
    }

    @Test
    public void testDifferentMaxRows() {
        String k1 = PlansCache.buildCacheKey(true, "herd", "SELECT 1", false, 10);
        String k2 = PlansCache.buildCacheKey(true, "herd", "SELECT 1", false, 20);
        assertNotEquals(k1, k2);
    }
}
