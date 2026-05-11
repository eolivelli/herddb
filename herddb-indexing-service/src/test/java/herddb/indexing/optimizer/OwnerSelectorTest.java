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
package herddb.indexing.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Properties;
import org.junit.Test;

/**
 * Unit tests for the {@link OwnerSelector} implementations and the
 * {@link OwnerSelectorFactory} (step 1). Step-2 wiring of a real
 * {@link InstanceLoadProbe} is covered separately in
 * {@code LeastLoadedOwnerSelectorIntegrationTest}.
 */
public class OwnerSelectorTest {

    private static final String TS = "tablespace-uuid";
    private static final String IX = "index-uuid";

    // -------------------------------------------------------------------------
    // Fixed0OwnerSelector
    // -------------------------------------------------------------------------

    @Test
    public void fixed0AlwaysReturnsZero() {
        Fixed0OwnerSelector sel = new Fixed0OwnerSelector();
        for (int i = 0; i < 5; i++) {
            assertEquals(0, sel.selectOwner(TS, IX));
        }
    }

    // -------------------------------------------------------------------------
    // RoundRobinOwnerSelector
    // -------------------------------------------------------------------------

    @Test
    public void roundRobinCyclesThroughInstances() {
        RoundRobinOwnerSelector sel = new RoundRobinOwnerSelector(() -> 3);
        assertEquals(0, sel.selectOwner(TS, IX));
        assertEquals(1, sel.selectOwner(TS, IX));
        assertEquals(2, sel.selectOwner(TS, IX));
        assertEquals(0, sel.selectOwner(TS, IX));
    }

    @Test
    public void roundRobinHandlesZeroInstancesAsOne() {
        RoundRobinOwnerSelector sel = new RoundRobinOwnerSelector(() -> 0);
        assertEquals(0, sel.selectOwner(TS, IX));
        assertEquals(0, sel.selectOwner(TS, IX));
    }

    // -------------------------------------------------------------------------
    // StaticAssignmentOwnerSelector
    // -------------------------------------------------------------------------

    @Test
    public void staticAssignmentRepeatsList() {
        StaticAssignmentOwnerSelector sel = new StaticAssignmentOwnerSelector(new int[]{1, 0, 1});
        assertEquals(1, sel.selectOwner(TS, IX));
        assertEquals(0, sel.selectOwner(TS, IX));
        assertEquals(1, sel.selectOwner(TS, IX));
        assertEquals(1, sel.selectOwner(TS, IX)); // wraps
    }

    @Test
    public void staticAssignmentRejectsEmpty() {
        try {
            new StaticAssignmentOwnerSelector(new int[0]);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            // expected
        }
    }

    @Test
    public void staticAssignmentRejectsNegative() {
        try {
            new StaticAssignmentOwnerSelector(new int[]{0, -1, 2});
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            // expected
        }
    }

    // -------------------------------------------------------------------------
    // LeastLoadedOwnerSelector
    // -------------------------------------------------------------------------

    @Test
    public void leastLoadedPicksMinBytes() {
        LeastLoadedOwnerSelector sel = new LeastLoadedOwnerSelector(
                (ts, ix) -> new long[]{500L, 200L, 800L});
        assertEquals(1, sel.selectOwner(TS, IX));
    }

    @Test
    public void leastLoadedBreaksTieAtLowestOrdinal() {
        LeastLoadedOwnerSelector sel = new LeastLoadedOwnerSelector(
                (ts, ix) -> new long[]{100L, 100L, 100L});
        assertEquals(0, sel.selectOwner(TS, IX));
    }

    @Test
    public void leastLoadedThrowsOnEmptyProbe() {
        LeastLoadedOwnerSelector sel = new LeastLoadedOwnerSelector((ts, ix) -> new long[0]);
        try {
            sel.selectOwner(TS, IX);
            fail("expected IllegalStateException");
        } catch (IllegalStateException ok) {
            assertTrue(ok.getMessage().contains("no live instances"));
        }
    }

    @Test
    public void leastLoadedThrowsOnNullProbeResult() {
        LeastLoadedOwnerSelector sel = new LeastLoadedOwnerSelector((ts, ix) -> null);
        try {
            sel.selectOwner(TS, IX);
            fail("expected IllegalStateException");
        } catch (IllegalStateException ok) {
            // expected
        }
    }

    // -------------------------------------------------------------------------
    // OwnerSelectorFactory
    // -------------------------------------------------------------------------

    @Test
    public void factoryDefaultsToFixedZero() {
        OwnerSelector sel = OwnerSelectorFactory.create(
                new OptimizerConfiguration(new Properties()), null);
        assertTrue(sel instanceof Fixed0OwnerSelector);
    }

    @Test
    public void factoryReturnsRoundRobinWhenPolicyIsRoundRobin() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY, "ROUND_ROBIN");
        p.setProperty(OptimizerConfiguration.PROPERTY_INDEXING_NUM_INSTANCES, "2");
        OwnerSelector sel = OwnerSelectorFactory.create(new OptimizerConfiguration(p), null);
        assertTrue(sel instanceof RoundRobinOwnerSelector);
        assertEquals(0, sel.selectOwner(TS, IX));
        assertEquals(1, sel.selectOwner(TS, IX));
        assertEquals(0, sel.selectOwner(TS, IX));
    }

    @Test
    public void factoryParsesStaticAssignmentCsv() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY, "STATIC");
        p.setProperty(OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_STATIC_ASSIGNMENT, "0, 1, 0, 1");
        OwnerSelector sel = OwnerSelectorFactory.create(new OptimizerConfiguration(p), null);
        assertTrue(sel instanceof StaticAssignmentOwnerSelector);
        assertEquals(0, sel.selectOwner(TS, IX));
        assertEquals(1, sel.selectOwner(TS, IX));
    }

    @Test
    public void factoryRejectsStaticWithoutAssignment() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY, "STATIC");
        try {
            OwnerSelectorFactory.create(new OptimizerConfiguration(p), null);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            // expected
        }
    }

    @Test
    public void factoryReturnsLeastLoadedWhenProbeProvided() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY, "LEAST_LOADED");
        InstanceLoadProbe probe = (ts, ix) -> new long[]{10L, 5L};
        OwnerSelector sel = OwnerSelectorFactory.create(new OptimizerConfiguration(p), probe);
        assertTrue(sel instanceof LeastLoadedOwnerSelector);
        assertEquals(1, sel.selectOwner(TS, IX));
    }

    @Test
    public void factoryLeastLoadedWithoutProbeFallsBackToFixedZero() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY, "LEAST_LOADED");
        OwnerSelector sel = OwnerSelectorFactory.create(new OptimizerConfiguration(p), null);
        assertTrue(sel instanceof Fixed0OwnerSelector);
    }

    @Test
    public void factoryIsCaseInsensitive() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY, " round_robin ");
        p.setProperty(OptimizerConfiguration.PROPERTY_INDEXING_NUM_INSTANCES, "1");
        OwnerSelector sel = OwnerSelectorFactory.create(new OptimizerConfiguration(p), null);
        assertTrue(sel instanceof RoundRobinOwnerSelector);
    }

    @Test
    public void factoryRejectsUnknownPolicy() {
        Properties p = new Properties();
        p.setProperty(OptimizerConfiguration.PROPERTY_OWNER_SELECTOR_POLICY, "MYSTERY");
        try {
            OwnerSelectorFactory.create(new OptimizerConfiguration(p), null);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            // expected
        }
    }
}
