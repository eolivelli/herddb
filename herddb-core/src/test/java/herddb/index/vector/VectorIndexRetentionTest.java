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

package herddb.index.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.index.vector.PersistentVectorStore.PendingDelete;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Tests for the retention-reaper decision logic in
 * {@link VectorIndexCompactor#partitionReapable}.
 */
public class VectorIndexRetentionTest {

    @Test
    public void entriesPastDeadlineAndAckedAreReapable() {
        long now = 1_000_000L;
        List<PendingDelete> pending = new ArrayList<>(Arrays.asList(
                new PendingDelete("s1:graph", now - 10, 5L),
                new PendingDelete("s1:map", now - 10, 5L)));

        VectorIndexCompactor.Partition p = VectorIndexCompactor.partitionReapable(
                pending, now, /*minShadowAcked*/ 6L);

        assertEquals(2, p.reapable.size());
        assertTrue(p.retained.isEmpty());
    }

    @Test
    public void deadlineInFutureKeepsEntryRetained() {
        long now = 1_000_000L;
        List<PendingDelete> pending = Arrays.asList(
                new PendingDelete("s1:graph", now + 10_000, 5L));
        VectorIndexCompactor.Partition p = VectorIndexCompactor.partitionReapable(
                pending, now, Long.MAX_VALUE);
        assertTrue(p.reapable.isEmpty());
        assertEquals(1, p.retained.size());
    }

    @Test
    public void shadowGateBlocksReapDespitePastDeadline() {
        long now = 1_000_000L;
        // Deadline elapsed, but sinceGeneration (10) > minShadowAcked (3)
        // -> must not reap.
        List<PendingDelete> pending = Arrays.asList(
                new PendingDelete("s1:graph", now - 1, 10L));
        VectorIndexCompactor.Partition p = VectorIndexCompactor.partitionReapable(
                pending, now, /*minShadowAcked*/ 3L);
        assertTrue(p.reapable.isEmpty());
        assertEquals(1, p.retained.size());
    }

    @Test
    public void noShadowsAreEquivalentToMaxValueGate() {
        long now = 1_000_000L;
        List<PendingDelete> pending = Arrays.asList(
                new PendingDelete("s1:graph", now - 1, 42L));
        VectorIndexCompactor.Partition p = VectorIndexCompactor.partitionReapable(
                pending, now, Long.MAX_VALUE);
        assertEquals(1, p.reapable.size());
    }

    @Test
    public void emptyInputIsEmptyPartition() {
        VectorIndexCompactor.Partition p = VectorIndexCompactor.partitionReapable(
                null, 0L, Long.MAX_VALUE);
        assertTrue(p.reapable.isEmpty());
        assertTrue(p.retained.isEmpty());
    }
}
