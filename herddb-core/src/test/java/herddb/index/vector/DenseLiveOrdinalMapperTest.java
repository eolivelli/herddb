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
import static org.junit.Assert.assertThrows;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.util.FixedBitSet;
import org.junit.Test;

/**
 * Unit tests for {@link VectorIndexCompactor.DenseLiveOrdinalMapper}, the
 * primitive-int dense remapper that drives streaming compaction (issue #485).
 *
 * <p>The mapper is consulted by {@link io.github.jbellis.jvector.graph.disk
 * .OnDiskGraphIndexCompactor} only on live-bitset-filtered ordinals, so the
 * tests cover that contract plus the broader well-formedness of
 * {@link OrdinalMapper}: dense {@code oldToNew} for live ordinals,
 * {@link OrdinalMapper#OMITTED} for dead/out-of-range ordinals, correct
 * {@code maxOrdinal()} across multi-source layouts, and defensive constructor
 * validation.
 */
public class DenseLiveOrdinalMapperTest {

    private static FixedBitSet bitsOf(int size, int... liveOrds) {
        FixedBitSet b = new FixedBitSet(Math.max(1, size));
        for (int o : liveOrds) {
            b.set(o);
        }
        return b;
    }

    @Test
    public void denseRemapAcrossThreeSourcesAtOffsetZeroIsContiguous() {
        // Source A: [0,1,2,3], live = {0,2}            -> new ords {0,1}
        // Source B: [0,1,2,3,4], live = {1,3,4}        -> new ords {2,3,4}
        // Source C: [0,1], live = {1}                  -> new ord  {5}
        VectorIndexCompactor.DenseLiveOrdinalMapper a =
                new VectorIndexCompactor.DenseLiveOrdinalMapper(
                        bitsOf(4, 0, 2), 4, 0);
        VectorIndexCompactor.DenseLiveOrdinalMapper b =
                new VectorIndexCompactor.DenseLiveOrdinalMapper(
                        bitsOf(5, 1, 3, 4), 5, a.liveCount());
        VectorIndexCompactor.DenseLiveOrdinalMapper c =
                new VectorIndexCompactor.DenseLiveOrdinalMapper(
                        bitsOf(2, 1), 2, a.liveCount() + b.liveCount());

        assertEquals(2, a.liveCount());
        assertEquals(3, b.liveCount());
        assertEquals(1, c.liveCount());

        // Per-source max ordinal (used by jvector's global max computation).
        assertEquals(1, a.maxOrdinal());
        assertEquals(4, b.maxOrdinal());
        assertEquals(5, c.maxOrdinal());

        // oldToNew is dense within each source.
        assertEquals(0, a.oldToNew(0));
        assertEquals(1, a.oldToNew(2));
        assertEquals(2, b.oldToNew(1));
        assertEquals(3, b.oldToNew(3));
        assertEquals(4, b.oldToNew(4));
        assertEquals(5, c.oldToNew(1));
    }

    @Test
    public void deadOrdinalReturnsOmitted() {
        VectorIndexCompactor.DenseLiveOrdinalMapper m =
                new VectorIndexCompactor.DenseLiveOrdinalMapper(
                        bitsOf(5, 0, 2, 4), 5, 100);
        // Dead ordinals.
        assertEquals(OrdinalMapper.OMITTED, m.oldToNew(1));
        assertEquals(OrdinalMapper.OMITTED, m.oldToNew(3));
        // Live ordinals get dense IDs starting at newBase.
        assertEquals(100, m.oldToNew(0));
        assertEquals(101, m.oldToNew(2));
        assertEquals(102, m.oldToNew(4));
        // maxOrdinal = newBase + liveCount - 1.
        assertEquals(102, m.maxOrdinal());
    }

    @Test
    public void oldToNewOutOfBoundsReturnsOmitted() {
        VectorIndexCompactor.DenseLiveOrdinalMapper m =
                new VectorIndexCompactor.DenseLiveOrdinalMapper(
                        bitsOf(3, 0, 2), 3, 0);
        assertEquals(OrdinalMapper.OMITTED, m.oldToNew(-1));
        assertEquals(OrdinalMapper.OMITTED, m.oldToNew(3));
        assertEquals(OrdinalMapper.OMITTED, m.oldToNew(Integer.MAX_VALUE));
    }

    @Test
    public void newToOldRoundTripsLiveOrdinals() {
        VectorIndexCompactor.DenseLiveOrdinalMapper m =
                new VectorIndexCompactor.DenseLiveOrdinalMapper(
                        bitsOf(6, 1, 3, 5), 6, 10);
        for (int oldOrd : new int[]{1, 3, 5}) {
            int newOrd = m.oldToNew(oldOrd);
            assertEquals(oldOrd, m.newToOld(newOrd));
        }
    }

    @Test
    public void newToOldOutOfBoundsReturnsOmitted() {
        VectorIndexCompactor.DenseLiveOrdinalMapper m =
                new VectorIndexCompactor.DenseLiveOrdinalMapper(
                        bitsOf(4, 0, 1, 2), 4, 7);
        // Live new ordinals are 7..9.
        assertEquals(OrdinalMapper.OMITTED, m.newToOld(6));
        assertEquals(OrdinalMapper.OMITTED, m.newToOld(10));
        assertEquals(OrdinalMapper.OMITTED, m.newToOld(-1));
    }

    @Test
    public void allAliveBehavesAsIdentityAfterOffset() {
        int srcSize = 8;
        FixedBitSet allAlive = new FixedBitSet(srcSize);
        for (int i = 0; i < srcSize; i++) {
            allAlive.set(i);
        }
        VectorIndexCompactor.DenseLiveOrdinalMapper m =
                new VectorIndexCompactor.DenseLiveOrdinalMapper(allAlive, srcSize, 50);
        for (int i = 0; i < srcSize; i++) {
            assertEquals(50 + i, m.oldToNew(i));
            assertEquals(i, m.newToOld(50 + i));
        }
        assertEquals(srcSize, m.liveCount());
        assertEquals(50 + srcSize - 1, m.maxOrdinal());
    }

    @Test
    public void allDeadHasZeroLiveCountAndMaxOrdinalReflectsBaseMinusOne() {
        VectorIndexCompactor.DenseLiveOrdinalMapper m =
                new VectorIndexCompactor.DenseLiveOrdinalMapper(
                        new FixedBitSet(8), 8, 4);
        assertEquals(0, m.liveCount());
        // newBase + 0 - 1 = newBase - 1: maxOrdinal "shrinks" so the global
        // max across all mappers is unaffected by an empty source.
        assertEquals(3, m.maxOrdinal());
        // Every old ordinal is dead.
        for (int i = 0; i < 8; i++) {
            assertEquals(OrdinalMapper.OMITTED, m.oldToNew(i));
        }
        // Every new ordinal in this source's range is OMITTED (no live nodes).
        assertEquals(OrdinalMapper.OMITTED, m.newToOld(4));
        assertEquals(OrdinalMapper.OMITTED, m.newToOld(3));
    }

    @Test
    public void constructorRejectsNullBitset() {
        assertThrows(IllegalArgumentException.class, () ->
                new VectorIndexCompactor.DenseLiveOrdinalMapper(null, 5, 0));
    }

    @Test
    public void constructorRejectsNegativeSourceSize() {
        assertThrows(IllegalArgumentException.class, () ->
                new VectorIndexCompactor.DenseLiveOrdinalMapper(
                        new FixedBitSet(1), -1, 0));
    }

    @Test
    public void constructorRejectsNegativeNewBase() {
        assertThrows(IllegalArgumentException.class, () ->
                new VectorIndexCompactor.DenseLiveOrdinalMapper(
                        new FixedBitSet(1), 1, -1));
    }
}
