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
package herddb.index.brin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import herddb.core.RandomPageReplacementPolicy;
import herddb.utils.Sized;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for BlockRangeMap
 *
 * @author enrico.olivelli
 */
public class BlockRangeMapTest {

    @Test
    public void testSimpleSplit() {

        BlockRangeMap<Sized<Integer>, Sized<String>> index
            = new BlockRangeMap<>(400, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        index.put(Sized.valueOf(2), Sized.valueOf("b"));
        index.put(Sized.valueOf(3), Sized.valueOf("c"));
        dumpIndex(index);
        assertEquals(Sized.valueOf("a"), index.search(Sized.valueOf(1)).get(0));
        assertEquals(Sized.valueOf("b"), index.search(Sized.valueOf(2)).get(0));
        assertEquals(Sized.valueOf("c"), index.search(Sized.valueOf(3)).get(0));
        assertEquals(2, index.getNumBlocks());
    }

    @Test
    public void testRemoveHead() {
        BlockRangeMap<Sized<Integer>, Sized<String>> index
            = new BlockRangeMap<>(1024, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        assertEquals(Sized.valueOf("a"), index.remove(Sized.valueOf(1)));
        List<Sized<String>> searchResult = index.search(Sized.valueOf(1));
        assertTrue(searchResult.isEmpty());
    }

    @Test
    public void testUnboundedSearch() {
        BlockRangeMap<Sized<Integer>, Sized<String>> index
            = new BlockRangeMap<>(1024, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        index.put(Sized.valueOf(2), Sized.valueOf("b"));
        index.put(Sized.valueOf(3), Sized.valueOf("c"));
        assertEquals(3, index.lookUpRange(Sized.valueOf(1), null).size());
        assertEquals(2, index.lookUpRange(null, Sized.valueOf(2)).size());

    }

    @Test
    public void lookupVeryFirstEntry() {
        BlockRangeMap<Sized<Integer>, Sized<String>> index
            = new BlockRangeMap<>(1024, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        index.put(Sized.valueOf(2), Sized.valueOf("b"));
        index.put(Sized.valueOf(3), Sized.valueOf("c"));
        index.put(Sized.valueOf(4), Sized.valueOf("d"));
        index.put(Sized.valueOf(5), Sized.valueOf("e"));
        index.put(Sized.valueOf(6), Sized.valueOf("f"));
        dumpIndex(index);
        List<Sized<String>> searchResult = index.search(Sized.valueOf(1));
        System.out.println("searchResult:" + searchResult);
        assertEquals(1, searchResult.size());

        List<Sized<String>> searchResult2 = index.lookUpRange(Sized.valueOf(1), Sized.valueOf(4));
        System.out.println("searchResult:" + searchResult2);
        assertEquals(4, searchResult2.size());
        assertEquals(Sized.valueOf("a"), searchResult2.get(0));
        assertEquals(Sized.valueOf("b"), searchResult2.get(1));
        assertEquals(Sized.valueOf("c"), searchResult2.get(2));
        assertEquals(Sized.valueOf("d"), searchResult2.get(3));
    }

    @Test
    public void testSimpleSplitInverse() {
        BlockRangeMap<Sized<Integer>, Sized<String>> index
            = new BlockRangeMap<>(400, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(3), Sized.valueOf("c"));
        index.put(Sized.valueOf(2), Sized.valueOf("b"));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        dumpIndex(index);

        assertEquals(Sized.valueOf("a"), index.search(Sized.valueOf(1)).get(0));
        assertEquals(Sized.valueOf("b"), index.search(Sized.valueOf(2)).get(0));
        assertEquals(Sized.valueOf("c"), index.search(Sized.valueOf(3)).get(0));
        assertEquals(2, index.getNumBlocks());
    }

    @Test
    public void testDelete() {
        BlockRangeMap<Sized<Integer>, Sized<String>> index
            = new BlockRangeMap<>(1024, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(3), Sized.valueOf("c"));
        index.put(Sized.valueOf(2), Sized.valueOf("b"));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        assertEquals(Sized.valueOf("a"), index.remove(Sized.valueOf(1)));
        assertTrue(index.search(Sized.valueOf(1)).isEmpty());
        assertEquals(Sized.valueOf("b"), index.search(Sized.valueOf(2)).get(0));
        assertEquals(Sized.valueOf("c"), index.search(Sized.valueOf(3)).get(0));

        assertEquals(Sized.valueOf("b"), index.remove(Sized.valueOf(2)));
        assertTrue(index.search(Sized.valueOf(2)).isEmpty());
        assertEquals(Sized.valueOf("c"), index.search(Sized.valueOf(3)).get(0));
        assertEquals(Sized.valueOf("c"), index.remove(Sized.valueOf(3)));
        assertTrue(index.search(Sized.valueOf(3)).isEmpty());
    }

    @Test
    public void testManySegments() {
        BlockRangeMap<Sized<Integer>, Sized<String>> index
            = new BlockRangeMap<>(1024, new RandomPageReplacementPolicy(10));
        for (int i = 0; i < 20; i++) {
            index.put(Sized.valueOf(i), Sized.valueOf("test_" + i));
        }
        List<Sized<String>> result = index.lookUpRange(Sized.valueOf(2), Sized.valueOf(10));
        System.out.println("result_" + result);
        for (int i = 2; i <= 10; i++) {
            assertTrue(result.contains(Sized.valueOf("test_" + i)));
        }
        assertEquals(9, result.size());
    }

    @Test
    public void testBasicPutGetDelete() {
        BlockRangeMap<Sized<Integer>, Sized<String>> index
            = new BlockRangeMap<>(1024, new RandomPageReplacementPolicy(10));
        for (int i = 0; i < 20; i++) {
            index.put(Sized.valueOf(i), Sized.valueOf("test_" + i));
        }

        for (int i = 0; i < 20; i++) {
            assertEquals(Sized.valueOf("test_" + i), index.get(Sized.valueOf(i)));
        }

        for (int i = 0; i < 20; i++) {
            assertEquals(Sized.valueOf("test_" + i), index.remove(Sized.valueOf(i)));
        }

        for (int i = 0; i < 20; i++) {
            assertNull(index.get(Sized.valueOf(i)));
        }

    }

    private void dumpIndex(BlockRangeMap<?, ?> index) {
        for (BlockRangeMap.Block b : index.getBlocks().values()) {
            System.out.println("BLOCK " + b);
        }
    }

}
