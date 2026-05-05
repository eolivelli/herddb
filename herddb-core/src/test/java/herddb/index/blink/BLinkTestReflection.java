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

package herddb.index.blink;

import herddb.utils.Bytes;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Test-only reflection helpers shared by the BLink off-heap unit tests.
 * Kept out of the production tree so SpotBugs/checkstyle don't have to
 * tolerate reflective access in shipped code.
 */
final class BLinkTestReflection {

    private BLinkTestReflection() {
    }

    /**
     * Iterates every loaded BLink node and returns {@code true} as soon as
     * any TreeMap key satisfies {@link Bytes#isOffHeap()}. Used to assert
     * issue #399 step-4 invariant: at least one node loaded from disk
     * should have its keys packed into an off-heap slab.
     *
     * @param indexInstance the {@code BLinkKeyToPageIndex} or
     *                      {@code IncrementalBLinkKeyToPageIndex} whose
     *                      `tree` field is a {@link BLink}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static boolean anyKeyOffHeap(Object indexInstance) throws Exception {
        Field treeField = indexInstance.getClass().getDeclaredField("tree");
        treeField.setAccessible(true);
        BLink<Bytes, Long> blink = (BLink<Bytes, Long>) treeField.get(indexInstance);
        Field nodes = BLink.class.getDeclaredField("nodes");
        nodes.setAccessible(true);
        ConcurrentMap<Long, ?> nodeMap = (ConcurrentMap<Long, ?>) nodes.get(blink);
        for (Object node : nodeMap.values()) {
            Field mapField = node.getClass().getDeclaredField("map");
            mapField.setAccessible(true);
            Object mapObj = mapField.get(node);
            if (!(mapObj instanceof Map)) {
                continue;
            }
            Map<?, ?> map = (Map<?, ?>) mapObj;
            for (Object k : map.keySet()) {
                if (k instanceof Bytes && ((Bytes) k).isOffHeap()) {
                    return true;
                }
            }
        }
        return false;
    }
}
