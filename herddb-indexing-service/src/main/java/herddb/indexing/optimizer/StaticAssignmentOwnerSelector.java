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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cycles through an operator-supplied list of instance ordinals (parsed from
 * {@code indexoptimizer.owner.selector.static.assignment}, e.g. {@code "0,1,0,1"}).
 * Used by tests to force a deterministic owner sequence without depending on
 * live IS discovery.
 */
public final class StaticAssignmentOwnerSelector implements OwnerSelector {

    private final int[] assignment;
    private final AtomicInteger cursor = new AtomicInteger();

    public StaticAssignmentOwnerSelector(int[] assignment) {
        if (assignment == null || assignment.length == 0) {
            throw new IllegalArgumentException("assignment must not be empty");
        }
        for (int ordinal : assignment) {
            if (ordinal < 0) {
                throw new IllegalArgumentException("assignment contains negative ordinal: " + ordinal);
            }
        }
        this.assignment = assignment.clone();
    }

    @Override
    public int selectOwner(String tablespaceUuid, String indexUuid) {
        int idx = Math.floorMod(cursor.getAndIncrement(), assignment.length);
        return assignment[idx];
    }
}
