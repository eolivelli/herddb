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

import herddb.index.blink.BLink.SizeEvaluator;
import herddb.utils.Bytes;

/**
 * Shared {@link SizeEvaluator} for {@code BLink<Bytes, Long>} trees.
 * Used by both {@link BLinkKeyToPageIndex} and vector index on-disk maps.
 */
public final class BytesLongSizeEvaluator implements SizeEvaluator<Bytes, Long> {

    /**
     * java.lang.Long object size: 12 (header) + 4 (padding) + 8 (value) = 24 bytes.
     */
    private static final long LONG_SIZE = 24L;

    public static final SizeEvaluator<Bytes, Long> INSTANCE = new BytesLongSizeEvaluator();

    private BytesLongSizeEvaluator() {
    }

    @Override
    public long evaluateKey(Bytes key) {
        return key.getEstimatedSize();
    }

    @Override
    public boolean isValueSizeConstant() {
        return true;
    }

    @Override
    public long constantValueSize() {
        return LONG_SIZE;
    }

    @Override
    public long evaluateValue(Long value) {
        return LONG_SIZE;
    }

    @Override
    public long evaluateAll(Bytes key, Long value) {
        return key.getEstimatedSize() + LONG_SIZE;
    }

    @Override
    public Bytes getPosiviveInfinityKey() {
        return Bytes.POSITIVE_INFINITY;
    }
}
