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
 * {@link SizeEvaluator} for {@code BLink<Bytes, Bytes>} trees.
 * Used by vector index on-disk nodeToPk map (ordinal-as-Bytes key, PK-as-Bytes value).
 */
public final class BytesBytesSizeEvaluator implements SizeEvaluator<Bytes, Bytes> {

    public static final SizeEvaluator<Bytes, Bytes> INSTANCE = new BytesBytesSizeEvaluator();

    private BytesBytesSizeEvaluator() {
    }

    @Override
    public long evaluateKey(Bytes key) {
        return key.getEstimatedSize();
    }

    @Override
    public long evaluateValue(Bytes value) {
        return value.getEstimatedSize();
    }

    @Override
    public long evaluateAll(Bytes key, Bytes value) {
        return key.getEstimatedSize() + value.getEstimatedSize();
    }

    @Override
    public Bytes getPosiviveInfinityKey() {
        return Bytes.POSITIVE_INFINITY;
    }
}
