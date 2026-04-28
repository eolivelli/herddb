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

package herddb.indexing;

import herddb.log.LogSequenceNumber;
import java.util.Objects;

/**
 * Immutable bundle of the durable indexing-service recovery state captured at
 * a successful checkpoint: the last applied {@link LogSequenceNumber} and the
 * engine's effective {@code numInstances} at that point. Persisted by every
 * {@link WatermarkStore} implementation.
 *
 * @author enrico.olivelli
 */
public final class WatermarkSnapshot {

    /**
     * Sentinel for "no recovery state yet" — used when the watermark file
     * does not exist on disk. {@link #numInstances} is 0, telling the engine
     * to fall back to its JVM-property bootstrap value.
     */
    public static final WatermarkSnapshot START_OF_TIME =
            new WatermarkSnapshot(LogSequenceNumber.START_OF_TIME, 0);

    public final LogSequenceNumber lsn;

    /**
     * Effective {@code numInstances} the engine was using at the time of the
     * checkpoint. Zero means "unknown" — typically because the watermark
     * file was written by a pre-feature build that did not persist this
     * value. The engine treats zero as "fall back to the bootstrap value".
     */
    public final int numInstances;

    public WatermarkSnapshot(LogSequenceNumber lsn, int numInstances) {
        this.lsn = Objects.requireNonNull(lsn, "lsn");
        if (numInstances < 0) {
            throw new IllegalArgumentException("numInstances must be >= 0, got " + numInstances);
        }
        this.numInstances = numInstances;
    }

    @Override
    public String toString() {
        return "WatermarkSnapshot{lsn=" + lsn + ", numInstances=" + numInstances + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WatermarkSnapshot)) {
            return false;
        }
        WatermarkSnapshot that = (WatermarkSnapshot) o;
        return numInstances == that.numInstances && lsn.equals(that.lsn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lsn, numInstances);
    }
}
