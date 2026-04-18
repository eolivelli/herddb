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

package herddb.model.commands;

import herddb.model.Statement;

/**
 * Blocks until every index in a tablespace has had its external tailers
 * (e.g. remote indexing services) process the commit log up to the LSN
 * observed when this statement starts executing, or the timeout expires.
 * <p>
 * Since checkpoints no longer synchronize clients with external tailers,
 * clients that need a tailer-consistent view (for example, before running
 * ANN queries) must call this explicitly:
 * {@code EXECUTE WAITFORINDEXES 'tablespace', timeoutSeconds}. The timeout
 * is mandatory and must be strictly positive.
 */
public class WaitForIndexesStatement extends Statement {

    private final long timeoutMs;

    public WaitForIndexesStatement(String tableSpace, long timeoutMs) {
        super(tableSpace);
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "WAITFORINDEXES timeoutMs must be strictly positive, got " + timeoutMs);
        }
        this.timeoutMs = timeoutMs;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }
}
