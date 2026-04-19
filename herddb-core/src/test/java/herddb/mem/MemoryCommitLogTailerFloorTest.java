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

package herddb.mem;

import herddb.log.CommitLog;
import herddb.log.LogSequenceNumber;
import org.junit.Test;

/**
 * MemoryCommitLog has no persistent state to drop, so {@code dropOldLedgers}
 * is a no-op. These tests simply verify the two-argument signature is
 * callable and that both {@code START_OF_TIME} and a real floor are
 * accepted without throwing.
 */
public class MemoryCommitLogTailerFloorTest {

    @Test
    public void dropOldLedgersWithStartOfTimeFloorIsANoOp() throws Exception {
        try (MemoryCommitLogManager manager = new MemoryCommitLogManager()) {
            manager.start();
            try (CommitLog log = manager.createCommitLog("ts", "ts", "n1")) {
                log.startWriting(1);
                log.dropOldLedgers(new LogSequenceNumber(1, 0), LogSequenceNumber.START_OF_TIME);
            }
        }
    }

    @Test
    public void dropOldLedgersWithTailersFloorIsANoOp() throws Exception {
        try (MemoryCommitLogManager manager = new MemoryCommitLogManager()) {
            manager.start();
            try (CommitLog log = manager.createCommitLog("ts", "ts", "n1")) {
                log.startWriting(1);
                log.dropOldLedgers(new LogSequenceNumber(10, 5), new LogSequenceNumber(3, 7));
            }
        }
    }
}
