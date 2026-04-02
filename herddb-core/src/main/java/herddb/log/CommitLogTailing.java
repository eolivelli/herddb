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
package herddb.log;

/**
 * Abstraction for tailing a commit log. Implementations continuously
 * read log entries and dispatch them to a consumer.
 *
 * @author enrico.olivelli
 */
public interface CommitLogTailing extends Runnable, AutoCloseable {

    /**
     * Functional interface for consuming log entries.
     */
    @FunctionalInterface
    interface EntryConsumer {
        void accept(LogSequenceNumber lsn, LogEntry entry);
    }

    /**
     * Returns the current watermark (last processed LSN).
     */
    LogSequenceNumber getWatermark();

    /**
     * Returns the total number of entries processed since this tailer was created.
     */
    long getEntriesProcessed();

    /**
     * Returns whether the tailer is currently running.
     */
    boolean isRunning();

    @Override
    void close();
}
