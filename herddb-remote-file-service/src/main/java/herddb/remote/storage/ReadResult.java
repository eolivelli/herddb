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

package herddb.remote.storage;

/**
 * Result of a {@link ObjectStorage#read(String)} operation.
 *
 * @author enrico.olivelli
 */
public final class ReadResult {

    public enum Status {
        FOUND,
        NOT_FOUND
    }

    private static final ReadResult NOT_FOUND_INSTANCE = new ReadResult(Status.NOT_FOUND, null);

    private final Status status;
    private final byte[] content;

    private ReadResult(Status status, byte[] content) {
        this.status = status;
        this.content = content;
    }

    public static ReadResult found(byte[] content) {
        return new ReadResult(Status.FOUND, content);
    }

    public static ReadResult notFound() {
        return NOT_FOUND_INSTANCE;
    }

    public Status status() {
        return status;
    }

    public byte[] content() {
        return content;
    }
}
