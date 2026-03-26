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

package herddb.remote;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import herddb.remote.proto.ListFilesEntry;
import herddb.remote.proto.ListFilesRequest;
import herddb.remote.storage.LocalObjectStorage;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that RemoteFileServiceImpl.listFiles() calls onError() when streaming fails mid-response,
 * instead of silently swallowing the exception.
 */
public class RemoteFileServiceImplStreamingErrorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Verifies that when the StreamObserver.onNext() throws during listFiles streaming,
     * onError() is called on the observer so the client receives a proper error signal
     * instead of a silent stream close.
     */
    @Test
    public void testListFilesStreamingErrorPropagatesViaOnError() throws Exception {
        // Write a couple of files into a real LocalObjectStorage so listFiles returns results
        LocalObjectStorage storage = new LocalObjectStorage(
                folder.newFolder("data").toPath(),
                Executors.newSingleThreadExecutor());
        storage.write("ts/tbl/data/1.page", "page1".getBytes(StandardCharsets.UTF_8)).get();
        storage.write("ts/tbl/data/2.page", "page2".getBytes(StandardCharsets.UTF_8)).get();

        RemoteFileServiceImpl impl = new RemoteFileServiceImpl(storage);

        // StreamObserver that throws on the first onNext(), then captures any onError() call
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        AtomicReference<Boolean> completedCalled = new AtomicReference<>(false);

        StreamObserver<ListFilesEntry> failingObserver = new StreamObserver<ListFilesEntry>() {
            @Override
            public void onNext(ListFilesEntry entry) {
                throw new RuntimeException("simulated stream write failure");
            }

            @Override
            public void onError(Throwable t) {
                capturedError.set(t);
                done.countDown();
            }

            @Override
            public void onCompleted() {
                completedCalled.set(true);
                done.countDown();
            }
        };

        impl.listFiles(
                ListFilesRequest.newBuilder().setPrefix("ts/tbl/data/").build(),
                failingObserver);

        assertTrue("onError() or onCompleted() should be called within 5 seconds",
                done.await(5, TimeUnit.SECONDS));
        assertNotNull("onError() must be called when streaming fails, not silently swallowed",
                capturedError.get());
        assertTrue("onError() must deliver a StatusRuntimeException with INTERNAL status",
                capturedError.get() instanceof StatusRuntimeException);
        StatusRuntimeException sre = (StatusRuntimeException) capturedError.get();
        assertTrue("Status must be INTERNAL",
                sre.getStatus().getCode() == Status.Code.INTERNAL);
    }
}
