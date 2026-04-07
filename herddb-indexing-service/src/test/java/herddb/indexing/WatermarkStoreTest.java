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

import static org.junit.Assert.assertEquals;
import herddb.log.LogSequenceNumber;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WatermarkStoreTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testLoadReturnsStartOfTimeWhenNoFile() throws IOException {
        LocalWatermarkStore store = new LocalWatermarkStore(folder.newFolder("empty").toPath());
        LogSequenceNumber lsn = store.load();
        assertEquals(LogSequenceNumber.START_OF_TIME, lsn);
    }

    @Test
    public void testSaveAndLoad() throws IOException {
        java.nio.file.Path dir = folder.newFolder("data").toPath();
        LocalWatermarkStore store = new LocalWatermarkStore(dir);

        LogSequenceNumber saved = new LogSequenceNumber(5, 42);
        store.save(saved);

        LogSequenceNumber loaded = store.load();
        assertEquals(saved.ledgerId, loaded.ledgerId);
        assertEquals(saved.offset, loaded.offset);
    }

    @Test
    public void testOverwrite() throws IOException {
        java.nio.file.Path dir = folder.newFolder("overwrite").toPath();
        LocalWatermarkStore store = new LocalWatermarkStore(dir);

        store.save(new LogSequenceNumber(1, 10));
        store.save(new LogSequenceNumber(2, 20));

        LogSequenceNumber loaded = store.load();
        assertEquals(2, loaded.ledgerId);
        assertEquals(20, loaded.offset);
    }
}
