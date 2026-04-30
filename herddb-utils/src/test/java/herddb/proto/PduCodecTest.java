/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.proto;

import static herddb.proto.PduCodec.ObjectListReader.isAllowFollowerReads;
import static herddb.proto.PduCodec.ObjectListReader.isDontKeepReadLocks;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.utils.RawString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Tests about PduCodec
 */
public class PduCodecTest {

    @Test
    public void readObjectsTrailer() throws Exception {
        long msgId = 1234;
        String ts = "dsfs";
        String query = "q";
        long scannerId = 2332;
        long tx = 353;
        List<Object> params = Arrays.asList("1", 12L, 3d);
        long statementId = 2342;
        int fetchSize = 12313;
        int maxRows = 1239;
        boolean keepReadLocks = false; // we need a TRAILER, since 0.20.0
        ByteBuf write = PduCodec.OpenScanner.write(msgId, ts, query, scannerId, tx, params, statementId, fetchSize, maxRows, keepReadLocks);
        try (Pdu pdu = PduCodec.decodePdu(write);) {
            assertEquals(msgId, pdu.messageId);
            assertEquals(Pdu.TYPE_OPENSCANNER, pdu.type);
            assertEquals(ts, PduCodec.OpenScanner.readTablespace(pdu));
            assertEquals(query, PduCodec.OpenScanner.readQuery(pdu));
            assertEquals(scannerId, PduCodec.OpenScanner.readScannerId(pdu));
            assertEquals(tx, PduCodec.OpenScanner.readTx(pdu));
            assertEquals(statementId, PduCodec.OpenScanner.readStatementId(pdu));
            assertEquals(fetchSize, PduCodec.OpenScanner.readFetchSize(pdu));
            assertEquals(maxRows, PduCodec.OpenScanner.readMaxRows(pdu));
            PduCodec.ObjectListReader paramsReader = PduCodec.OpenScanner.startReadParameters(pdu);
            assertEquals(params.size(), paramsReader.getNumParams());
            assertEquals(RawString.of((String) params.get(0)), paramsReader.nextObject());
            assertEquals(params.get(1), paramsReader.nextObject());
            assertEquals(params.get(2), paramsReader.nextObject());
            byte trailer = paramsReader.readTrailer();
            assertTrue(isDontKeepReadLocks(trailer));
        }

        keepReadLocks = true; // NO TRAILER, this is what 0.19.0 clients did
        write = PduCodec.OpenScanner.write(msgId, ts, query, scannerId, tx, params, statementId, fetchSize, maxRows, keepReadLocks);
        try (Pdu pdu = PduCodec.decodePdu(write);) {
            assertEquals(msgId, pdu.messageId);
            assertEquals(Pdu.TYPE_OPENSCANNER, pdu.type);
            assertEquals(ts, PduCodec.OpenScanner.readTablespace(pdu));
            assertEquals(query, PduCodec.OpenScanner.readQuery(pdu));
            assertEquals(scannerId, PduCodec.OpenScanner.readScannerId(pdu));
            assertEquals(tx, PduCodec.OpenScanner.readTx(pdu));
            assertEquals(statementId, PduCodec.OpenScanner.readStatementId(pdu));
            assertEquals(fetchSize, PduCodec.OpenScanner.readFetchSize(pdu));
            assertEquals(maxRows, PduCodec.OpenScanner.readMaxRows(pdu));
            PduCodec.ObjectListReader paramsReader = PduCodec.OpenScanner.startReadParameters(pdu);
            assertEquals(params.size(), paramsReader.getNumParams());
            assertEquals(RawString.of((String) params.get(0)), paramsReader.nextObject());
            assertEquals(params.get(1), paramsReader.nextObject());
            assertEquals(params.get(2), paramsReader.nextObject());
            byte trailer = paramsReader.readTrailer();
            assertEquals(0, trailer);
            assertFalse(isDontKeepReadLocks(trailer));
        }

    }

    @Test
    public void readObjectsTrailerWithFollowerReads() throws Exception {
        long msgId = 1234;
        String ts = "dsfs";
        String query = "q";
        long scannerId = 2332;
        long tx = 353;
        List<Object> params = Arrays.asList("1", 12L, 3d);
        long statementId = 2342;
        int fetchSize = 12313;
        int maxRows = 1239;

        // allowFollowerReads=true, keepReadLocks=false -> trailer should have both bits
        ByteBuf write = PduCodec.OpenScanner.write(msgId, ts, query, scannerId, tx, params, statementId, fetchSize, maxRows, false, true);
        try (Pdu pdu = PduCodec.decodePdu(write)) {
            PduCodec.ObjectListReader paramsReader = PduCodec.OpenScanner.startReadParameters(pdu);
            for (int i = 0; i < paramsReader.getNumParams(); i++) {
                paramsReader.nextObject();
            }
            byte trailer = paramsReader.readTrailer();
            assertTrue(isDontKeepReadLocks(trailer));
            assertTrue(isAllowFollowerReads(trailer));
        }

        // allowFollowerReads=true, keepReadLocks=true -> trailer should have only follower reads bit
        write = PduCodec.OpenScanner.write(msgId, ts, query, scannerId, tx, params, statementId, fetchSize, maxRows, true, true);
        try (Pdu pdu = PduCodec.decodePdu(write)) {
            PduCodec.ObjectListReader paramsReader = PduCodec.OpenScanner.startReadParameters(pdu);
            for (int i = 0; i < paramsReader.getNumParams(); i++) {
                paramsReader.nextObject();
            }
            byte trailer = paramsReader.readTrailer();
            assertFalse(isDontKeepReadLocks(trailer));
            assertTrue(isAllowFollowerReads(trailer));
        }

        // allowFollowerReads=false, keepReadLocks=false -> trailer should have only dontKeepReadLocks bit (backward compat)
        write = PduCodec.OpenScanner.write(msgId, ts, query, scannerId, tx, params, statementId, fetchSize, maxRows, false, false);
        try (Pdu pdu = PduCodec.decodePdu(write)) {
            PduCodec.ObjectListReader paramsReader = PduCodec.OpenScanner.startReadParameters(pdu);
            for (int i = 0; i < paramsReader.getNumParams(); i++) {
                paramsReader.nextObject();
            }
            byte trailer = paramsReader.readTrailer();
            assertTrue(isDontKeepReadLocks(trailer));
            assertFalse(isAllowFollowerReads(trailer));
        }

        // allowFollowerReads=false, keepReadLocks=true -> no trailer
        write = PduCodec.OpenScanner.write(msgId, ts, query, scannerId, tx, params, statementId, fetchSize, maxRows, true, false);
        try (Pdu pdu = PduCodec.decodePdu(write)) {
            PduCodec.ObjectListReader paramsReader = PduCodec.OpenScanner.startReadParameters(pdu);
            for (int i = 0; i < paramsReader.getNumParams(); i++) {
                paramsReader.nextObject();
            }
            byte trailer = paramsReader.readTrailer();
            assertEquals(0, trailer);
            assertFalse(isDontKeepReadLocks(trailer));
            assertFalse(isAllowFollowerReads(trailer));
        }
    }

    /**
     * Verifies that {@link PduCodec.ExecuteStatements#write} correctly round-trips
     * a batch that contains large {@code float[]} vector parameters (the primary
     * source of buffer reallocations reported in issue #378).  The test also
     * asserts that the pre-allocated buffer capacity was sufficient so that no
     * internal reallocation was needed.
     */
    @Test
    public void testExecuteStatementsWithFloatArrayParams() throws Exception {
        long msgId = 42L;
        String tableSpace = "myts";
        String query = "INSERT INTO vectors(v) VALUES(?)";
        long tx = 0L;
        boolean returnValues = false;
        long statementId = 99L;

        // Build a batch of two statements, each containing a 1536-element float vector.
        float[] vector1 = new float[1536];
        float[] vector2 = new float[1536];
        for (int i = 0; i < 1536; i++) {
            vector1[i] = i * 0.001f;
            vector2[i] = i * 0.002f;
        }
        List<List<Object>> statements = Arrays.asList(
                Arrays.asList((Object) vector1),
                Arrays.asList((Object) vector2)
        );

        ByteBuf buf = PduCodec.ExecuteStatements.write(msgId, tableSpace, query, tx, returnValues, statementId, statements);
        // Pdu.close() releases the underlying buffer; no explicit buf.release() needed.
        // Snapshot the capacity right after write(): if pre-sizing was correct Netty
        // will not have reallocated, so writableBytes() should be a small over-estimate.
        // A reallocation would at least double the capacity, leaving writableBytes >> writerIndex.
        int writtenBytes = buf.writerIndex();
        int allocatedCapacity = buf.capacity();
        assertTrue("Pre-sized capacity should not have been exceeded (no reallocation)",
                writtenBytes <= allocatedCapacity);
        assertTrue("Pre-sized capacity should be a tight estimate, not a doubling reallocation",
                allocatedCapacity < writtenBytes * 2);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(msgId, pdu.messageId);
            assertEquals(Pdu.TYPE_EXECUTE_STATEMENTS, pdu.type);
            assertEquals(tableSpace, PduCodec.ExecuteStatements.readTablespace(pdu));
            assertEquals(query, PduCodec.ExecuteStatements.readQuery(pdu));
            assertEquals(tx, PduCodec.ExecuteStatements.readTx(pdu));
            assertEquals(statementId, PduCodec.ExecuteStatements.readStatementId(pdu));

            PduCodec.ListOfListsReader reader = PduCodec.ExecuteStatements.startReadStatementsParameters(pdu);
            assertEquals(2, reader.getNumLists());

            PduCodec.ObjectListReader list1 = reader.nextList();
            assertEquals(1, list1.getNumParams());
            assertArrayEquals(vector1, (float[]) list1.nextObject(), 0.0f);

            PduCodec.ObjectListReader list2 = reader.nextList();
            assertEquals(1, list2.getNumParams());
            assertArrayEquals(vector2, (float[]) list2.nextObject(), 0.0f);
        }
    }

    /**
     * Verifies that {@link PduCodec.ExecuteStatement#write} correctly round-trips
     * a single statement containing a large {@code float[]} vector parameter and that
     * the pre-allocated buffer capacity was sufficient (no reallocation).
     */
    @Test
    public void testExecuteStatementWithFloatArrayParam() throws Exception {
        long msgId = 7L;
        String tableSpace = "default";
        String query = "INSERT INTO t(v) VALUES(?)";
        long tx = 0L;
        boolean returnValues = true;
        long statementId = 55L;

        float[] vector = new float[512];
        for (int i = 0; i < 512; i++) {
            vector[i] = i * 0.01f;
        }
        List<Object> params = Arrays.asList((Object) vector, "extra-string-param", 42L);

        ByteBuf buf = PduCodec.ExecuteStatement.write(msgId, tableSpace, query, tx, returnValues, statementId, params);
        // Pdu.close() releases the underlying buffer; no explicit buf.release() needed.
        int writtenBytes = buf.writerIndex();
        int allocatedCapacity = buf.capacity();
        assertTrue("Pre-sized capacity should not have been exceeded (no reallocation)",
                writtenBytes <= allocatedCapacity);
        assertTrue("Pre-sized capacity should be a tight estimate, not a doubling reallocation",
                allocatedCapacity < writtenBytes * 2);
        try (Pdu pdu = PduCodec.decodePdu(buf)) {
            assertEquals(msgId, pdu.messageId);
            assertEquals(Pdu.TYPE_EXECUTE_STATEMENT, pdu.type);
            assertEquals(tableSpace, PduCodec.ExecuteStatement.readTablespace(pdu));
            assertEquals(query, PduCodec.ExecuteStatement.readQuery(pdu));
            assertEquals(tx, PduCodec.ExecuteStatement.readTx(pdu));
            assertEquals(statementId, PduCodec.ExecuteStatement.readStatementId(pdu));
            assertTrue(PduCodec.ExecuteStatement.readReturnValues(pdu));

            PduCodec.ObjectListReader paramsReader = PduCodec.ExecuteStatement.startReadParameters(pdu);
            assertEquals(params.size(), paramsReader.getNumParams());
            assertArrayEquals(vector, (float[]) paramsReader.nextObject(), 0.0f);
            assertEquals(RawString.of("extra-string-param"), paramsReader.nextObject());
            assertEquals(42L, paramsReader.nextObject());
        }
    }

    @Test
    public void testNormalizeParametersListWriteReadObject() {
        long now = System.currentTimeMillis();
        List<Object> parameters = Arrays.asList(null, "a", RawString.of("b"), 1, 1L, 1f, 1d, (short) 1, (byte) 1, true, new java.sql.Timestamp(now), new java.util.Date(now), new java.sql.Date(now),
                new byte[20]);
        List<Object> expResult = Arrays.asList(null,
                RawString.of("a"), // String - RawString
                RawString.of("b"),
                1, 1L, 1d,
                1d, // float -> double
                (short) 1,
                (byte) 1, true,
                new java.sql.Timestamp(now),
                new java.sql.Timestamp(now), // java.util.Date -> java.sql.Timestamp
                new java.sql.Timestamp(now), // java.sql.Date -> java.sql.Timestamp
                new byte[20]);
        List<Object> actualResult = PduCodec.normalizeParametersList(parameters);
        for (int i = 0; i < parameters.size(); i++) {
            Object value = parameters.get(i);
            Object expected = expResult.get(i);
            Object result = actualResult.get(i);
            ByteBuf mashalled = Unpooled.buffer();
            PduCodec.writeObject(mashalled, value);
            Object unmashalled = PduCodec.readObject(mashalled);

            if (expected != null && expected.getClass() == byte[].class) {
                assertArrayEquals((byte[]) expected, (byte[]) result);
                assertArrayEquals((byte[]) expected, (byte[]) unmashalled);
            } else {
                assertEquals(expected, result);
                assertEquals(expected, unmashalled);
            }
        }

    }
}
