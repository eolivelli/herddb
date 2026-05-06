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

package herddb.proto;

import herddb.utils.ByteBufUtils;
import herddb.utils.DataAccessor;
import herddb.utils.IntHolder;
import herddb.utils.KeyValue;
import herddb.utils.RawString;
import herddb.utils.RecordsBatch;
import herddb.utils.SystemProperties;
import herddb.utils.TuplesList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Codec for PDUs
 *
 * @author enrico.olivelli
 */
public abstract class PduCodec {

    private static final boolean SEND_FULL_STACKTRACES = SystemProperties.getBooleanSystemProperty("herddb.network.sendstacktraces", true);

    public static final byte VERSION_3 = 3;

    public static Pdu decodePdu(ByteBuf in) throws IOException {
        byte version = in.getByte(0);
        if (version == VERSION_3) {
            byte flags = in.getByte(1);
            byte type = in.getByte(2);
            long messageId = in.getLong(3);
            return Pdu.newPdu(in, type, flags, messageId);
        }
        throw new IOException("Cannot decode version " + version);
    }

    private static final int ONE_BYTE = 1;
    private static final int ONE_INT = 4;
    private static final int ONE_LONG = 8;
    private static final int MSGID_SIZE = 8;
    private static final int TYPE_SIZE = 1;
    private static final int FLAGS_SIZE = 1;
    private static final int VERSION_SIZE = 1;
    /**
     * Estimated vint size for counts (number of statements, number of params per
     * statement).  Two bytes cover values up to 16 383 — more than any realistic
     * batch size.
     */
    private static final int VINT_COUNT_SIZE = 2;
    /**
     * Estimated vint size for data-payload lengths (string byte-length, array
     * element count).  Four bytes cover values up to 268 435 455, which is far
     * larger than any realistic parameter value.
     */
    private static final int VINT_LENGTH_SIZE = 4;

    private static final int NULLABLE_FIELD_PRESENT = 1;
    private static final int NULLABLE_FIELD_ABSENT = 0;

    public static final byte TYPE_STRING = 0;
    public static final byte TYPE_LONG = 1;
    public static final byte TYPE_INTEGER = 2;
    public static final byte TYPE_BYTEARRAY = 3;
    public static final byte TYPE_TIMESTAMP = 4;
    public static final byte TYPE_NULL = 5;
    public static final byte TYPE_DOUBLE = 6;
    public static final byte TYPE_BOOLEAN = 7;
    public static final byte TYPE_SHORT = 8;
    public static final byte TYPE_BYTE = 9;

    public static final byte TYPE_FLOATARRAY = 10;

    public abstract static class ExecuteStatementsResult {

        public static ByteBuf write(long replyId, List<Long> updateCounts, List<Map<String, Object>> otherdata, long tx) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_LONG
                                    + ONE_LONG);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_EXECUTE_STATEMENTS_RESULT);
            byteBuf.writeLong(replyId);
            byteBuf.writeLong(tx);
            byteBuf.writeInt(updateCounts.size());
            for (Long updateCount : updateCounts) {
                byteBuf.writeLong(updateCount);
            }
            byteBuf.writeInt(otherdata.size());
            for (Map<String, Object> record : otherdata) {
                // the Map is serialized as a list of objects (k1,v1,k2,v2...)
                int size = record != null ? record.size() : 0;
                ByteBufUtils.writeVInt(byteBuf, size * 2);
                if (record != null) {
                    for (Map.Entry<String, Object> entry : record.entrySet()) {
                        writeObject(byteBuf, entry.getKey());
                        writeObject(byteBuf, entry.getValue());
                    }
                }
            }

            return byteBuf;
        }

        public static long readTx(Pdu pdu) {
            return pdu.buffer.getLong(
                    VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE);
        }

        public static List<Long> readUpdateCounts(Pdu pdu) {
            pdu.buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG);
            int numStatements = pdu.buffer.readInt();
            List<Long> res = new ArrayList<>(numStatements);
            for (int i = 0; i < numStatements; i++) {
                res.add(pdu.buffer.readLong());
            }
            return res;
        }

        public static ListOfListsReader startResultRecords(Pdu pdu) {
            final ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG);
            int numStatements = buffer.readInt();
            for (int i = 0; i < numStatements; i++) {
                buffer.skipBytes(ONE_LONG);
            }
            int numLists = ByteBufUtils.readVInt(buffer);
            return new ListOfListsReader(pdu, numLists);
        }
    }

    public abstract static class ExecuteStatementResult {

        public static ByteBuf write(
                long messageId, long updateCount, long tx, Map<String, Object> record
        ) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_LONG
                                    + ONE_LONG);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_EXECUTE_STATEMENT_RESULT);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(updateCount);
            byteBuf.writeLong(tx);

            // the Map is serialized as a list of objects (k1,v1,k2,v2...)
            int size = record != null ? record.size() : 0;
            ByteBufUtils.writeVInt(byteBuf, size * 2);
            if (record != null) {
                for (Map.Entry<String, Object> entry : record.entrySet()) {
                    writeObject(byteBuf, entry.getKey());
                    writeObject(byteBuf, entry.getValue());
                }
            }
            return byteBuf;
        }

        public static boolean hasRecord(Pdu pdu) {
            return pdu.buffer.writerIndex() > VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG;
        }

        public static ObjectListReader readRecord(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
            );
            int numParams = ByteBufUtils.readVInt(buffer);
            return new ObjectListReader(pdu, numParams);
        }

        public static long readUpdateCount(Pdu pdu) {
            return pdu.buffer.getLong(
                    VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE);
        }

        public static long readTx(Pdu pdu) {
            return pdu.buffer.getLong(
                    VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + ONE_LONG /* update count */);
        }

    }

    public abstract static class PrepareStatementResult {

        public static ByteBuf write(
                long messageId, long statementId
        ) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_LONG);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_PREPARE_STATEMENT_RESULT);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(statementId);
            return byteBuf;
        }

        public static long readStatementId(Pdu pdu) {
            return pdu.buffer.getLong(
                    VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE);
        }

    }

    public abstract static class SaslTokenMessageRequest {

        public static ByteBuf write(long messageId, String saslMech, byte[] firstToken) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + 64);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_SASL_TOKEN_MESSAGE_REQUEST);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, saslMech);
            ByteBufUtils.writeArray(byteBuf, firstToken);
            return byteBuf;
        }

        public static String readMech(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            return new String(ByteBufUtils.readArray(buffer), StandardCharsets.UTF_8);
        }

        public static byte[] readToken(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            ByteBufUtils.skipArray(buffer);
            return ByteBufUtils.readArray(buffer);
        }
    }

    public abstract static class SaslTokenMessageToken {

        public static ByteBuf write(long messageId, byte[] token) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + 1 + (token != null ? token.length : 0));
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_SASL_TOKEN_MESSAGE_TOKEN);
            byteBuf.writeLong(messageId);
            if (token == null) {
                byteBuf.writeByte(NULLABLE_FIELD_ABSENT);
            } else {
                byteBuf.writeByte(NULLABLE_FIELD_PRESENT);
                ByteBufUtils.writeArray(byteBuf, token);
            }
            return byteBuf;
        }

        public static byte[] readToken(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            byte tokenPresent = buffer.readByte();
            if (tokenPresent == NULLABLE_FIELD_PRESENT) {
                return ByteBufUtils.readArray(buffer);
            } else {
                return null;
            }
        }
    }

    public static class SaslTokenServerResponse {

        public static ByteBuf write(long messageId, byte[] token) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + 64);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_SASL_TOKEN_SERVER_RESPONSE);
            byteBuf.writeLong(messageId);
            if (token != null) {
                ByteBufUtils.writeArray(byteBuf, token);
            }
            return byteBuf;
        }

        public static byte[] readToken(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            if (buffer.writerIndex() > VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE) {
                buffer.readerIndex(0);
                buffer.skipBytes(VERSION_SIZE
                        + FLAGS_SIZE
                        + TYPE_SIZE
                        + MSGID_SIZE);
                return ByteBufUtils.readArray(buffer);
            } else {
                return null;
            }
        }

    }

    public static class AckResponse {

        public static ByteBuf write(long messageId) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_ACK);
            byteBuf.writeLong(messageId);
            return byteBuf;
        }
    }

    public static class ErrorResponse {

        public static final byte FLAG_NONE = 0;
        public static final byte FLAG_NOT_LEADER = 1;
        public static final byte FLAG_MISSING_PREPARED_STATEMENT = 2;
        public static final byte FLAG_DUPLICATEPRIMARY_KEY_ERROR = 4;

        public static ByteBuf write(long messageId, String error) {
            return write(messageId, error, false, false, false);
        }

        public static ByteBuf writeNotLeaderError(long messageId, String message) {
            return write(messageId, message, true, false, false);
        }

        public static ByteBuf writeMissingPreparedStatementError(long messageId, String message) {
            return write(messageId, message, false, true, false);
        }

        public static ByteBuf writeNotLeaderError(long messageId, Throwable message) {
            return write(messageId, message.toString(), true, false, false);
        }

        public static ByteBuf writeSqlIntegrityConstraintsViolation(long messageId, Throwable message) {
            return write(messageId, message.toString(), false, false, true);
        }

        private static ByteBuf write(long messageId, String error, boolean notLeader, boolean missingPreparedStatement, boolean sqlIntegrityConstraintViolation) {
            if (error == null) {
                error = "";
            }
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_BYTE
                                    + error.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_ERROR);
            byteBuf.writeLong(messageId);
            byte flags = FLAG_NONE;
            if (notLeader) {
                flags = (byte) (flags | FLAG_NOT_LEADER);
            }
            if (missingPreparedStatement) {
                flags = (byte) (flags | FLAG_MISSING_PREPARED_STATEMENT);
            }
            if (sqlIntegrityConstraintViolation) {
                flags = (byte) (flags | FLAG_DUPLICATEPRIMARY_KEY_ERROR);
            }
            byteBuf.writeByte(flags);
            ByteBufUtils.writeString(byteBuf, error);
            return byteBuf;
        }

        public static ByteBuf write(long messageId, Throwable error, boolean notLeader, boolean missingPreparedStatementError) {
            String errorMessageForClient;
            if (SEND_FULL_STACKTRACES) {
                StringWriter writer = new StringWriter();
                error.printStackTrace(new PrintWriter(writer));
                errorMessageForClient = writer.toString();
            } else {
                // no stacktrace
                errorMessageForClient = error + "";
            }
            return write(messageId, errorMessageForClient, notLeader, missingPreparedStatementError, false);
        }

        public static ByteBuf write(long messageId, Throwable error) {
            return write(messageId, error, false, false);
        }

        public static String readError(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE);
            return ByteBufUtils.readString(buffer);
        }

        public static boolean readIsNotLeader(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            byte read = buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            return (read & FLAG_NOT_LEADER) == FLAG_NOT_LEADER;
        }

        public static boolean readIsMissingPreparedStatementError(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            byte read = buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            return (read & FLAG_MISSING_PREPARED_STATEMENT) == FLAG_MISSING_PREPARED_STATEMENT;
        }

        public static boolean readIsSqlIntegrityViolationError(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            byte read = buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            return (read & FLAG_DUPLICATEPRIMARY_KEY_ERROR) == FLAG_DUPLICATEPRIMARY_KEY_ERROR;
        }
    }

    public abstract static class TxCommand {

        public static final byte TX_COMMAND_ROLLBACK_TRANSACTION = 1;
        public static final byte TX_COMMAND_COMMIT_TRANSACTION = 2;
        public static final byte TX_COMMAND_BEGIN_TRANSACTION = 3;

        public static ByteBuf write(long messageId, byte command, long tx, String tableSpace) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_BYTE
                                    + ONE_LONG
                                    + tableSpace.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_TX_COMMAND);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(command);
            byteBuf.writeLong(tx);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            return byteBuf;
        }

        public static byte readCommand(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);

        }

        public static long readTx(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE);

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;

            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG);
            return ByteBufUtils.readString(buffer);

        }
    }

    public abstract static class TxCommandResult {

        public static ByteBuf write(long messageId, long tx) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_TX_COMMAND_RESULT);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(tx);
            return byteBuf;
        }

        public static long readTx(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);

        }
    }

    public static class OpenScanner {

        public static ByteBuf write(
                long messageId, String tableSpace, String query,
                long scannerId, long tx, List<Object> params, long statementId, int fetchSize, int maxRows,
                boolean keepReadLocks
        ) {
            return write(messageId, tableSpace, query, scannerId, tx, params, statementId, fetchSize, maxRows, keepReadLocks, false);
        }

        public static ByteBuf write(
                long messageId, String tableSpace, String query,
                long scannerId, long tx, List<Object> params, long statementId, int fetchSize, int maxRows,
                boolean keepReadLocks, boolean allowFollowerReads
        ) {

            // Pre-compute an accurate upper-bound capacity to avoid internal
            // ByteBuf reallocation and copy, especially for large float[] vector params.
            int paramsPayload = VINT_COUNT_SIZE; // vint(numParams)
            for (Object p : params) {
                paramsPayload += estimateObjectSize(p);
            }
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_LONG  // tx
                                    + ONE_LONG  // statementId
                                    + ONE_INT   // fetchSize
                                    + ONE_INT   // maxRows
                                    + ONE_LONG  // scannerId
                                    + estimateStringSize(tableSpace)
                                    + estimateStringSize(query)
                                    + paramsPayload
                                    + ONE_BYTE); // optional trailer byte

            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_OPENSCANNER);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(tx);
            byteBuf.writeLong(statementId);
            byteBuf.writeInt(fetchSize);
            byteBuf.writeInt(maxRows);
            byteBuf.writeLong(scannerId);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, query);

            ByteBufUtils.writeVInt(byteBuf, params.size());
            for (Object p : params) {
                writeObject(byteBuf, p);
            }
            // trailer
            byte trailer = 0;
            if (!keepReadLocks) {
                trailer |= Pdu.FLAGS_OPENSCANNER_DONTKEEP_READ_LOCKS;
            }
            if (allowFollowerReads) {
                trailer |= Pdu.FLAGS_OPENSCANNER_ALLOW_FOLLOWER_READS;
            }
            if (trailer != 0) {
                byteBuf.writeByte(trailer);
            }
            return byteBuf;

        }

        public static long readTx(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }

        public static long readStatementId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG);
        }

        public static int readFetchSize(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getInt(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG);
        }

        public static int readMaxRows(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getInt(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_INT);
        }

        public static long readScannerId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_INT
                    + ONE_INT
            );
        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_INT
                    + ONE_INT
                    + ONE_LONG);
            return ByteBufUtils.readString(buffer);
        }

        public static String readQuery(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_INT
                    + ONE_INT
                    + ONE_LONG);
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

        public static ObjectListReader startReadParameters(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_INT
                    + ONE_INT
                    + ONE_LONG
            );

            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // query
            int numParams = ByteBufUtils.readVInt(buffer);
            return new ObjectListReader(pdu, numParams);
        }
    }

    public static class ResultSetChunk {

        private static int estimateTupleListSize(TuplesList data) {
            return data.tuples.size() * 1024 + data.columnNames.length * 64;
        }

        public static ByteBuf write(long messageId, TuplesList tuplesList, boolean last, long tx) {
            int dataSize = estimateTupleListSize(tuplesList);
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_LONG
                                    + ONE_BYTE
                                    + dataSize);

            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_RESULTSET_CHUNK);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(tx);
            byteBuf.writeByte(last ? 1 : 0);

            int numColumns = tuplesList.columnNames.length;
            byteBuf.writeInt(numColumns);
            for (String columnName : tuplesList.columnNames) {
                ByteBufUtils.writeString(byteBuf, columnName);
            }

            // num records
            byteBuf.writeInt(tuplesList.tuples.size());
            for (DataAccessor da : tuplesList.tuples) {
                IntHolder currentColumn = new IntHolder();
                da.forEach((String key, Object value) -> {
                    String expectedColumnName = tuplesList.columnNames[currentColumn.value];
                    while (!key.equals(expectedColumnName)) {
                        // nulls are not returned for some special accessors, like DataAccessorForFullRecord
                        writeObject(byteBuf, null);
                        currentColumn.value++;
                        expectedColumnName = tuplesList.columnNames[currentColumn.value];
                    }
                    writeObject(byteBuf, value);
                    currentColumn.value++;
                });
                // fill with nulls
                while (currentColumn.value < numColumns) {
                    writeObject(byteBuf, null);
                    currentColumn.value++;
                }
                if (currentColumn.value > numColumns) {
                    throw new RuntimeException("unexpected number of columns " + currentColumn.value + " > " + numColumns);
                }
            }
            return byteBuf;
        }

        public static long readTx(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }

        public static boolean readIsLast(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
            ) == 1;
        }

        public static RecordsBatch startReadingData(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_BYTE);
            return new RecordsBatch(pdu);
        }
    }

    public static class FetchScannerData {

        public static ByteBuf write(long messageId, long scannerId, int fetchSize) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_LONG
                                    + ONE_INT);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FETCHSCANNERDATA);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(scannerId);
            byteBuf.writeInt(fetchSize);
            return byteBuf;
        }

        public static long readScannerId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }

        public static int readFetchSize(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getInt(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG);
        }
    }

    public static class CloseScanner {

        public static ByteBuf write(long messageId, long scannerId) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_LONG);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_CLOSESCANNER);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(scannerId);
            return byteBuf;
        }

        public static long readScannerId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }
    }

    public static class ExecuteStatements {

        public static ByteBuf write(
                long messageId, String tableSpace, String query,
                long tx, boolean returnValues, long statementId, List<List<Object>> statements
        ) {

            // Pre-compute an accurate upper-bound capacity to avoid internal
            // ByteBuf reallocation and copy, especially for large float[] vector params.
            int statementsPayload = VINT_COUNT_SIZE; // vint(numStatements)
            for (List<Object> list : statements) {
                statementsPayload += VINT_COUNT_SIZE; // vint(numParams per statement)
                for (Object param : list) {
                    statementsPayload += estimateObjectSize(param);
                }
            }
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_BYTE  // returnValues
                                    + ONE_LONG  // tx
                                    + ONE_LONG  // statementId
                                    + estimateStringSize(tableSpace)
                                    + estimateStringSize(query)
                                    + statementsPayload);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_EXECUTE_STATEMENTS);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(returnValues ? 1 : 0);
            byteBuf.writeLong(tx);
            byteBuf.writeLong(statementId);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, query);

            // number of statements
            ByteBufUtils.writeVInt(byteBuf, statements.size());
            for (List<Object> list : statements) {

                // number of params
                ByteBufUtils.writeVInt(byteBuf, list.size());
                for (Object param : list) {
                    writeObject(byteBuf, param);
                }
            }

            return byteBuf;

        }

        public static boolean readReturnValues(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE) == 1;
        }

        public static long readTx(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE);
        }

        public static long readStatementId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG);
        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG
                    + ONE_LONG
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readQuery(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

        public static ListOfListsReader startReadStatementsParameters(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // query
            int numLists = ByteBufUtils.readVInt(buffer);
            return new ListOfListsReader(pdu, numLists);
        }

    }

    public static class ExecuteStatement {

        public static ByteBuf write(
                long messageId, String tableSpace, String query, long tx,
                boolean returnValues, long statementId,
                List<Object> params
        ) {

            // Pre-compute an accurate upper-bound capacity to avoid internal
            // ByteBuf reallocation and copy, especially for large float[] vector params.
            int paramsPayload = VINT_COUNT_SIZE; // vint(numParams)
            for (Object p : params) {
                paramsPayload += estimateObjectSize(p);
            }
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_BYTE  // returnValues
                                    + ONE_LONG  // tx
                                    + ONE_LONG  // statementId
                                    + estimateStringSize(tableSpace)
                                    + estimateStringSize(query)
                                    + paramsPayload);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_EXECUTE_STATEMENT);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(returnValues ? 1 : 0);
            byteBuf.writeLong(tx);
            byteBuf.writeLong(statementId);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, query);

            ByteBufUtils.writeVInt(byteBuf, params.size());
            for (Object p : params) {
                writeObject(byteBuf, p);
            }

            return byteBuf;

        }

        public static boolean readReturnValues(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE) == 1;
        }

        public static long readTx(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE);
        }

        public static long readStatementId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG);
        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG
                    + ONE_LONG
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readQuery(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

        public static ObjectListReader startReadParameters(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // query
            int numParams = ByteBufUtils.readVInt(buffer);
            return new ObjectListReader(pdu, numParams);
        }

    }

    public static class PrepareStatement {

        public static ByteBuf write(long messageId, String tableSpace, String query) {

            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_PREPARE_STATEMENT);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, query);

            return byteBuf;

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readQuery(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

    }

    public static class RequestTablespaceDump {

        public static ByteBuf write(long messageId, String tableSpace, String dumpId, int fetchSize, boolean includeTransactionLog) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_BYTE
                                    + ONE_INT
                                    + tableSpace.length()
                                    + dumpId.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_REQUEST_TABLESPACE_DUMP);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(includeTransactionLog ? 1 : 0);
            byteBuf.writeInt(fetchSize);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, dumpId);

            return byteBuf;

        }

        public static boolean readInludeTransactionLog(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE) == 1;
        }

        public static int readFetchSize(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getInt(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE);
        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_INT
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readDumpId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_INT
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }
    }

    public static class TablespaceDumpData {

        public static ByteBuf write(
                long messageId, String tableSpace, String dumpId,
                String command, byte[] tableDefinition, long estimatedSize,
                long dumpLedgerid, long dumpOffset, List<byte[]> indexesDefinition,
                List<KeyValue> records
        ) {
            if (tableDefinition == null) {
                tableDefinition = new byte[0];
            }
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_BYTE
                                    + ONE_INT
                                    + tableDefinition.length
                                    + tableSpace.length()
                                    + dumpId.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_TABLESPACE_DUMP_DATA);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(dumpLedgerid);
            byteBuf.writeLong(dumpOffset);
            byteBuf.writeLong(estimatedSize);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, dumpId);
            ByteBufUtils.writeString(byteBuf, command);
            ByteBufUtils.writeArray(byteBuf, tableDefinition);

            if (indexesDefinition == null) {
                byteBuf.writeInt(0);
            } else {
                byteBuf.writeInt(indexesDefinition.size());
                for (int i = 0; i < indexesDefinition.size(); i++) {
                    ByteBufUtils.writeArray(byteBuf, indexesDefinition.get(i));
                }
            }

            if (records == null) {
                byteBuf.writeInt(0);
            } else {
                byteBuf.writeInt(records.size());
                for (KeyValue kv : records) {
                    ByteBufUtils.writeArray(byteBuf, kv.key);
                    ByteBufUtils.writeArray(byteBuf, kv.value);
                }
            }

            return byteBuf;

        }

        public static long readLedgerId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }

        public static long readOffset(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
            );
        }

        public static long readEstimatedSize(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
            );
        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_LONG
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readDumpId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

        public static String readCommand(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // dumpId
            return ByteBufUtils.readString(buffer);
        }

        public static byte[] readTableDefinition(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // dumpId
            ByteBufUtils.skipArray(buffer); // command
            return ByteBufUtils.readArray(buffer);
        }

        public static List<byte[]> readIndexesDefinition(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // dumpId
            ByteBufUtils.skipArray(buffer); // command
            ByteBufUtils.skipArray(buffer); // tableDefinition
            int num = buffer.readInt();
            List<byte[]> res = new ArrayList<>();
            for (int i = 0; i < num; i++) {
                res.add(ByteBufUtils.readArray(buffer));
            }
            return res;
        }

        public static void readRecords(Pdu pdu, BiConsumer<byte[], byte[]> consumer) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // dumpId
            ByteBufUtils.skipArray(buffer); // command
            ByteBufUtils.skipArray(buffer); // tableDefinition
            int num = buffer.readInt();
            for (int i = 0; i < num; i++) {
                ByteBufUtils.skipArray(buffer);
            }
            int numRecords = buffer.readInt();
            for (int i = 0; i < numRecords; i++) {
                byte[] key = ByteBufUtils.readArray(buffer);
                byte[] value = ByteBufUtils.readArray(buffer);
                consumer.accept(key, value);
            }
        }
    }

    public static class RequestTableRestore {

        public static ByteBuf write(
                long messageId, String tableSpace, byte[] tableDefinition,
                long dumpLedgerId, long dumpOffset
        ) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_LONG
                                    + ONE_LONG
                                    + tableSpace.length()
                                    + tableDefinition.length
                    );
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_REQUEST_TABLE_RESTORE);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(dumpLedgerId);
            byteBuf.writeLong(dumpOffset);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeArray(byteBuf, tableDefinition);
            return byteBuf;

        }

        public static long readLedgerId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }

        public static long readOffset(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
            );
        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
            );
            return ByteBufUtils.readString(buffer);
        }

        public static byte[] readTableDefinition(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readArray(buffer);
        }

    }

    public static class TableRestoreFinished {

        public static ByteBuf write(
                long messageId, String tableSpace, String tableName,
                List<byte[]> indexesDefinition
        ) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + tableSpace.length()
                                    + tableName.length()
                                    + (indexesDefinition == null ? 0 : (indexesDefinition.size() * 64)));
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_TABLE_RESTORE_FINISHED);
            byteBuf.writeLong(messageId);

            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, tableName);

            if (indexesDefinition == null) {
                byteBuf.writeInt(0);
            } else {
                byteBuf.writeInt(indexesDefinition.size());
                for (int i = 0; i < indexesDefinition.size(); i++) {
                    ByteBufUtils.writeArray(byteBuf, indexesDefinition.get(i));
                }
            }

            return byteBuf;

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readTableName(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

        public static List<byte[]> readIndexesDefinition(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // tableName
            int num = buffer.readInt();
            List<byte[]> res = new ArrayList<>();
            for (int i = 0; i < num; i++) {
                res.add(ByteBufUtils.readArray(buffer));
            }
            return res;
        }

    }

    public static class RestoreFinished {

        public static ByteBuf write(long messageId, String tableSpace) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + tableSpace.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_RESTORE_FINISHED);
            byteBuf.writeLong(messageId);

            ByteBufUtils.writeString(byteBuf, tableSpace);

            return byteBuf;

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            return ByteBufUtils.readString(buffer);
        }

    }

    public static class PushTableData {

        public static ByteBuf write(long messageId, String tableSpace, String tableName, List<KeyValue> records) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + tableSpace.length()
                                    + tableName.length()
                                    + records.size() * 512);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_PUSH_TABLE_DATA);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, tableName);

            byteBuf.writeInt(records.size());
            for (KeyValue kv : records) {
                ByteBufUtils.writeArray(byteBuf, kv.key);
                ByteBufUtils.writeArray(byteBuf, kv.value);
            }

            return byteBuf;

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readTablename(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

        public static void readRecords(Pdu pdu, BiConsumer<byte[], byte[]> consumer) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // tablename
            int numRecords = buffer.readInt();
            for (int i = 0; i < numRecords; i++) {
                byte[] key = ByteBufUtils.readArray(buffer);
                byte[] value = ByteBufUtils.readArray(buffer);
                consumer.accept(key, value);
            }
        }
    }

    public static class PushTxLogChunk {

        public static ByteBuf write(long messageId, String tableSpace, List<KeyValue> records) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + tableSpace.length()
                                    + records.size() * 512);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_PUSH_TXLOGCHUNK);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, tableSpace);

            byteBuf.writeInt(records.size());
            for (KeyValue kv : records) {
                ByteBufUtils.writeArray(byteBuf, kv.key);
                ByteBufUtils.writeArray(byteBuf, kv.value);
            }

            return byteBuf;

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            return ByteBufUtils.readString(buffer);
        }

        public static void readRecords(Pdu pdu, BiConsumer<byte[], byte[]> consumer) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace

            int numRecords = buffer.readInt();
            for (int i = 0; i < numRecords; i++) {
                byte[] key = ByteBufUtils.readArray(buffer);
                byte[] value = ByteBufUtils.readArray(buffer);
                consumer.accept(key, value);
            }
        }
    }

    public static class PushTransactionsBlock {

        public static ByteBuf write(long messageId, String tableSpace, List<byte[]> records) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + tableSpace.length()
                                    + records.size() * 512);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_PUSH_TRANSACTIONSBLOCK);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, tableSpace);

            byteBuf.writeInt(records.size());
            for (byte[] tx : records) {
                ByteBufUtils.writeArray(byteBuf, tx);
            }

            return byteBuf;

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            return ByteBufUtils.readString(buffer);
        }

        public static void readTransactions(Pdu pdu, Consumer<byte[]> consumer) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace

            int numRecords = buffer.readInt();
            for (int i = 0; i < numRecords; i++) {
                byte[] key = ByteBufUtils.readArray(buffer);
                consumer.accept(key);
            }
        }
    }

    public static class ListOfListsReader {

        private final Pdu pdu;
        private final int numLists;

        public ListOfListsReader(Pdu pdu, int numLists) {
            this.pdu = pdu;
            this.numLists = numLists;
        }

        public int getNumLists() {
            return numLists;
        }

        public ObjectListReader nextList() {
            // assuming that the readerIndex is not altered but other direct accesses to the ByteBuf
            int numValues = ByteBufUtils.readVInt(pdu.buffer);
            return new ObjectListReader(pdu, numValues);
        }

    }

    public static class ObjectListReader {

        private final Pdu pdu;
        private final int numParams;

        public ObjectListReader(Pdu pdu, int numParams) {
            this.pdu = pdu;
            this.numParams = numParams;
        }

        public int getNumParams() {
            return numParams;
        }

        public Object nextObject() {
            // assuming that the readerIndex is not altered but other direct accesses to the ByteBuf
            return readObject(pdu.buffer);
        }

        public byte readTrailer() {
            // assuming that the readerIndex is not altered but other direct accesses to the ByteBuf
            if (pdu.buffer.isReadable()) {
                return pdu.buffer.readByte();
            } else {
                return 0;
            }
        }

        public static boolean isDontKeepReadLocks(byte trailer) {
            return ((trailer & Pdu.FLAGS_OPENSCANNER_DONTKEEP_READ_LOCKS) == Pdu.FLAGS_OPENSCANNER_DONTKEEP_READ_LOCKS);
        }

        public static boolean isAllowFollowerReads(byte trailer) {
            return ((trailer & Pdu.FLAGS_OPENSCANNER_ALLOW_FOLLOWER_READS) == Pdu.FLAGS_OPENSCANNER_ALLOW_FOLLOWER_READS);
        }

    }

    /**
     * Returns a practical upper-bound estimate of the number of bytes that
     * {@link #writeObject(ByteBuf, Object)} will write for {@code v}.
     * <p>
     * Assumptions:
     * <ul>
     *   <li>String characters are single-byte (ASCII/Latin-1 content is the norm
     *       for SQL parameters; the estimate may be slightly low for non-ASCII but
     *       Netty will expand transparently in that rare case).</li>
     *   <li>Length-prefix vints for payload sizes use at most
     *       {@value #VINT_LENGTH_SIZE} bytes (covers up to ~268 M elements).</li>
     * </ul>
     */
    static int estimateObjectSize(Object v) {
        // Every path in writeObject starts with ONE_BYTE for the type discriminator.
        if (v == null) {
            return ONE_BYTE;
        } else if (v instanceof RawString) {
            // type byte + vint(len) + raw bytes (byte length, not char count)
            return ONE_BYTE + VINT_LENGTH_SIZE + ((RawString) v).getLength();
        } else if (v instanceof String) {
            // type byte + vint(len) + 1 byte per char (ASCII assumption)
            return ONE_BYTE + VINT_LENGTH_SIZE + ((String) v).length();
        } else if (v instanceof Long) {
            return ONE_BYTE + ONE_LONG;
        } else if (v instanceof Integer) {
            return ONE_BYTE + ONE_INT;
        } else if (v instanceof Boolean) {
            return ONE_BYTE + ONE_BYTE;
        } else if (v instanceof java.util.Date) {
            return ONE_BYTE + ONE_LONG;
        } else if (v instanceof Double) {
            return ONE_BYTE + ONE_LONG;
        } else if (v instanceof Float) {
            // Float is promoted to double on the wire (see writeObject)
            return ONE_BYTE + ONE_LONG;
        } else if (v instanceof Short) {
            return ONE_BYTE + 2;
        } else if (v instanceof byte[]) {
            return ONE_BYTE + VINT_LENGTH_SIZE + ((byte[]) v).length;
        } else if (v instanceof Byte) {
            return ONE_BYTE + ONE_BYTE;
        } else if (v instanceof float[]) {
            // type byte + vint(len) + 4 bytes per float element
            return ONE_BYTE + VINT_LENGTH_SIZE + ((float[]) v).length * 4;
        } else if (v instanceof List) {
            // List<Number> written as a float array: 4 bytes per element
            return ONE_BYTE + VINT_LENGTH_SIZE + ((List<?>) v).size() * 4;
        } else {
            // Unknown type — writeObject will throw, but return a safe non-zero estimate.
            return ONE_BYTE + 16;
        }
    }

    /**
     * Returns a practical upper-bound estimate of the bytes needed to serialise a
     * {@link String} via {@link ByteBufUtils#writeString(ByteBuf, String)}: a vint
     * length prefix ({@value #VINT_LENGTH_SIZE} bytes) plus 1 byte per character
     * (ASCII assumption — adequate for table-space names and SQL query text).
     */
    private static int estimateStringSize(String s) {
        return VINT_LENGTH_SIZE + s.length();
    }

    static void writeObject(ByteBuf byteBuf, Object v) {
        if (v == null) {
            byteBuf.writeByte(TYPE_NULL);
        } else if (v instanceof RawString) {
            byteBuf.writeByte(TYPE_STRING);
            ByteBufUtils.writeRawString(byteBuf, (RawString) v);
        } else if (v instanceof String) {
            byteBuf.writeByte(TYPE_STRING);
            ByteBufUtils.writeString(byteBuf, (String) v);
        } else if (v instanceof Long) {
            byteBuf.writeByte(TYPE_LONG);
            byteBuf.writeLong((Long) v);
        } else if (v instanceof Integer) {
            byteBuf.writeByte(TYPE_INTEGER);
            byteBuf.writeInt((Integer) v);
        } else if (v instanceof Boolean) {
            byteBuf.writeByte(TYPE_BOOLEAN);
            byteBuf.writeBoolean((Boolean) v);
        } else if (v instanceof java.util.Date) {
            byteBuf.writeByte(TYPE_TIMESTAMP);
            byteBuf.writeLong(((java.util.Date) v).getTime());
        } else if (v instanceof Double) {
            byteBuf.writeByte(TYPE_DOUBLE);
            byteBuf.writeDouble((Double) v);
        } else if (v instanceof Float) {
            byteBuf.writeByte(TYPE_DOUBLE);
            byteBuf.writeDouble((Float) v);
        } else if (v instanceof Short) {
            byteBuf.writeByte(TYPE_SHORT);
            byteBuf.writeShort((Short) v);
        } else if (v instanceof byte[]) {
            byteBuf.writeByte(TYPE_BYTEARRAY);
            ByteBufUtils.writeArray(byteBuf, (byte[]) v);
        } else if (v instanceof Byte) {
            byteBuf.writeByte(TYPE_BYTE);
            byteBuf.writeByte((Byte) v);
        } else if (v instanceof float[]) {
            byteBuf.writeByte(TYPE_FLOATARRAY);
            ByteBufUtils.writeFloatArray(byteBuf, (float[]) v);
        } else if (v instanceof List) {
            byteBuf.writeByte(TYPE_FLOATARRAY);
            ByteBufUtils.writeFloatArray(byteBuf, (List<Number>) v);
        } else {
            throw new IllegalArgumentException("bad data type " + v.getClass());
        }

    }

    public static Object readObject(ByteBuf dii) {

        int type = ByteBufUtils.readVInt(dii);

        switch (type) {
            case TYPE_BYTEARRAY:
                return ByteBufUtils.readArray(dii);
            case TYPE_FLOATARRAY:
                return ByteBufUtils.readFloatArray(dii);
            case TYPE_LONG:
                return dii.readLong();
            case TYPE_INTEGER:
                return dii.readInt();
            case TYPE_SHORT:
                return dii.readShort();
            case TYPE_BYTE:
                return dii.readByte();
            case TYPE_STRING:
                return ByteBufUtils.readUnpooledRawString(dii);
            case TYPE_TIMESTAMP:
                return new java.sql.Timestamp(dii.readLong());
            case TYPE_NULL:
                return null;
            case TYPE_BOOLEAN:
                return dii.readBoolean();
            case TYPE_DOUBLE:
                return dii.readDouble();
            default:
                throw new IllegalArgumentException("bad column type " + type);
        }
    }
/**
     * Ensure that every parameter matches the same type as when we are marshalling/unmarshalling
     * it. This is useful for "local" mode: we do not want a different behaviour in local mode vs network mode
     * and also we do not want unexpected JDBC parameter types on server-side processing.
     * @param parameters the JDBC parameters
     * @return a new list with converted JDBC parameters
     * @see #writeObject(io.netty.buffer.ByteBuf, java.lang.Object)
     * @see #readObject(io.netty.buffer.ByteBuf)
     */
    public static List<Object> normalizeParametersList(List<Object> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            return parameters;
        }
        List<Object> result = new ArrayList<>(parameters.size());
        for (Object v : parameters) {
            if (v == null) {
                result.add(null);
            } else if (v instanceof String) {
                result.add(RawString.of((String) v));
            } else if (v instanceof RawString) {
                result.add(v);
            } else if (v instanceof Long) {
                result.add(v);
            } else if (v instanceof Integer) {
                result.add(v);
            } else if (v instanceof Boolean) {
                result.add(v);
            } else if (v instanceof java.sql.Timestamp) {
                result.add(v);
            } else if (v instanceof java.util.Date) {
                result.add(new java.sql.Timestamp(((java.util.Date) v).getTime()));
            } else if (v instanceof Double) {
                result.add(v);
            } else if (v instanceof Float) {
                result.add(((Float) v).doubleValue());
            } else if (v instanceof Short) {
                result.add(v);
            } else if (v instanceof byte[]) {
                result.add(v);
            } else if (v instanceof Byte) {
                result.add(v);
            } else if (v instanceof List) {
                result.add(v);
            } else {
                throw new IllegalArgumentException("bad data type " + v.getClass());
            }
        }
        return result;
    }

    // =========================================================================
    // File-server PDUs (issue #425).
    //
    // Each pair of writeRequest/writeResponse + readXxx methods follows the
    // existing herddb-net pattern: a fixed PDU header (version + flags + type
    // + messageId) followed by a type-specific payload. All payloads are
    // length-prefixed (ByteBufUtils.writeArray / writeString) so a corrupted
    // frame fails fast on the receiver rather than draining into the next
    // PDU. Buffers are pooled directBuffers; ownership transfers to the
    // network layer on send and to the Pdu wrapper on receive.
    //
    // Request/response disambiguation is via the FLAGS_ISREQUEST /
    // FLAGS_ISRESPONSE bit, identical to the core-server PDUs.
    // =========================================================================

    /** WRITE_FILE: client → server, returns total bytes written. */
    public abstract static class WriteFileRequest {

        public static ByteBuf write(long messageId, String path, ByteBuf content) {
            int contentLen = content.readableBytes();
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                                    + VINT_LENGTH_SIZE + path.length()
                                    + ONE_INT + contentLen);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FS_WRITE_FILE);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, path);
            byteBuf.writeInt(contentLen);
            byteBuf.writeBytes(content, content.readerIndex(), contentLen);
            return byteBuf;
        }

        public static ByteBuf write(long messageId, String path, byte[] content, int offset, int length) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                                    + VINT_LENGTH_SIZE + path.length()
                                    + ONE_INT + length);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FS_WRITE_FILE);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, path);
            byteBuf.writeInt(length);
            byteBuf.writeBytes(content, offset, length);
            return byteBuf;
        }

        public static String readPath(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            return ByteBufUtils.readString(buffer);
        }

        /**
         * Returns a retained slice of the request body. The caller is
         * responsible for releasing it once consumed. Slicing avoids a
         * heap copy of the payload — the storage backend can read directly
         * from the inbound network buffer.
         */
        public static ByteBuf readContent(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            ByteBufUtils.skipArray(buffer);
            int len = buffer.readInt();
            return buffer.retainedSlice(buffer.readerIndex(), len);
        }
    }

    /** Response to a {@link WriteFileRequest}: total bytes written. */
    public abstract static class WriteFileResponse {

        public static ByteBuf write(long messageId, long writtenSize) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE + ONE_LONG);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_WRITE_FILE);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(writtenSize);
            return byteBuf;
        }

        public static long readWrittenSize(Pdu pdu) {
            return pdu.buffer.getLong(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
        }
    }

    /** WRITE_FILE_BLOCK: client → server, writes one multipart block. */
    public abstract static class WriteFileBlockRequest {

        public static ByteBuf write(long messageId, String path, long blockIndex, ByteBuf content) {
            int contentLen = content.readableBytes();
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                                    + VINT_LENGTH_SIZE + path.length()
                                    + ONE_LONG + ONE_INT + contentLen);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FS_WRITE_FILE_BLOCK);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, path);
            byteBuf.writeLong(blockIndex);
            byteBuf.writeInt(contentLen);
            byteBuf.writeBytes(content, content.readerIndex(), contentLen);
            return byteBuf;
        }

        public static ByteBuf write(long messageId, String path, long blockIndex, byte[] content) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                                    + VINT_LENGTH_SIZE + path.length()
                                    + ONE_LONG + ONE_INT + content.length);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FS_WRITE_FILE_BLOCK);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, path);
            byteBuf.writeLong(blockIndex);
            byteBuf.writeInt(content.length);
            byteBuf.writeBytes(content);
            return byteBuf;
        }

        public static String readPath(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            return ByteBufUtils.readString(buffer);
        }

        public static long readBlockIndex(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            ByteBufUtils.skipArray(buffer);
            return buffer.readLong();
        }

        public static ByteBuf readContent(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            ByteBufUtils.skipArray(buffer);
            buffer.skipBytes(ONE_LONG);
            int len = buffer.readInt();
            return buffer.retainedSlice(buffer.readerIndex(), len);
        }
    }

    /** Response to a {@link WriteFileBlockRequest}: bytes written for that block. */
    public abstract static class WriteFileBlockResponse {

        public static ByteBuf write(long messageId, long writtenSize) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE + ONE_LONG);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_WRITE_FILE_BLOCK);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(writtenSize);
            return byteBuf;
        }

        public static long readWrittenSize(Pdu pdu) {
            return pdu.buffer.getLong(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
        }
    }

    /** READ_FILE: client → server, returns full file content (or found=false). */
    public abstract static class ReadFileRequest {

        public static ByteBuf write(long messageId, String path) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                                    + VINT_LENGTH_SIZE + path.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FS_READ_FILE);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, path);
            return byteBuf;
        }

        public static String readPath(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            return ByteBufUtils.readString(buffer);
        }
    }

    /**
     * Response to a {@link ReadFileRequest}.
     * Wire layout: {@code [hdr] [byte found:0|1] [int contentLen] [contentBytes...]}.
     * When {@code found=false}, {@code contentLen=0} and no payload follows.
     */
    public abstract static class ReadFileResponse {

        /** Writes a "not found" response (no payload). */
        public static ByteBuf writeNotFound(long messageId) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                            + ONE_BYTE + ONE_INT);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_READ_FILE);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(0);
            byteBuf.writeInt(0);
            return byteBuf;
        }

        /**
         * Writes a "found" response. {@code content} is consumed (its readable
         * bytes are appended into the response buffer). Caller still owns
         * {@code content} and is responsible for releasing it.
         */
        public static ByteBuf writeFound(long messageId, ByteBuf content) {
            int contentLen = content.readableBytes();
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                            + ONE_BYTE + ONE_INT + contentLen);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_READ_FILE);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(1);
            byteBuf.writeInt(contentLen);
            byteBuf.writeBytes(content, content.readerIndex(), contentLen);
            return byteBuf;
        }

        public static ByteBuf writeFound(long messageId, byte[] content, int offset, int length) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                            + ONE_BYTE + ONE_INT + length);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_READ_FILE);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(1);
            byteBuf.writeInt(length);
            byteBuf.writeBytes(content, offset, length);
            return byteBuf;
        }

        public static boolean readFound(Pdu pdu) {
            return pdu.buffer.getByte(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE) == 1;
        }

        public static int readContentLength(Pdu pdu) {
            return pdu.buffer.getInt(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE + ONE_BYTE);
        }

        /**
         * Returns a retained slice over the response payload. Caller owns
         * the returned slice and must {@code release()} it. Returns an
         * empty (still-retained) slice when {@code found=false}.
         */
        public static ByteBuf readContent(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            int hdr = VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE;
            int len = buffer.getInt(hdr + ONE_BYTE);
            return buffer.retainedSlice(hdr + ONE_BYTE + ONE_INT, len);
        }
    }

    /** READ_FILE_RANGE: client → server, returns a byte range from a (possibly multipart) file. */
    public abstract static class ReadFileRangeRequest {

        public static ByteBuf write(long messageId, String path, long offset, int length, int blockSize) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                                    + VINT_LENGTH_SIZE + path.length()
                                    + ONE_LONG + ONE_INT + ONE_INT);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FS_READ_FILE_RANGE);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, path);
            byteBuf.writeLong(offset);
            byteBuf.writeInt(length);
            byteBuf.writeInt(blockSize);
            return byteBuf;
        }

        public static String readPath(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            return ByteBufUtils.readString(buffer);
        }

        public static long readOffset(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            ByteBufUtils.skipArray(buffer);
            return buffer.readLong();
        }

        public static int readLength(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            ByteBufUtils.skipArray(buffer);
            buffer.skipBytes(ONE_LONG);
            return buffer.readInt();
        }

        public static int readBlockSize(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            ByteBufUtils.skipArray(buffer);
            buffer.skipBytes(ONE_LONG + ONE_INT);
            return buffer.readInt();
        }
    }

    /** Response to a {@link ReadFileRangeRequest}; identical wire shape to {@link ReadFileResponse}. */
    public abstract static class ReadFileRangeResponse {

        public static ByteBuf writeNotFound(long messageId) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                            + ONE_BYTE + ONE_INT);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_READ_FILE_RANGE);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(0);
            byteBuf.writeInt(0);
            return byteBuf;
        }

        public static ByteBuf writeFound(long messageId, ByteBuf content) {
            int contentLen = content.readableBytes();
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                            + ONE_BYTE + ONE_INT + contentLen);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_READ_FILE_RANGE);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(1);
            byteBuf.writeInt(contentLen);
            byteBuf.writeBytes(content, content.readerIndex(), contentLen);
            return byteBuf;
        }

        public static ByteBuf writeFound(long messageId, byte[] content, int offset, int length) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                            + ONE_BYTE + ONE_INT + length);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_READ_FILE_RANGE);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(1);
            byteBuf.writeInt(length);
            byteBuf.writeBytes(content, offset, length);
            return byteBuf;
        }

        public static boolean readFound(Pdu pdu) {
            return pdu.buffer.getByte(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE) == 1;
        }

        public static int readContentLength(Pdu pdu) {
            return pdu.buffer.getInt(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE + ONE_BYTE);
        }

        public static ByteBuf readContent(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            int hdr = VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE;
            int len = buffer.getInt(hdr + ONE_BYTE);
            return buffer.retainedSlice(hdr + ONE_BYTE + ONE_INT, len);
        }
    }

    /** DELETE_FILE: client → server, returns whether the path existed. */
    public abstract static class DeleteFileRequest {

        public static ByteBuf write(long messageId, String path) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                                    + VINT_LENGTH_SIZE + path.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FS_DELETE_FILE);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, path);
            return byteBuf;
        }

        public static String readPath(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            return ByteBufUtils.readString(buffer);
        }
    }

    /** Response to a {@link DeleteFileRequest}. */
    public abstract static class DeleteFileResponse {

        public static ByteBuf write(long messageId, boolean deleted) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE + ONE_BYTE);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_DELETE_FILE);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(deleted ? 1 : 0);
            return byteBuf;
        }

        public static boolean readDeleted(Pdu pdu) {
            return pdu.buffer.getByte(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE) == 1;
        }
    }

    /**
     * DELETE_FILES: client → server, batch logical-file deletion (issue #398).
     * All paths in a single request must route to the same server (the client
     * groups paths by consistent-hash router before dispatch). Each path is
     * processed independently; the response carries per-path outcomes.
     */
    public abstract static class DeleteFilesRequest {

        public static ByteBuf write(long messageId, List<String> paths) {
            int sizeHint = VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                    + ONE_INT + 32 * paths.size();
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(sizeHint);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FS_DELETE_FILES);
            byteBuf.writeLong(messageId);
            byteBuf.writeInt(paths.size());
            for (String p : paths) {
                ByteBufUtils.writeString(byteBuf, p);
            }
            return byteBuf;
        }

        public static List<String> readPaths(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            int count = buffer.readInt();
            List<String> result = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                result.add(ByteBufUtils.readString(buffer));
            }
            return result;
        }
    }

    /**
     * Response to a {@link DeleteFilesRequest}. Per-path outcome carries the
     * deletion flag and (optionally) a non-empty error message describing why
     * the per-path delete failed. A path that did not exist is reported with
     * {@code deleted=false} and an empty error string.
     */
    public abstract static class DeleteFilesResponse {

        /** Per-path outcome: path, whether it was deleted, optional error message. */
        public static final class Outcome {
            public final String path;
            public final boolean deleted;
            public final String error;

            public Outcome(String path, boolean deleted, String error) {
                this.path = path;
                this.deleted = deleted;
                this.error = error == null ? "" : error;
            }
        }

        public static ByteBuf write(long messageId, List<Outcome> outcomes) {
            int sizeHint = VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                    + ONE_INT + 64 * outcomes.size();
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(sizeHint);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_DELETE_FILES);
            byteBuf.writeLong(messageId);
            byteBuf.writeInt(outcomes.size());
            for (Outcome o : outcomes) {
                ByteBufUtils.writeString(byteBuf, o.path);
                byteBuf.writeByte(o.deleted ? 1 : 0);
                ByteBufUtils.writeString(byteBuf, o.error == null ? "" : o.error);
            }
            return byteBuf;
        }

        public static List<Outcome> readOutcomes(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            int count = buffer.readInt();
            List<Outcome> result = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                String path = ByteBufUtils.readString(buffer);
                boolean deleted = buffer.readByte() == 1;
                String error = ByteBufUtils.readString(buffer);
                result.add(new Outcome(path, deleted, error));
            }
            return result;
        }
    }

    /**
     * LIST_FILES: client → server, returns all logical paths under the prefix
     * in a single response PDU. The previous gRPC server-streaming form is
     * collapsed into one message because the server materializes the full list
     * before responding anyway, and response sizes for typical HerdDB
     * workloads (per-tablespace page directories) are small enough that
     * single-PDU delivery has no measurable cost. If a use case ever needs
     * per-entry streaming, add a second PDU type without breaking this one.
     */
    public abstract static class ListFilesRequest {

        public static ByteBuf write(long messageId, String prefix) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                                    + VINT_LENGTH_SIZE + prefix.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FS_LIST_FILES);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, prefix);
            return byteBuf;
        }

        public static String readPrefix(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            return ByteBufUtils.readString(buffer);
        }
    }

    /** Response to a {@link ListFilesRequest}. */
    public abstract static class ListFilesResponse {

        public static ByteBuf write(long messageId, List<String> paths) {
            int sizeHint = VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                    + ONE_INT + 64 * paths.size();
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(sizeHint);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_LIST_FILES);
            byteBuf.writeLong(messageId);
            byteBuf.writeInt(paths.size());
            for (String p : paths) {
                ByteBufUtils.writeString(byteBuf, p);
            }
            return byteBuf;
        }

        public static List<String> readPaths(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            int count = buffer.readInt();
            List<String> result = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                result.add(ByteBufUtils.readString(buffer));
            }
            return result;
        }
    }

    /** DELETE_BY_PREFIX: client → server, bulk-delete all paths matching the prefix. */
    public abstract static class DeleteByPrefixRequest {

        public static ByteBuf write(long messageId, String prefix) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE
                                    + VINT_LENGTH_SIZE + prefix.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FS_DELETE_BY_PREFIX);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, prefix);
            return byteBuf;
        }

        public static String readPrefix(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            return ByteBufUtils.readString(buffer);
        }
    }

    /** Response to a {@link DeleteByPrefixRequest}: number of files deleted. */
    public abstract static class DeleteByPrefixResponse {

        public static ByteBuf write(long messageId, int deletedCount) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE + ONE_INT);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_DELETE_BY_PREFIX);
            byteBuf.writeLong(messageId);
            byteBuf.writeInt(deletedCount);
            return byteBuf;
        }

        public static int readDeletedCount(Pdu pdu) {
            return pdu.buffer.getInt(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
        }
    }

    /** GET_SERVER_INFO: client → server, no payload. Admin RPC. */
    public abstract static class GetServerInfoRequest {

        public static ByteBuf write(long messageId) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FS_GET_SERVER_INFO);
            byteBuf.writeLong(messageId);
            return byteBuf;
        }
    }

    /**
     * Response to a {@link GetServerInfoRequest}. Mirrors the legacy
     * {@code GetServerInfoResponse} protobuf field-for-field; absent
     * subsystems are reported as zero.
     */
    public abstract static class GetServerInfoResponse {

        /** Plain holder mirroring the gRPC {@code GetServerInfoResponse} message. */
        public static final class Info {
            public String host;
            public int port;
            public String storageMode;
            public long jvmHeapUsedBytes;
            public long jvmHeapMaxBytes;
            public long diskCacheMaxBytes;
            public long diskCacheHitCount;
            public long diskCacheMissCount;
            public long diskCacheEvictionCount;
            public long diskCacheHitBytes;
            public long diskCacheMissBytes;
            public long diskCacheEstimatedEntries;
            public long blockCacheMaxBytes;
            public long blockCacheEstimatedBytes;
            public long blockCacheEstimatedEntries;
            public long blockCacheHits;
            public long blockCacheMisses;
            public long blockCacheEvictions;
        }

        public static ByteBuf write(long messageId, Info info) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE + 256);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_GET_SERVER_INFO);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, info.host == null ? "" : info.host);
            byteBuf.writeInt(info.port);
            ByteBufUtils.writeString(byteBuf, info.storageMode == null ? "" : info.storageMode);
            byteBuf.writeLong(info.jvmHeapUsedBytes);
            byteBuf.writeLong(info.jvmHeapMaxBytes);
            byteBuf.writeLong(info.diskCacheMaxBytes);
            byteBuf.writeLong(info.diskCacheHitCount);
            byteBuf.writeLong(info.diskCacheMissCount);
            byteBuf.writeLong(info.diskCacheEvictionCount);
            byteBuf.writeLong(info.diskCacheHitBytes);
            byteBuf.writeLong(info.diskCacheMissBytes);
            byteBuf.writeLong(info.diskCacheEstimatedEntries);
            byteBuf.writeLong(info.blockCacheMaxBytes);
            byteBuf.writeLong(info.blockCacheEstimatedBytes);
            byteBuf.writeLong(info.blockCacheEstimatedEntries);
            byteBuf.writeLong(info.blockCacheHits);
            byteBuf.writeLong(info.blockCacheMisses);
            byteBuf.writeLong(info.blockCacheEvictions);
            return byteBuf;
        }

        public static Info read(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
            Info info = new Info();
            info.host = ByteBufUtils.readString(buffer);
            info.port = buffer.readInt();
            info.storageMode = ByteBufUtils.readString(buffer);
            info.jvmHeapUsedBytes = buffer.readLong();
            info.jvmHeapMaxBytes = buffer.readLong();
            info.diskCacheMaxBytes = buffer.readLong();
            info.diskCacheHitCount = buffer.readLong();
            info.diskCacheMissCount = buffer.readLong();
            info.diskCacheEvictionCount = buffer.readLong();
            info.diskCacheHitBytes = buffer.readLong();
            info.diskCacheMissBytes = buffer.readLong();
            info.diskCacheEstimatedEntries = buffer.readLong();
            info.blockCacheMaxBytes = buffer.readLong();
            info.blockCacheEstimatedBytes = buffer.readLong();
            info.blockCacheEstimatedEntries = buffer.readLong();
            info.blockCacheHits = buffer.readLong();
            info.blockCacheMisses = buffer.readLong();
            info.blockCacheEvictions = buffer.readLong();
            return info;
        }
    }

    /** RESIZE_DISK_CACHE: client → server, dynamically resize the disk-LRU. Admin RPC. */
    public abstract static class ResizeDiskCacheRequest {

        public static ByteBuf write(long messageId, long newMaxBytes) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE + ONE_LONG);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FS_RESIZE_DISK_CACHE);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(newMaxBytes);
            return byteBuf;
        }

        public static long readNewMaxBytes(Pdu pdu) {
            return pdu.buffer.getLong(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
        }
    }

    /** Response to a {@link ResizeDiskCacheRequest}. */
    public abstract static class ResizeDiskCacheResponse {

        public static ByteBuf write(long messageId, long previousMaxBytes, long newMaxBytes) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE + 2 * ONE_LONG);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_FS_RESIZE_DISK_CACHE);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(previousMaxBytes);
            byteBuf.writeLong(newMaxBytes);
            return byteBuf;
        }

        public static long readPreviousMaxBytes(Pdu pdu) {
            return pdu.buffer.getLong(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE);
        }

        public static long readNewMaxBytes(Pdu pdu) {
            return pdu.buffer.getLong(VERSION_SIZE + FLAGS_SIZE + TYPE_SIZE + MSGID_SIZE + ONE_LONG);
        }
    }

}
