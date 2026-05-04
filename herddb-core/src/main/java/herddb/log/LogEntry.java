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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.utils.ByteBufUtils;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.SystemProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * An entry on the log
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class LogEntry {

    public final short type;
    public final long transactionId;
    public final String tableName;
    public final Bytes key;
    public final Bytes value;
    public final long timestamp;

    public LogEntry(long timestamp, short type, long transactionId, String tableName, Bytes key, Bytes value) {
        this.timestamp = timestamp;
        this.type = type;
        this.transactionId = transactionId;
        this.key = key;
        this.value = value;
        this.tableName = tableName;
    }

    public byte[] serialize() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream doo = new ExtendedDataOutputStream(out)) {
            serialize(doo);
            return out.toByteArray();
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    private static final int DEFAULT_BUFFER_SIZE = SystemProperties.getIntSystemProperty("herddb.log.initentrysize", 2024);

    public ByteBuf serializeAsByteBuf() {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(DEFAULT_BUFFER_SIZE);
        boolean handedOff = false;
        try {
            serializeTo(buffer);
            handedOff = true;
            return buffer;
        } finally {
            // Plug a latent leak: release the pooled buffer if encoding fails
            // (e.g. an unsupported entry type, or an Error such as OOM while
            // the buffer is being grown by Netty). The previous implementation
            // wrapped only IOException and leaked the buffer on every other
            // throwable.
            if (!handedOff) {
                buffer.release();
            }
        }
    }

    /**
     * Writes this entry directly into {@code buffer} using vint-prefixed standard
     * UTF-8 strings. This avoids the per-call {@code DataOutputStream} /
     * {@code ByteBufOutputStream} wrapper allocations and the modified-UTF-8
     * {@code byte[]} temporary that {@code DataOutputStream.writeUTF} allocates
     * internally — both of which show up at the top of the allocation profile on
     * the commit-log hot path. Wire-compatible with
     * {@link #serialize(ExtendedDataOutputStream)} so a single
     * {@link #deserialize(ExtendedDataInputStream)} reads both write paths.
     */
    private void serializeTo(ByteBuf buffer) {
        buffer.writeLong(timestamp); // 8
        buffer.writeShort(type);     // 2
        buffer.writeLong(transactionId); // 8
        switch (type) {
            case LogEntryType.UPDATE:
            case LogEntryType.INSERT:
                ByteBufUtils.writeUtf8String(buffer, tableName);
                ByteBufUtils.writeArrayOrNull(buffer, key);
                ByteBufUtils.writeArrayOrNull(buffer, value);
                break;
            case LogEntryType.DELETE:
                ByteBufUtils.writeUtf8String(buffer, tableName);
                ByteBufUtils.writeArrayOrNull(buffer, key);
                break;
            case LogEntryType.CREATE_TABLE:
            case LogEntryType.ALTER_TABLE:
                // value contains the table definition
                ByteBufUtils.writeUtf8String(buffer, tableName);
                ByteBufUtils.writeArrayOrNull(buffer, value);
                break;
            case LogEntryType.CREATE_INDEX:
                // value contains the index definition
                ByteBufUtils.writeUtf8String(buffer, tableName);
                ByteBufUtils.writeArrayOrNull(buffer, value);
                break;
            case LogEntryType.DROP_TABLE:
            case LogEntryType.TRUNCATE_TABLE:
                ByteBufUtils.writeUtf8String(buffer, tableName);
                break;
            case LogEntryType.DROP_INDEX:
                ByteBufUtils.writeArrayOrNull(buffer, this.value);
                break;
            case LogEntryType.BEGINTRANSACTION:
            case LogEntryType.COMMITTRANSACTION:
            case LogEntryType.ROLLBACKTRANSACTION:
                break;
            case LogEntryType.NOOP:
                break;
            case LogEntryType.TABLE_CONSISTENCY_CHECK:
                ByteBufUtils.writeUtf8String(buffer, tableName);
                //value contains checksum and query
                ByteBufUtils.writeArrayOrNull(buffer, value);
                break;
            case LogEntryType.INDEXING_SERVICE_REBALANCE:
                // value contains a serialized IndexingServiceRebalanceDescriptor;
                // tableName/key are unused.
                ByteBufUtils.writeArrayOrNull(buffer, value);
                break;
            default:
                throw new IllegalArgumentException("unsupported type " + type);
        }
    }

    /**
     * @param doo
     * @return an estimate on the number of written bytes
     * @throws IOException
     */
    public int serialize(ExtendedDataOutputStream doo) throws IOException {
        int startingsize = doo.size();
        doo.writeLong(timestamp); // 8
        doo.writeShort(type);  // 2
        doo.writeLong(transactionId); // 8
        switch (type) {
            case LogEntryType.UPDATE:
            case LogEntryType.INSERT:
                doo.writeUtf8String(tableName);
                doo.writeArray(key);
                doo.writeArray(value);
                break;
            case LogEntryType.DELETE:
                doo.writeUtf8String(tableName);
                doo.writeArray(key);
                break;
            case LogEntryType.CREATE_TABLE:
            case LogEntryType.ALTER_TABLE:
                // value contains the table definition
                doo.writeUtf8String(tableName);
                doo.writeArray(value);
                break;
            case LogEntryType.CREATE_INDEX:
                // value contains the index definition
                doo.writeUtf8String(tableName);
                doo.writeArray(value);
                break;
            case LogEntryType.DROP_TABLE:
            case LogEntryType.TRUNCATE_TABLE:
                doo.writeUtf8String(tableName);
                break;
            case LogEntryType.DROP_INDEX:
                doo.writeArray(this.value);
                break;
            case LogEntryType.BEGINTRANSACTION:
            case LogEntryType.COMMITTRANSACTION:
            case LogEntryType.ROLLBACKTRANSACTION:
                break;
            case LogEntryType.NOOP:
                break;
            case LogEntryType.TABLE_CONSISTENCY_CHECK:
                doo.writeUtf8String(tableName);
                //value contains checksum and query
                doo.writeArray(value);
                break;
            case LogEntryType.INDEXING_SERVICE_REBALANCE:
                // value contains a serialized IndexingServiceRebalanceDescriptor;
                // tableName/key are unused.
                doo.writeArray(value);
                break;
            default:
                throw new IllegalArgumentException("unsupported type " + type);
        }
        return doo.size() - startingsize;
    }

    public static LogEntry deserialize(byte[] data) throws EOFException {
        SimpleByteArrayInputStream in = new SimpleByteArrayInputStream(data);
        ExtendedDataInputStream dis = new ExtendedDataInputStream(in);
        return deserialize(dis);
    }

    public static LogEntry deserialize(ExtendedDataInputStream dis) throws EOFException {
        try {
            long timestamp = dis.readLong();
            short type = dis.readShort();
            long transactionId = dis.readLong();

            Bytes key = null;
            Bytes value = null;
            String tableName = null;
            switch (type) {
                case LogEntryType.UPDATE:
                case LogEntryType.INSERT:
                    tableName = dis.readUtf8String();
                    key = dis.readBytes();
                    value = dis.readBytes();
                    break;
                case LogEntryType.DELETE:
                    tableName = dis.readUtf8String();
                    key = dis.readBytes();
                    break;
                case LogEntryType.DROP_TABLE:
                case LogEntryType.TRUNCATE_TABLE:
                    tableName = dis.readUtf8String();
                    break;
                case LogEntryType.DROP_INDEX:
                    value = dis.readBytes();
                    break;
                case LogEntryType.CREATE_INDEX:
                    tableName = dis.readUtf8String();
                    value = dis.readBytes();
                    break;
                case LogEntryType.CREATE_TABLE:
                case LogEntryType.ALTER_TABLE:
                    // value contains the table definition
                    tableName = dis.readUtf8String();
                    value = dis.readBytes();
                    break;
                case LogEntryType.BEGINTRANSACTION:
                case LogEntryType.COMMITTRANSACTION:
                case LogEntryType.ROLLBACKTRANSACTION:
                    break;
                case LogEntryType.NOOP:
                    break;
                case LogEntryType.TABLE_CONSISTENCY_CHECK:
                    tableName = dis.readUtf8String();
                    value = dis.readBytes();
                    break;
                case LogEntryType.INDEXING_SERVICE_REBALANCE:
                    value = dis.readBytes();
                    break;
                default:
                    throw new IllegalArgumentException("unsupported type " + type);
            }
            return new LogEntry(timestamp, type, transactionId, tableName, key, value);
        } catch (EOFException e) {
            /* Entry "corrupted" cause stream premature end, propagate */
            throw e;
        } catch (IOException err) {
            /* Stream read error */
            throw new RuntimeException(err);
        }
    }

    @Override
    public String toString() {
        // this string is printed on logs during debug...better to save space
        return "LE{" + "t=" + type + ",tx=" + transactionId + ",tn=" + tableName + ",k=" + key + ",v=" + value + ",ts=" + timestamp + '}';
    }

}
