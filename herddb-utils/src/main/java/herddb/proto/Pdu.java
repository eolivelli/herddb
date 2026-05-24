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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;

/**
 * This represent a generic PDU in the protocol.
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class Pdu implements AutoCloseable {

    public static final byte TYPE_ACK = 0;
    public static final byte TYPE_CLIENT_SHUTDOWN = 3;
    public static final byte TYPE_ERROR = 4;
    public static final byte TYPE_EXECUTE_STATEMENT = 5;
    public static final byte TYPE_EXECUTE_STATEMENT_RESULT = 6;
    public static final byte TYPE_OPENSCANNER = 7;
    public static final byte TYPE_RESULTSET_CHUNK = 8;
    public static final byte TYPE_CLOSESCANNER = 9;
    public static final byte TYPE_FETCHSCANNERDATA = 10;
    public static final byte TYPE_REQUEST_TABLESPACE_DUMP = 11;
    public static final byte TYPE_TABLESPACE_DUMP_DATA = 12;
    public static final byte TYPE_REQUEST_TABLE_RESTORE = 13;
    public static final byte TYPE_PUSH_TABLE_DATA = 14;
    public static final byte TYPE_EXECUTE_STATEMENTS = 15;
    public static final byte TYPE_EXECUTE_STATEMENTS_RESULT = 16;
    public static final byte TYPE_PUSH_TXLOGCHUNK = 17;
    public static final byte TYPE_TABLE_RESTORE_FINISHED = 19;
    public static final byte TYPE_PUSH_TRANSACTIONSBLOCK = 20;
    public static final byte TYPE_RESTORE_FINISHED = 23;
    public static final byte TYPE_TX_COMMAND = 24;
    public static final byte TYPE_TX_COMMAND_RESULT = 25;
    // ---------------------------------------------------------------------
    // File-server PDU types (issue #425). Allocated in the 50..69 range so
    // the service the client is talking to is identifiable from a single
    // type-byte read on the wire (HerdDB core-server types occupy 0..25
    // and 100..104). Request and response PDUs share the same type byte
    // and disambiguate via FLAGS_ISREQUEST / FLAGS_ISRESPONSE.
    // ---------------------------------------------------------------------
    public static final byte TYPE_FS_WRITE_FILE = 50;
    // Op-code 51 (TYPE_FS_WRITE_FILE_BLOCK) was retired in issue #650 along
    // with the per-block multipart layout; the slot is intentionally left
    // unused. Servers receiving a 51 op-code today will respond with the
    // generic "unknown op-code" path in RemoteFileServiceImpl#handle.
    public static final byte TYPE_FS_READ_FILE = 52;
    public static final byte TYPE_FS_READ_FILE_RANGE = 53;
    public static final byte TYPE_FS_DELETE_FILE = 54;
    public static final byte TYPE_FS_DELETE_FILES = 55;
    public static final byte TYPE_FS_LIST_FILES = 56;
    public static final byte TYPE_FS_DELETE_BY_PREFIX = 57;
    /** Issue #650: prefetch a (path, offset, length, blockSize) slice into the file-server cache. */
    public static final byte TYPE_FS_PREFETCH_FILE_RANGE = 58;
    // 59 reserved for future file-server data ops.
    public static final byte TYPE_FS_GET_SERVER_INFO = 60;
    public static final byte TYPE_FS_RESIZE_DISK_CACHE = 61;
    // 62..69 reserved for future file-server admin ops.
    public static final byte TYPE_SASL_TOKEN_MESSAGE_REQUEST = 100;
    public static final byte TYPE_SASL_TOKEN_SERVER_RESPONSE = 101;
    public static final byte TYPE_SASL_TOKEN_MESSAGE_TOKEN = 102;
    public static final byte TYPE_PREPARE_STATEMENT = 103;
    public static final byte TYPE_PREPARE_STATEMENT_RESULT = 104;

    public static final byte FLAGS_ISREQUEST = 1;
    public static final byte FLAGS_ISRESPONSE = 2;
    public static final byte FLAGS_OPENSCANNER_DONTKEEP_READ_LOCKS = 4;
    public static final byte FLAGS_OPENSCANNER_ALLOW_FOLLOWER_READS = 8;


    private static final Recycler<Pdu> RECYCLER = new Recycler<Pdu>() {
        @Override
        protected Pdu newObject(Recycler.Handle<Pdu> handle) {
            return new Pdu(handle);
        }
    };

    public static Pdu newPdu(ByteBuf buffer, byte type, byte flags, long messageId) {
        Pdu res = RECYCLER.get();
        res.type = type;
        res.flags = flags;
        res.messageId = messageId;
        res.buffer = buffer;
        return res;
    }

    private Pdu(Recycler.Handle<Pdu> handle) {
        this.handle = handle;
    }

    private final Recycler.Handle<Pdu> handle;
    public ByteBuf buffer;
    public byte flags;
    public byte type;
    public long messageId;

    public boolean isRequest() {
        return (flags & FLAGS_ISREQUEST) == FLAGS_ISREQUEST;
    }

    public boolean isResponse() {
        return (flags & FLAGS_ISRESPONSE) == FLAGS_ISRESPONSE;
    }

    @Override
    public void close() {
        ReferenceCountUtil.release(buffer);
        buffer = null;
        messageId = 0;
        handle.recycle(this);
    }

}
