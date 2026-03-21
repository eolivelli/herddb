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

package herddb.mysql.protocol;

/**
 * MySQL column type codes.
 */
public enum MySQLColumnType {

    MYSQL_TYPE_TINY(1),
    MYSQL_TYPE_LONG(3),
    MYSQL_TYPE_FLOAT(4),
    MYSQL_TYPE_DOUBLE(5),
    MYSQL_TYPE_NULL(6),
    MYSQL_TYPE_LONGLONG(8),
    MYSQL_TYPE_DATETIME(12),
    MYSQL_TYPE_BLOB(252),
    MYSQL_TYPE_VAR_STRING(253);

    private final int code;

    MySQLColumnType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
