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
package herddb.sql;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.model.ColumnTypes;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableContext;
import herddb.sql.expressions.CompiledSQLExpression;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

/**
 * Issue #391: regression tests pinning the contract that
 * {@link SQLRecordKeyFunction#computeNewValue} rewraps any codec-level
 * {@link RuntimeException} as a user-friendly
 * {@link StatementExecutionException}, matching the pre-PR behaviour where
 * the multi-PK branch was guarded by a broad {@code catch (Exception)}.
 *
 * <p>The PR narrowed that catch from {@code Exception} to
 * {@code RuntimeException} (which subsumes {@code IllegalArgumentException},
 * {@code ClassCastException}, and the {@code RuntimeException(IOException)}
 * wrapper from {@code RecordSerializer.serializePrimaryKeyRaw}). These tests
 * make sure that narrowing did not regress the JDBC-facing error contract.</p>
 */
public class SQLRecordKeyFunctionTest {

    /**
     * Wrong-typed multi-PK value: an {@code Integer} flowing through a
     * {@code BYTEARRAY} primary-key column. {@code RecordSerializer.convert(BYTEARRAY, Integer)}
     * passes the value through unchanged (it only intercepts {@code RawString}),
     * so the wrong-typed value reaches {@code serializeTo}, where the unchecked
     * {@code (byte[]) v} cast triggers a {@link ClassCastException}. The
     * outer {@code catch (RuntimeException)} in {@code computeNewValue} must
     * rewrap that as {@link StatementExecutionException} with the
     * "error while converting primary key" prefix, otherwise a raw CCE
     * would surface at the JDBC layer.
     */
    @Test
    public void testWrongTypedMultiPkBytearrayValueIsRewrapped() {
        Table table = Table.builder()
                .name("t")
                .column("pk1", ColumnTypes.STRING)
                .column("pk2", ColumnTypes.BYTEARRAY)
                .column("v", ColumnTypes.LONG)
                .primaryKey("pk1")
                .primaryKey("pk2")
                .build();

        // Two non-constant expressions so SQLRecordKeyFunction.isConstant=false
        // (avoids the constant-cache short-circuit and forces the codec path).
        CompiledSQLExpression pk1Expr = (bean, ctx) -> "okay";
        CompiledSQLExpression pk2Expr = (bean, ctx) -> Integer.valueOf(42); // wrong type

        SQLRecordKeyFunction f = new SQLRecordKeyFunction(
                Arrays.asList("pk1", "pk2"),
                Arrays.asList(pk1Expr, pk2Expr),
                table);

        SQLStatementEvaluationContext ctx = new SQLStatementEvaluationContext(
                "test query", Collections.emptyList(), false, false);

        try {
            f.computeNewValue(null, ctx, fixedTableContext(table));
            fail("Expected StatementExecutionException, but no exception was thrown");
        } catch (StatementExecutionException expected) {
            // The rewrap must include the user-facing prefix and chain the
            // original ClassCastException as the cause so the operator can
            // diagnose the underlying type mismatch.
            assertTrue("rewrap message should mention 'primary key': " + expected.getMessage(),
                    expected.getMessage().contains("primary key"));
            Throwable cause = expected.getCause();
            assertNotNull("rewrap must preserve the original cause", cause);
            assertTrue("cause should be a RuntimeException (CCE or its wrap), got " + cause,
                    cause instanceof RuntimeException);
        }
    }

    /**
     * Successful happy path on the same multi-PK table to make sure the
     * regression test above is not green only because the function is broken
     * in some other way.
     */
    @Test
    public void testCorrectlyTypedMultiPkValueProducesBytes() throws Exception {
        Table table = Table.builder()
                .name("t")
                .column("pk1", ColumnTypes.STRING)
                .column("pk2", ColumnTypes.BYTEARRAY)
                .column("v", ColumnTypes.LONG)
                .primaryKey("pk1")
                .primaryKey("pk2")
                .build();

        CompiledSQLExpression pk1Expr = (bean, ctx) -> "okay";
        CompiledSQLExpression pk2Expr = (bean, ctx) -> new byte[]{1, 2, 3, 4};

        SQLRecordKeyFunction f = new SQLRecordKeyFunction(
                Arrays.asList("pk1", "pk2"),
                Arrays.asList(pk1Expr, pk2Expr),
                table);

        SQLStatementEvaluationContext ctx = new SQLStatementEvaluationContext(
                "test query", Collections.emptyList(), false, false);

        herddb.utils.Bytes encoded = f.computeNewValue(null, ctx, fixedTableContext(table));
        assertNotNull(encoded);
        // Cross-check by re-encoding through RecordSerializer directly with
        // the same input — the two paths must produce identical bytes.
        java.util.Map<String, Object> pk = new java.util.HashMap<>();
        pk.put("pk1", "okay");
        pk.put("pk2", new byte[]{1, 2, 3, 4});
        herddb.utils.Bytes direct = herddb.codec.RecordSerializer.serializePrimaryKeyRaw(
                pk, table, table.getPrimaryKey());
        org.junit.Assert.assertArrayEquals(direct.to_array(), encoded.to_array());
    }

    /**
     * Issue #391: a single-column {@code BYTEARRAY} PK takes a different
     * branch in {@code computeNewValue} (it calls {@code RecordSerializer.serialize}
     * rather than {@code serializePrimaryKeyRaw}). On the single-PK branch
     * a wrong-typed value reaches {@code serialize} and surfaces as a raw
     * {@code ClassCastException} both pre-PR and post-PR, so we just pin
     * the existing contract here.
     */
    @Test
    public void testWrongTypedSingleColumnBytearrayPkSurfacesClassCast() {
        Table table = Table.builder()
                .name("t")
                .column("pk", ColumnTypes.BYTEARRAY)
                .column("v", ColumnTypes.LONG)
                .primaryKey("pk")
                .build();

        CompiledSQLExpression pkExpr = (bean, ctx) -> Integer.valueOf(42);

        SQLRecordKeyFunction f = new SQLRecordKeyFunction(
                Collections.singletonList("pk"),
                Collections.<CompiledSQLExpression>singletonList(pkExpr),
                table);

        SQLStatementEvaluationContext ctx = new SQLStatementEvaluationContext(
                "test query", Collections.emptyList(), false, false);

        // Either a raw ClassCastException (legacy) or a wrapped
        // StatementExecutionException is acceptable on this branch — the
        // important property is that it is one of the two and the worker
        // does not silently swallow the failure.
        try {
            f.computeNewValue(null, ctx, fixedTableContext(table));
            fail("Expected an exception for wrong-typed single-column BYTEARRAY PK");
        } catch (ClassCastException | StatementExecutionException expected) {
            // OK
        }
    }

    private static TableContext fixedTableContext(Table table) {
        return new TableContext() {
            @Override
            public byte[] computeNewPrimaryKeyValue() {
                throw new UnsupportedOperationException("not used by this test");
            }

            @Override
            public Table getTable() {
                return table;
            }
        };
    }
}
