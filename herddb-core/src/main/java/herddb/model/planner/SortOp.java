/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0
direction
 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package herddb.model.planner;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.core.MaterializedRecordSet;
import herddb.core.SimpleDataScanner;
import herddb.core.TableSpaceManager;
import herddb.model.Column;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.TupleComparator;
import herddb.model.commands.ScanStatement;
import herddb.model.planner.ProjectOp.IdentityProjection;
import herddb.model.planner.ProjectOp.ZeroCopyProjection;
import herddb.utils.DataAccessor;
import herddb.utils.SQLRecordPredicateFunctions;
import herddb.utils.Wrapper;
import java.util.Arrays;

/**
 * Generic Sort
 *
 * @author eolivelli
 */
@SuppressFBWarnings(value = "EI_EXPOSE_REP2")
public class SortOp implements PlannerOp, TupleComparator {

    private final PlannerOp input;
    private final boolean[] directions;
    private final boolean[] nullLastDirections;
    private final int[] fields;
    private boolean onlyPrimaryKeyAndAscending;

    public SortOp(PlannerOp input, boolean[] directions, int[] fields, boolean[] nullLastDirections) {
        this.input = input.optimize();
        this.directions = directions;
        this.fields = fields;
        this.nullLastDirections = nullLastDirections;
    }

    @Override
    public String getTablespace() {
        return input.getTablespace();
    }

    @Override
    public StatementExecutionResult execute(
            TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext, StatementEvaluationContext context, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        // TODO merge projection + scan + sort + limit
        StatementExecutionResult input = this.input.execute(tableSpaceManager, transactionContext, context, lockRequired, forWrite);
        ScanResult downstreamScanResult = (ScanResult) input;
        final DataScanner inputScanner = downstreamScanResult.dataScanner;

        try (DataScanner dataScanner = inputScanner) {
            MaterializedRecordSet recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
                    .createRecordSet(inputScanner.getFieldNames(),
                            inputScanner.getSchema());
            while (dataScanner.hasNext()) {
                DataAccessor row = dataScanner.next();
                recordSet.add(row);
            }
            recordSet.writeFinished();
            recordSet.sort(this);
            SimpleDataScanner result = new SimpleDataScanner(downstreamScanResult.dataScanner.getTransaction(), recordSet);
            return new ScanResult(downstreamScanResult.transactionId, result);
        } catch (DataScannerException ex) {
            throw new StatementExecutionException(ex);
        }
    }

    @Override
    public PlannerOp optimize() {
        if (input instanceof BindableTableScanOp) {
            BindableTableScanOp op = (BindableTableScanOp) input;
            // we can change the statement, this node will be lost and the tablescan too
            ScanStatement statement = op.getStatement();
            statement.setComparator(this);

            if (fields.length == 1 && directions[0]) {
                Table tableDef = statement.getTableDef();
                if (tableDef.getPrimaryKey().length == 1) {
                    if (statement.getProjection() != null && statement.getProjection() instanceof ZeroCopyProjection) {
                        ZeroCopyProjection zeroCopyProjection = (ZeroCopyProjection) statement.getProjection();
                        int index = zeroCopyProjection.mapPosition(fields[0]);
                        Column col = tableDef.resolveColumName(index);
                        if (col.name.equals(tableDef.getPrimaryKey()[0])) {
                            this.onlyPrimaryKeyAndAscending = true;
                        }
                    } else if (statement.getProjection() != null && statement.getProjection() instanceof IdentityProjection) {
                        Column col = tableDef.resolveColumName(fields[0]);
                        if (col.name.equals(tableDef.getPrimaryKey()[0])) {
                            this.onlyPrimaryKeyAndAscending = true;
                        }
                    }
                }
            }
            return new SortedBindableTableScanOp(statement);
        } else if (input instanceof TableScanOp) {
            TableScanOp op = (TableScanOp) input;
            // we can change the statement, this node will be lost and the tablescan too
            ScanStatement statement = op.getStatement();
            statement.setComparator(this);

            if (fields.length == 1 && directions[0]) {
                Table tableDef = statement.getTableDef();
                if (tableDef.getPrimaryKey().length == 1) {
                    if (statement.getProjection() != null && statement.getProjection() instanceof ZeroCopyProjection) {
                        ZeroCopyProjection zeroCopyProjection = (ZeroCopyProjection) statement.getProjection();
                        int index = zeroCopyProjection.mapPosition(fields[0]);
                        Column col = tableDef.resolveColumName(index);
                        if (col.name.equals(tableDef.getPrimaryKey()[0])) {
                            this.onlyPrimaryKeyAndAscending = true;
                        }
                    } else if (statement.getProjection() != null && statement.getProjection() instanceof IdentityProjection) {
                        Column col = tableDef.resolveColumName(fields[0]);
                        if (col.name.equals(tableDef.getPrimaryKey()[0])) {
                            this.onlyPrimaryKeyAndAscending = true;
                        }
                    }
                }
            }
            return new SortedTableScanOp(statement);
        }
        return this;
    }

    @Override
    public boolean isOnlyPrimaryKeyAndAscending() {
        return onlyPrimaryKeyAndAscending;
    }

    @Override
    @SuppressFBWarnings("RV_NEGATING_RESULT_OF_COMPARETO")
    public int compare(DataAccessor o1, DataAccessor o2) {
        for (int i = 0; i < fields.length; i++) {
            int index = fields[i];
            Object value1 = o1.get(index);
            Object value2 = o2.get(index);
            int result = compareValues(i, value1, value2);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    /**
     * Antisymmetric three-way comparator that honours both
     * {@code ASC}/{@code DESC} and {@code NULLS FIRST}/{@code NULLS LAST}.
     *
     * <p>The previous implementation collapsed both
     * {@code (null, x)} and {@code (x, null)} pairs into the same
     * non-zero answer (always {@code -1} for {@code NULLS FIRST} or
     * {@code +1} for {@code NULLS LAST}), which violates
     * {@link Comparator}'s antisymmetry contract
     * ({@code compare(a, b) == -compare(b, a)} when neither is zero).
     * That meant the sort result depended on the order in which the
     * sort algorithm happened to compare elements, and downstream
     * iteration order (e.g. of the {@link java.util.concurrent.ConcurrentHashMap}
     * that backs the table) could swing the visible result. Issue #429
     * surfaced this by changing {@link CompareBytesUtils#hashCode} (the
     * stronger mix changes CHM bin distribution and so the iteration
     * order), but the bug pre-dates this change and is masked on
     * master only by the pre-existing iteration order being lucky.
     *
     * <p>The contract we now implement is:
     * <ul>
     *   <li>{@code (null, null) == 0} regardless of NULLS FIRST/LAST.</li>
     *   <li>For {@code NULLS FIRST}, a {@code null} compares less than
     *       any non-null value, so {@code (null, x) < 0}
     *       and {@code (x, null) > 0}.</li>
     *   <li>For {@code NULLS LAST}, a {@code null} compares greater
     *       than any non-null value, so {@code (null, x) > 0}
     *       and {@code (x, null) < 0}.</li>
     *   <li>{@code ASC}/{@code DESC} flips the sign only for the
     *       non-null/non-null case &mdash; the position of {@code null}
     *       is purely a function of NULLS FIRST/LAST, independent of
     *       direction. (PostgreSQL and Calcite agree on this.)</li>
     * </ul>
     */
    private int compareValues(int i, Object value1, Object value2) throws IllegalStateException {
        final boolean v1Null = value1 == null;
        final boolean v2Null = value2 == null;
        if (v1Null && v2Null) {
            return 0;
        }
        final boolean nullLast = nullLastDirections[i];
        if (v1Null) {
            // NULLS LAST: null is greater (sort to end). NULLS FIRST:
            // null is less (sort to start).
            return nullLast ? 1 : -1;
        }
        if (v2Null) {
            return nullLast ? -1 : 1;
        }
        // Neither side is null: regular comparison with ASC/DESC sign.
        final SQLRecordPredicateFunctions.CompareResult res =
                SQLRecordPredicateFunctions.compareConsiderNull(value1, value2);
        final int delta;
        switch (res) {
            case EQUALS:
                return 0;
            case GREATER:
                delta = 1;
                break;
            case MINOR:
                delta = -1;
                break;
            default:
                // Should be unreachable: we already handled the null cases.
                throw new IllegalStateException(res + "");
        }
        return directions[i] ? delta : -delta;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        T unwrapped = input.unwrap(clazz);
        if (unwrapped != null) {
            return unwrapped;
        }
        return Wrapper.unwrap(this, clazz);
    }

    @Override
    public String toString() {
        return "SortOp{fields=" + Arrays.toString(fields) + ",dirs=" + Arrays.toString(directions) + ",nullDirs=" + Arrays.toString(nullLastDirections) + ",onlyPrimaryKeyAndAscending=" + onlyPrimaryKeyAndAscending
                + "\ninput=" + input + '}';
    }

    @Override
    public Column[] getOutputSchema() {
        return input.getOutputSchema();
    }

    public PlannerOp getInput() {
        return input;
    }

    public boolean[] getDirections() {
        return directions;
    }

    public int[] getFields() {
        return fields;
    }

    public boolean[] getNullLastDirections() {
        return nullLastDirections;
    }

}
