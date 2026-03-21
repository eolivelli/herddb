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

package herddb.model.planner;

import herddb.codec.RecordSerializer;
import herddb.core.AbstractIndexManager;
import herddb.core.MaterializedRecordSet;
import herddb.core.SimpleDataScanner;
import herddb.core.TableSpaceManager;
import herddb.index.vector.VectorIndexManager;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.GetResult;
import herddb.model.Predicate;
import herddb.model.Projection;
import herddb.model.Record;
import herddb.model.DataScannerException;
import herddb.model.LimitedDataScanner;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.model.TransactionContext;
import herddb.model.commands.GetStatement;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.Wrapper;
import java.util.List;
import java.util.Map;

/**
 * Uses the VectorIndexManager (jvector) for ORDER BY ann_of() queries.
 * Falls back to brute-force execution if no vector index is found.
 *
 * @author eolivelli
 */
public class VectorANNScanOp implements PlannerOp {

    private final String tableSpace;
    private final Table tableDef;
    private final String columnName;
    private final CompiledSQLExpression queryVectorExpr;
    private final PlannerOp fallback;
    private final Predicate predicate;
    private final Projection scanProjection;
    private final Projection innerProjection;
    private final CompiledSQLExpression limitExpr;
    private final CompiledSQLExpression offsetExpr;

    public VectorANNScanOp(
            String tableSpace,
            Table tableDef,
            String columnName,
            CompiledSQLExpression queryVectorExpr,
            PlannerOp fallback,
            Predicate predicate,
            Projection scanProjection,
            Projection innerProjection
    ) {
        this(tableSpace, tableDef, columnName, queryVectorExpr, fallback,
                predicate, scanProjection, innerProjection, null, null);
    }

    private VectorANNScanOp(
            String tableSpace,
            Table tableDef,
            String columnName,
            CompiledSQLExpression queryVectorExpr,
            PlannerOp fallback,
            Predicate predicate,
            Projection scanProjection,
            Projection innerProjection,
            CompiledSQLExpression limitExpr,
            CompiledSQLExpression offsetExpr
    ) {
        this.tableSpace = tableSpace;
        this.tableDef = tableDef;
        this.columnName = columnName;
        this.queryVectorExpr = queryVectorExpr;
        this.fallback = fallback;
        this.predicate = predicate;
        this.scanProjection = scanProjection;
        this.innerProjection = innerProjection;
        this.limitExpr = limitExpr;
        this.offsetExpr = offsetExpr;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public boolean hasLimit() {
        return limitExpr != null;
    }

    /**
     * Returns a new VectorANNScanOp with limit/offset pushed down into the scan.
     */
    public VectorANNScanOp withLimit(CompiledSQLExpression limitExpr, CompiledSQLExpression offsetExpr) {
        return new VectorANNScanOp(tableSpace, tableDef, columnName, queryVectorExpr,
                fallback, predicate, scanProjection, innerProjection, limitExpr, offsetExpr);
    }

    @Override
    public String getTablespace() {
        return tableSpace;
    }

    @Override
    public Column[] getOutputSchema() {
        return innerProjection != null ? innerProjection.getColumns() : tableDef.columns;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return Wrapper.unwrap(this, clazz);
    }

    @Override
    public StatementExecutionResult execute(
            TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext,
            StatementEvaluationContext context,
            boolean lockRequired,
            boolean forWrite
    ) throws StatementExecutionException {
        VectorIndexManager vim = findVectorIndex(tableSpaceManager);
        if (vim == null) {
            if (fallback != null) {
                StatementExecutionResult fallbackResult = fallback.execute(
                        tableSpaceManager, transactionContext, context, lockRequired, forWrite);
                // When limit was pushed into this op (LimitOp removed), apply
                // limit/offset to the fallback result as well.
                if (limitExpr != null) {
                    try {
                        ScanResult sr = (ScanResult) fallbackResult;
                        int lim = ((Number) limitExpr.evaluate(DataAccessor.NULL, context)).intValue();
                        int off = offsetExpr != null
                                ? ((Number) offsetExpr.evaluate(DataAccessor.NULL, context)).intValue() : 0;
                        return new ScanResult(sr.transactionId,
                                new LimitedDataScanner(sr.dataScanner, lim, off, context));
                    } catch (DataScannerException ex) {
                        throw new StatementExecutionException(ex);
                    }
                }
                return fallbackResult;
            } else {
                throw new StatementExecutionException("No vector index found for column '" + columnName + "' on table " + tableDef.name);
            }
        }

        Object qvObj = queryVectorExpr.evaluate(DataAccessor.NULL, context);
        float[] queryVector = (float[]) RecordSerializer.convert(ColumnTypes.FLOATARRAY, qvObj);

        int topK;
        int limit = -1;
        int offset = 0;
        if (limitExpr != null) {
            limit = ((Number) limitExpr.evaluate(DataAccessor.NULL, context)).intValue();
            offset = offsetExpr != null
                    ? ((Number) offsetExpr.evaluate(DataAccessor.NULL, context)).intValue() : 0;
            topK = limit + offset;
            if (topK <= 0) {
                topK = Integer.MAX_VALUE;
            }
        } else {
            topK = Integer.MAX_VALUE;
        }

        List<Map.Entry<Bytes, Float>> annResults = vim.search(queryVector, topK);

        Transaction transaction = tableSpaceManager.getTransaction(transactionContext.transactionId);
        String[] fieldNames = (innerProjection != null) ? innerProjection.getFieldNames()
                : Column.buildFieldNamesList(tableDef.columns);
        Column[] cols = (innerProjection != null) ? innerProjection.getColumns() : tableDef.columns;
        MaterializedRecordSet recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
                .createRecordSet(fieldNames, cols);

        int skipped = 0;
        int added = 0;
        for (Map.Entry<Bytes, Float> entry : annResults) {
            Bytes pk = entry.getKey();
            GetStatement get = new GetStatement(tableSpace, tableDef.name, pk, null, false);
            GetResult getResult = tableSpaceManager.getDbmanager().get(get, context, transactionContext);
            if (!getResult.found()) {
                continue;
            }
            Record record = getResult.getRecord();
            if (predicate != null && !predicate.evaluate(record, context)) {
                continue;
            }
            if (limitExpr != null && skipped < offset) {
                skipped++;
                continue;
            }
            DataAccessor fullRow = record.getDataAccessor(tableDef);
            DataAccessor scanRow = (scanProjection != null) ? scanProjection.map(fullRow, context) : fullRow;
            DataAccessor projectedRow = (innerProjection != null) ? innerProjection.map(scanRow, context) : scanRow;
            recordSet.add(projectedRow);
            added++;
            if (limit > 0 && added >= limit) {
                break;
            }
        }

        recordSet.writeFinished();
        return new ScanResult(transactionContext.transactionId, new SimpleDataScanner(transaction, recordSet));
    }

    private VectorIndexManager findVectorIndex(TableSpaceManager tableSpaceManager) {
        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(tableDef.name);
        if (indexes == null) {
            return null;
        }
        for (AbstractIndexManager aim : indexes.values()) {
            if (aim instanceof VectorIndexManager) {
                VectorIndexManager v = (VectorIndexManager) aim;
                for (String col : v.getIndex().columnNames) {
                    if (col.equalsIgnoreCase(columnName)) {
                        return v;
                    }
                }
            }
        }
        return null;
    }
}
