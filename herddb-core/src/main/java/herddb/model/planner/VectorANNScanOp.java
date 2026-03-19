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
        this.tableSpace = tableSpace;
        this.tableDef = tableDef;
        this.columnName = columnName;
        this.queryVectorExpr = queryVectorExpr;
        this.fallback = fallback;
        this.predicate = predicate;
        this.scanProjection = scanProjection;
        this.innerProjection = innerProjection;
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
                return fallback.execute(tableSpaceManager, transactionContext, context, lockRequired, forWrite);
            } else {
                throw new StatementExecutionException("No vector index found for column '" + columnName + "' on table " + tableDef.name);
            }
        }

        Object qvObj = queryVectorExpr.evaluate(DataAccessor.NULL, context);
        float[] queryVector = (float[]) RecordSerializer.convert(ColumnTypes.FLOATARRAY, qvObj);

        List<Map.Entry<Bytes, Float>> annResults = vim.search(queryVector, Integer.MAX_VALUE);

        Transaction transaction = tableSpaceManager.getTransaction(transactionContext.transactionId);
        String[] fieldNames = (innerProjection != null) ? innerProjection.getFieldNames()
                : Column.buildFieldNamesList(tableDef.columns);
        Column[] cols = (innerProjection != null) ? innerProjection.getColumns() : tableDef.columns;
        MaterializedRecordSet recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
                .createRecordSet(fieldNames, cols);

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
            DataAccessor fullRow = record.getDataAccessor(tableDef);
            DataAccessor scanRow = (scanProjection != null) ? scanProjection.map(fullRow, context) : fullRow;
            DataAccessor projectedRow = (innerProjection != null) ? innerProjection.map(scanRow, context) : scanRow;
            recordSet.add(projectedRow);
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
