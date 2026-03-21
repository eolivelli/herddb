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
import herddb.model.TransactionContext;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.DataAccessor;
import herddb.utils.Wrapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Sort with LIMIT optimization using a bounded PriorityQueue (top-K).
 * Instead of materializing all rows and sorting, maintains only the
 * top (limit + offset) rows in a heap.
 */
public class LimitedSortOp implements PlannerOp {

    private final PlannerOp input;
    private final SortOp comparator;
    private final CompiledSQLExpression maxRows;
    private final CompiledSQLExpression offset;

    public LimitedSortOp(PlannerOp input, SortOp comparator,
                          CompiledSQLExpression maxRows, CompiledSQLExpression offset) {
        this.input = input;
        this.comparator = comparator;
        this.maxRows = maxRows;
        this.offset = offset;
    }

    @Override
    public String getTablespace() {
        return input.getTablespace();
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
    public Column[] getOutputSchema() {
        return input.getOutputSchema();
    }

    @Override
    public StatementExecutionResult execute(
            TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext,
            StatementEvaluationContext context,
            boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        int limit = maxRows == null ? -1 : ((Number) maxRows.evaluate(DataAccessor.NULL, context)).intValue();
        int off = offset == null ? 0 : ((Number) offset.evaluate(DataAccessor.NULL, context)).intValue();

        StatementExecutionResult inputResult = this.input.execute(
                tableSpaceManager, transactionContext, context, lockRequired, forWrite);
        ScanResult downstreamScanResult = (ScanResult) inputResult;
        final DataScanner inputScanner = downstreamScanResult.dataScanner;

        try (DataScanner dataScanner = inputScanner) {
            if (limit <= 0) {
                // No effective limit — fall back to full materialization + sort
                return fullSort(tableSpaceManager, downstreamScanResult, dataScanner);
            }

            int capacity = limit + off;
            // Reversed comparator: the "worst" element sits at the head of the queue
            // so it gets evicted first when the queue exceeds capacity
            Comparator<DataAccessor> reversed = (a, b) -> comparator.compare(b, a);
            PriorityQueue<DataAccessor> heap = new PriorityQueue<>(capacity + 1, reversed);

            while (dataScanner.hasNext()) {
                DataAccessor row = dataScanner.next();
                heap.offer(row);
                if (heap.size() > capacity) {
                    heap.poll();
                }
            }

            // Drain heap and sort in correct order
            List<DataAccessor> sorted = new ArrayList<>(heap.size());
            while (!heap.isEmpty()) {
                sorted.add(heap.poll());
            }
            Collections.sort(sorted, comparator);

            // Apply offset: skip first 'off' rows
            int fromIndex = Math.min(off, sorted.size());
            int toIndex = Math.min(off + limit, sorted.size());

            MaterializedRecordSet recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
                    .createRecordSet(inputScanner.getFieldNames(), inputScanner.getSchema());
            for (int i = fromIndex; i < toIndex; i++) {
                recordSet.add(sorted.get(i));
            }
            recordSet.writeFinished();

            SimpleDataScanner result = new SimpleDataScanner(
                    downstreamScanResult.dataScanner.getTransaction(), recordSet);
            return new ScanResult(downstreamScanResult.transactionId, result);
        } catch (DataScannerException ex) {
            throw new StatementExecutionException(ex);
        }
    }

    private StatementExecutionResult fullSort(
            TableSpaceManager tableSpaceManager,
            ScanResult downstreamScanResult,
            DataScanner dataScanner
    ) throws DataScannerException {
        MaterializedRecordSet recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
                .createRecordSet(dataScanner.getFieldNames(), dataScanner.getSchema());
        while (dataScanner.hasNext()) {
            DataAccessor row = dataScanner.next();
            recordSet.add(row);
        }
        recordSet.writeFinished();
        recordSet.sort(comparator);
        SimpleDataScanner result = new SimpleDataScanner(
                downstreamScanResult.dataScanner.getTransaction(), recordSet);
        return new ScanResult(downstreamScanResult.transactionId, result);
    }

    @Override
    public String toString() {
        return String.format("LimitedSortOp{maxRows=%s, offset=%s, comparator=%s, input={%s}}",
                maxRows, offset, comparator, input);
    }
}
