package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.streaming.AppendOnlySpillList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class ConsistencyPartitionProcessor {

    private final ConsistencyRule rule;
    private final List<String> sourceOrder;
    private final DataComparator comparator;
    private final ConflictResolver resolver;
    private final ComparisonResult result;
    private final AppendOnlySpillList<DifferenceRecord> differences;
    private final AppendOnlySpillList<DifferenceRecord> resolvedDifferences;
    private final AppendOnlySpillList<Map<String, Object>> resolvedRows;

    ConsistencyPartitionProcessor(ConsistencyRule rule,
                                  List<String> sourceOrder,
                                  DataComparator comparator,
                                  ConflictResolver resolver,
                                  ComparisonResult result,
                                  AppendOnlySpillList<DifferenceRecord> differences,
                                  AppendOnlySpillList<DifferenceRecord> resolvedDifferences,
                                  AppendOnlySpillList<Map<String, Object>> resolvedRows) {
        this.rule = rule;
        this.sourceOrder = sourceOrder;
        this.comparator = comparator;
        this.resolver = resolver;
        this.result = result;
        this.differences = differences;
        this.resolvedDifferences = resolvedDifferences;
        this.resolvedRows = resolvedRows;
    }

    List<DifferenceRecord> processGroups(Map<String, LinkedHashMap<String, Map<String, Object>>> groups) {
        List<DifferenceRecord> resolvedBatch = new ArrayList<>();
        for (Map.Entry<String, LinkedHashMap<String, Map<String, Object>>> entry : groups.entrySet()) {
            result.setTotalRecords(result.getTotalRecords() + 1);
            DifferenceRecord difference = comparator.compareKeyGroup(
                    entry.getKey(),
                    rule.getMatchKeys(),
                    sourceOrder,
                    entry.getValue()
            );
            if (difference == null) {
                result.incrementConsistent();
                continue;
            }

            result.incrementInconsistent();
            for (String field : difference.getDifferences().keySet()) {
                result.addFieldDiscrepancy(field, 1);
            }
            differences.add(difference);

            if (resolver != null && resolver.canResolve(difference)) {
                difference.setResolutionResult(resolver.resolve(difference));
                result.incrementResolved();
                resolvedDifferences.add(difference);
                resolvedBatch.add(difference);

                if (difference.getResolutionResult() != null && difference.getResolutionResult().getResolvedValues() != null) {
                    Map<String, Object> resolvedRow = new LinkedHashMap<>(difference.getResolutionResult().getResolvedValues());
                    if (difference.getMatchKeyValues() != null) {
                        resolvedRow.putAll(difference.getMatchKeyValues());
                    }
                    resolvedRows.add(resolvedRow);
                }
            }
        }
        return resolvedBatch;
    }
}
