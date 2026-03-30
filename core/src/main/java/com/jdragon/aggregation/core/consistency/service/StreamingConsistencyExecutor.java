package com.jdragon.aggregation.core.consistency.service;

import com.alibaba.fastjson.TypeReference;
import com.jdragon.aggregation.core.consistency.i18n.MessageResource;
import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import com.jdragon.aggregation.core.consistency.model.UpdatePlan;
import com.jdragon.aggregation.core.consistency.model.UpdateResult;
import com.jdragon.aggregation.core.streaming.AppendOnlySpillList;
import com.jdragon.aggregation.core.streaming.CompositeKey;
import com.jdragon.aggregation.core.streaming.PartitionReader;
import com.jdragon.aggregation.core.streaming.PartitionedSpillStore;
import com.jdragon.aggregation.core.streaming.PartitionedSpillStore.PartitionRow;
import com.jdragon.aggregation.core.streaming.StreamExecutionOptions;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

@Slf4j
public class StreamingConsistencyExecutor {

    private static final int MAX_REBALANCE_DEPTH = 3;

    private final DataSourcePluginManager pluginManager;
    private final ResultRecorder resultRecorder;

    public StreamingConsistencyExecutor(DataSourcePluginManager pluginManager, ResultRecorder resultRecorder) {
        this.pluginManager = pluginManager;
        this.resultRecorder = resultRecorder;
    }

    public ComparisonResult execute(ConsistencyRule rule, DataFetcher fetcher) {
        ComparisonResult result = new ComparisonResult();
        result.setResultId("result-" + System.currentTimeMillis());
        result.setRuleId(rule.getRuleId());
        result.setStatus(ComparisonResult.Status.RUNNING);

        StreamExecutionOptions options = StreamExecutionOptions.fromConsistencyRule(rule);
        String jobId = sanitizeJobId(rule.getRuleId());
        List<String> sourceOrder = rule.getDataSources().stream()
                .map(DataSourceConfig::getSourceId)
                .collect(Collectors.toList());

        AppendOnlySpillList<DifferenceRecord> differences = new AppendOnlySpillList<>(
                "consistency-differences",
                DifferenceRecord.class,
                options.getSpillPath()
        );
        AppendOnlySpillList<DifferenceRecord> resolvedDifferences = new AppendOnlySpillList<>(
                "consistency-resolved-differences",
                DifferenceRecord.class,
                options.getSpillPath()
        );
        Type resolvedRowType = new TypeReference<Map<String, Object>>() {}.getType();
        AppendOnlySpillList<Map<String, Object>> resolvedRows = new AppendOnlySpillList<>(
                "consistency-resolved-rows",
                resolvedRowType,
                options.getSpillPath()
        );
        result.setResolvedRows(resolvedRows);

        Map<String, LongAdder> sourceCounts = new LinkedHashMap<>();
        PartitionedSpillStore spillStore = new PartitionedSpillStore(jobId, options.getSpillPath(), options.getPartitionCount(), options.isKeepTempFiles());

        try {
            int parallelism = Boolean.TRUE.equals(rule.getParallelFetch())
                    ? options.getParallelSourceCount()
                    : 1;
            fetcher.scanSources(rule.getDataSources(), parallelism, (sourceId, row) -> {
                sourceCounts.computeIfAbsent(sourceId, key -> new LongAdder()).increment();
                spillStore.append(sourceId, CompositeKey.fromRecord(row, rule.getMatchKeys()), row);
            });
            spillStore.close();

            result.setSourceDataCount(sourceCounts.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue().sum(),
                            (left, right) -> left,
                            LinkedHashMap::new
                    )));

            OutputConfig outputConfig = rule.getOutputConfig() != null ? rule.getOutputConfig() : new OutputConfig();
            MessageResource messages = MessageResource.forLanguage(outputConfig.getReportLanguage());
            DataComparator comparator = new DataComparator(rule.getToleranceThreshold(), rule.getCompareFields(), messages);
            ConflictResolver resolver = buildResolver(rule);
            ConsistencyPartitionProcessor processor = new ConsistencyPartitionProcessor(
                    rule,
                    sourceOrder,
                    comparator,
                    resolver,
                    result,
                    differences,
                    resolvedDifferences,
                    resolvedRows
            );

            DataSourceConfig targetDataSource = findTargetDataSource(rule);
            UpdateResult mergedUpdateResult = null;
            UpdatePlanningStrategy updatePlanningStrategy = targetDataSource != null
                    ? UpdatePlanningStrategyFactory.createStrategy(rule)
                    : null;

            for (int partition = 0; partition < spillStore.getPartitionCount(); partition++) {
                if (!spillStore.partitionExists(partition)) {
                    continue;
                }
                List<DifferenceRecord> partitionResolved = processPartitionPath(
                        spillStore.getPartitionPath(partition),
                        options,
                        rule,
                        sourceOrder,
                        processor,
                        spillStore.getPartitionCount(),
                        partition,
                        0
                );
                if (rule.getAutoApplyResolutions() && targetDataSource != null && !partitionResolved.isEmpty()) {
                    applyUpdatePlans(partitionResolved, rule, targetDataSource, updatePlanningStrategy);
                    UpdateExecutor updateExecutor = new UpdateExecutor(pluginManager);
                    UpdateResult partitionUpdate = updateExecutor.executeUpdates(
                            targetDataSource,
                            partitionResolved,
                            rule.getMatchKeys(),
                            targetDataSource.getFieldMappings(),
                            rule.getUpdateBufferSize() != null ? rule.getUpdateBufferSize() : 100,
                            rule.getUpdateRetryAttempts() != null ? rule.getUpdateRetryAttempts() : 0,
                            rule.getUpdateRetryDelayMs() != null ? rule.getUpdateRetryDelayMs() : 1000L,
                            rule.getUpdateRetryBackoffMultiplier() != null ? rule.getUpdateRetryBackoffMultiplier() : 1.5,
                            Boolean.TRUE.equals(rule.getValidateBeforeUpdate()),
                            Boolean.TRUE.equals(rule.getAllowInsert()),
                            Boolean.TRUE.equals(rule.getAllowDelete()),
                            Boolean.TRUE.equals(rule.getSkipUnchangedUpdates())
                    );
                    mergedUpdateResult = mergeUpdateResult(mergedUpdateResult, partitionUpdate);
                }
            }

            result.setConsistentRecords(result.getTotalRecords() - result.getInconsistentRecords());
            result.setStatus(result.getInconsistentRecords() == 0
                    ? ComparisonResult.Status.SUCCESS
                    : ComparisonResult.Status.PARTIAL_SUCCESS);

            if (mergedUpdateResult != null) {
                mergedUpdateResult.setResultId(result.getResultId());
                result.setUpdateResult(mergedUpdateResult);
            }

            recordResults(result, differences, resolvedDifferences, rule);
            result.setDifferenceRecords(differences);
        } catch (Exception e) {
            log.error("Failed to execute streaming consistency rule: {}", rule.getRuleId(), e);
            result.setStatus(ComparisonResult.Status.FAILED);
            result.getSummary().put("error", e.getMessage());
        } finally {
            spillStore.cleanup();
        }

        return result;
    }

    private void applyUpdatePlans(List<DifferenceRecord> resolvedDifferences,
                                  ConsistencyRule rule,
                                  DataSourceConfig targetDataSource,
                                  UpdatePlanningStrategy updatePlanningStrategy) {
        if (resolvedDifferences == null || resolvedDifferences.isEmpty()
                || updatePlanningStrategy == null || targetDataSource == null) {
            return;
        }
        for (DifferenceRecord differenceRecord : resolvedDifferences) {
            UpdatePlan updatePlan = updatePlanningStrategy.plan(differenceRecord, rule, targetDataSource);
            differenceRecord.setUpdatePlan(updatePlan);
        }
    }

    private List<DifferenceRecord> processPartitionPath(Path partitionPath,
                                                        StreamExecutionOptions options,
                                                        ConsistencyRule rule,
                                                        List<String> sourceOrder,
                                                        ConsistencyPartitionProcessor processor,
                                                        int currentPartitionCount,
                                                        int partition,
                                                        int depth) throws IOException {
        Map<String, LinkedHashMap<String, Map<String, Object>>> groups = new LinkedHashMap<>();
        Map<String, Integer> duplicateCount = new LinkedHashMap<>();
        PartitionedSpillStore[] rebalanceStoreHolder = new PartitionedSpillStore[1];

        try (PartitionReader reader = new PartitionReader(partitionPath)) {
            reader.readAll(row -> {
                PartitionedSpillStore rebalanceStore = rebalanceStoreHolder[0];
                if (rebalanceStore != null) {
                    rebalanceStore.append(row);
                    return;
                }

                LinkedHashMap<String, Map<String, Object>> sourceRows =
                        groups.computeIfAbsent(row.getKey(), key -> new LinkedHashMap<>());
                if (!sourceRows.containsKey(row.getSourceId())) {
                    sourceRows.put(row.getSourceId(), row.getRow());
                } else {
                    duplicateCount.merge(row.getSourceId(), 1, Integer::sum);
                }

                if (groups.size() > options.getMaxKeysPerPartition() && depth < MAX_REBALANCE_DEPTH) {
                    int childPartitionCount = options.getRebalancePartitionCount(currentPartitionCount);
                    PartitionedSpillStore childStore = new PartitionedSpillStore(
                            sanitizeJobId(rule.getRuleId()) + "-p" + partition + "-d" + depth,
                            options.getSpillPath(),
                            childPartitionCount,
                            options.isKeepTempFiles()
                    );
                    rebalanceStoreHolder[0] = childStore;
                    for (Map.Entry<String, LinkedHashMap<String, Map<String, Object>>> groupEntry : groups.entrySet()) {
                        CompositeKey key = CompositeKey.fromEncoded(groupEntry.getKey(), rule.getMatchKeys());
                        for (Map.Entry<String, Map<String, Object>> sourceEntry : groupEntry.getValue().entrySet()) {
                            childStore.append(sourceEntry.getKey(), key, sourceEntry.getValue());
                        }
                    }
                    groups.clear();
                }
            });
        }

        if (!duplicateCount.isEmpty()) {
            resultDuplicateSummary(processor, duplicateCount);
        }

        PartitionedSpillStore.cleanupConsumedPartition(partitionPath, options.isKeepTempFiles(), null);

        PartitionedSpillStore rebalanceStore = rebalanceStoreHolder[0];
        if (rebalanceStore != null) {
            rebalanceStore.close();
            try {
                List<DifferenceRecord> resolved = new ArrayList<>();
                for (int childPartition = 0; childPartition < rebalanceStore.getPartitionCount(); childPartition++) {
                    if (!rebalanceStore.partitionExists(childPartition)) {
                        continue;
                    }
                    resolved.addAll(processPartitionPath(
                            rebalanceStore.getPartitionPath(childPartition),
                            options,
                            rule,
                            sourceOrder,
                            processor,
                            rebalanceStore.getPartitionCount(),
                            childPartition,
                            depth + 1
                    ));
                }
                return resolved;
            } finally {
                rebalanceStore.cleanup();
            }
        }

        return processor.processGroups(groups);
    }

    private void resultDuplicateSummary(ConsistencyPartitionProcessor processor, Map<String, Integer> duplicateCount) {
        if (duplicateCount.isEmpty()) {
            return;
        }
        int duplicates = duplicateCount.values().stream().mapToInt(Integer::intValue).sum();
        log.info("Ignored {} duplicate rows after first-row semantics", duplicates);
    }

    private ConflictResolver buildResolver(ConsistencyRule rule) {
        if (rule.getConflictResolutionStrategy() == null
                || rule.getConflictResolutionStrategy().name().equals("NO_RESOLVE")) {
            return null;
        }
        return ConflictResolverFactory.createResolver(
                rule.getConflictResolutionStrategy(),
                rule.getDataSources(),
                rule.getResolutionParams()
        );
    }

    private void recordResults(ComparisonResult result,
                               List<DifferenceRecord> allDifferences,
                               List<DifferenceRecord> resolvedDifferences,
                               ConsistencyRule rule) {

        if (rule != null) {
            if (rule.getCompareFields() != null) {
                result.getMetadata().put("compareFields", rule.getCompareFields());
            }
            if (rule.getMatchKeys() != null) {
                result.getMetadata().put("matchKeys", rule.getMatchKeys());
            }
        }

        resultRecorder.recordComparisonResult(result);

        if (allDifferences != null && !allDifferences.isEmpty()) {
            String differencesRecordPath = resultRecorder.recordDifferences(allDifferences);
            result.setDifferenceOutputAbsPath(differencesRecordPath);
            result.setDifferenceRecords(allDifferences);
        }

        if (resolvedDifferences != null && !resolvedDifferences.isEmpty()) {
            List<ResolutionResult> resolutionResults = resultRecorder.recordResolutionResults(result, resolvedDifferences);
            result.setResolutionResults(resolutionResults);
        }

        OutputConfig outputConfig = rule != null ? rule.getOutputConfig() : null;
        if (outputConfig == null || Boolean.TRUE.equals(outputConfig.getGenerateReport())) {
            String reportPath = resultRecorder.generateReport(result, allDifferences, outputConfig);
            result.setReportPath(reportPath);
        }
    }

    private UpdateResult mergeUpdateResult(UpdateResult current, UpdateResult incoming) {
        if (incoming == null) {
            return current;
        }
        if (current == null) {
            return incoming;
        }
        current.setTargetSourceId(current.getTargetSourceId() != null ? current.getTargetSourceId() : incoming.getTargetSourceId());
        current.setTotalUpdates(current.getTotalUpdates() + incoming.getTotalUpdates());
        current.setSuccessfulUpdates(current.getSuccessfulUpdates() + incoming.getSuccessfulUpdates());
        current.setFailedUpdates(current.getFailedUpdates() + incoming.getFailedUpdates());
        current.setInsertCount(current.getInsertCount() + incoming.getInsertCount());
        current.setUpdateCount(current.getUpdateCount() + incoming.getUpdateCount());
        current.setDeleteCount(current.getDeleteCount() + incoming.getDeleteCount());
        current.setSkipCount(current.getSkipCount() + incoming.getSkipCount());
        current.getOperationTypes().putAll(incoming.getOperationTypes());
        current.getFailures().addAll(incoming.getFailures());
        return current;
    }

    private DataSourceConfig findTargetDataSource(ConsistencyRule rule) {
        String targetSourceId = rule.getUpdateTargetSourceId();
        if (targetSourceId == null) {
            return null;
        }
        for (DataSourceConfig dataSource : rule.getDataSources()) {
            if (targetSourceId.equals(dataSource.getSourceId())) {
                return dataSource;
            }
        }
        return null;
    }

    private String sanitizeJobId(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "consistency";
        }
        return value.replaceAll("[^a-zA-Z0-9-_]", "_");
    }
}
