package com.jdragon.aggregation.core.consistency.service;

import com.alibaba.fastjson.TypeReference;
import com.jdragon.aggregation.core.consistency.i18n.MessageResource;
import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import com.jdragon.aggregation.core.consistency.model.UpdateResult;
import com.jdragon.aggregation.core.sortmerge.AdaptiveMergeConfig;
import com.jdragon.aggregation.core.sortmerge.AdaptiveMergeCoordinator;
import com.jdragon.aggregation.core.sortmerge.OrderedKey;
import com.jdragon.aggregation.core.sortmerge.OrderedKeySchema;
import com.jdragon.aggregation.core.sortmerge.OrderedSourceCursor;
import com.jdragon.aggregation.core.sortmerge.OverflowBucketStore;
import com.jdragon.aggregation.core.sortmerge.SortMergeStats;
import com.jdragon.aggregation.core.streaming.AppendOnlySpillList;
import com.jdragon.aggregation.core.streaming.PartitionReader;
import com.jdragon.aggregation.core.streaming.PartitionedSpillStore;
import com.jdragon.aggregation.core.streaming.SpillGuard;
import com.jdragon.aggregation.core.streaming.SpillLimitExceededException;
import com.jdragon.aggregation.core.streaming.SourceRowScanner;
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
import java.util.stream.Collectors;

/**
 * consistency 的自适应 sort-merge 执行器。
 *
 * <p>key 在内存窗口内一旦可判定就立即参与比对；差异结果、resolved rows 等较大输出
 * 则放在 spill-backed list 中。若等待窗口溢出，剩余工作会退回既有 partition processor，
 * 以保持当前规则语义不变。
 */
@Slf4j
public class AdaptiveSortMergeConsistencyExecutor {

    private static final int MAX_REBALANCE_DEPTH = 3;

    private final DataSourcePluginManager pluginManager;
    private final ResultRecorder resultRecorder;

    public AdaptiveSortMergeConsistencyExecutor(DataSourcePluginManager pluginManager, ResultRecorder resultRecorder) {
        this.pluginManager = pluginManager;
        this.resultRecorder = resultRecorder;
    }

    public ComparisonResult execute(ConsistencyRule rule) {
        ComparisonResult result = new ComparisonResult();
        result.setResultId("result-" + System.currentTimeMillis());
        result.setRuleId(rule.getRuleId());
        result.setStatus(ComparisonResult.Status.RUNNING);

        StreamExecutionOptions options = StreamExecutionOptions.fromConsistencyRule(rule);
        AdaptiveMergeConfig adaptiveMergeConfig = rule.getAdaptiveMergeConfig();
        options.setRebalancePartitionMultiplier(adaptiveMergeConfig.getRebalancePartitionMultiplier());
        SpillGuard spillGuard = new SpillGuard(
                adaptiveMergeConfig.getMaxSpillBytes(),
                adaptiveMergeConfig.getMinFreeDiskBytes()
        );
        AppendOnlySpillList<DifferenceRecord> differences = new AppendOnlySpillList<DifferenceRecord>(
                "consistency-differences",
                DifferenceRecord.class,
                options.getSpillPath()
        );
        AppendOnlySpillList<DifferenceRecord> resolvedDifferences = new AppendOnlySpillList<DifferenceRecord>(
                "consistency-resolved-differences",
                DifferenceRecord.class,
                options.getSpillPath()
        );
        Type resolvedRowType = new TypeReference<Map<String, Object>>() {
        }.getType();
        AppendOnlySpillList<Map<String, Object>> resolvedRows = new AppendOnlySpillList<Map<String, Object>>(
                "consistency-resolved-rows",
                resolvedRowType,
                options.getSpillPath()
        );
        result.setResolvedRows(resolvedRows);

        List<String> sourceOrder = rule.getDataSources().stream()
                .map(DataSourceConfig::getSourceId)
                .collect(Collectors.toList());
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

        SourceRowScanner rowScanner = new SourceRowScanner(pluginManager);
        OrderedKeySchema keySchema = new OrderedKeySchema(rule.getMatchKeys(), adaptiveMergeConfig.getKeyTypes());
        List<OrderedSourceCursor> cursors = new ArrayList<OrderedSourceCursor>();
        for (DataSourceConfig dataSourceConfig : rule.getDataSources()) {
            cursors.add(new OrderedSourceCursor(
                    rowScanner,
                    dataSourceConfig,
                    keySchema,
                    rule.getMatchKeys(),
                    adaptiveMergeConfig.isPreferOrderedQuery(),
                    adaptiveMergeConfig
            ));
        }

        UpdateResult mergedUpdateResult = null;
        final UpdateResult[] directUpdateResultHolder = new UpdateResult[1];
        DataSourceConfig targetDataSource = findTargetDataSource(rule);
        UpdatePlanningStrategy updatePlanningStrategy = targetDataSource != null
                ? UpdatePlanningStrategyFactory.createStrategy(rule)
                : null;
        OverflowBucketStore overflowBucketStore = null;

        try {
            AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(
                    adaptiveMergeConfig,
                    keySchema,
                    sourceOrder,
                    spillGuard,
                    options.isKeepTempFiles()
            );
            AdaptiveMergeCoordinator.Result coordinatorResult = coordinator.execute(cursors,
                    new AdaptiveMergeCoordinator.ResolvedGroupHandler() {
                        @Override
                        public void handle(OrderedKey key, Map<String, Map<String, Object>> firstRowsBySource) {
                            Map<String, LinkedHashMap<String, Map<String, Object>>> groups = new LinkedHashMap<String, LinkedHashMap<String, Map<String, Object>>>();
                            LinkedHashMap<String, Map<String, Object>> sourceRows = new LinkedHashMap<String, Map<String, Object>>();
                            for (Map.Entry<String, Map<String, Object>> entry : firstRowsBySource.entrySet()) {
                                sourceRows.put(entry.getKey(), entry.getValue());
                            }
                            groups.put(key.getEncoded(), sourceRows);
                            List<DifferenceRecord> resolvedBatch = processor.processGroups(groups);
                            if (rule.getAutoApplyResolutions() && targetDataSource != null && !resolvedBatch.isEmpty()) {
                                applyUpdatePlans(resolvedBatch, rule, targetDataSource, updatePlanningStrategy);
                                UpdateExecutor updateExecutor = new UpdateExecutor(pluginManager);
                                UpdateResult partitionUpdate = updateExecutor.executeUpdates(
                                        targetDataSource,
                                        resolvedBatch,
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
                                synchronized (directUpdateResultHolder) {
                                    directUpdateResultHolder[0] = mergeUpdateResult(directUpdateResultHolder[0], partitionUpdate);
                                }
                            }
                        }
                    });
            overflowBucketStore = coordinatorResult.getOverflowBucketStore();
            SortMergeStats stats = coordinatorResult.getStats();
            result.setExecutionEngine(stats.getExecutionEngine());
            result.getSummary().put("executionEngine", stats.getExecutionEngine());
            result.getSummary().put("mergeResolvedKeyCount", stats.getMergeResolvedKeyCount());
            result.getSummary().put("mergeSpilledKeyCount", stats.getMergeSpilledKeyCount());
            result.getSummary().put("duplicateIgnoredCount", stats.getDuplicateIgnoredCount());
            result.getSummary().put("localReorderedGroupCount", stats.getLocalReorderedGroupCount());
            result.getSummary().put("orderRecoveryCount", stats.getOrderRecoveryCount());
            if (stats.getFallbackReason() != null) {
                result.getSummary().put("fallbackReason", stats.getFallbackReason());
            }
            result.setSourceDataCount(new LinkedHashMap<String, Number>(stats.getSourceRecordCounts()));

            if (directUpdateResultHolder[0] != null) {
                mergedUpdateResult = mergeUpdateResult(mergedUpdateResult, directUpdateResultHolder[0]);
            }

            if (overflowBucketStore != null && overflowBucketStore.getSpilledRows() > 0) {
                mergedUpdateResult = processOverflowStore(
                        overflowBucketStore,
                        options,
                        rule,
                        sourceOrder,
                        processor,
                        targetDataSource,
                        updatePlanningStrategy,
                        mergedUpdateResult,
                        spillGuard
                );
            }

            result.setConsistentRecords(result.getTotalRecords() - result.getInconsistentRecords());
            result.setStatus(result.getInconsistentRecords() == 0
                    ? ComparisonResult.Status.SUCCESS
                    : ComparisonResult.Status.PARTIAL_SUCCESS);
            result.getSummary().put("spillBytes", spillGuard.getTotalReservedBytes());
            result.getSummary().put("activeSpillBytes", spillGuard.getActiveReservedBytes());
            if (mergedUpdateResult != null) {
                mergedUpdateResult.setResultId(result.getResultId());
                result.setUpdateResult(mergedUpdateResult);
            }
            recordResults(result, differences, resolvedDifferences, rule);
            result.setDifferenceRecords(differences);
            return result;
        } catch (SpillLimitExceededException e) {
            log.error("Spill guard blocked adaptive sort-merge consistency rule: {}", rule.getRuleId(), e);
            result.setStatus(ComparisonResult.Status.FAILED);
            result.getSummary().put("error", e.getMessage());
            result.getSummary().put("spillGuardTriggered", true);
            result.getSummary().put("spillGuardReason", e.getMessage());
            result.getSummary().put("spillBytes", spillGuard.getTotalReservedBytes());
            result.getSummary().put("activeSpillBytes", spillGuard.getActiveReservedBytes());
            return result;
        } catch (Exception e) {
            log.error("Failed to execute adaptive sort-merge consistency rule: {}", rule.getRuleId(), e);
            result.setStatus(ComparisonResult.Status.FAILED);
            result.getSummary().put("error", e.getMessage());
            result.getSummary().put("spillBytes", spillGuard.getTotalReservedBytes());
            result.getSummary().put("activeSpillBytes", spillGuard.getActiveReservedBytes());
            return result;
        } finally {
            if (overflowBucketStore != null) {
                overflowBucketStore.cleanup();
            }
            result.getSummary().put("spillBytes", spillGuard.getTotalReservedBytes());
            result.getSummary().put("activeSpillBytes", spillGuard.getActiveReservedBytes());
        }
    }

    private UpdateResult processOverflowStore(OverflowBucketStore overflowBucketStore,
                                              StreamExecutionOptions options,
                                              ConsistencyRule rule,
                                              List<String> sourceOrder,
                                              ConsistencyPartitionProcessor processor,
                                              DataSourceConfig targetDataSource,
                                              UpdatePlanningStrategy updatePlanningStrategy,
                                              UpdateResult currentUpdateResult,
                                              SpillGuard spillGuard) throws IOException {
        PartitionedSpillStore spillStore = overflowBucketStore.getStore();
        spillStore.close();
        UpdateResult merged = currentUpdateResult;
        for (int partition = 0; partition < spillStore.getPartitionCount(); partition++) {
            if (!spillStore.partitionExists(partition)) {
                continue;
            }
            // consistency 这里同样按 hash partition 顺序回放；
            // 所以 partition 的遍历次序会直接影响差异输出顺序，以及 auto-apply 时的执行顺序。
            List<DifferenceRecord> partitionResolved = processPartitionPath(
                    spillStore.getPartitionPath(partition),
                    options,
                    rule,
                    sourceOrder,
                    processor,
                    spillStore.getPartitionCount(),
                    partition,
                    0,
                    spillGuard
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
                merged = mergeUpdateResult(merged, partitionUpdate);
            }
        }
        return merged;
    }

    private List<DifferenceRecord> processPartitionPath(Path partitionPath,
                                                        StreamExecutionOptions options,
                                                        ConsistencyRule rule,
                                                        List<String> sourceOrder,
                                                        ConsistencyPartitionProcessor processor,
                                                        int currentPartitionCount,
                                                        int partition,
                                                        int depth,
                                                        SpillGuard spillGuard) throws IOException {
        Map<String, LinkedHashMap<String, Map<String, Object>>> groups = new LinkedHashMap<String, LinkedHashMap<String, Map<String, Object>>>();
        PartitionedSpillStore rebalanceStore = null;

        try (PartitionReader reader = new PartitionReader(partitionPath)) {
            final PartitionedSpillStore[] rebalanceHolder = new PartitionedSpillStore[1];
            reader.readAll(row -> {
                if (rebalanceHolder[0] != null) {
                    // rebalanceHolder 一旦初始化，当前 reader 剩余数据就全部改走下一层分桶，
                    // 当前层只负责把已经收集到的 groups 搬迁出去，不再继续扩张内存窗口。
                    rebalanceHolder[0].append(row);
                    return;
                }
                LinkedHashMap<String, Map<String, Object>> sourceRows = groups.get(row.getKey());
                if (sourceRows == null) {
                    sourceRows = new LinkedHashMap<String, Map<String, Object>>();
                    groups.put(row.getKey(), sourceRows);
                }
                if (!sourceRows.containsKey(row.getSourceId())) {
                    sourceRows.put(row.getSourceId(), row.getRow());
                }
                if (groups.size() > options.getMaxKeysPerPartition() && depth < MAX_REBALANCE_DEPTH) {
                    int childPartitionCount = options.getRebalancePartitionCount(currentPartitionCount);
                    rebalanceHolder[0] = new PartitionedSpillStore(
                            sanitizeJobId(rule.getRuleId()) + "-overflow-p" + partition + "-d" + depth,
                            options.getSpillPath(),
                            childPartitionCount,
                            false,
                            spillGuard
                    );
                    for (Map.Entry<String, LinkedHashMap<String, Map<String, Object>>> groupEntry : groups.entrySet()) {
                        com.jdragon.aggregation.core.streaming.CompositeKey key =
                                com.jdragon.aggregation.core.streaming.CompositeKey.fromEncoded(groupEntry.getKey(), rule.getMatchKeys());
                        for (Map.Entry<String, Map<String, Object>> sourceEntry : groupEntry.getValue().entrySet()) {
                            rebalanceHolder[0].append(sourceEntry.getKey(), key, sourceEntry.getValue());
                        }
                    }
                    // 当前层状态清空后，后续 row 全靠 rebalanceHolder 推进到子分区。
                    groups.clear();
                }
            });
            rebalanceStore = rebalanceHolder[0];
        }

        PartitionedSpillStore.cleanupConsumedPartition(partitionPath, options.isKeepTempFiles(), spillGuard);

        if (rebalanceStore != null) {
            rebalanceStore.close();
            try {
                List<DifferenceRecord> resolved = new ArrayList<DifferenceRecord>();
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
                            depth + 1,
                            spillGuard
                    ));
                }
                return resolved;
            } finally {
                rebalanceStore.cleanup();
            }
        }

        return processor.processGroups(groups);
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
            differenceRecord.setUpdatePlan(updatePlanningStrategy.plan(differenceRecord, rule, targetDataSource));
        }
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
            return "consistency-sortmerge";
        }
        return value.replaceAll("[^a-zA-Z0-9-_]", "_");
    }
}
