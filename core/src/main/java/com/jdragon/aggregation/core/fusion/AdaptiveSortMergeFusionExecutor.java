package com.jdragon.aggregation.core.fusion;

import com.jdragon.aggregation.commons.element.BoolColumn;
import com.jdragon.aggregation.commons.element.BytesColumn;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.DateColumn;
import com.jdragon.aggregation.commons.element.DoubleColumn;
import com.jdragon.aggregation.commons.element.LongColumn;
import com.jdragon.aggregation.commons.element.ObjectColumn;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.core.fusion.config.SourceConfig;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.sortmerge.AdaptiveMergeConfig;
import com.jdragon.aggregation.core.sortmerge.AdaptiveMergeCoordinator;
import com.jdragon.aggregation.core.sortmerge.OrderedKey;
import com.jdragon.aggregation.core.sortmerge.OrderedKeySchema;
import com.jdragon.aggregation.core.sortmerge.OrderedSourceCursor;
import com.jdragon.aggregation.core.sortmerge.OverflowBucketStore;
import com.jdragon.aggregation.core.sortmerge.SortMergeStats;
import com.jdragon.aggregation.core.streaming.PartitionReader;
import com.jdragon.aggregation.core.streaming.PartitionedSpillStore;
import com.jdragon.aggregation.core.streaming.SourceRowScanner;
import com.jdragon.aggregation.core.streaming.StreamExecutionOptions;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * fusion 的自适应 sort-merge 执行器。
 *
 * <p>它优先走有序内存归并，只有在等待窗口过大或输入顺序不再可靠时，才把剩余 key
 * 交给 overflow bucket 并通过既有 partition processor 回放，从而保持与旧实现兼容。
 */
public class AdaptiveSortMergeFusionExecutor {

    private static final int MAX_REBALANCE_DEPTH = 3;

    private final DataSourcePluginManager pluginManager;
    private final FusionConfig fusionConfig;
    private final FusionContext fusionContext;

    public AdaptiveSortMergeFusionExecutor(DataSourcePluginManager pluginManager,
                                           FusionConfig fusionConfig,
                                           FusionContext fusionContext) {
        this.pluginManager = pluginManager;
        this.fusionConfig = fusionConfig;
        this.fusionContext = fusionContext;
    }

    public SortMergeStats execute(RecordSender recordSender) throws Exception {
        StreamExecutionOptions options = StreamExecutionOptions.fromFusionConfig(fusionConfig);
        AdaptiveMergeConfig adaptiveMergeConfig = fusionConfig.getAdaptiveMergeConfig();
        List<DataSourceConfig> dataSourceConfigs = convertToDataSourceConfigs();
        OrderedKeySchema keySchema = new OrderedKeySchema(fusionConfig.getJoinKeys(), adaptiveMergeConfig.getKeyTypes());
        List<OrderedSourceCursor> cursors = new ArrayList<>();
        SourceRowScanner rowScanner = new SourceRowScanner(pluginManager);
        for (DataSourceConfig dataSourceConfig : dataSourceConfigs) {
            cursors.add(new OrderedSourceCursor(rowScanner, dataSourceConfig, fusionConfig.getJoinKeys(), true));
        }

        OverflowBucketStore overflowBucketStore = null;
        try {
            FusionPartitionProcessor processor = new FusionPartitionProcessor(fusionConfig, fusionContext, recordSender);
            AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(adaptiveMergeConfig, keySchema, collectSourceOrder());
            AdaptiveMergeCoordinator.Result result = coordinator.execute(cursors, (key, firstRowsBySource) -> {
                updateIncrementalValues(firstRowsBySource);
                Map<String, LinkedHashMap<String, Map<String, Object>>> groups = new LinkedHashMap<>();
                LinkedHashMap<String, Map<String, Object>> sourceRows = new LinkedHashMap<>(firstRowsBySource);
                groups.put(key.getEncoded(), sourceRows);
                processor.processGroups(groups);
            });
            overflowBucketStore = result.getOverflowBucketStore();
            if (overflowBucketStore != null && overflowBucketStore.getSpilledRows() > 0) {
                processOverflowStore(overflowBucketStore, options, processor);
            }
            publishIncrementalValues();
            return result.getStats();
        } finally {
            if (overflowBucketStore != null) {
                overflowBucketStore.cleanup();
            }
        }
    }

    private void processOverflowStore(OverflowBucketStore overflowBucketStore,
                                      StreamExecutionOptions options,
                                      FusionPartitionProcessor processor) throws IOException {
        PartitionedSpillStore spillStore = overflowBucketStore.getStore();
        spillStore.close();
        for (int partition = 0; partition < spillStore.getPartitionCount(); partition++) {
            if (!spillStore.partitionExists(partition)) {
                continue;
            }
            processPartitionPath(spillStore.getPartitionPath(partition), options, processor, partition, 0);
        }
    }

    private void processPartitionPath(Path partitionPath,
                                      StreamExecutionOptions options,
                                      FusionPartitionProcessor processor,
                                      int partition,
                                      int depth) throws IOException {
        Map<String, LinkedHashMap<String, Map<String, Object>>> groups = new LinkedHashMap<String, LinkedHashMap<String, Map<String, Object>>>();
        PartitionedSpillStore rebalanceStore = null;

        try (PartitionReader reader = new PartitionReader(partitionPath)) {
            final PartitionedSpillStore[] rebalanceHolder = new PartitionedSpillStore[1];
            reader.readAll(row -> {
                if (rebalanceHolder[0] != null) {
                    rebalanceHolder[0].append(row);
                    return;
                }
                LinkedHashMap<String, Map<String, Object>> sourceRows = groups.get(row.getKey());
                if (sourceRows == null) {
                    sourceRows = new LinkedHashMap<String, Map<String, Object>>();
                    groups.put(row.getKey(), sourceRows);
                }
                sourceRows.putIfAbsent(row.getSourceId(), row.getRow());

                if (groups.size() > options.getMaxKeysPerPartition() && depth < MAX_REBALANCE_DEPTH) {
                    rebalanceHolder[0] = new PartitionedSpillStore(
                            "fusion-sortmerge-overflow-p" + partition + "-d" + depth,
                            options.getSpillPath(),
                            Math.max(4, options.getPartitionCount()),
                            false
                    );
                    for (Map.Entry<String, LinkedHashMap<String, Map<String, Object>>> groupEntry : groups.entrySet()) {
                        com.jdragon.aggregation.core.streaming.CompositeKey key =
                                com.jdragon.aggregation.core.streaming.CompositeKey.fromEncoded(groupEntry.getKey(), fusionConfig.getJoinKeys());
                        for (Map.Entry<String, Map<String, Object>> sourceEntry : groupEntry.getValue().entrySet()) {
                            rebalanceHolder[0].append(sourceEntry.getKey(), key, sourceEntry.getValue());
                        }
                    }
                    groups.clear();
                }
            });
            rebalanceStore = rebalanceHolder[0];
        }

        if (rebalanceStore != null) {
            rebalanceStore.close();
            try {
                for (int childPartition = 0; childPartition < rebalanceStore.getPartitionCount(); childPartition++) {
                    if (!rebalanceStore.partitionExists(childPartition)) {
                        continue;
                    }
                    processPartitionPath(rebalanceStore.getPartitionPath(childPartition), options, processor, childPartition, depth + 1);
                }
                return;
            } finally {
                rebalanceStore.cleanup();
            }
        }

        updateIncrementalValuesFromGroups(groups);
        processor.processGroups(groups);
    }

    private List<DataSourceConfig> convertToDataSourceConfigs() {
        List<DataSourceConfig> configs = new ArrayList<DataSourceConfig>();
        for (SourceConfig source : fusionConfig.getSources()) {
            DataSourceConfig dataSourceConfig = new DataSourceConfig();
            dataSourceConfig.setSourceId(source.getSourceId());
            dataSourceConfig.setSourceName(source.getSourceName());
            dataSourceConfig.setPluginName(source.getPluginType());
            dataSourceConfig.setConnectionConfig(source.getPluginConfig());
            dataSourceConfig.setQuerySql(source.getQuerySql());
            dataSourceConfig.setTableName(source.getTableName());
            dataSourceConfig.setConfidenceWeight(source.getConfidence());
            dataSourceConfig.setPriority(source.getPriority());
            dataSourceConfig.setMaxRecords(source.getMaxRecords());
            dataSourceConfig.setFieldMappings(source.getFieldMappings());
            dataSourceConfig.setExtConfig(source.getExtConfig());
            configs.add(dataSourceConfig);
        }
        return configs;
    }

    private List<String> collectSourceOrder() {
        List<String> sourceOrder = new ArrayList<String>();
        for (SourceConfig source : fusionConfig.getSources()) {
            sourceOrder.add(source.getSourceId());
        }
        return sourceOrder;
    }

    private void updateIncrementalValuesFromGroups(Map<String, LinkedHashMap<String, Map<String, Object>>> groups) {
        for (LinkedHashMap<String, Map<String, Object>> sourceRows : groups.values()) {
            updateIncrementalValues(sourceRows);
        }
    }

    private void updateIncrementalValues(Map<String, Map<String, Object>> sourceRows) {
        for (Map.Entry<String, Map<String, Object>> entry : sourceRows.entrySet()) {
            updateIncrValue(entry.getKey(), entry.getValue());
        }
    }

    private void updateIncrValue(String sourceId, Map<String, Object> row) {
        String incrColumn = fusionContext.getSourceIncrColumn().get(sourceId);
        if (incrColumn == null || row == null || !row.containsKey(incrColumn)) {
            return;
        }
        fusionContext.updateIncrValue(sourceId, object2Column(row.get(incrColumn)));
    }

    private void publishIncrementalValues() {
        Map<String, String> sourceIncrColumn = fusionContext.getSourceIncrColumn();
        Map<String, Column> sourceMaxIncrValues = fusionContext.getSourceMaxIncrValues();
        Map<String, Column> incrColumnValue = new LinkedHashMap<String, Column>();

        for (Map.Entry<String, String> entry : sourceIncrColumn.entrySet()) {
            String sourceId = entry.getKey();
            String incrColumn = entry.getValue();
            Column column = sourceMaxIncrValues.get(sourceId);
            if (column == null) {
                continue;
            }
            if (fusionConfig.getJoinKeys().contains(incrColumn)) {
                Column current = incrColumnValue.get(incrColumn);
                if (current == null || current.compareTo(column) > 0) {
                    incrColumnValue.put(incrColumn, column);
                }
            }
        }

        for (Map.Entry<String, String> entry : sourceIncrColumn.entrySet()) {
            String sourceId = entry.getKey();
            String incrColumn = entry.getValue();
            Column column = incrColumnValue.get(incrColumn);
            if (column == null) {
                column = sourceMaxIncrValues.get(sourceId);
            }
            if (column != null && fusionContext.getJobPointReporter() != null) {
                fusionContext.getJobPointReporter().put("pkValue_" + sourceId, column.asString());
            }
        }
    }

    private Column object2Column(Object value) {
        if (value == null) {
            return new StringColumn(null);
        }
        if (value instanceof String) {
            return new StringColumn((String) value);
        }
        if (value instanceof Integer) {
            return new LongColumn((Integer) value);
        }
        if (value instanceof Long) {
            return new LongColumn((Long) value);
        }
        if (value instanceof Double) {
            return new DoubleColumn((Double) value);
        }
        if (value instanceof Boolean) {
            return new BoolColumn((Boolean) value);
        }
        if (value instanceof Float) {
            return new DoubleColumn((Float) value);
        }
        if (value instanceof Date) {
            return new DateColumn((Date) value);
        }
        if (value instanceof byte[]) {
            return new BytesColumn((byte[]) value);
        }
        return new ObjectColumn(value);
    }
}
