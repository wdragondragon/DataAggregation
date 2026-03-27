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
import com.jdragon.aggregation.core.consistency.service.DataFetcher;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.core.fusion.config.SourceConfig;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.streaming.CompositeKey;
import com.jdragon.aggregation.core.streaming.PartitionReader;
import com.jdragon.aggregation.core.streaming.PartitionedSpillStore;
import com.jdragon.aggregation.core.streaming.StreamExecutionOptions;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StreamingFusionExecutor {

    private static final int MAX_REBALANCE_DEPTH = 3;

    private final DataFetcher dataFetcher;
    private final FusionConfig fusionConfig;
    private final FusionContext fusionContext;

    public StreamingFusionExecutor(DataFetcher dataFetcher, FusionConfig fusionConfig, FusionContext fusionContext) {
        this.dataFetcher = dataFetcher;
        this.fusionConfig = fusionConfig;
        this.fusionContext = fusionContext;
    }

    public void execute(RecordSender recordSender) throws IOException {
        StreamExecutionOptions options = StreamExecutionOptions.fromFusionConfig(fusionConfig);
        List<DataSourceConfig> dataSourceConfigs = convertToDataSourceConfigs();
        PartitionedSpillStore spillStore = new PartitionedSpillStore("fusion", options.getSpillPath(), options.getPartitionCount(), options.isKeepTempFiles());

        try {
            dataFetcher.scanSources(dataSourceConfigs, options.getParallelSourceCount(), (sourceId, row) -> {
                updateIncrValue(sourceId, row);
                spillStore.append(sourceId, CompositeKey.fromRecord(row, fusionConfig.getJoinKeys()), row);
            });
            spillStore.close();
            publishIncrementalValues();

            FusionPartitionProcessor processor = new FusionPartitionProcessor(fusionConfig, fusionContext, recordSender);
            for (int partition = 0; partition < spillStore.getPartitionCount(); partition++) {
                if (!spillStore.partitionExists(partition)) {
                    continue;
                }
                processPartitionPath(spillStore.getPartitionPath(partition), options, processor, partition, 0);
            }
        } finally {
            spillStore.cleanup();
        }
    }

    private void processPartitionPath(Path partitionPath,
                                      StreamExecutionOptions options,
                                      FusionPartitionProcessor processor,
                                      int partition,
                                      int depth) throws IOException {
        Map<String, LinkedHashMap<String, Map<String, Object>>> groups = new LinkedHashMap<>();
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
                sourceRows.putIfAbsent(row.getSourceId(), row.getRow());

                if (groups.size() > options.getMaxKeysPerPartition() && depth < MAX_REBALANCE_DEPTH) {
                    PartitionedSpillStore childStore = new PartitionedSpillStore(
                            "fusion-p" + partition + "-d" + depth,
                            options.getSpillPath(),
                            Math.max(4, options.getPartitionCount()),
                            options.isKeepTempFiles()
                    );
                    rebalanceStoreHolder[0] = childStore;
                    for (Map.Entry<String, LinkedHashMap<String, Map<String, Object>>> groupEntry : groups.entrySet()) {
                        CompositeKey key = CompositeKey.fromEncoded(groupEntry.getKey(), fusionConfig.getJoinKeys());
                        for (Map.Entry<String, Map<String, Object>> sourceEntry : groupEntry.getValue().entrySet()) {
                            childStore.append(sourceEntry.getKey(), key, sourceEntry.getValue());
                        }
                    }
                    groups.clear();
                }
            });
        }

        PartitionedSpillStore rebalanceStore = rebalanceStoreHolder[0];
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

        processor.processGroups(groups);
    }

    private List<DataSourceConfig> convertToDataSourceConfigs() {
        List<DataSourceConfig> configs = new ArrayList<>();
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
        Map<String, Column> incrColumnValue = new LinkedHashMap<>();

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
