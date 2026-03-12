package com.jdragon.aggregation.core.fusion;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.core.consistency.service.DataFetcher;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.core.fusion.config.FieldMapping;
import com.jdragon.aggregation.core.fusion.strategy.FusionStrategy;
import com.jdragon.aggregation.core.fusion.strategy.FusionStrategyFactory;

import java.util.*;

/**
 * 数据融合引擎
 * 负责将分组后的源数据进行字段融合
 */
public class FusionEngine {

    private final FusionConfig fusionConfig;
    private final FusionContext fusionContext;
    private final Map<String, FieldMapping> fieldMappingMap; // 目标字段名 -> FieldMapping映射

    public FusionEngine(FusionConfig fusionConfig, FusionContext fusionContext) {
        this.fusionConfig = fusionConfig;
        this.fusionContext = fusionContext;
        this.fieldMappingMap = buildFieldMappingMap();
    }

    /**
     * 构建字段映射映射表
     */
    private Map<String, FieldMapping> buildFieldMappingMap() {
        Map<String, FieldMapping> map = new HashMap<>();
        if (fusionConfig.getFieldMappings() != null) {
            for (FieldMapping mapping : fusionConfig.getFieldMappings()) {
                map.put(mapping.getTargetField(), mapping);
            }
        }
        return map;
    }

    /**
     * 执行数据融合
     * @param groupedData 分组后的源数据（sourceId -> matchKey -> 记录列表）
     * @return 融合后的记录列表
     */
    public List<Record> fuse(Map<String, Map<String, List<Map<String, Object>>>> groupedData) {
        List<Record> fusedRecords = new ArrayList<>();

        // 获取所有匹配键
        Set<String> allMatchKeys = collectAllMatchKeys(groupedData);


        for (String matchKey : allMatchKeys) {
            // 设置当前处理的连接键到上下文
            fusionContext.setCurrentJoinKey(matchKey);

            // 收集每个数据源对应匹配键的记录
            Map<String, Map<String, Object>> sourceRows = new HashMap<>();
            for (Map.Entry<String, Map<String, List<Map<String, Object>>>> sourceEntry : groupedData.entrySet()) {
                String sourceId = sourceEntry.getKey();
                Map<String, List<Map<String, Object>>> sourceGrouped = sourceEntry.getValue();
                List<Map<String, Object>> rows = sourceGrouped.get(matchKey);
                if (rows != null && !rows.isEmpty()) {
                    // 取第一个记录（假设连接键唯一）
                    sourceRows.put(sourceId, rows.get(0));
                }
            }

            // 根据连接类型决定是否处理该匹配键
            if (shouldProcess(sourceRows)) {
                Record fusedRecord = fuseRows(sourceRows);
                fusedRecords.add(fusedRecord);
            }
        }

        return fusedRecords;
    }

    /**
     * 收集所有匹配键
     */
    private Set<String> collectAllMatchKeys(Map<String, Map<String, List<Map<String, Object>>>> groupedData) {
        Set<String> allKeys = new HashSet<>();
        for (Map<String, List<Map<String, Object>>> sourceGrouped : groupedData.values()) {
            allKeys.addAll(sourceGrouped.keySet());
        }
        return allKeys;
    }

    /**
     * 根据连接类型判断是否处理该组记录
     */
    private boolean shouldProcess(Map<String, Map<String, Object>> sourceRows) {
        FusionConfig.JoinType joinType = fusionConfig.getJoinType();
        List<String> sourceIds = new ArrayList<>(sourceRows.keySet());

        switch (joinType) {
            case INNER:
                // 所有数据源都必须有记录
                return sourceRows.values().stream().allMatch(Objects::nonNull);
            case LEFT:
                // 第一个数据源必须有记录
                return sourceRows.get(sourceIds.get(0)) != null;
            case RIGHT:
                // 最后一个数据源必须有记录
                return sourceRows.get(sourceIds.get(sourceIds.size() - 1)) != null;
            case FULL:
                // 至少一个数据源有记录
                return sourceRows.values().stream().anyMatch(Objects::nonNull);
            default:
                return false;
        }
    }

    /**
     * 融合多行记录
     */
    private Record fuseRows(Map<String, Map<String, Object>> sourceRows) {
        // 创建目标记录
        Record fusedRecord = new com.jdragon.aggregation.core.transport.record.DefaultRecord();

        // 如果字段映射配置为空，回退到旧逻辑（收集所有字段名）
        if (fieldMappingMap.isEmpty()) {
            return fuseRowsLegacy(sourceRows, fusedRecord);
        }

        // 按目标字段顺序排序
        List<String> targetColumns = fusionContext.getTargetColumns();

        // 使用字段映射配置进行融合
        for (Map.Entry<String, FieldMapping> entry : fieldMappingMap.entrySet()) {
            String targetFieldName = entry.getKey();
            int index = targetColumns.indexOf(targetFieldName);
            if (index == -1) {
                continue;
            }
            FieldMapping fieldMapping = entry.getValue();

            // 执行字段映射
            Column mappedValue = fieldMapping.map(convertSourceRowsToValues(sourceRows), fusionContext);

            if (mappedValue != null) {
                fusedRecord.setColumn(index, mappedValue);
            } else {
                fusedRecord.setColumn(index, new StringColumn(null));
            }
        }

        return fusedRecord;
    }

    /**
     * 旧版融合逻辑（兼容字段映射为空的情况）
     */
    private Record fuseRowsLegacy(Map<String, Map<String, Object>> sourceRows, Record fusedRecord) {
        // 收集所有字段名
        Set<String> allFieldNames = new HashSet<>();
        for (Map<String, Object> row : sourceRows.values()) {
            if (row != null) {
                allFieldNames.addAll(row.keySet());
            }
        }

        // 为每个字段选择或计算值
        for (String fieldName : allFieldNames) {
            // 收集各数据源该字段的值
            Map<String, Column> sourceValues = collectSourceValues(fieldName, sourceRows);

            // 选择融合策略（使用字段映射配置中的策略）
            FieldMapping fieldMapping = getFieldMapping(fieldName);
            FusionStrategy strategy = selectFusionStrategy(fieldName, fieldMapping);
            Column fusedValue = strategy.select(fieldName, sourceValues, fusionContext);

            if (fusedValue != null) {
                fusedRecord.addColumn(fusedValue);
            }
        }

        return fusedRecord;
    }

    /**
     * 收集各数据源指定字段的值
     */
    private Map<String, Column> collectSourceValues(String fieldName, Map<String, Map<String, Object>> sourceRows) {
        Map<String, Column> sourceValues = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : sourceRows.entrySet()) {
            String sourceId = entry.getKey();
            Map<String, Object> row = entry.getValue();
            if (row != null && row.containsKey(fieldName)) {
                Object value = row.get(fieldName);
                Column column = convertObjectToColumn(value);
                sourceValues.put(sourceId, column);
            }
        }
        return sourceValues;
    }

    /**
     * 转换源行数据为字段映射所需的格式
     */
    private Map<String, Map<String, Column>> convertSourceRowsToValues(Map<String, Map<String, Object>> sourceRows) {
        Map<String, Map<String, Column>> result = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : sourceRows.entrySet()) {
            String sourceId = entry.getKey();
            Map<String, Object> row = entry.getValue();
            Map<String, Column> columnMap = new HashMap<>();
            if (row != null) {
                for (Map.Entry<String, Object> fieldEntry : row.entrySet()) {
                    columnMap.put(fieldEntry.getKey(), convertObjectToColumn(fieldEntry.getValue()));
                }
            }
            result.put(sourceId, columnMap);
        }
        return result;
    }

    /**
     * 获取字段映射配置
     */
    private FieldMapping getFieldMapping(String targetFieldName) {
        return fieldMappingMap.get(targetFieldName);
    }

    /**
     * 选择融合策略
     * 优先级：FieldMapping.strategy > fieldStrategies > defaultStrategy > PRIORITY
     */
    private FusionStrategy selectFusionStrategy(String fieldName, FieldMapping fieldMapping) {
        // 1. 优先使用字段映射中定义的策略
        if (fieldMapping != null && fieldMapping.getFusionStrategy() != null) {
            String strategyName = fieldMapping.getFusionStrategy();
            FusionStrategy strategy = FusionStrategyFactory.getStrategy(strategyName);
            if (strategy != null) {
                return strategy;
            }
        }

        // 2. 使用默认策略
        String defaultStrategyName = fusionConfig.getDefaultStrategy();
        FusionStrategy defaultStrategy = FusionStrategyFactory.getStrategy(defaultStrategyName);
        if (defaultStrategy != null) {
            return defaultStrategy;
        }

        // 3. 回退到优先级策略
        return FusionStrategyFactory.getStrategy("PRIORITY");
    }

    /**
     * 将对象转换为Column
     */
    private Column convertObjectToColumn(Object value) {
        if (value == null) {
            return new com.jdragon.aggregation.commons.element.ObjectColumn(null);
        } else if (value instanceof String) {
            return new com.jdragon.aggregation.commons.element.StringColumn((String) value);
        } else if (value instanceof Integer) {
            return new com.jdragon.aggregation.commons.element.LongColumn(((Integer) value).longValue());
        } else if (value instanceof Long) {
            return new com.jdragon.aggregation.commons.element.LongColumn((Long) value);
        } else if (value instanceof Double) {
            return new com.jdragon.aggregation.commons.element.DoubleColumn((Double) value);
        } else if (value instanceof Boolean) {
            return new com.jdragon.aggregation.commons.element.BoolColumn((Boolean) value);
        } else {
            // 默认转为String
            return new com.jdragon.aggregation.commons.element.StringColumn(value.toString());
        }
    }
}