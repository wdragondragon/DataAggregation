package com.jdragon.aggregation.core.fusion;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.core.consistency.service.DataFetcher;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.core.fusion.config.FusionDetailConfig;
import com.jdragon.aggregation.core.fusion.config.FieldMapping;
import com.jdragon.aggregation.core.fusion.config.DirectFieldMapping;
import com.jdragon.aggregation.core.fusion.config.ExpressionFieldMapping;
import com.jdragon.aggregation.core.fusion.config.ConditionalFieldMapping;
import com.jdragon.aggregation.core.fusion.strategy.FusionStrategy;
import com.jdragon.aggregation.core.fusion.strategy.FusionStrategyFactory;
import com.jdragon.aggregation.core.fusion.detail.FusionDetail;
import com.jdragon.aggregation.core.fusion.detail.FieldDetail;

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

        // 创建融合详情（如果需要记录）
        FusionDetail fusionDetail = null;
        if (fusionContext.shouldRecordFusionDetail()) {
            fusionDetail = createFusionDetail(sourceRows);
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

            // 记录字段级详情
            if (fusionDetail != null) {
                FieldDetail fieldDetail = createFieldDetail(fieldMapping, sourceRows, mappedValue);
                fusionDetail.addFieldDetail(fieldDetail);
            }
        }

        // 记录融合详情
        if (fusionDetail != null) {
            fusionDetail.setStatus("SUCCESS");
            fusionContext.recordFusionDetail(fusionDetail);
        }

        return fusedRecord;
    }

    /**
     * 旧版融合逻辑（兼容字段映射为空的情况）
     */
    private Record fuseRowsLegacy(Map<String, Map<String, Object>> sourceRows, Record fusedRecord) {
        // 创建融合详情（如果需要记录）
        FusionDetail fusionDetail = null;
        if (fusionContext.shouldRecordFusionDetail()) {
            fusionDetail = createFusionDetail(sourceRows);
        }

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

            // 记录字段级详情
            if (fusionDetail != null) {
                FieldDetail fieldDetail = createLegacyFieldDetail(fieldName, sourceRows, sourceValues, fusedValue, strategy);
                fusionDetail.addFieldDetail(fieldDetail);
            }
        }

        // 记录融合详情
        if (fusionDetail != null) {
            fusionDetail.setStatus("SUCCESS");
            fusionContext.recordFusionDetail(fusionDetail);
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
    
    /**
     * 创建融合详情
     */
    private FusionDetail createFusionDetail(Map<String, Map<String, Object>> sourceRows) {
        FusionDetail detail = new FusionDetail();
        detail.setJoinKey(fusionContext.getCurrentJoinKey());
        detail.setTimestamp(System.currentTimeMillis());
        detail.setStatus("PROCESSING");
        
        // 添加源数据快照（如果需要）
        FusionDetailConfig detailConfig = fusionConfig.getDetailConfig();
        if (detailConfig != null && detailConfig.isIncludeSourceData()) {
            for (Map.Entry<String, Map<String, Object>> entry : sourceRows.entrySet()) {
                if (entry.getValue() != null) {
                    // 深拷贝避免后续修改
                    Map<String, Object> rowCopy = new HashMap<>(entry.getValue());
                    detail.addSourceRow(entry.getKey(), rowCopy);
                }
            }
        }
        
        return detail;
    }
    
    /**
     * 创建字段级详情
     */
    private FieldDetail createFieldDetail(FieldMapping fieldMapping, Map<String, Map<String, Object>> sourceRows, Column fusedValue) {
        FieldDetail fieldDetail = new FieldDetail();
        fieldDetail.setTargetField(fieldMapping.getTargetField());
        fieldDetail.setMappingType(fieldMapping.getMappingType().name());
        fieldDetail.setStrategyUsed(fieldMapping.getFusionStrategy());
        fieldDetail.setStatus("SUCCESS");
        
        // 设置源引用（根据映射类型）
        FieldMapping.MappingType mappingType = fieldMapping.getMappingType();
        switch (mappingType) {
            case DIRECT:
                if (fieldMapping instanceof DirectFieldMapping) {
                    fieldDetail.setSourceRef(((DirectFieldMapping) fieldMapping).getSourceField());
                } else {
                    fieldDetail.setSourceRef("DIRECT");
                }
                break;
            case EXPRESSION:
                if (fieldMapping instanceof ExpressionFieldMapping) {
                    fieldDetail.setSourceRef("Expression: " + ((ExpressionFieldMapping) fieldMapping).getExpression());
                } else {
                    fieldDetail.setSourceRef("EXPRESSION");
                }
                break;
            case CONDITIONAL:
                if (fieldMapping instanceof ConditionalFieldMapping) {
                    fieldDetail.setSourceRef("Conditional: " + ((ConditionalFieldMapping) fieldMapping).getCondition());
                } else {
                    fieldDetail.setSourceRef("CONDITIONAL");
                }
                break;
            case GROOVY:
                fieldDetail.setSourceRef("Groovy Script");
                break;
            case CONSTANT:
                fieldDetail.setSourceRef("Constant");
                break;
        }
        
        // 收集源值
        FusionDetailConfig detailConfig = fusionConfig.getDetailConfig();
        if (detailConfig != null && detailConfig.isIncludeFieldDetails()) {
            // 对于直接映射，收集各数据源的值
            if (mappingType == FieldMapping.MappingType.DIRECT && fieldMapping instanceof DirectFieldMapping) {
                String sourceField = ((DirectFieldMapping) fieldMapping).getSourceField();
                if (sourceField != null) {
                    // 解析源引用
                    String fieldRef = sourceField;
                    if (fieldRef.startsWith("${") && fieldRef.endsWith("}")) {
                        fieldRef = fieldRef.substring(2, fieldRef.length() - 1);
                    }
                    
                    String[] parts = fieldRef.split("\\.");
                    if (parts.length == 2) {
                        // 明确源
                        String sourceId = parts[0];
                        String fieldName = parts[1];
                        Map<String, Object> row = sourceRows.get(sourceId);
                        if (row != null && row.containsKey(fieldName)) {
                            fieldDetail.addSourceValue(sourceId, row.get(fieldName));
                        }
                    } else if (parts.length == 1) {
                        // 模糊源：从所有数据源收集
                        String fieldName = parts[0];
                        for (Map.Entry<String, Map<String, Object>> entry : sourceRows.entrySet()) {
                            Map<String, Object> row = entry.getValue();
                            if (row != null && row.containsKey(fieldName)) {
                                fieldDetail.addSourceValue(entry.getKey(), row.get(fieldName));
                            }
                        }
                    }
                }
            } else {
                // 对于其他映射类型，可以尝试收集表达式中引用的字段值
                // 简化处理：暂时不收集
            }
        }
        
        // 设置融合值
        if (fusedValue != null) {
            // 将Column转换为Java对象
            if (fusedValue.getType() == com.jdragon.aggregation.commons.element.Column.Type.INT ||
                fusedValue.getType() == com.jdragon.aggregation.commons.element.Column.Type.LONG) {
                fieldDetail.setFusedValue(fusedValue.asLong());
            } else if (fusedValue.getType() == com.jdragon.aggregation.commons.element.Column.Type.DOUBLE) {
                fieldDetail.setFusedValue(fusedValue.asDouble());
            } else if (fusedValue.getType() == com.jdragon.aggregation.commons.element.Column.Type.STRING) {
                fieldDetail.setFusedValue(fusedValue.asString());
            } else if (fusedValue.getType() == com.jdragon.aggregation.commons.element.Column.Type.BOOL) {
                fieldDetail.setFusedValue(fusedValue.asBoolean());
            } else {
                fieldDetail.setFusedValue(fusedValue.asString());
            }
        }
        
        return fieldDetail;
    }
    
    /**
     * 创建旧版融合逻辑的字段级详情
     */
    private FieldDetail createLegacyFieldDetail(String fieldName, Map<String, Map<String, Object>> sourceRows, 
                                               Map<String, Column> sourceValues, Column fusedValue, FusionStrategy strategy) {
        FieldDetail fieldDetail = new FieldDetail();
        fieldDetail.setTargetField(fieldName);
        fieldDetail.setMappingType("LEGACY");
        fieldDetail.setStrategyUsed(strategy != null ? strategy.getClass().getSimpleName() : "UNKNOWN");
        fieldDetail.setStatus("SUCCESS");
        
        // 设置源引用（收集所有数据源）
        StringBuilder sourceRefBuilder = new StringBuilder();
        for (Map.Entry<String, Column> entry : sourceValues.entrySet()) {
            if (sourceRefBuilder.length() > 0) {
                sourceRefBuilder.append(", ");
            }
            sourceRefBuilder.append(entry.getKey()).append(".").append(fieldName);
        }
        fieldDetail.setSourceRef(sourceRefBuilder.toString());
        
        // 收集源值
        FusionDetailConfig detailConfig = fusionConfig.getDetailConfig();
        if (detailConfig != null && detailConfig.isIncludeFieldDetails()) {
            for (Map.Entry<String, Column> entry : sourceValues.entrySet()) {
                String sourceId = entry.getKey();
                Column column = entry.getValue();
                // 将Column转换为Java对象
                Object value = null;
                if (column != null) {
                    if (column.getType() == com.jdragon.aggregation.commons.element.Column.Type.INT ||
                        column.getType() == com.jdragon.aggregation.commons.element.Column.Type.LONG) {
                        value = column.asLong();
                    } else if (column.getType() == com.jdragon.aggregation.commons.element.Column.Type.DOUBLE) {
                        value = column.asDouble();
                    } else if (column.getType() == com.jdragon.aggregation.commons.element.Column.Type.STRING) {
                        value = column.asString();
                    } else if (column.getType() == com.jdragon.aggregation.commons.element.Column.Type.BOOL) {
                        value = column.asBoolean();
                    } else {
                        value = column.asString();
                    }
                }
                fieldDetail.addSourceValue(sourceId, value);
            }
        }
        
        // 设置融合值
        if (fusedValue != null) {
            if (fusedValue.getType() == com.jdragon.aggregation.commons.element.Column.Type.INT ||
                fusedValue.getType() == com.jdragon.aggregation.commons.element.Column.Type.LONG) {
                fieldDetail.setFusedValue(fusedValue.asLong());
            } else if (fusedValue.getType() == com.jdragon.aggregation.commons.element.Column.Type.DOUBLE) {
                fieldDetail.setFusedValue(fusedValue.asDouble());
            } else if (fusedValue.getType() == com.jdragon.aggregation.commons.element.Column.Type.STRING) {
                fieldDetail.setFusedValue(fusedValue.asString());
            } else if (fusedValue.getType() == com.jdragon.aggregation.commons.element.Column.Type.BOOL) {
                fieldDetail.setFusedValue(fusedValue.asBoolean());
            } else {
                fieldDetail.setFusedValue(fusedValue.asString());
            }
        }
        
        return fieldDetail;
    }
}