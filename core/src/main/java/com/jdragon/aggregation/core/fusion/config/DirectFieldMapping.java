package com.jdragon.aggregation.core.fusion.config;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.core.fusion.FusionContext;
import com.jdragon.aggregation.core.fusion.strategy.FusionStrategy;
import com.jdragon.aggregation.core.fusion.strategy.FusionStrategyFactory;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

/**
 * 直接字段映射
 * 将源字段直接复制到目标字段
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class DirectFieldMapping extends FieldMapping {

    private String sourceField; // 源字段引用格式：sourceId.fieldName

    /**
     * 从Configuration解析DirectFieldMapping
     */
    public static DirectFieldMapping fromConfig(Configuration config) {
        DirectFieldMapping mapping = new DirectFieldMapping();
        mapping.setMappingType(MappingType.DIRECT);
        mapping.setTargetField(config.getString("targetField"));
        mapping.setSourceField(config.getString("sourceField"));
        mapping.setResultType(config.getString("resultType"));
        mapping.setErrorMode(config.getString("errorMode", "DEFAULT"));
        mapping.setFusionStrategy(config.getString("strategy"));
        return mapping;
    }

    @Override
    public Column map(Map<String, Map<String, Column>> sourceValues, FusionContext context) {
        try {
            // 解析源字段引用：支持两种格式：
            // 1. sourceId.fieldName - 明确源，直接取值
            // 2. fieldName - 模糊源，从所有数据源收集值并应用融合策略
            String[] parts = sourceField.split("\\.");

            if (parts.length == 2) {
                // 明确源：直接取值
                String sourceId = parts[0];
                String fieldName = parts[1];

                // 获取源数据
                Map<String, Column> sourceData = sourceValues.get(sourceId);
                if (sourceData == null) {
                    return context.handleFieldError(targetField,
                            "数据源不存在: " + sourceId, null);
                }

                // 获取字段值
                Column value = sourceData.get(fieldName);
                if (value == null) {
                    return context.handleFieldError(targetField,
                            "字段不存在: " + sourceId + "." + fieldName, null);
                }

                return value;

            } else if (parts.length == 1) {
                // 模糊源：从所有数据源收集该字段值，应用融合策略
                String fieldName = parts[0];
                Map<String, Column> collectedValues = new HashMap<>();

                for (Map.Entry<String, Map<String, Column>> sourceEntry : sourceValues.entrySet()) {
                    String sourceId = sourceEntry.getKey();
                    Map<String, Column> sourceData = sourceEntry.getValue();
                    if (sourceData != null && sourceData.containsKey(fieldName)) {
                        collectedValues.put(sourceId, sourceData.get(fieldName));
                    }
                }

                if (collectedValues.isEmpty()) {
                    return context.handleFieldError(targetField,
                            "字段不存在于任何数据源: " + fieldName, null);
                }

                // 如果有多个源的值，应用融合策略
                if (collectedValues.size() > 1) {
                    // 选择融合策略：优先使用字段映射中定义的策略
                    String strategyName = getFusionStrategy();
                    if (strategyName == null) {
                        // 如果没有定义字段级策略，使用默认策略
                        strategyName = "PRIORITY";
                    }

                    FusionStrategy strategy = FusionStrategyFactory.getStrategy(strategyName);
                    if (strategy != null) {
                        return strategy.select(targetField, collectedValues, context);
                    }
                }

                // 只有一个源有值，直接返回
                return collectedValues.values().iterator().next();

            } else {
                return context.handleFieldError(targetField,
                        "源字段格式错误，应为 sourceId.fieldName 或 fieldName: " + sourceField, null);
            }

        } catch (Exception e) {
            return context.handleFieldError(targetField,
                    "直接映射执行失败: " + e.getMessage(), e);
        }
    }

    @Override
    public void validate() {
        if (targetField == null || targetField.trim().isEmpty()) {
            throw new IllegalArgumentException("目标字段不能为空");
        }

        if (sourceField == null || sourceField.trim().isEmpty()) {
            throw new IllegalArgumentException("源字段不能为空");
        }

        // 检查源字段格式：支持 sourceId.fieldName 或 fieldName
        String[] parts = sourceField.split("\\.");
        if (parts.length < 1 || parts.length > 2) {
            throw new IllegalArgumentException("源字段格式错误，应为 sourceId.fieldName 或 fieldName: " + sourceField);
        }

        // 检查字段名
        if (parts[0].trim().isEmpty()) {
            throw new IllegalArgumentException("源字段中的字段名不能为空: " + sourceField);
        }

        // 如果包含sourceId，检查sourceId
        if (parts.length == 2 && parts[0].trim().isEmpty()) {
            throw new IllegalArgumentException("源字段中的源ID不能为空: " + sourceField);
        }
    }

    @Override
    public String toString() {
        return String.format("DirectMapping{target='%s', source='%s'}", targetField, sourceField);
    }
}