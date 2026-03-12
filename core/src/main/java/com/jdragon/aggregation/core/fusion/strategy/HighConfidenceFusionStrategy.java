package com.jdragon.aggregation.core.fusion.strategy;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.core.fusion.FusionContext;

import java.util.Map;

/**
 * 高置信度融合策略
 * 为每个字段选择置信度权重最高的数据源的值
 */
public class HighConfidenceFusionStrategy implements FusionStrategy {
    
    public static final String NAME = "HIGH_CONFIDENCE";
    
    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public Column select(String fieldName, Map<String, Column> sourceValues, FusionContext context) {
        if (sourceValues == null || sourceValues.isEmpty()) {
            return null;
        }
        
        String selectedSourceId = null;
        double highestWeight = -1.0;
        int highestPriority = Integer.MIN_VALUE;
        
        for (Map.Entry<String, Column> entry : sourceValues.entrySet()) {
            String sourceId = entry.getKey();
            Column value = entry.getValue();
            
            // 跳过空值
            if (value == null) {
                continue;
            }
            
            double weight = context.getSourceWeight(sourceId);
            int priority = context.getSourcePriority(sourceId);
            
            // 比较权重
            if (weight > highestWeight) {
                selectedSourceId = sourceId;
                highestWeight = weight;
                highestPriority = priority;
            } else if (weight == highestWeight) {
                // 权重相同，比较优先级
                if (priority > highestPriority) {
                    selectedSourceId = sourceId;
                    highestPriority = priority;
                }
            }
        }
        
        if (selectedSourceId == null) {
            return null;
        }
        
        return sourceValues.get(selectedSourceId);
    }
    
    @Override
    public String getDescription() {
        return "高置信度融合策略：为每个字段选择置信度权重最高的数据源的值";
    }
}