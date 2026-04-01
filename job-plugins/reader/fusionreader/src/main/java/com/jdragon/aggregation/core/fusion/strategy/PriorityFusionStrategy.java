package com.jdragon.aggregation.core.fusion.strategy;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.core.fusion.FusionContext;

import java.util.Map;

/**
 * 优先级融合策略
 * 选择优先级最高的数据源的值（优先级数值越大优先级越高）
 */
public class PriorityFusionStrategy implements FusionStrategy {
    
    public static final String NAME = "PRIORITY";
    
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
        int highestPriority = Integer.MIN_VALUE;
        double highestWeight = -1.0;
        
        for (Map.Entry<String, Column> entry : sourceValues.entrySet()) {
            String sourceId = entry.getKey();
            Column value = entry.getValue();
            
            // 跳过空值
            if (value == null) {
                continue;
            }
            
            int priority = context.getSourcePriority(sourceId);
            double weight = context.getSourceWeight(sourceId);
            
            // 比较优先级
            if (priority > highestPriority) {
                selectedSourceId = sourceId;
                highestPriority = priority;
                highestWeight = weight;
            } else if (priority == highestPriority) {
                // 优先级相同，比较权重
                if (weight > highestWeight) {
                    selectedSourceId = sourceId;
                    highestWeight = weight;
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
        return "优先级融合策略：选择优先级最高的数据源的值（优先级数值越大优先级越高）";
    }
}