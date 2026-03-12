package com.jdragon.aggregation.core.fusion.strategy;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.core.fusion.FusionContext;

import java.util.HashMap;
import java.util.Map;

/**
 * 加权平均融合策略
 * 对数值型字段进行加权平均计算，非数值型字段使用多数投票
 */
public class WeightedAverageFusionStrategy implements FusionStrategy {
    
    public static final String NAME = "WEIGHTED_AVERAGE";
    
    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public Column select(String fieldName, Map<String, Column> sourceValues, FusionContext context) {
        if (sourceValues == null || sourceValues.isEmpty()) {
            return null;
        }
        
        // 检查字段是否为数值型
        boolean isNumeric = isNumericField(fieldName, sourceValues);
        
        if (isNumeric) {
            return calculateWeightedAverage(fieldName, sourceValues, context);
        } else {
            return getMostCommonValue(fieldName, sourceValues);
        }
    }
    
    /**
     * 计算加权平均值
     */
    private Column calculateWeightedAverage(String fieldName, Map<String, Column> sourceValues, FusionContext context) {
        double weightedSum = 0.0;
        double effectiveTotalWeight = 0.0;
        
        for (Map.Entry<String, Column> entry : sourceValues.entrySet()) {
            String sourceId = entry.getKey();
            Column column = entry.getValue();
            
            if (column == null) {
                continue;
            }
            
            Double numericValue = convertToDouble(column);
            if (numericValue == null) {
                continue;
            }
            
            double weight = context.getSourceWeight(sourceId);
            weightedSum += numericValue * weight;
            effectiveTotalWeight += weight;
        }
        
        if (effectiveTotalWeight == 0.0) {
            return null;
        }
        
        double resultValue = weightedSum / effectiveTotalWeight;
        // 创建DoubleColumn
        return new com.jdragon.aggregation.commons.element.DoubleColumn(resultValue);
    }
    
    /**
     * 获取最常见的值（多数投票）
     */
    private Column getMostCommonValue(String fieldName, Map<String, Column> sourceValues) {
        Map<Column, Integer> frequency = new HashMap<>();
        
        for (Column column : sourceValues.values()) {
            if (column == null) {
                continue;
            }
            frequency.put(column, frequency.getOrDefault(column, 0) + 1);
        }
        
        Column mostCommon = null;
        int maxCount = 0;
        
        for (Map.Entry<Column, Integer> entry : frequency.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                mostCommon = entry.getKey();
            }
        }
        
        return mostCommon;
    }
    
    /**
     * 检查字段是否为数值型
     */
    private boolean isNumericField(String fieldName, Map<String, Column> sourceValues) {
        for (Column column : sourceValues.values()) {
            if (column == null) {
                continue;
            }
            if (!isNumeric(column)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * 判断Column是否为数值型
     */
    private boolean isNumeric(Column column) {
        if (column == null) {
            return false;
        }
        
        // 根据Column类型判断
        Column.Type type = column.getType();
        return type == Column.Type.INT || 
               type == Column.Type.LONG || 
               type == Column.Type.DOUBLE;
    }
    
    /**
     * 将Column转换为Double
     */
    private Double convertToDouble(Column column) {
        if (column == null) {
            return null;
        }
        
        try {
            return column.asDouble();
        } catch (Exception e) {
            return null;
        }
    }
    
    @Override
    public String getDescription() {
        return "加权平均融合策略：对数值型字段进行加权平均计算，非数值型字段使用多数投票";
    }
}