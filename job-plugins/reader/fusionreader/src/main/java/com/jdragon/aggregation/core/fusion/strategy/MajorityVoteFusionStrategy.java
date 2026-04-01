package com.jdragon.aggregation.core.fusion.strategy;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.core.fusion.FusionContext;

import java.util.HashMap;
import java.util.Map;

/**
 * 多数投票融合策略
 * 对每个字段，选择出现次数最多（考虑数据源权重）的值
 */
public class MajorityVoteFusionStrategy implements FusionStrategy {
    
    public static final String NAME = "MAJORITY_VOTE";
    
    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public Column select(String fieldName, Map<String, Column> sourceValues, FusionContext context) {
        if (sourceValues == null || sourceValues.isEmpty()) {
            return null;
        }
        
        // 统计频率（不考虑权重）
        Map<Object, Integer> countFrequency = new HashMap<>();
        // 统计加权频率
        Map<Object, Double> weightedFrequency = new HashMap<>();
        
        for (Map.Entry<String, Column> entry : sourceValues.entrySet()) {
            String sourceId = entry.getKey();
            Column column = entry.getValue();
            
            if (column == null) {
                continue;
            }
            
            Object valueKey = getValueKey(column);
            double weight = context.getSourceWeight(sourceId);
            
            countFrequency.put(valueKey, countFrequency.getOrDefault(valueKey, 0) + 1);
            weightedFrequency.put(valueKey, weightedFrequency.getOrDefault(valueKey, 0.0) + weight);
        }
        
        if (countFrequency.isEmpty()) {
            return null;
        }
        
        // 找出按计数最多的值
        Object majorityByCount = findMajorityByCount(countFrequency);
        // 找出按权重最多的值
        Object majorityByWeight = findMajorityByWeight(weightedFrequency);
        
        // 如果两者一致，返回该值
        if (majorityByCount != null && majorityByWeight != null && 
            majorityByCount.equals(majorityByWeight)) {
            return findColumnByValueKey(sourceValues, majorityByCount);
        }
        
        // 优先返回加权多数
        if (majorityByWeight != null) {
            return findColumnByValueKey(sourceValues, majorityByWeight);
        }
        
        // 返回计数多数
        if (majorityByCount != null) {
            return findColumnByValueKey(sourceValues, majorityByCount);
        }
        
        return null;
    }
    
    /**
     * 获取Column的可比较键（基于rawData）
     */
    private Object getValueKey(Column column) {
        if (column == null) {
            return null;
        }
        Object rawData = column.getRawData();
        // 对于数值类型，标准化处理
        if (rawData instanceof String) {
            return rawData;
        } else if (rawData instanceof Number) {
            // 统一转换为BigDecimal进行比较？
            // 暂时使用原始对象，因为Column内部已经统一为String存储
            return rawData;
        }
        return rawData;
    }
    
    /**
     * 根据值键找到对应的Column对象
     */
    private Column findColumnByValueKey(Map<String, Column> sourceValues, Object valueKey) {
        for (Column column : sourceValues.values()) {
            if (column == null) {
                continue;
            }
            Object key = getValueKey(column);
            if (valueKey == null && key == null) {
                return column;
            }
            if (valueKey != null && valueKey.equals(key)) {
                return column;
            }
        }
        return null;
    }
    
    /**
     * 按计数找出多数值
     */
    private Object findMajorityByCount(Map<Object, Integer> frequency) {
        Object majority = null;
        int maxCount = 0;
        
        for (Map.Entry<Object, Integer> entry : frequency.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                majority = entry.getKey();
            }
        }
        
        return majority;
    }
    
    /**
     * 按权重找出多数值
     */
    private Object findMajorityByWeight(Map<Object, Double> weightedFrequency) {
        Object majority = null;
        double maxWeight = 0.0;
        
        for (Map.Entry<Object, Double> entry : weightedFrequency.entrySet()) {
            if (entry.getValue() > maxWeight) {
                maxWeight = entry.getValue();
                majority = entry.getKey();
            }
        }
        
        return majority;
    }
    
    @Override
    public String getDescription() {
        return "多数投票融合策略：对每个字段，选择出现次数最多（考虑数据源权重）的值";
    }
}