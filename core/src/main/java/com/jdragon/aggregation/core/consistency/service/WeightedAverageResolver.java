package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WeightedAverageResolver extends BaseConflictResolver {
    
    public WeightedAverageResolver(List<DataSourceConfig> dataSourceConfigs) {
        super(dataSourceConfigs);
    }
    
    @Override
    public ResolutionResult resolve(DifferenceRecord differenceRecord) {
        ResolutionResult result = createResolutionResult(ConflictResolutionStrategy.WEIGHTED_AVERAGE);
        
        Map<String, Object> resolvedValues = new HashMap<>();
        Map<String, Double> sourceWeights = new HashMap<>();
        
        for (String sourceId : differenceRecord.getSourceValues().keySet()) {
            double weight = getSourceWeight(sourceId);
            sourceWeights.put(sourceId, weight);
        }
        
        Map<String, Object> firstRecord = differenceRecord.getSourceValues().values().iterator().next();
        Set<String> conflictFields = getConflictFields(differenceRecord);
        
        for (String field : firstRecord.keySet()) {
            Object resolvedValue;
            if (conflictFields.contains(field)) {
                resolvedValue = resolveField(field, differenceRecord, sourceWeights);
            } else {
                resolvedValue = firstRecord.get(field);
            }
            resolvedValues.put(field, resolvedValue);
        }
        
        result.setResolvedValues(resolvedValues);

        return result;
    }
    
    @Override
    public ConflictResolutionStrategy getStrategy() {
        return ConflictResolutionStrategy.WEIGHTED_AVERAGE;
    }
    
    @Override
    public boolean canResolve(DifferenceRecord differenceRecord) {
        if (!super.canResolve(differenceRecord)) {
            return false;
        }
        
        Set<String> conflictFields = getConflictFields(differenceRecord);
        return !conflictFields.isEmpty();
    }
    
    private Set<String> getConflictFields(DifferenceRecord differenceRecord) {
        Set<String> conflictFields = new HashSet<>();
        for (String field : differenceRecord.getDifferences().keySet()) {
            if (!field.equals("missing_data")) {
                conflictFields.add(field);
            }
        }
        return conflictFields;
    }
    
    private Object resolveField(String field, DifferenceRecord differenceRecord, 
                               Map<String, Double> sourceWeights) {
        
        if (!isNumericField(field, differenceRecord)) {
            return getMostCommonValue(field, differenceRecord);
        }
        
        double weightedSum = 0.0;
        double effectiveTotalWeight = 0.0;
        for (Map.Entry<String, Map<String, Object>> sourceEntry : differenceRecord.getSourceValues().entrySet()) {
            String sourceId = sourceEntry.getKey();
            Map<String, Object> record = sourceEntry.getValue();
            Object value = record.get(field);
            
            if (value != null) {
                double numericValue = convertToDouble(value);
                double weight = sourceWeights.get(sourceId);
                weightedSum += numericValue * weight;
                effectiveTotalWeight += weight;
            }
        }
        
        if (effectiveTotalWeight == 0.0) {
            return null;
        }
        
        return weightedSum / effectiveTotalWeight;
    }
    
    private boolean isNumericField(String field, DifferenceRecord differenceRecord) {
        for (Map<String, Object> record : differenceRecord.getSourceValues().values()) {
            Object value = record.get(field);
            if (value != null && !isNumeric(value)) {
                return false;
            }
        }
        return true;
    }
    
    private boolean isNumeric(Object value) {
        if (value instanceof Number) {
            return true;
        }
        try {
            Double.parseDouble(value.toString());
            return true;
        } catch (NumberFormatException | NullPointerException e) {
            return false;
        }
    }
    
    private double convertToDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(value.toString());
    }
    
    private Object getMostCommonValue(String field, DifferenceRecord differenceRecord) {
        Map<Object, Integer> frequency = new HashMap<>();
        for (Map<String, Object> record : differenceRecord.getSourceValues().values()) {
            Object value = record.get(field);
            frequency.put(value, frequency.getOrDefault(value, 0) + 1);
        }
        
        Object mostCommon = null;
        int maxCount = 0;
        for (Map.Entry<Object, Integer> entry : frequency.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                mostCommon = entry.getKey();
            }
        }
        
        return mostCommon;
    }
}