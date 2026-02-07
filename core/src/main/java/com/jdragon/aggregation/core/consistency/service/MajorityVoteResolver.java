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

public class MajorityVoteResolver extends BaseConflictResolver {
    
    public MajorityVoteResolver(List<DataSourceConfig> dataSourceConfigs) {
        super(dataSourceConfigs);
    }
    
    @Override
    public ResolutionResult resolve(DifferenceRecord differenceRecord) {
        ResolutionResult result = createResolutionResult(ConflictResolutionStrategy.MAJORITY_VOTE);
        
        Map<String, Object> resolvedValues = new HashMap<>();
        Map<String, Object> firstRecord = differenceRecord.getSourceValues().values().iterator().next();
        Set<String> conflictFields = getConflictFields(differenceRecord);
        
        for (String field : firstRecord.keySet()) {
            Object resolvedValue;
            if (conflictFields.contains(field)) {
                resolvedValue = resolveFieldByMajority(field, differenceRecord);
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
        return ConflictResolutionStrategy.MAJORITY_VOTE;
    }
    
    private Object resolveFieldByMajority(String field, DifferenceRecord differenceRecord) {
        Map<Object, Integer> frequency = new HashMap<>();
        Map<Object, Double> weightedFrequency = new HashMap<>();
        
        for (Map.Entry<String, Map<String, Object>> sourceEntry : differenceRecord.getSourceValues().entrySet()) {
            String sourceId = sourceEntry.getKey();
            Map<String, Object> record = sourceEntry.getValue();
            
            if (isMissingSource(record)) {
                continue;
            }
            
            Object value = record.get(field);
            double weight = getSourceWeight(sourceId);
            
            frequency.put(value, frequency.getOrDefault(value, 0) + 1);
            weightedFrequency.put(value, weightedFrequency.getOrDefault(value, 0.0) + weight);
        }
        
        Object majorityByCount = findMajorityByCount(frequency);
        Object majorityByWeight = findMajorityByWeight(weightedFrequency);
        
        if (majorityByCount != null && majorityByWeight != null && 
            majorityByCount.equals(majorityByWeight)) {
            return majorityByCount;
        }
        
        if (majorityByWeight != null) {
            return majorityByWeight;
        }
        
        return majorityByCount;
    }
    
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
    
    private Set<String> getConflictFields(DifferenceRecord differenceRecord) {
        Set<String> conflictFields = new HashSet<>();
        for (String field : differenceRecord.getDifferences().keySet()) {
            if (!field.equals("missing_data")) {
                conflictFields.add(field);
            }
        }
        return conflictFields;
    }
    
    private boolean isMissingSource(Map<String, Object> record) {
        for (Object value : record.values()) {
            if (value != null) {
                return false;
            }
        }
        return true;
    }
}