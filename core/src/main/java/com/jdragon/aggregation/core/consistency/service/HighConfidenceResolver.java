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

public class HighConfidenceResolver extends BaseConflictResolver {
    
    public HighConfidenceResolver(List<DataSourceConfig> dataSourceConfigs) {
        super(dataSourceConfigs);
    }
    
    @Override
    public ResolutionResult resolve(DifferenceRecord differenceRecord) {
        ResolutionResult result = createResolutionResult(ConflictResolutionStrategy.HIGH_CONFIDENCE);
        
        String winningSource = findHighestConfidenceSource(differenceRecord);
        if (winningSource == null) {
            winningSource = differenceRecord.getSourceValues().keySet().iterator().next();
        }
        Map<String, Object> winningValues = differenceRecord.getSourceValues().get(winningSource);
        Map<String, Object> firstRecord = differenceRecord.getSourceValues().values().iterator().next();
        Set<String> conflictFields = getConflictFields(differenceRecord);
        
        Map<String, Object> resolvedValues = new HashMap<>();
        for (String field : firstRecord.keySet()) {
            Object resolvedValue;
            if (conflictFields.contains(field)) {
                resolvedValue = winningValues.get(field);
            } else {
                resolvedValue = firstRecord.get(field);
            }
            resolvedValues.put(field, resolvedValue);
        }
        
        result.setResolvedValues(resolvedValues);
        result.setWinningSource(winningSource);

        return result;
    }
    
    @Override
    public ConflictResolutionStrategy getStrategy() {
        return ConflictResolutionStrategy.HIGH_CONFIDENCE;
    }
    
    private String findHighestConfidenceSource(DifferenceRecord differenceRecord) {
        String winningSource = null;
        double maxWeight = -1.0;
        
        for (String sourceId : differenceRecord.getSourceValues().keySet()) {
            if (isMissingSource(differenceRecord.getSourceValues().get(sourceId))) {
                continue;
            }
            double weight = getSourceWeight(sourceId);
            if (weight > maxWeight) {
                maxWeight = weight;
                winningSource = sourceId;
            } else if (weight == maxWeight && winningSource != null) {
                int currentPriority = getSourcePriority(winningSource);
                int newPriority = getSourcePriority(sourceId);
                if (newPriority > currentPriority) {
                    winningSource = sourceId;
                }
            }
        }
        
        return winningSource;
    }
    
    private boolean isMissingSource(Map<String, Object> record) {
        for (Object value : record.values()) {
            if (value != null) {
                return false;
            }
        }
        return true;
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
}