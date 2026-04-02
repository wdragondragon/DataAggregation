package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseConflictResolver implements ConflictResolver {
    
    protected final Map<String, DataSourceConfig> dataSourceConfigs;
    
    protected BaseConflictResolver(List<DataSourceConfig> dataSourceConfigs) {
        this.dataSourceConfigs = dataSourceConfigs.stream()
                .collect(Collectors.toMap(DataSourceConfig::getSourceId, config -> config));
    }
    
    @Override
    public boolean canResolve(DifferenceRecord differenceRecord) {
        return differenceRecord != null && 
               differenceRecord.getSourceValues() != null && 
               !differenceRecord.getSourceValues().isEmpty();
    }
    
    protected Double getSourceWeight(String sourceId) {
        DataSourceConfig config = dataSourceConfigs.get(sourceId);
        return config != null ? config.getConfidenceWeight() : 1.0;
    }
    
    protected int getSourcePriority(String sourceId) {
        DataSourceConfig config = dataSourceConfigs.get(sourceId);
        return config != null ? config.getPriority() : 0;
    }
    
    protected ResolutionResult createResolutionResult(ConflictResolutionStrategy strategy) {
        ResolutionResult result = new ResolutionResult();
        result.setStrategyUsed(strategy);
        result.setResolutionId(java.util.UUID.randomUUID().toString());
        return result;
    }

    protected boolean isMissingSource(DifferenceRecord differenceRecord, String sourceId) {
        if (differenceRecord == null || sourceId == null) {
            return false;
        }
        List<String> missingSources = differenceRecord.getMissingSources();
        if (missingSources != null && missingSources.contains(sourceId)) {
            return true;
        }
        Map<String, Object> record = differenceRecord.getSourceValues() != null
                ? differenceRecord.getSourceValues().get(sourceId)
                : null;
        return isPlaceholderRecord(record);
    }

    protected boolean isPlaceholderRecord(Map<String, Object> record) {
        if (record == null || record.isEmpty()) {
            return true;
        }
        for (Object value : record.values()) {
            if (value != null) {
                return false;
            }
        }
        return true;
    }
}
