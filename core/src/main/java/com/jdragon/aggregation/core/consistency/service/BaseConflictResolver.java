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
}