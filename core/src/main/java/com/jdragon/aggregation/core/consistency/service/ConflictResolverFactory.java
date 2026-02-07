package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;

import java.util.List;
import java.util.Map;

public class ConflictResolverFactory {
    
    public static ConflictResolver createResolver(
            ConflictResolutionStrategy strategy,
            List<DataSourceConfig> dataSourceConfigs,
            Map<String, Object> resolutionParams) {
        
        switch (strategy) {
            case HIGH_CONFIDENCE:
                return new HighConfidenceResolver(dataSourceConfigs);
                
            case WEIGHTED_AVERAGE:
                return new WeightedAverageResolver(dataSourceConfigs);
                
            case MAJORITY_VOTE:
                return new MajorityVoteResolver(dataSourceConfigs);
                
            case CUSTOM_RULE:
                return createCustomResolver(resolutionParams, dataSourceConfigs);
                
            case MANUAL_REVIEW:
                return new ManualReviewResolver(dataSourceConfigs);
                
            default:
                throw new IllegalArgumentException("Unsupported conflict resolution strategy: " + strategy);
        }
    }
    
    private static ConflictResolver createCustomResolver(
            Map<String, Object> resolutionParams,
            List<DataSourceConfig> dataSourceConfigs) {
        
        String customClassName = (String) resolutionParams.get("customClass");
        if (customClassName == null) {
            throw new IllegalArgumentException("Custom resolver class name not specified in resolutionParams");
        }
        
        try {
            Class<?> clazz = Class.forName(customClassName);
            if (!ConflictResolver.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Class " + customClassName + " does not implement ConflictResolver");
            }
            
            return (ConflictResolver) clazz.getConstructor(List.class).newInstance(dataSourceConfigs);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create custom resolver: " + customClassName, e);
        }
    }
    
    static class ManualReviewResolver extends BaseConflictResolver {
        
        public ManualReviewResolver(List<DataSourceConfig> dataSourceConfigs) {
            super(dataSourceConfigs);
        }
        
        @Override
        public com.jdragon.aggregation.core.consistency.model.ResolutionResult resolve(
                com.jdragon.aggregation.core.consistency.model.DifferenceRecord differenceRecord) {
            
            com.jdragon.aggregation.core.consistency.model.ResolutionResult result = 
                createResolutionResult(ConflictResolutionStrategy.MANUAL_REVIEW);
            
            result.setManuallyReviewed(false);
            
            return result;
        }
        
        @Override
        public ConflictResolutionStrategy getStrategy() {
            return ConflictResolutionStrategy.MANUAL_REVIEW;
        }
    }
}