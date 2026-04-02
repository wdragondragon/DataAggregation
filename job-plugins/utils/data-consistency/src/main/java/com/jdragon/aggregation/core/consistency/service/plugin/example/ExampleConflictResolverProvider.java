package com.jdragon.aggregation.core.consistency.service.plugin.example;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import com.jdragon.aggregation.core.consistency.service.BaseConflictResolver;
import com.jdragon.aggregation.core.consistency.service.ConflictResolver;
import com.jdragon.aggregation.core.consistency.service.plugin.ConflictResolverProvider;

import java.util.List;
import java.util.Map;

/**
 * 示例冲突解决器提供者
 * 演示如何从外部注册自定义冲突解决器
 */
public class ExampleConflictResolverProvider implements ConflictResolverProvider {
    
    @Override
    public String getStrategyName() {
        return "EXAMPLE_STRATEGY";
    }
    
    @Override
    public String getDescription() {
        return "示例策略：选择第一个非空数据源的值";
    }
    
    @Override
    public ConflictResolver createResolver(List<DataSourceConfig> dataSourceConfigs, 
                                          Map<String, Object> resolutionParams) {
        return new ExampleResolver(dataSourceConfigs);
    }
    
    /**
     * 示例冲突解决器实现
     * 简单策略：选择第一个非空数据源的值
     */
    public static class ExampleResolver extends BaseConflictResolver {
        
        public ExampleResolver(List<DataSourceConfig> dataSourceConfigs) {
            super(dataSourceConfigs);
        }
        
        @Override
        public ResolutionResult resolve(DifferenceRecord differenceRecord) {
            ResolutionResult result = createResolutionResult(
                    ConflictResolutionStrategy.valueOf("CUSTOM_RULE"));
            
            if (differenceRecord.getSourceValues() == null || 
                differenceRecord.getSourceValues().isEmpty()) {
                return result;
            }
            
            // 简单策略：选择第一个数据源的值
            String firstSourceId = differenceRecord.getSourceValues().keySet().iterator().next();
            Map<String, Object> sourceValues = differenceRecord.getSourceValues().get(firstSourceId);
            
            if (sourceValues != null) {
                // 复制所有字段值
                result.setResolvedValues(new java.util.HashMap<>(sourceValues));
                result.setWinningSource(firstSourceId);
            }
            
            return result;
        }
        
        @Override
        public ConflictResolutionStrategy getStrategy() {
            // 返回CUSTOM_RULE，因为我们没有在枚举中定义EXAMPLE_STRATEGY
            return ConflictResolutionStrategy.CUSTOM_RULE;
        }
        
        @Override
        public boolean canResolve(DifferenceRecord differenceRecord) {
            return super.canResolve(differenceRecord) && 
                   differenceRecord.getSourceValues() != null &&
                   !differenceRecord.getSourceValues().isEmpty();
        }
    }
}