package com.jdragon.aggregation.core.consistency.service.plugin;

import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.service.ConflictResolver;

import java.util.List;
import java.util.Map;

/**
 * 冲突解决器提供者接口
 * 用于SPI机制自动发现和注册外部冲突解决器
 */
public interface ConflictResolverProvider {
    
    /**
     * 获取策略名称
     * @return 策略名称（如"MY_CUSTOM_STRATEGY"）
     */
    String getStrategyName();
    
    /**
     * 获取策略描述
     * @return 策略描述信息
     */
    default String getDescription() {
        return "Custom conflict resolution strategy";
    }
    
    /**
     * 创建冲突解决器实例
     * @param dataSourceConfigs 数据源配置列表
     * @param resolutionParams 解决参数
     * @return 冲突解决器实例
     */
    ConflictResolver createResolver(List<DataSourceConfig> dataSourceConfigs, 
                                   Map<String, Object> resolutionParams);
}