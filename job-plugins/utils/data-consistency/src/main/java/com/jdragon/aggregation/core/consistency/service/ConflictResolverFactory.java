package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import com.jdragon.aggregation.core.consistency.service.plugin.ConflictResolverRegistry;

import java.util.List;
import java.util.Map;

/**
 * 冲突解决器工厂（已更新为支持插件化架构）
 * 现在支持从外部注册自定义冲突解决器
 */
public class ConflictResolverFactory {

    /**
     * 创建冲突解决器（兼容原有API）
     * @param strategy 冲突解决策略
     * @param dataSourceConfigs 数据源配置列表
     * @param resolutionParams 解决参数
     * @return 冲突解决器实例
     */
    public static ConflictResolver createResolver(
            ConflictResolutionStrategy strategy,
            List<DataSourceConfig> dataSourceConfigs,
            Map<String, Object> resolutionParams) {

        return createResolver(strategy.name(), dataSourceConfigs, resolutionParams);
    }

    /**
     * 创建冲突解决器（新API，支持字符串策略名）
     * @param strategyName 策略名称（可以是枚举名或自定义策略名）
     * @param dataSourceConfigs 数据源配置列表
     * @param resolutionParams 解决参数
     * @return 冲突解决器实例
     */
    public static ConflictResolver createResolver(
            String strategyName,
            List<DataSourceConfig> dataSourceConfigs,
            Map<String, Object> resolutionParams) {

        // 使用插件注册表创建解决器
        return ConflictResolverRegistry.getInstance().createResolver(
                strategyName, dataSourceConfigs, resolutionParams);
    }

    /**
     * 向后兼容的辅助方法：创建自定义解决器
     * 现在委托给插件注册表处理
     */
    private static ConflictResolver createCustomResolver(
            Map<String, Object> resolutionParams,
            List<DataSourceConfig> dataSourceConfigs) {

        String customClassName = (String) resolutionParams.get("customClass");
        if (customClassName == null) {
            throw new IllegalArgumentException("Custom resolver class name not specified in resolutionParams");
        }

        // 使用插件注册表注册并创建自定义解决器
        ConflictResolverRegistry registry = ConflictResolverRegistry.getInstance();
        if (!registry.hasStrategy("CUSTOM_RULE")) {
            registry.registerResolverClass("CUSTOM_RULE", customClassName);
        }

        return registry.createResolver("CUSTOM_RULE", dataSourceConfigs, resolutionParams);
    }

    /**
     * 获取冲突解决器注册表实例（用于高级用法）
     */
    public static ConflictResolverRegistry getRegistry() {
        return ConflictResolverRegistry.getInstance();
    }

    /**
     * 手动审核解决器（保持原有实现，作为内置策略）
     */
    public static class ManualReviewResolver extends BaseConflictResolver {

        public ManualReviewResolver(List<DataSourceConfig> dataSourceConfigs) {
            super(dataSourceConfigs);
        }

        @Override
        public ResolutionResult resolve(
                DifferenceRecord differenceRecord) {

            ResolutionResult result = createResolutionResult(ConflictResolutionStrategy.MANUAL_REVIEW);

            result.setManuallyReviewed(false);

            return result;
        }

        @Override
        public ConflictResolutionStrategy getStrategy() {
            return ConflictResolutionStrategy.MANUAL_REVIEW;
        }
    }
}