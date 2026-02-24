package com.jdragon.aggregation.core.consistency.service.plugin;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.service.ConflictResolver;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 冲突解决器注册表
 * 支持从外部注册自定义冲突解决器，无需修改core代码
 */
@Slf4j
public class ConflictResolverRegistry {
    
    private static final ConflictResolverRegistry INSTANCE = new ConflictResolverRegistry();
    
    private final Map<String, ResolverFactory> resolverFactories = new ConcurrentHashMap<>();
    
    private final Map<String, String> strategyToFactoryMap = new ConcurrentHashMap<>();
    
    private ConflictResolverRegistry() {
        // 私有构造函数，单例模式
        initBuiltInResolvers();
        loadSpiResolvers();
    }
    
    public static ConflictResolverRegistry getInstance() {
        return INSTANCE;
    }
    
    /**
     * 初始化内置的冲突解决器
     */
    private void initBuiltInResolvers() {
        // 注册内置策略，这些策略在core模块中已实现
        registerBuiltInResolver(ConflictResolutionStrategy.HIGH_CONFIDENCE.name(), 
                "com.jdragon.aggregation.core.consistency.service.HighConfidenceResolver");
        registerBuiltInResolver(ConflictResolutionStrategy.WEIGHTED_AVERAGE.name(), 
                "com.jdragon.aggregation.core.consistency.service.WeightedAverageResolver");
        registerBuiltInResolver(ConflictResolutionStrategy.MAJORITY_VOTE.name(), 
                "com.jdragon.aggregation.core.consistency.service.MajorityVoteResolver");
        registerBuiltInResolver(ConflictResolutionStrategy.MANUAL_REVIEW.name(), 
                "com.jdragon.aggregation.core.consistency.service.ConflictResolverFactory$ManualReviewResolver");
        
        log.info("Initialized {} built-in conflict resolvers", resolverFactories.size());
    }
    
    /**
     * 加载SPI机制发现的外部冲突解决器
     */
    private void loadSpiResolvers() {
        try {
            ServiceLoader<ConflictResolverProvider> loader = ServiceLoader.load(ConflictResolverProvider.class);
            int count = 0;
            for (ConflictResolverProvider provider : loader) {
                registerResolverProvider(provider);
                count++;
            }
            if (count > 0) {
                log.info("Loaded {} conflict resolver providers via SPI", count);
            }
        } catch (Exception e) {
            log.warn("Failed to load conflict resolver providers via SPI", e);
        }
    }
    
    /**
     * 注册内置解决器
     */
    private void registerBuiltInResolver(String strategyName, String className) {
        resolverFactories.put(className, new ClassResolverFactory(className));
        strategyToFactoryMap.put(strategyName.toUpperCase(), className);
    }
    
    /**
     * 注册冲突解决器提供者
     */
    public void registerResolverProvider(ConflictResolverProvider provider) {
        if (provider == null || provider.getStrategyName() == null) {
            log.warn("Invalid conflict resolver provider, skipping registration");
            return;
        }
        
        String strategyName = provider.getStrategyName().toUpperCase();
        String factoryKey = "provider:" + provider.getClass().getName();
        
        resolverFactories.put(factoryKey, new ProviderResolverFactory(provider));
        strategyToFactoryMap.put(strategyName, factoryKey);
        
        log.info("Registered conflict resolver provider for strategy: {}", strategyName);
    }
    
    /**
     * 注册自定义冲突解决器类
     * @param strategyName 策略名称（如"CUSTOM_RULE"、"MY_CUSTOM_STRATEGY"）
     * @param resolverClassName 冲突解决器类名，必须实现ConflictResolver接口
     */
    public void registerResolverClass(String strategyName, String resolverClassName) {
        if (strategyName == null || resolverClassName == null) {
            throw new IllegalArgumentException("Strategy name and resolver class name cannot be null");
        }
        
        String upperStrategyName = strategyName.toUpperCase();
        if (strategyToFactoryMap.containsKey(upperStrategyName)) {
            log.warn("Overriding existing resolver for strategy: {}", upperStrategyName);
        }
        
        resolverFactories.put(resolverClassName, new ClassResolverFactory(resolverClassName));
        strategyToFactoryMap.put(upperStrategyName, resolverClassName);
        
        log.info("Registered conflict resolver class {} for strategy: {}", resolverClassName, upperStrategyName);
    }
    
    /**
     * 注册冲突解决器实例
     * @param strategyName 策略名称
     * @param resolver 冲突解决器实例
     */
    public void registerResolverInstance(String strategyName, ConflictResolver resolver) {
        if (strategyName == null || resolver == null) {
            throw new IllegalArgumentException("Strategy name and resolver cannot be null");
        }
        
        String upperStrategyName = strategyName.toUpperCase();
        String instanceKey = "instance:" + System.identityHashCode(resolver);
        
        resolverFactories.put(instanceKey, new InstanceResolverFactory(resolver));
        strategyToFactoryMap.put(upperStrategyName, instanceKey);
        
        log.info("Registered conflict resolver instance for strategy: {}", upperStrategyName);
    }
    
    /**
     * 创建冲突解决器
     * @param strategyName 策略名称
     * @param dataSourceConfigs 数据源配置列表
     * @param resolutionParams 解决参数
     * @return 冲突解决器实例
     */
    public ConflictResolver createResolver(String strategyName, 
                                          List<DataSourceConfig> dataSourceConfigs,
                                          Map<String, Object> resolutionParams) {
        if (strategyName == null) {
            throw new IllegalArgumentException("Strategy name cannot be null");
        }
        
        String upperStrategyName = strategyName.toUpperCase();
        String factoryKey = strategyToFactoryMap.get(upperStrategyName);
        
        if (factoryKey == null) {
            // 尝试从CUSTOM_RULE参数中获取类名（向后兼容）
            if ("CUSTOM_RULE".equals(upperStrategyName) && resolutionParams != null) {
                // 首先检查是否指定了策略名（用于插件化架构）
                String customStrategyName = (String) resolutionParams.get("strategyName");
                if (customStrategyName != null) {
                    String customStrategyNameUpper = customStrategyName.toUpperCase();
                    factoryKey = strategyToFactoryMap.get(customStrategyNameUpper);
                    if (factoryKey == null) {
                        // 如果策略未注册，检查是否有customClass参数
                        String customClassName = (String) resolutionParams.get("customClass");
                        if (customClassName != null) {
                            // 使用自定义策略名注册解决器类
                            registerResolverClass(customStrategyNameUpper, customClassName);
                            factoryKey = customClassName;
                        }
                    }
                } else {
                    // 原始的向后兼容逻辑：使用customClass参数
                    String customClassName = (String) resolutionParams.get("customClass");
                    if (customClassName != null) {
                        factoryKey = customClassName;
                        if (!resolverFactories.containsKey(factoryKey)) {
                            registerResolverClass(upperStrategyName, customClassName);
                        }
                    }
                }
            }
            
            if (factoryKey == null) {
                throw new IllegalArgumentException("No resolver registered for strategy: " + strategyName);
            }
        }
        
        ResolverFactory factory = resolverFactories.get(factoryKey);
        if (factory == null) {
            throw new IllegalStateException("Resolver factory not found for key: " + factoryKey);
        }
        
        try {
            return factory.create(dataSourceConfigs, resolutionParams);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create resolver for strategy: " + strategyName, e);
        }
    }
    
    /**
     * 获取所有已注册的策略名称
     */
    public Set<String> getRegisteredStrategies() {
        return new HashSet<>(strategyToFactoryMap.keySet());
    }
    
    /**
     * 检查策略是否已注册
     */
    public boolean hasStrategy(String strategyName) {
        return strategyName != null && strategyToFactoryMap.containsKey(strategyName.toUpperCase());
    }
    
    /**
     * 移除策略注册
     */
    public void unregisterStrategy(String strategyName) {
        if (strategyName == null) {
            return;
        }
        
        String upperStrategyName = strategyName.toUpperCase();
        String factoryKey = strategyToFactoryMap.remove(upperStrategyName);
        if (factoryKey != null) {
            resolverFactories.remove(factoryKey);
            log.info("Unregistered conflict resolver for strategy: {}", upperStrategyName);
        }
    }
    
    /**
     * 清除所有注册
     */
    public void clear() {
        strategyToFactoryMap.clear();
        resolverFactories.clear();
        log.info("Cleared all conflict resolver registrations");
        // 重新初始化内置解决器
        initBuiltInResolvers();
    }
    
    // ========== 内部工厂接口和实现 ==========
    
    private interface ResolverFactory {
        ConflictResolver create(List<DataSourceConfig> dataSourceConfigs, Map<String, Object> resolutionParams) 
                throws Exception;
    }
    
    private static class ClassResolverFactory implements ResolverFactory {
        private final String className;
        
        ClassResolverFactory(String className) {
            this.className = className;
        }
        
        @Override
        public ConflictResolver create(List<DataSourceConfig> dataSourceConfigs, Map<String, Object> resolutionParams) 
                throws Exception {
            Class<?> clazz = Class.forName(className);
            if (!ConflictResolver.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Class " + className + " does not implement ConflictResolver");
            }
            
            try {
                // 尝试使用List<DataSourceConfig>构造函数
                return (ConflictResolver) clazz.getConstructor(List.class).newInstance(dataSourceConfigs);
            } catch (NoSuchMethodException e) {
                // 尝试使用无参构造函数
                return (ConflictResolver) clazz.getDeclaredConstructor().newInstance();
            }
        }
    }
    
    private static class InstanceResolverFactory implements ResolverFactory {
        private final ConflictResolver instance;
        
        InstanceResolverFactory(ConflictResolver instance) {
            this.instance = instance;
        }
        
        @Override
        public ConflictResolver create(List<DataSourceConfig> dataSourceConfigs, Map<String, Object> resolutionParams) {
            return instance;
        }
    }
    
    private static class ProviderResolverFactory implements ResolverFactory {
        private final ConflictResolverProvider provider;
        
        ProviderResolverFactory(ConflictResolverProvider provider) {
            this.provider = provider;
        }
        
        @Override
        public ConflictResolver create(List<DataSourceConfig> dataSourceConfigs, Map<String, Object> resolutionParams) 
                throws Exception {
            return provider.createResolver(dataSourceConfigs, resolutionParams);
        }
    }
}