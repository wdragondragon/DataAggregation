package com.jdragon.aggregation.core.fusion.strategy;

import java.util.HashMap;
import java.util.Map;

/**
 * 融合策略工厂
 */
public class FusionStrategyFactory {
    
    private static final Map<String, FusionStrategy> STRATEGIES = new HashMap<>();
    
    static {
        initDefaultStrategies();
    }
    
    /**
     * 初始化默认策略
     */
    public static void initDefaultStrategies() {
        registerStrategy(new PriorityFusionStrategy());
        registerStrategy(new WeightedAverageFusionStrategy());
        registerStrategy(new HighConfidenceFusionStrategy());
        registerStrategy(new MajorityVoteFusionStrategy());
    }
    
    /**
     * 注册策略
     */
    public static void registerStrategy(FusionStrategy strategy) {
        STRATEGIES.put(strategy.getName(), strategy);
    }
    
    /**
     * 获取策略
     */
    public static FusionStrategy getStrategy(String name) {
        return STRATEGIES.get(name.toUpperCase());
    }
    
    /**
     * 获取所有已注册策略
     */
    public static Map<String, FusionStrategy> getAllStrategies() {
        return new HashMap<>(STRATEGIES);
    }
}