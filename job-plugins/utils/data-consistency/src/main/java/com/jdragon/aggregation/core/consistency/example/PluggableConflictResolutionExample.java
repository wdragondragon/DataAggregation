package com.jdragon.aggregation.core.consistency.example;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.*;
import com.jdragon.aggregation.core.consistency.service.DataConsistencyService;
import com.jdragon.aggregation.core.consistency.service.ConflictResolver;
import com.jdragon.aggregation.core.consistency.service.plugin.ConflictResolverProvider;
import com.jdragon.aggregation.core.consistency.service.plugin.ConflictResolverRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * 插件化冲突解决示例
 * 演示三种注册自定义冲突解决器的方式：
 * 1. SPI自动发现
 * 2. 编程式注册
 * 3. 配置式注册（向后兼容）
 */
@Slf4j
public class PluggableConflictResolutionExample {

    public static void main(String[] args) {
        log.info("Starting Pluggable Conflict Resolution Example");
        
        // 初始化数据一致性服务
        DataConsistencyService service = new DataConsistencyService("./plugin-example-results");
        
        try {
            // 演示1: SPI自动发现（需要META-INF/services配置文件）
            demoSpiAutoDiscovery(service);
            
            // 演示2: 编程式注册
            demoProgrammaticRegistration(service);
            
            // 演示3: 配置式注册（向后兼容）
            demoConfigurationBasedRegistration(service);
            
            log.info("All demonstrations completed successfully!");
            
        } catch (Exception e) {
            log.error("Error during pluggable conflict resolution example", e);
        } finally {
            service.shutdown();
        }
    }
    
    /**
     * 演示1: SPI自动发现
     * 通过实现ConflictResolverProvider接口和META-INF/services配置文件自动注册
     */
    private static void demoSpiAutoDiscovery(DataConsistencyService service) {
        log.info("=== 演示1: SPI自动发现 ===");
        
        // 注意：需要外部模块提供META-INF/services配置文件
        // 这里我们手动注册一个SPI提供者来模拟
        ConflictResolverProvider spiProvider = new ExampleSpiProvider();
        ConflictResolverRegistry.getInstance().registerResolverProvider(spiProvider);
        
        // 创建使用SPI注册策略的规则
        ConsistencyRule spiRule = createRuleWithCustomStrategy(
                "spi-rule-001", 
                "SPI策略示例规则",
                "EXAMPLE_SPI_STRATEGY",  // SPI提供者注册的策略名
                null  // 无需额外参数
        );
        
        service.addRule(spiRule);
        log.info("Added SPI-based rule with strategy: {}", spiRule.getConflictResolutionStrategy());
        
        // 注意：实际执行需要真实数据源，这里仅演示注册过程
        log.info("SPI自动发现演示完成（规则已注册，需要真实数据源才能执行）");
    }
    
    /**
     * 演示2: 编程式注册
     * 通过API在运行时注册自定义冲突解决器
     */
    private static void demoProgrammaticRegistration(DataConsistencyService service) {
        log.info("=== 演示2: 编程式注册 ===");
        
        // 方法A: 注册冲突解决器类
        ConflictResolverRegistry.getInstance().registerResolverClass(
                "PROGRAMMATIC_STRATEGY",
                ProgrammaticResolver.class.getName()
        );
        
        // 方法B: 注册冲突解决器实例
        ConflictResolver instance = new ProgrammaticResolver(Collections.emptyList());
        ConflictResolverRegistry.getInstance().registerResolverInstance(
                "INSTANCE_STRATEGY",
                instance
        );
        
        // 创建使用编程式注册策略的规则
        ConsistencyRule programmaticRule = createRuleWithCustomStrategy(
                "programmatic-rule-001",
                "编程式策略示例规则",
                "PROGRAMMATIC_STRATEGY",
                null
        );
        
        service.addRule(programmaticRule);
        log.info("Added programmatic rule with strategy: {}", programmaticRule.getConflictResolutionStrategy());
        
        log.info("编程式注册演示完成");
    }
    
    /**
     * 演示3: 配置式注册（向后兼容）
     * 使用CUSTOM_RULE策略并在resolutionParams中指定customClass参数
     */
    private static void demoConfigurationBasedRegistration(DataConsistencyService service) {
        log.info("=== 演示3: 配置式注册（向后兼容） ===");
        
        // 创建配置式规则
        ConsistencyRule configRule = new ConsistencyRule();
        configRule.setRuleId("config-rule-001");
        configRule.setRuleName("配置式策略示例规则");
        configRule.setDescription("使用CUSTOM_RULE和customClass参数的向后兼容示例");
        configRule.setEnabled(true);
        configRule.setConflictResolutionStrategy(ConflictResolutionStrategy.CUSTOM_RULE);
        
        // 设置自定义解决器类名
        Map<String, Object> resolutionParams = new HashMap<>();
        resolutionParams.put("customClass", ConfigurationBasedResolver.class.getName());
        configRule.setResolutionParams(resolutionParams);
        
        // 设置模拟数据源（实际使用时需要真实数据源）
        configRule.setDataSources(createMockDataSources());
        configRule.setCompareFields(Arrays.asList("field1", "field2"));
        configRule.setMatchKeys(Arrays.asList("id"));
        
        service.addRule(configRule);
        log.info("Added configuration-based rule with CUSTOM_RULE strategy");
        
        log.info("配置式注册演示完成");
    }
    
    /**
     * 创建使用自定义策略的规则
     */
    private static ConsistencyRule createRuleWithCustomStrategy(
            String ruleId, String ruleName, String strategyName, Map<String, Object> params) {
        
        ConsistencyRule rule = new ConsistencyRule();
        rule.setRuleId(ruleId);
        rule.setRuleName(ruleName);
        rule.setDescription("插件化冲突解决示例规则 - " + strategyName);
        rule.setEnabled(true);
        
        // 使用字符串策略名（可以是内置枚举名或自定义策略名）
        rule.setConflictResolutionStrategy(ConflictResolutionStrategy.valueOf("CUSTOM_RULE"));
        // 注意：实际使用时，系统会识别字符串策略名
        // 这里我们通过resolutionParams传递策略名
        Map<String, Object> resolutionParams = new HashMap<>();
        if (params != null) {
            resolutionParams.putAll(params);
        }
        resolutionParams.put("strategyName", strategyName);
        rule.setResolutionParams(resolutionParams);
        
        // 设置模拟数据源
        rule.setDataSources(createMockDataSources());
        rule.setCompareFields(Arrays.asList("name", "value", "score"));
        rule.setMatchKeys(Arrays.asList("id", "code"));
        
        return rule;
    }
    
    /**
     * 创建模拟数据源配置
     */
    private static List<DataSourceConfig> createMockDataSources() {
        // 创建模拟数据源配置
        // 实际示例中应该使用真实数据源配置
        DataSourceConfig source1 = new DataSourceConfig();
        source1.setSourceId("mock-source-1");
        source1.setSourceName("模拟数据源1");
        source1.setPluginName("mock-plugin");
        source1.setConfidenceWeight(1.0);
        source1.setPriority(1);
        
        DataSourceConfig source2 = new DataSourceConfig();
        source2.setSourceId("mock-source-2");
        source2.setSourceName("模拟数据源2");
        source2.setPluginName("mock-plugin");
        source2.setConfidenceWeight(0.8);
        source2.setPriority(2);
        
        return Arrays.asList(source1, source2);
    }
    
    // ========== 示例SPI提供者 ==========
    
    /**
     * 示例SPI提供者
     * 需要在META-INF/services/com.jdragon.aggregation.core.consistency.service.plugin.ConflictResolverProvider
     * 文件中注册此类
     */
    public static class ExampleSpiProvider implements ConflictResolverProvider {
        
        @Override
        public String getStrategyName() {
            return "EXAMPLE_SPI_STRATEGY";
        }
        
        @Override
        public String getDescription() {
            return "示例SPI策略：选择最新时间戳的数据源";
        }
        
        @Override
        public ConflictResolver createResolver(List<DataSourceConfig> dataSourceConfigs,
                                              Map<String, Object> resolutionParams) {
            return new SpiExampleResolver(dataSourceConfigs);
        }
    }
    
    /**
     * SPI示例解决器
     */
    public static class SpiExampleResolver extends com.jdragon.aggregation.core.consistency.service.BaseConflictResolver {
        
        public SpiExampleResolver(List<DataSourceConfig> dataSourceConfigs) {
            super(dataSourceConfigs);
        }
        
        @Override
        public ResolutionResult resolve(DifferenceRecord differenceRecord) {
            ResolutionResult result = createResolutionResult(ConflictResolutionStrategy.CUSTOM_RULE);
            
            // 简单示例逻辑：选择第一个数据源的值
            if (differenceRecord.getSourceValues() != null && !differenceRecord.getSourceValues().isEmpty()) {
                String firstSourceId = differenceRecord.getSourceValues().keySet().iterator().next();
                Map<String, Object> sourceValues = differenceRecord.getSourceValues().get(firstSourceId);
                
                if (sourceValues != null) {
                    result.setResolvedValues(new HashMap<>(sourceValues));
                    result.setWinningSource(firstSourceId);
                }
            }
            
            return result;
        }
        
        @Override
        public ConflictResolutionStrategy getStrategy() {
            return ConflictResolutionStrategy.CUSTOM_RULE;
        }
    }
    
    // ========== 编程式注册示例解决器 ==========
    
    /**
     * 编程式注册示例解决器
     */
    public static class ProgrammaticResolver extends com.jdragon.aggregation.core.consistency.service.BaseConflictResolver {
        
        public ProgrammaticResolver(List<DataSourceConfig> dataSourceConfigs) {
            super(dataSourceConfigs);
        }
        
        @Override
        public ResolutionResult resolve(DifferenceRecord differenceRecord) {
            ResolutionResult result = createResolutionResult(ConflictResolutionStrategy.CUSTOM_RULE);
            
            // 简单示例逻辑：选择优先级最高的数据源
            if (differenceRecord.getSourceValues() != null && !differenceRecord.getSourceValues().isEmpty()) {
                // 这里简化处理，实际应根据数据源优先级选择
                String selectedSourceId = differenceRecord.getSourceValues().keySet().iterator().next();
                Map<String, Object> sourceValues = differenceRecord.getSourceValues().get(selectedSourceId);
                
                if (sourceValues != null) {
                    result.setResolvedValues(new HashMap<>(sourceValues));
                    result.setWinningSource(selectedSourceId);
                }
            }
            
            return result;
        }
        
        @Override
        public ConflictResolutionStrategy getStrategy() {
            return ConflictResolutionStrategy.CUSTOM_RULE;
        }
    }
    
    // ========== 配置式注册示例解决器 ==========
    
    /**
     * 配置式注册示例解决器
     */
    public static class ConfigurationBasedResolver extends com.jdragon.aggregation.core.consistency.service.BaseConflictResolver {
        
        public ConfigurationBasedResolver(List<DataSourceConfig> dataSourceConfigs) {
            super(dataSourceConfigs);
        }
        
        @Override
        public ResolutionResult resolve(DifferenceRecord differenceRecord) {
            ResolutionResult result = createResolutionResult(ConflictResolutionStrategy.CUSTOM_RULE);
            
            // 简单示例逻辑：选择置信权重最高的数据源
            if (differenceRecord.getSourceValues() != null && !differenceRecord.getSourceValues().isEmpty()) {
                // 这里简化处理，实际应根据数据源置信权重选择
                String selectedSourceId = differenceRecord.getSourceValues().keySet().iterator().next();
                Map<String, Object> sourceValues = differenceRecord.getSourceValues().get(selectedSourceId);
                
                if (sourceValues != null) {
                    result.setResolvedValues(new HashMap<>(sourceValues));
                    result.setWinningSource(selectedSourceId);
                }
            }
            
            return result;
        }
        
        @Override
        public ConflictResolutionStrategy getStrategy() {
            return ConflictResolutionStrategy.CUSTOM_RULE;
        }
    }
}