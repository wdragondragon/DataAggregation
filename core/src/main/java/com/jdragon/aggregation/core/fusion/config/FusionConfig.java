package com.jdragon.aggregation.core.fusion.config;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.fusion.config.FieldMapping;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据融合配置
 * 包含多源数据融合的所有配置信息
 */
@Data
public class FusionConfig {
    
    /**
     * 连接类型枚举
     */
    public enum JoinType {
        INNER,      // 内连接：只返回匹配的记录
        LEFT,       // 左外连接：返回左表所有记录，右表匹配的记录
        RIGHT,      // 右外连接：返回右表所有记录，左表匹配的记录
        FULL        // 全外连接：返回所有记录，无匹配的用null填充
    }
    
    /**
     * 错误处理模式
     */
    public enum ErrorHandlingMode {
        STRICT,     // 严格模式：任何错误立即失败
        LENIENT,    // 宽松模式：跳过错误，记录警告
        MIXED       // 混合模式：可配置不同字段的严格程度
    }
    
    private List<SourceConfig> sources = new ArrayList<>();  // 数据源配置列表
    private List<String> joinKeys = new ArrayList<>();       // 关联键字段列表
    private JoinType joinType = JoinType.INNER;              // 连接类型
    private List<FieldMapping> fieldMappings = new ArrayList<>(); // 字段映射规则
    private String defaultStrategy = "WEIGHTED_AVERAGE";     // 默认融合策略
    private Map<String, String> fieldStrategies = new HashMap<>(); // 字段级策略覆盖
    private ErrorHandlingMode errorMode = ErrorHandlingMode.LENIENT; // 错误处理模式
    private Map<String, ErrorHandlingMode> fieldErrorModes = new HashMap<>(); // 字段级错误模式
    
    // 缓存配置
    private CacheConfig cacheConfig = new CacheConfig();
    
    // 性能配置
    private PerformanceConfig performanceConfig = new PerformanceConfig();
    
    // 融合详情配置
    private FusionDetailConfig detailConfig = new FusionDetailConfig();
    
    /**
     * 从Configuration解析FusionConfig
     */
    public static FusionConfig fromConfig(Configuration config) {
        FusionConfig fusionConfig = new FusionConfig();
        
        // 解析数据源配置
        List<Configuration> sourceConfigs = config.getListConfiguration("sources");
        if (sourceConfigs != null) {
            for (Configuration sourceConfig : sourceConfigs) {
                fusionConfig.getSources().add(SourceConfig.fromConfig(sourceConfig));
            }
        }
        
        // 解析关联键
        List<String> joinKeys = config.getList("join.keys", String.class);
        if (joinKeys != null) {
            fusionConfig.setJoinKeys(joinKeys);
        }
        
        // 解析连接类型
        String joinTypeStr = config.getString("join.type", "INNER");
        fusionConfig.setJoinType(JoinType.valueOf(joinTypeStr.toUpperCase()));
        
        // 解析字段映射
        List<Configuration> mappingConfigs = config.getListConfiguration("fieldMappings");
        if (mappingConfigs != null) {
            for (Configuration mappingConfig : mappingConfigs) {
                fusionConfig.getFieldMappings().add(FieldMapping.fromConfig(mappingConfig));
            }
        }
        
        // 解析策略配置
        fusionConfig.setDefaultStrategy(config.getString("defaultStrategy", "WEIGHTED_AVERAGE"));
        
        // 解析错误处理模式
        String errorModeStr = config.getString("errorMode", "LENIENT");
        fusionConfig.setErrorMode(ErrorHandlingMode.valueOf(errorModeStr.toUpperCase()));
        
        // 解析字段级错误模式
        Configuration errorModesConfig = config.getConfiguration("errorModes");
        if (errorModesConfig != null) {
            Map<String, String> fieldErrorModes = errorModesConfig.getMap("fieldOverrides", String.class);
            if (fieldErrorModes != null) {
                for (Map.Entry<String, String> entry : fieldErrorModes.entrySet()) {
                    fusionConfig.getFieldErrorModes().put(
                        entry.getKey(), 
                        ErrorHandlingMode.valueOf(entry.getValue().toUpperCase())
                    );
                }
            }
        }
        
        // 解析缓存配置
        Configuration cacheConfig = config.getConfiguration("cache");
        if (cacheConfig != null) {
            fusionConfig.setCacheConfig(CacheConfig.fromConfig(cacheConfig));
        }
        
        // 解析性能配置
        Configuration perfConfig = config.getConfiguration("performance");
        if (perfConfig != null) {
            fusionConfig.setPerformanceConfig(PerformanceConfig.fromConfig(perfConfig));
        }
        
        // 解析融合详情配置
        Configuration detailConfig = config.getConfiguration("detailConfig");
        if (detailConfig != null) {
            fusionConfig.setDetailConfig(FusionDetailConfig.fromConfig(detailConfig));
        }
        
        return fusionConfig;
    }
    
    /**
     * 缓存配置
     */
    @Data
    public static class CacheConfig {
        private String type = "memory";           // 缓存类型：memory, partition, external
        private int maxSize = 100000;             // 最大缓存记录数
        private int partitionCount = 10;          // 分区数量（分区缓存时使用）
        private String externalStorageType;       // 外部存储类型：redis, file
        
        public static CacheConfig fromConfig(Configuration config) {
            CacheConfig cacheConfig = new CacheConfig();
            cacheConfig.setType(config.getString("type", "memory"));
            cacheConfig.setMaxSize(config.getInt("maxSize", 100000));
            cacheConfig.setPartitionCount(config.getInt("partitionCount", 10));
            cacheConfig.setExternalStorageType(config.getString("externalStorageType"));
            return cacheConfig;
        }
    }
    
    /**
     * 性能配置
     */
    @Data
    public static class PerformanceConfig {
        private int batchSize = 1000;             // 批量处理大小
        private int parallelSourceCount = 2;      // 并行加载的数据源数量
        private boolean enableLazyLoading = true; // 是否启用懒加载
        private int memoryLimitMB = 1024;         // 内存限制（MB）
        
        public static PerformanceConfig fromConfig(Configuration config) {
            PerformanceConfig perfConfig = new PerformanceConfig();
            perfConfig.setBatchSize(config.getInt("batchSize", 1000));
            perfConfig.setParallelSourceCount(config.getInt("parallelSourceCount", 2));
            perfConfig.setEnableLazyLoading(config.getBool("enableLazyLoading", true));
            perfConfig.setMemoryLimitMB(config.getInt("memoryLimitMB", 1024));
            return perfConfig;
        }
    }
    
    /**
     * 验证配置有效性
     */
    public void validate() {
        if (sources == null || sources.isEmpty()) {
            throw new IllegalArgumentException("至少需要配置一个数据源");
        }
        
        if (joinKeys == null || joinKeys.isEmpty()) {
            throw new IllegalArgumentException("必须配置关联键字段");
        }
        
        if (fieldMappings == null || fieldMappings.isEmpty()) {
            throw new IllegalArgumentException("必须配置字段映射规则");
        }
        
        // 检查数据源ID唯一性
        Map<String, Boolean> sourceIdMap = new HashMap<>();
        for (SourceConfig source : sources) {
            if (sourceIdMap.containsKey(source.getSourceId())) {
                throw new IllegalArgumentException("数据源ID必须唯一: " + source.getSourceId());
            }
            sourceIdMap.put(source.getSourceId(), true);
        }
        
        // 验证每个字段映射配置
        Map<String, Boolean> targetFieldMap = new HashMap<>();
        for (FieldMapping mapping : fieldMappings) {
            // 验证映射配置本身
            mapping.validate();
            
            // 检查目标字段唯一性
            String targetField = mapping.getTargetField();
            if (targetFieldMap.containsKey(targetField)) {
                throw new IllegalArgumentException("目标字段名必须唯一: " + targetField);
            }
            targetFieldMap.put(targetField, true);
            
            // 验证源字段引用（如果适用）
            validateFieldMappingSources(mapping);
        }
        
        // 验证融合详情配置
        detailConfig.validate();
    }
    
    /**
     * 验证字段映射的源引用
     */
    private void validateFieldMappingSources(FieldMapping mapping) {
        // 根据映射类型进行验证
        switch (mapping.getMappingType()) {
            case DIRECT:
                validateDirectFieldMapping((DirectFieldMapping) mapping);
                break;
            case EXPRESSION:
                validateExpressionFieldMapping((ExpressionFieldMapping) mapping);
                break;
            case CONDITIONAL:
                validateConditionalFieldMapping((ConditionalFieldMapping) mapping);
                break;
            case GROOVY:
                // Groovy脚本验证暂不检查源引用
                break;
            case CONSTANT:
                // 常量映射无需验证源引用
                break;
            default:
                throw new IllegalArgumentException("不支持的字段映射类型: " + mapping.getMappingType());
        }
    }
    
    /**
     * 验证直接字段映射的源引用
     */
    private void validateDirectFieldMapping(DirectFieldMapping mapping) {
        String sourceField = mapping.getSourceField();
        if (sourceField == null) return;
        
        // 去除 ${} 包装
        String fieldRef = sourceField;
        if (fieldRef.startsWith("${") && fieldRef.endsWith("}")) {
            fieldRef = fieldRef.substring(2, fieldRef.length() - 1);
        }
        String[] parts = fieldRef.split("\\.");
        if (parts.length == 2) {
            // 格式: sourceId.fieldName
            String sourceId = parts[0];
            // 检查数据源是否存在
            boolean sourceExists = sources.stream().anyMatch(s -> s.getSourceId().equals(sourceId));
            if (!sourceExists) {
                throw new IllegalArgumentException("直接字段映射引用了不存在的数据源: " + sourceId + " (字段: " + mapping.getTargetField() + ")");
            }
        }
        // 格式为fieldName的情况无需验证，将在运行时从所有数据源查找
    }
    
    /**
     * 验证表达式字段映射的源引用
     */
    private void validateExpressionFieldMapping(ExpressionFieldMapping mapping) {
        // 表达式验证已在mapping.validate()中通过表达式引擎完成
        // 这里可以额外检查引用的数据源是否存在
        // 暂时跳过，因为表达式引擎会在运行时处理
    }
    
    /**
     * 验证条件字段映射的源引用
     */
    private void validateConditionalFieldMapping(ConditionalFieldMapping mapping) {
        // 条件表达式验证将在mapping.validate()中完成
        // 这里可以检查trueValue和falseValue中的源引用
        // 暂时跳过
    }
    
    /**
     * 获取数据源配置
     */
    public SourceConfig getSourceConfig(String sourceId) {
        for (SourceConfig source : sources) {
            if (source.getSourceId().equals(sourceId)) {
                return source;
            }
        }
        return null;
    }
}