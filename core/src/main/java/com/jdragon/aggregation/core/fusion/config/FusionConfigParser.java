package com.jdragon.aggregation.core.fusion.config;

import com.jdragon.aggregation.commons.util.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 融合配置解析器
 * 将Configuration转换为FusionConfig
 */
public class FusionConfigParser {
    
    /**
     * 解析融合配置
     * @param pluginJobConf 插件作业配置
     * @return 融合配置
     */
    public static FusionConfig parse(Configuration pluginJobConf) {
        FusionConfig fusionConfig = new FusionConfig();
        
        // 解析数据源配置
        List<Configuration> sourceConfigs = pluginJobConf.getListConfiguration("sources");
        List<SourceConfig> sources = new ArrayList<>();
        for (Configuration sourceConf : sourceConfigs) {
            SourceConfig source = parseSourceConfig(sourceConf);
            sources.add(source);
        }
        fusionConfig.setSources(sources);
        
        // 解析连接配置
        Configuration joinConf = pluginJobConf.getConfiguration("join");
        if (joinConf != null) {
            fusionConfig.setJoinKeys(joinConf.getList("keys", String.class));
            fusionConfig.setJoinType(FusionConfig.JoinType.valueOf(joinConf.getString("type", "INNER").toUpperCase()));
        }
        
        // 解析字段映射
        List<Configuration> mappingConfigs = pluginJobConf.getListConfiguration("fieldMappings");
        List<FieldMapping> fieldMappings = new ArrayList<>();
        if (mappingConfigs != null) {
            for (Configuration mappingConf : mappingConfigs) {
                FieldMapping mapping = FieldMapping.fromConfig(mappingConf);
                if (mapping != null) {
                    fieldMappings.add(mapping);
                }
            }
        }
        fusionConfig.setFieldMappings(fieldMappings);
        
        // 解析错误处理模式
        fusionConfig.setErrorMode(FusionConfig.ErrorHandlingMode.valueOf(pluginJobConf.getString("errorMode", "STRICT").toUpperCase()));
        
        // 解析缓存配置
        Configuration cacheConf = pluginJobConf.getConfiguration("cache");
        if (cacheConf != null) {
            fusionConfig.setCacheConfig(FusionConfig.CacheConfig.fromConfig(cacheConf));
        }
        
        // 解析性能配置
        Configuration perfConf = pluginJobConf.getConfiguration("performance");
        if (perfConf != null) {
            fusionConfig.setPerformanceConfig(FusionConfig.PerformanceConfig.fromConfig(perfConf));
        }
        
        return fusionConfig;
    }
    
    /**
     * 解析单个数据源配置
     */
    private static SourceConfig parseSourceConfig(Configuration sourceConf) {
        SourceConfig source = new SourceConfig();
        source.setSourceId(sourceConf.getString("sourceId"));
        source.setPluginType(sourceConf.getString("pluginType"));

        source.setPluginConfig(sourceConf.getConfiguration("pluginConfig"));
        source.setWeight(sourceConf.getDouble("weight", 1.0));
        source.setPriority(sourceConf.getInt("priority", 0));
        source.setConfidence(sourceConf.getDouble("confidence", 1.0));
        return source;
    }
}