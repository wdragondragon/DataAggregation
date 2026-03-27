package com.jdragon.aggregation.core.consistency.model;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class ConsistencyRule {

    private String ruleId;

    private String ruleName;

    private String description;

    private List<DataSourceConfig> dataSources = new ArrayList<>();

    private List<String> compareFields = new ArrayList<>();

    private List<String> matchKeys = new ArrayList<>();

    private Double toleranceThreshold;

    private ConflictResolutionStrategy conflictResolutionStrategy;

    private Map<String, Object> resolutionParams;

    private OutputConfig outputConfig = new OutputConfig();

    private Boolean enabled = true;

    private Boolean parallelFetch = true;

    private String updateTargetSourceId;

    private Boolean autoApplyResolutions = false;

    private Boolean validateBeforeUpdate = false;

    private Integer updateBufferSize = 100;

    private Integer updateRetryAttempts = 0;

    private Long updateRetryDelayMs = 1000L;

    private Double updateRetryBackoffMultiplier = 1.5;

    private Boolean allowInsert = true;

    private Boolean allowDelete = true;

    private Boolean skipUnchangedUpdates = true;

    private StreamCacheConfig cacheConfig = new StreamCacheConfig();

    private StreamPerformanceConfig performanceConfig = new StreamPerformanceConfig();

    public static ConsistencyRule fromConfig(Configuration config) {
        ConsistencyRule rule = new ConsistencyRule();
        rule.setRuleId(config.getString("ruleId"));
        rule.setRuleName(config.getString("ruleName", config.getString("name", rule.getRuleId())));
        rule.setDescription(config.getString("description"));
        rule.setEnabled(config.getBool("enabled", true));
        rule.setParallelFetch(config.getBool("parallelFetch", true));
        rule.setToleranceThreshold(config.getDouble("toleranceThreshold", 0.0));

        String strategy = config.getString("conflictResolutionStrategy", "HIGH_CONFIDENCE");
        rule.setConflictResolutionStrategy(ConflictResolutionStrategy.valueOf(strategy.toUpperCase()));
        rule.setResolutionParams(config.getMap("resolutionParams"));

        List<String> compareFields = config.getList("compareFields", String.class);
        if (compareFields != null) {
            rule.setCompareFields(compareFields);
        }

        List<String> matchKeys = config.getList("matchKeys", String.class);
        if (matchKeys == null) {
            matchKeys = config.getList("join.keys", String.class);
        }
        if (matchKeys != null) {
            rule.setMatchKeys(matchKeys);
        }

        List<Configuration> sourceConfigs = config.getListConfiguration("dataSources");
        if (sourceConfigs == null) {
            sourceConfigs = config.getListConfiguration("sources");
        }
        if (sourceConfigs != null) {
            List<DataSourceConfig> dataSources = new ArrayList<>();
            for (Configuration sourceConfig : sourceConfigs) {
                dataSources.add(DataSourceConfig.fromConfig(sourceConfig));
            }
            rule.setDataSources(dataSources);
        }

        Configuration outputConfig = config.getConfiguration("outputConfig");
        if (outputConfig != null) {
            rule.setOutputConfig(OutputConfig.fromConfig(outputConfig));
        }

        Configuration cacheConfig = config.getConfiguration("cache");
        if (cacheConfig != null) {
            rule.setCacheConfig(StreamCacheConfig.fromConfig(cacheConfig));
        }

        Configuration performanceConfig = config.getConfiguration("performance");
        if (performanceConfig != null) {
            rule.setPerformanceConfig(StreamPerformanceConfig.fromConfig(performanceConfig));
        }

        rule.setUpdateTargetSourceId(config.getString("updateTargetSourceId"));
        rule.setAutoApplyResolutions(config.getBool("autoApplyResolutions", false));
        rule.setValidateBeforeUpdate(config.getBool("validateBeforeUpdate", false));
        rule.setUpdateBufferSize(config.getInt("updateBufferSize", 1024));
        rule.setUpdateRetryAttempts(config.getInt("updateRetryAttempts", 0));
        rule.setUpdateRetryDelayMs(config.getLong("updateRetryDelayMs", 1000L));
        rule.setUpdateRetryBackoffMultiplier(config.getDouble("updateRetryBackoffMultiplier", 1.5));
        rule.setAllowInsert(config.getBool("allowInsert", true));
        rule.setAllowDelete(config.getBool("allowDelete", true));
        rule.setSkipUnchangedUpdates(config.getBool("skipUnchangedUpdates", true));

        return rule;
    }

    public Configuration toConfig() {
        Configuration config = Configuration.newDefault();
        config.set("ruleId", ruleId);
        config.set("ruleName", ruleName);
        config.set("description", description);
        config.set("enabled", enabled);
        config.set("parallelFetch", parallelFetch);
        config.set("toleranceThreshold", toleranceThreshold);
        config.set("conflictResolutionStrategy", conflictResolutionStrategy != null ? conflictResolutionStrategy.name() : null);
        config.set("resolutionParams", resolutionParams);
        config.set("dataSources", dataSources);
        config.set("compareFields", compareFields);
        config.set("matchKeys", matchKeys);
        config.set("outputConfig", outputConfig);
        config.set("cache", cacheConfig);
        config.set("performance", performanceConfig);
        config.set("updateTargetSourceId", updateTargetSourceId);
        config.set("autoApplyResolutions", autoApplyResolutions);
        config.set("validateBeforeUpdate", validateBeforeUpdate);
        config.set("updateBufferSize", updateBufferSize);
        config.set("updateRetryAttempts", updateRetryAttempts);
        config.set("updateRetryDelayMs", updateRetryDelayMs);
        config.set("updateRetryBackoffMultiplier", updateRetryBackoffMultiplier);
        config.set("allowInsert", allowInsert);
        config.set("allowDelete", allowDelete);
        config.set("skipUnchangedUpdates", skipUnchangedUpdates);
        return config;
    }

    public JSONObject toJson() {
        return (JSONObject) JSONObject.toJSON(this);
    }

    public static ConsistencyRule fromJson(String json) {
        return JSONObject.parseObject(json, ConsistencyRule.class);
    }

    @Data
    public static class StreamCacheConfig {
        private int partitionCount = 16;
        private int maxSize = 100000;
        private String spillPath;
        private Boolean keepTempFiles = false;

        public static StreamCacheConfig fromConfig(Configuration config) {
            StreamCacheConfig cacheConfig = new StreamCacheConfig();
            cacheConfig.setPartitionCount(config.getInt("partitionCount", 16));
            cacheConfig.setMaxSize(config.getInt("maxSize", 100000));
            cacheConfig.setSpillPath(config.getString("spillPath"));
            cacheConfig.setKeepTempFiles(config.getBool("keepTempFiles", false));
            return cacheConfig;
        }
    }

    @Data
    public static class StreamPerformanceConfig {
        private int batchSize = 1000;
        private int parallelSourceCount = 1;
        private int memoryLimitMB = 512;

        public static StreamPerformanceConfig fromConfig(Configuration config) {
            StreamPerformanceConfig performanceConfig = new StreamPerformanceConfig();
            performanceConfig.setBatchSize(config.getInt("batchSize", 1000));
            performanceConfig.setParallelSourceCount(config.getInt("parallelSourceCount", 1));
            performanceConfig.setMemoryLimitMB(config.getInt("memoryLimitMB", 512));
            return performanceConfig;
        }
    }
}
