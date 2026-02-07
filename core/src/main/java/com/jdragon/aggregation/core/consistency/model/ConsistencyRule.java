package com.jdragon.aggregation.core.consistency.model;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ConsistencyRule {

    private String ruleId;

    private String ruleName;

    private String description;

    private List<DataSourceConfig> dataSources;

    private List<String> compareFields;

    private List<String> matchKeys;

    private Double toleranceThreshold;

    private ConflictResolutionStrategy conflictResolutionStrategy;

    private Map<String, Object> resolutionParams;

    private OutputConfig outputConfig;

    private boolean enabled = true;

    private boolean parallelFetch = true;

    private String updateTargetSourceId;

    private boolean autoApplyResolutions = false;

    private Integer updateBufferSize = 100;

    private ScheduleConfig schedule;

    public static ConsistencyRule fromConfig(Configuration config) {
        ConsistencyRule rule = new ConsistencyRule();
        rule.setRuleId(config.getString("ruleId"));
        rule.setRuleName(config.getString("ruleName"));
        rule.setDescription(config.getString("description"));
        rule.setEnabled(config.getBool("enabled", true));
        rule.setParallelFetch(config.getBool("parallelFetch", true));
        rule.setToleranceThreshold(config.getDouble("toleranceThreshold", 0.0));

        String strategy = config.getString("conflictResolutionStrategy", "HIGH_CONFIDENCE");
        rule.setConflictResolutionStrategy(ConflictResolutionStrategy.valueOf(strategy));
        rule.setResolutionParams(config.getMap("resolutionParams"));

        rule.setUpdateTargetSourceId(config.getString("updateTargetSourceId"));
        rule.setAutoApplyResolutions(config.getBool("autoApplyResolutions", false));
        rule.setUpdateBufferSize(config.getInt("updateBufferSize", 1024));

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
        config.set("conflictResolutionStrategy", conflictResolutionStrategy.name());
        config.set("resolutionParams", resolutionParams);
        config.set("updateTargetSourceId", updateTargetSourceId);
        config.set("autoApplyResolutions", autoApplyResolutions);
        config.set("updateBufferSize", updateBufferSize);
        return config;
    }

    public JSONObject toJson() {
        return (JSONObject) JSONObject.toJSON(this);
    }

    public static ConsistencyRule fromJson(String json) {
        return JSONObject.parseObject(json, ConsistencyRule.class);
    }
}