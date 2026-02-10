package com.jdragon.aggregation.core.consistency.model;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ConsistencyRule {

    private String ruleId; // 规则ID，唯一标识符

    private String ruleName; // 规则名称，用于显示

    private String description; // 规则描述

    private List<DataSourceConfig> dataSources; // 数据源配置列表

    private List<String> compareFields; // 需要对比的字段列表

    private List<String> matchKeys; // 匹配键字段列表，用于数据关联

    private Double toleranceThreshold; // 数值字段的容差阈值

    private ConflictResolutionStrategy conflictResolutionStrategy; // 冲突解决策略

    private Map<String, Object> resolutionParams; // 冲突解决策略参数

    private OutputConfig outputConfig; // 输出配置

    private Boolean enabled = true; // 规则是否启用

    private Boolean parallelFetch = true; // 是否并行获取数据

    private String updateTargetSourceId; // 更新目标数据源ID

    private Boolean autoApplyResolutions = false; // 是否自动应用解决结果

    private Boolean validateBeforeUpdate = false; // 更新前是否验证目标记录存在

    private Integer updateBufferSize = 100; // 更新操作的缓冲区大小

    private Integer updateRetryAttempts = 0; // 更新失败重试次数
    
    private Long updateRetryDelayMs = 1000L; // 更新重试延迟（毫秒）
    
    private Double updateRetryBackoffMultiplier = 1.5; // 更新重试退避乘数

    private Boolean allowInsert = true; // 是否允许插入操作
    
    private Boolean allowDelete = true; // 是否允许删除操作
    
    private Boolean skipUnchangedUpdates = true; // 是否跳过值未变化的更新操作

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
}