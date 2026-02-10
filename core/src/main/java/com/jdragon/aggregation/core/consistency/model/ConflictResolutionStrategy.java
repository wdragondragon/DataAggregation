package com.jdragon.aggregation.core.consistency.model;

public enum ConflictResolutionStrategy {
    
    HIGH_CONFIDENCE("高可信度源为准"), // 高可信度源策略：选择置信度最高的数据源
    
    WEIGHTED_AVERAGE("加权平均值"), // 加权平均策略：对数值字段进行加权平均
    
    MAJORITY_VOTE("多数投票"), // 多数投票策略：选择出现次数最多的值
    
    CUSTOM_RULE("自定义规则"), // 自定义规则策略：使用用户自定义规则
    
    MANUAL_REVIEW("人工审核"); // 人工审核策略：需要人工介入审核
    
    private final String description;
    
    ConflictResolutionStrategy(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return description;
    }
}