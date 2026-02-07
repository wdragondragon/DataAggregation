package com.jdragon.aggregation.core.consistency.model;

public enum ConflictResolutionStrategy {
    
    HIGH_CONFIDENCE("高可信度源为准"),
    
    WEIGHTED_AVERAGE("加权平均值"),
    
    MAJORITY_VOTE("多数投票"),
    
    CUSTOM_RULE("自定义规则"),
    
    MANUAL_REVIEW("人工审核");
    
    private final String description;
    
    ConflictResolutionStrategy(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return description;
    }
}