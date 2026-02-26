package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

import java.util.Date;
import java.util.Map;

@Data
public class ResolutionResult {

    private String recordId; // 记录ID，唯一标识符

    private String resolutionId; // 解决结果ID，唯一标识符

    private ConflictResolutionStrategy strategyUsed; // 使用的冲突解决策略

    private Map<String, Object> resolvedValues; // 解决后的字段值

    private String winningSource; // 获胜数据源（如果适用）

    private Date resolutionTime; // 解决时间

    private Boolean manuallyReviewed = false; // 是否经过人工审核

    private String operationType;

    private Map<String, Object> matchKeyValues; // 匹配键值对

    public ResolutionResult() {
        this.resolutionTime = new Date();
    }
}