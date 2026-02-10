package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class DifferenceRecord {
    
    private String recordId; // 记录ID，唯一标识符
    
    private Map<String, Object> matchKeyValues; // 匹配键值对
    
    private Map<String, Map<String, Object>> sourceValues; // 各数据源的字段值
    
    private Map<String, String> differences; // 差异描述，字段名->差异描述
    
    private ResolutionResult resolutionResult; // 解决结果
    
    private String conflictType; // 冲突类型
    
    private double discrepancyScore; // 差异分数（0.0-1.0）
    
    private List<String> missingSources; // 缺失数据的数据源列表
    
    public DifferenceRecord() {
        this.sourceValues = new HashMap<>();
        this.differences = new HashMap<>();
        this.matchKeyValues = new HashMap<>();
        this.missingSources = new ArrayList<>();
    }
    
    public void addSourceValue(String sourceId, Map<String, Object> values) {
        sourceValues.put(sourceId, values);
    }
    
    public void addDifference(String field, String description) {
        differences.put(field, description);
    }
}