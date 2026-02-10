package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class DifferenceRecord {
    
    private String recordId;
    
    private Map<String, Object> matchKeyValues;
    
    private Map<String, Map<String, Object>> sourceValues;
    
    private Map<String, String> differences;
    
    private ResolutionResult resolutionResult;
    
    private String conflictType;
    
    private double discrepancyScore;
    
    private List<String> missingSources;
    
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