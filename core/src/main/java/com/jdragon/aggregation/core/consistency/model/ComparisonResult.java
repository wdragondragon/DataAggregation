package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class ComparisonResult {
    
    private String resultId;
    
    private String ruleId;
    
    private Date executionTime;
    
    private Status status;
    
    private int totalRecords;
    
    private int consistentRecords;
    
    private int inconsistentRecords;
    
    private int resolvedRecords;
    
    private Map<String, Integer> fieldDiscrepancies;
    
    private Map<String, Object> summary;
    
    private String reportPath;
    
    private Map<String, Object> metadata;
    
    private List<Map<String, Object>> resolvedRows;
    
    private UpdateResult updateResult;
    
    public ComparisonResult() {
        this.executionTime = new Date();
        this.fieldDiscrepancies = new HashMap<>();
        this.summary = new HashMap<>();
        this.metadata = new HashMap<>();
        this.resolvedRows = new ArrayList<>();
    }
    
    public void incrementConsistent() {
        consistentRecords++;
    }
    
    public void incrementInconsistent() {
        inconsistentRecords++;
    }
    
    public void incrementResolved() {
        resolvedRecords++;
    }
    
    public void addFieldDiscrepancy(String field, int count) {
        fieldDiscrepancies.put(field, fieldDiscrepancies.getOrDefault(field, 0) + count);
    }
    
    public enum Status {
        SUCCESS,
        PARTIAL_SUCCESS,
        FAILED,
        RUNNING
    }
}