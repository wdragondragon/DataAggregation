package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class UpdateResult {

    private String resultId;

    private String ruleId;

    private Date executionTime;

    private String targetSourceId;

    private int totalUpdates;

    private int successfulUpdates;

    private int failedUpdates;

    private int insertCount;
    
    private int updateCount;
    
    private int deleteCount;
    
    private int skipCount;
    
    private Map<String, String> operationTypes; // recordId -> operation type

    private List<UpdateFailure> failures;

    private Map<String, Object> summary;

    public UpdateResult() {
        this.executionTime = new Date();
        this.failures = new ArrayList<>();
        this.successfulUpdates = 0;
        this.failedUpdates = 0;
        this.totalUpdates = 0;
        this.insertCount = 0;
        this.updateCount = 0;
        this.deleteCount = 0;
        this.skipCount = 0;
        this.operationTypes = new HashMap<>();
    }

    public void incrementSuccessful() {
        successfulUpdates++;
        totalUpdates++;
    }

    public void incrementFailed(String recordId, String reason) {
        failedUpdates++;
        totalUpdates++;
        failures.add(new UpdateFailure(recordId, reason));
    }

    public void incrementFailed(String recordId, String reason, Map<String, Object> matchKeys) {
        failedUpdates++;
        totalUpdates++;
        failures.add(new UpdateFailure(recordId, reason, matchKeys));
    }

    public void incrementInsert(String recordId) {
        insertCount++;
        operationTypes.put(recordId, "INSERT");
    }

    public void incrementUpdate(String recordId) {
        updateCount++;
        operationTypes.put(recordId, "UPDATE");
    }

    public void incrementDelete(String recordId) {
        deleteCount++;
        operationTypes.put(recordId, "DELETE");
    }
    
    public void incrementSkip(String recordId, String reason) {
        skipCount++;
        operationTypes.put(recordId, "SKIP");
    }

    public int getInsertCount() {
        return insertCount;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public int getDeleteCount() {
        return deleteCount;
    }
    
    public int getSkipCount() {
        return skipCount;
    }

    public Map<String, String> getOperationTypes() {
        return operationTypes;
    }

    @Data
    public static class UpdateFailure {
        private String recordId;
        private Map<String, Object> matchKeys;
        private String reason;
        private Date failureTime;

        public UpdateFailure(String recordId, String reason) {
            this.recordId = recordId;
            this.reason = reason;
            this.failureTime = new Date();
        }

        public UpdateFailure(String recordId, String reason, Map<String, Object> matchKeys) {
            this.recordId = recordId;
            this.reason = reason;
            this.matchKeys = matchKeys;
            this.failureTime = new Date();
        }
    }
}