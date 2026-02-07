package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
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

    private List<UpdateFailure> failures;

    private Map<String, Object> summary;

    public UpdateResult() {
        this.executionTime = new Date();
        this.failures = new ArrayList<>();
        this.successfulUpdates = 0;
        this.failedUpdates = 0;
        this.totalUpdates = 0;
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