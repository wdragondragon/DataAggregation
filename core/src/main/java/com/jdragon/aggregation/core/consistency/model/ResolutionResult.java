package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

import java.util.Date;
import java.util.Map;

@Data
public class ResolutionResult {

    private String resolutionId;

    private ConflictResolutionStrategy strategyUsed;

    private Map<String, Object> resolvedValues;

    private String winningSource;

    private Date resolutionTime;

    private boolean manuallyReviewed = false;

    public ResolutionResult() {
        this.resolutionTime = new Date();
    }
}