package com.jdragon.aggregation.transfer.model;

import java.time.Instant;

public record TransferCheckpoint(String runId,
                                 String itemId,
                                 String sourceIdentity,
                                 String targetIdentity,
                                 SourceSnapshot sourceSnapshot,
                                 String temporaryPath,
                                 long confirmedOffset,
                                 Instant updatedAt) {

    public TransferCheckpoint {
        if (confirmedOffset < 0) {
            throw new IllegalArgumentException("confirmedOffset must be non-negative");
        }
        updatedAt = updatedAt == null ? Instant.now() : updatedAt;
    }

    public boolean matches(TransferPlan plan, TransferPlanItem item) {
        return plan.runId().equals(runId)
                && item.itemId().equals(itemId)
                && item.sourceIdentity().equals(sourceIdentity)
                && item.targetIdentity().equals(targetIdentity)
                && item.sourceSnapshot().equals(sourceSnapshot)
                && item.temporaryPath().equals(temporaryPath);
    }
}
