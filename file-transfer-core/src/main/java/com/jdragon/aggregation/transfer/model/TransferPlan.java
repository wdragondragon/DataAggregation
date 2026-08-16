package com.jdragon.aggregation.transfer.model;

import java.time.Instant;
import java.util.List;

public record TransferPlan(String runId,
                           Instant plannedAt,
                           List<TransferPlanItem> items) {

    public TransferPlan {
        if (runId == null || runId.isBlank()) {
            throw new IllegalArgumentException("runId is required");
        }
        plannedAt = plannedAt == null ? Instant.now() : plannedAt;
        items = items == null ? List.of() : List.copyOf(items);
    }
}
