package com.jdragon.aggregation.transfer.model;

import java.time.Instant;
import java.util.List;

public record TransferResult(String runId,
                             TransferRunStatus status,
                             List<TransferItemResult> items,
                             long totalFiles,
                             long successFiles,
                             long skippedFiles,
                             long failedFiles,
                             long totalBytes,
                             long transferredBytes,
                             long resumedBytes,
                             Instant startedAt,
                             Instant finishedAt) {

    public TransferResult {
        items = items == null ? List.of() : List.copyOf(items);
    }
}
