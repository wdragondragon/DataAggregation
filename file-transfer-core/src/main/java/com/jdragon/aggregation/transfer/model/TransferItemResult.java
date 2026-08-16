package com.jdragon.aggregation.transfer.model;

import java.time.Instant;

public record TransferItemResult(String itemId,
                                 TransferItemStatus status,
                                 long totalBytes,
                                 long transferredBytes,
                                 long resumedBytes,
                                 int attempts,
                                 String sourceChecksum,
                                 String targetChecksum,
                                 String errorCode,
                                 String errorMessage,
                                 Instant startedAt,
                                 Instant finishedAt) {
}
