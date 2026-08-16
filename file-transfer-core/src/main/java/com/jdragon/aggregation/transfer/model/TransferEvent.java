package com.jdragon.aggregation.transfer.model;

import java.time.Instant;
import java.util.Map;

public record TransferEvent(TransferEventType type,
                            Instant timestamp,
                            String runId,
                            String itemId,
                            TransferItemStatus itemStatus,
                            long transferredBytes,
                            long totalBytes,
                            int attempt,
                            String message,
                            String errorCode,
                            Map<String, Object> details) {

    public TransferEvent {
        if (type == null) {
            throw new IllegalArgumentException("event type is required");
        }
        timestamp = timestamp == null ? Instant.now() : timestamp;
        details = details == null ? Map.of() : Map.copyOf(details);
    }
}
