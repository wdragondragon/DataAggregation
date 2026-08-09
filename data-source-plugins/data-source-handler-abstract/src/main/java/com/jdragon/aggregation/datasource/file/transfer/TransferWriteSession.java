package com.jdragon.aggregation.datasource.file.transfer;

import java.util.Map;

public record TransferWriteSession(String sessionId,
                                   String temporaryPath,
                                   long confirmedOffset,
                                   Map<String, String> metadata) {

    public TransferWriteSession {
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
