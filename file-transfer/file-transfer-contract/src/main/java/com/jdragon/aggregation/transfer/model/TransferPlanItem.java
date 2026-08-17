package com.jdragon.aggregation.transfer.model;

public record TransferPlanItem(String itemId,
                               String sourceIdentity,
                               String targetIdentity,
                               String sourcePath,
                               String targetPath,
                               String temporaryPath,
                               String relativePath,
                               SourceSnapshot sourceSnapshot) {

    public TransferPlanItem {
        if (itemId == null || itemId.isBlank() || sourceSnapshot == null) {
            throw new IllegalArgumentException("itemId and sourceSnapshot are required");
        }
    }
}
