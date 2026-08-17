package com.jdragon.aggregation.transfer.model;

public record TransferMapping(String targetRootPath,
                              boolean preserveRelativePath,
                              String targetPathTemplate) {

    public TransferMapping {
        if (targetRootPath == null || targetRootPath.isBlank()) {
            throw new IllegalArgumentException("mapping targetRootPath is required");
        }
    }
}
