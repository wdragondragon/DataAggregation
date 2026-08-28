package com.jdragon.aggregation.transfer.model;

public record TransferMapping(String targetRootPath,
                              boolean preserveRelativePath,
                              String targetPathTemplate) {

    public TransferMapping {
        // Validation of resolved expressions belongs to TransferPlanner, where
        // the error can identify mapping.targetRootPath precisely. Preserve null
        // and blank values here for that validation step.
    }
}
