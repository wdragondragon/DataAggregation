package com.jdragon.aggregation.transfer.model;

public record TransferPolicy(ConflictPolicy conflictPolicy,
                             String checksumAlgorithm,
                             SourceSuccessAction sourceSuccessAction,
                             String sourceBackupRootPath) {

    public TransferPolicy {
        conflictPolicy = conflictPolicy == null ? ConflictPolicy.FAIL : conflictPolicy;
        checksumAlgorithm = checksumAlgorithm == null || checksumAlgorithm.isBlank()
                ? "SHA-256"
                : checksumAlgorithm.toUpperCase();
        if (!"SHA-256".equals(checksumAlgorithm)) {
            throw new IllegalArgumentException("only SHA-256 is supported as the canonical checksum");
        }
        sourceSuccessAction = sourceSuccessAction == null ? SourceSuccessAction.KEEP : sourceSuccessAction;
        if (sourceSuccessAction == SourceSuccessAction.BACKUP
                && (sourceBackupRootPath == null || sourceBackupRootPath.isBlank())) {
            throw new IllegalArgumentException("sourceBackupRootPath is required for source BACKUP action");
        }
    }

    public static TransferPolicy defaults() {
        return new TransferPolicy(ConflictPolicy.FAIL, "SHA-256", SourceSuccessAction.KEEP, null);
    }
}
