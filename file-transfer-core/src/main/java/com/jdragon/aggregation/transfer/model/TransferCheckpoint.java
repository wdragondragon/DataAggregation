package com.jdragon.aggregation.transfer.model;

import java.time.Instant;

public record TransferCheckpoint(String runId,
                                 String itemId,
                                 String sourceIdentity,
                                 String targetIdentity,
                                 SourceSnapshot sourceSnapshot,
                                 String temporaryPath,
                                 long confirmedOffset,
                                 int schemaVersion,
                                 String checksumAlgorithm,
                                 String checksumStateFormat,
                                 String checksumStateBase64,
                                 long checksumStateOffset,
                                 String sourceChecksum,
                                 long headOffset,
                                 int headLength,
                                 String headSha256,
                                 long boundaryOffset,
                                 int boundaryLength,
                                 String boundarySha256,
                                 Instant updatedAt) {

    public static final int CURRENT_SCHEMA_VERSION = 2;

    public TransferCheckpoint(String runId,
                              String itemId,
                              String sourceIdentity,
                              String targetIdentity,
                              SourceSnapshot sourceSnapshot,
                              String temporaryPath,
                              long confirmedOffset,
                              Instant updatedAt) {
        this(runId, itemId, sourceIdentity, targetIdentity, sourceSnapshot, temporaryPath,
                confirmedOffset, 1, null, null, null, 0L, null, 0L, 0, null,
                0L, 0, null, updatedAt);
    }

    public TransferCheckpoint {
        if (confirmedOffset < 0) {
            throw new IllegalArgumentException("confirmedOffset must be non-negative");
        }
        schemaVersion = schemaVersion <= 0 ? 1 : schemaVersion;
        if (checksumStateOffset < 0 || checksumStateOffset > confirmedOffset) {
            throw new IllegalArgumentException("checksumStateOffset must be within confirmedOffset");
        }
        if (boundaryOffset < 0 || boundaryLength < 0
                || boundaryOffset + boundaryLength > confirmedOffset) {
            throw new IllegalArgumentException("checkpoint boundary must be within confirmedOffset");
        }
        if (headOffset < 0 || headLength < 0 || headOffset + headLength > confirmedOffset) {
            throw new IllegalArgumentException("checkpoint head must be within confirmedOffset");
        }
        updatedAt = updatedAt == null ? Instant.now() : updatedAt;
    }

    public boolean hasResumableChecksumState() {
        return schemaVersion >= CURRENT_SCHEMA_VERSION
                && "SHA-256".equalsIgnoreCase(checksumAlgorithm)
                && "BC-SHA256-ENCODED-V1".equals(checksumStateFormat)
                && checksumStateBase64 != null
                && !checksumStateBase64.isBlank()
                && checksumStateOffset == confirmedOffset;
    }

    public boolean hasBoundaryFingerprint() {
        return headOffset == 0L
                && headLength > 0
                && headSha256 != null
                && !headSha256.isBlank()
                && boundaryLength > 0
                && boundarySha256 != null
                && !boundarySha256.isBlank()
                && boundaryOffset + boundaryLength == confirmedOffset;
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
