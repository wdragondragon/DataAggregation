package com.jdragon.aggregation.transfer.model;

public record TransferPolicy(ConflictPolicy conflictPolicy,
                             String checksumAlgorithm,
                             VerificationMode verificationMode,
                             Integer verificationFrameCount,
                             Long verificationFrameSizeBytes,
                             SourceSuccessAction sourceSuccessAction,
                             String sourceBackupRootPath) {

    public static final int DEFAULT_VERIFICATION_FRAME_COUNT = 16;
    public static final long DEFAULT_VERIFICATION_FRAME_SIZE_BYTES = 1024L * 1024L;
    public static final int MIN_VERIFICATION_FRAME_COUNT = 1;
    public static final int MAX_VERIFICATION_FRAME_COUNT = 64;
    public static final long MIN_VERIFICATION_FRAME_SIZE_BYTES = 64L * 1024L;
    public static final long MAX_VERIFICATION_FRAME_SIZE_BYTES = 4L * 1024L * 1024L;
    public static final long VERIFICATION_FRAME_SIZE_ALIGNMENT = 64L * 1024L;

    public TransferPolicy(ConflictPolicy conflictPolicy,
                          String checksumAlgorithm,
                          SourceSuccessAction sourceSuccessAction,
                          String sourceBackupRootPath) {
        this(conflictPolicy, checksumAlgorithm, VerificationMode.STRONG,
                DEFAULT_VERIFICATION_FRAME_COUNT, DEFAULT_VERIFICATION_FRAME_SIZE_BYTES,
                sourceSuccessAction, sourceBackupRootPath);
    }

    public TransferPolicy {
        conflictPolicy = conflictPolicy == null ? ConflictPolicy.FAIL : conflictPolicy;
        checksumAlgorithm = checksumAlgorithm == null || checksumAlgorithm.isBlank()
                ? "SHA-256"
                : checksumAlgorithm.toUpperCase();
        if (!"SHA-256".equals(checksumAlgorithm)) {
            throw new IllegalArgumentException("only SHA-256 is supported as the canonical checksum");
        }
        verificationMode = verificationMode == null ? VerificationMode.STRONG : verificationMode;
        verificationFrameCount = verificationFrameCount == null
                ? DEFAULT_VERIFICATION_FRAME_COUNT
                : verificationFrameCount;
        verificationFrameSizeBytes = verificationFrameSizeBytes == null
                ? DEFAULT_VERIFICATION_FRAME_SIZE_BYTES
                : verificationFrameSizeBytes;
        if (verificationFrameCount < MIN_VERIFICATION_FRAME_COUNT
                || verificationFrameCount > MAX_VERIFICATION_FRAME_COUNT) {
            throw new IllegalArgumentException("verificationFrameCount must be between 1 and 64");
        }
        if (verificationFrameSizeBytes < MIN_VERIFICATION_FRAME_SIZE_BYTES
                || verificationFrameSizeBytes > MAX_VERIFICATION_FRAME_SIZE_BYTES
                || verificationFrameSizeBytes % VERIFICATION_FRAME_SIZE_ALIGNMENT != 0L) {
            throw new IllegalArgumentException(
                    "verificationFrameSizeBytes must be a 64 KiB multiple between 64 KiB and 4 MiB");
        }
        sourceSuccessAction = sourceSuccessAction == null ? SourceSuccessAction.KEEP : sourceSuccessAction;
        if (verificationMode == VerificationMode.NONE && sourceSuccessAction != SourceSuccessAction.KEEP) {
            throw new IllegalArgumentException("verificationMode NONE only supports sourceSuccessAction KEEP");
        }
        if (sourceSuccessAction == SourceSuccessAction.BACKUP
                && (sourceBackupRootPath == null || sourceBackupRootPath.isBlank())) {
            throw new IllegalArgumentException("sourceBackupRootPath is required for source BACKUP action");
        }
    }

    public static TransferPolicy defaults() {
        return new TransferPolicy(ConflictPolicy.FAIL, "SHA-256", VerificationMode.STRONG,
                DEFAULT_VERIFICATION_FRAME_COUNT, DEFAULT_VERIFICATION_FRAME_SIZE_BYTES,
                SourceSuccessAction.KEEP, null);
    }

    public VerificationMode effectiveVerificationMode(long fileSize) {
        if (verificationMode != VerificationMode.PARTIAL) {
            return verificationMode;
        }
        long configuredSampleBytes = Math.multiplyExact(
                (long) verificationFrameCount, verificationFrameSizeBytes);
        return fileSize <= 0L || configuredSampleBytes >= fileSize
                ? VerificationMode.STRONG
                : VerificationMode.PARTIAL;
    }
}
