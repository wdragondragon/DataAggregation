package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.transfer.model.VerificationMode;

import java.util.List;

public record TransferVerificationPlan(VerificationMode configuredMode,
                                       VerificationMode effectiveMode,
                                       long fileSize,
                                       List<TransferVerificationFrame> frames,
                                       long verificationBytes) {

    public TransferVerificationPlan {
        if (configuredMode == null || effectiveMode == null || fileSize < 0L
                || verificationBytes < 0L) {
            throw new IllegalArgumentException("verification plan is invalid");
        }
        frames = frames == null ? List.of() : List.copyOf(frames);
    }

    public String phase() {
        return switch (effectiveMode) {
            case PARTIAL -> "TARGET_SAMPLE";
            case STRONG -> "TARGET_CHECKSUM";
            case NONE -> "STRUCTURE";
        };
    }
}
