package com.jdragon.aggregation.transfer;

public record TransferVerificationFrame(long offset, long length) {

    public TransferVerificationFrame {
        if (offset < 0L || length <= 0L) {
            throw new IllegalArgumentException("verification frame offset and length are invalid");
        }
    }
}
