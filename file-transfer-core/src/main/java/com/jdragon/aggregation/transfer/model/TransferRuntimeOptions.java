package com.jdragon.aggregation.transfer.model;

import java.util.List;

public record TransferRuntimeOptions(int concurrency,
                                     int maxRetries,
                                     List<Long> retryBackoffMillis,
                                     long chunkSizeBytes,
                                     long checkpointIntervalMillis,
                                     long maxBytesPerSecond,
                                     CheckpointRecoveryMode checkpointRecoveryMode) {

    public TransferRuntimeOptions(int concurrency,
                                  int maxRetries,
                                  List<Long> retryBackoffMillis,
                                  long chunkSizeBytes,
                                  long checkpointIntervalMillis,
                                  long maxBytesPerSecond) {
        this(concurrency, maxRetries, retryBackoffMillis, chunkSizeBytes,
                checkpointIntervalMillis, maxBytesPerSecond, CheckpointRecoveryMode.FAST);
    }

    public TransferRuntimeOptions {
        if (concurrency < 1 || concurrency > 32) {
            throw new IllegalArgumentException("runtime concurrency must be between 1 and 32");
        }
        if (maxRetries < 0 || maxRetries > 20) {
            throw new IllegalArgumentException("runtime maxRetries must be between 0 and 20");
        }
        retryBackoffMillis = retryBackoffMillis == null || retryBackoffMillis.isEmpty()
                ? List.of(1_000L, 5_000L, 15_000L)
                : List.copyOf(retryBackoffMillis);
        if (retryBackoffMillis.stream().anyMatch(value -> value == null || value < 0)) {
            throw new IllegalArgumentException("runtime retry backoff values must be non-negative");
        }
        if (chunkSizeBytes < 64 * 1024 || chunkSizeBytes > 64L * 1024 * 1024) {
            throw new IllegalArgumentException("runtime chunkSizeBytes must be between 64 KiB and 64 MiB");
        }
        if (checkpointIntervalMillis <= 0) {
            throw new IllegalArgumentException("runtime checkpointIntervalMillis must be positive");
        }
        if (maxBytesPerSecond < 0) {
            throw new IllegalArgumentException("runtime maxBytesPerSecond must be non-negative");
        }
        checkpointRecoveryMode = checkpointRecoveryMode == null
                ? CheckpointRecoveryMode.FAST
                : checkpointRecoveryMode;
    }

    public static TransferRuntimeOptions defaults() {
        return new TransferRuntimeOptions(4, 3, List.of(1_000L, 5_000L, 15_000L),
                8L * 1024 * 1024, 1_000L, 0L, CheckpointRecoveryMode.FAST);
    }

    public long retryDelayMillis(int retryNumber) {
        int index = Math.max(0, Math.min(retryNumber - 1, retryBackoffMillis.size() - 1));
        return retryBackoffMillis.get(index);
    }
}
