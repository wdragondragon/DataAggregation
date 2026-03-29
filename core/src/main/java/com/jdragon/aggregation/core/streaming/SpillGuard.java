package com.jdragon.aggregation.core.streaming;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

public class SpillGuard {

    private final long maxSpillBytes;
    private final long minFreeDiskBytes;
    private long totalReservedBytes;

    public SpillGuard(long maxSpillBytes, long minFreeDiskBytes) {
        this.maxSpillBytes = Math.max(1L, maxSpillBytes);
        this.minFreeDiskBytes = Math.max(1L, minFreeDiskBytes);
    }

    public synchronized void reserve(Path workingDirectory, long bytes) {
        long requestedBytes = Math.max(1L, bytes);
        if (totalReservedBytes + requestedBytes > maxSpillBytes) {
            throw new SpillLimitExceededException(
                    "Spill limit exceeded: requested=" + requestedBytes
                            + ", reserved=" + totalReservedBytes
                            + ", max=" + maxSpillBytes
            );
        }

        try {
            FileStore fileStore = Files.getFileStore(workingDirectory);
            long usableSpace = fileStore.getUsableSpace();
            if (usableSpace - requestedBytes < minFreeDiskBytes) {
                throw new SpillLimitExceededException(
                        "Insufficient free disk for spill: requested=" + requestedBytes
                                + ", usable=" + usableSpace
                                + ", minFree=" + minFreeDiskBytes
                );
            }
        } catch (IOException e) {
            throw new SpillLimitExceededException("Failed to inspect spill filesystem", e);
        }

        totalReservedBytes += requestedBytes;
    }

    public synchronized long getTotalReservedBytes() {
        return totalReservedBytes;
    }
}
