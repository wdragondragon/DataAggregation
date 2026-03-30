package com.jdragon.aggregation.core.streaming;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

public class SpillGuard {

    private final long maxSpillBytes;
    private final long minFreeDiskBytes;
    private long activeReservedBytes;
    private long cumulativeReservedBytes;

    public SpillGuard(long maxSpillBytes, long minFreeDiskBytes) {
        this.maxSpillBytes = Math.max(1L, maxSpillBytes);
        this.minFreeDiskBytes = Math.max(1L, minFreeDiskBytes);
    }

    public synchronized void reserve(Path workingDirectory, long bytes) {
        long requestedBytes = Math.max(1L, bytes);
        if (activeReservedBytes + requestedBytes > maxSpillBytes) {
            throw new SpillLimitExceededException(
                    "Spill limit exceeded: requested=" + requestedBytes
                            + ", activeReserved=" + activeReservedBytes
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

        activeReservedBytes += requestedBytes;
        cumulativeReservedBytes += requestedBytes;
    }

    public synchronized void release(long bytes) {
        long releasedBytes = Math.max(0L, bytes);
        activeReservedBytes = Math.max(0L, activeReservedBytes - releasedBytes);
    }

    public synchronized long getActiveReservedBytes() {
        return activeReservedBytes;
    }

    public synchronized long getTotalReservedBytes() {
        return cumulativeReservedBytes;
    }
}
