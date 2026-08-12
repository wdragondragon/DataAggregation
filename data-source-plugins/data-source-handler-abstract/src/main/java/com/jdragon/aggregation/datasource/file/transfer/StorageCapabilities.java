package com.jdragon.aggregation.datasource.file.transfer;

import java.util.Set;

public record StorageCapabilities(boolean rangeRead,
                                  boolean resumableWrite,
                                  boolean atomicMove,
                                  boolean serverSideCopy,
                                  boolean nativePaging,
                                  boolean multipartWrite,
                                  Set<String> checksumAlgorithms,
                                  boolean pathCaseSensitive) {

    public StorageCapabilities(boolean rangeRead, boolean resumableWrite, boolean atomicMove,
                               boolean serverSideCopy, boolean nativePaging, boolean multipartWrite,
                               Set<String> checksumAlgorithms) {
        this(rangeRead, resumableWrite, atomicMove, serverSideCopy, nativePaging, multipartWrite,
                checksumAlgorithms, true);
    }

    public StorageCapabilities {
        checksumAlgorithms = checksumAlgorithms == null ? Set.of() : Set.copyOf(checksumAlgorithms);
    }
}
