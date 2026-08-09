package com.jdragon.aggregation.datasource.file.transfer;

import java.util.Set;

public record StorageCapabilities(boolean rangeRead,
                                  boolean resumableWrite,
                                  boolean atomicMove,
                                  boolean serverSideCopy,
                                  boolean nativePaging,
                                  boolean multipartWrite,
                                  Set<String> checksumAlgorithms) {

    public StorageCapabilities {
        checksumAlgorithms = checksumAlgorithms == null ? Set.of() : Set.copyOf(checksumAlgorithms);
    }
}
