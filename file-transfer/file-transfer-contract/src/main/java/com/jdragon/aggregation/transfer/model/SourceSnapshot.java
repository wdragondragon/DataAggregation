package com.jdragon.aggregation.transfer.model;

public record SourceSnapshot(String path,
                             long size,
                             long modifiedTimeMillis,
                             String etag) {
}
