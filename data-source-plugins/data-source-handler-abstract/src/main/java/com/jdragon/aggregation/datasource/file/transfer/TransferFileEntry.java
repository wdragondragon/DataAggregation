package com.jdragon.aggregation.datasource.file.transfer;

public record TransferFileEntry(String path,
                                String name,
                                boolean directory,
                                long size,
                                long modifiedTimeMillis,
                                String etag) {
}
