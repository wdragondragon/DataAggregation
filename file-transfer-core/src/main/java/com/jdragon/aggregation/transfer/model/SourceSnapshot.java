package com.jdragon.aggregation.transfer.model;

import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;

public record SourceSnapshot(String path,
                             long size,
                             long modifiedTimeMillis,
                             String etag) {

    public static SourceSnapshot from(TransferFileEntry entry) {
        return new SourceSnapshot(entry.path(), entry.size(), entry.modifiedTimeMillis(), entry.etag());
    }

    public boolean matches(TransferFileEntry entry) {
        if (entry == null || entry.directory()) {
            return false;
        }
        if (size != entry.size() || modifiedTimeMillis != entry.modifiedTimeMillis()) {
            return false;
        }
        return etag == null || entry.etag() == null || etag.equals(entry.etag());
    }
}
