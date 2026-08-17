package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.transfer.model.SourceSnapshot;

public final class SourceSnapshots {

    private SourceSnapshots() {
    }

    public static SourceSnapshot from(TransferFileEntry entry) {
        if (entry == null || entry.directory()) {
            throw new IllegalArgumentException("source snapshot requires a file entry");
        }
        return new SourceSnapshot(entry.path(), entry.size(), entry.modifiedTimeMillis(), entry.etag());
    }

    public static boolean matches(SourceSnapshot snapshot, TransferFileEntry entry) {
        if (snapshot == null || entry == null || entry.directory()) {
            return false;
        }
        if (snapshot.size() != entry.size()
                || snapshot.modifiedTimeMillis() != entry.modifiedTimeMillis()) {
            return false;
        }
        return snapshot.etag() == null || entry.etag() == null
                || snapshot.etag().equals(entry.etag());
    }
}
