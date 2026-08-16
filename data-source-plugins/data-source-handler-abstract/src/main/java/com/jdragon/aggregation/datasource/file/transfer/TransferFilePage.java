package com.jdragon.aggregation.datasource.file.transfer;

import java.util.List;

public record TransferFilePage(List<TransferFileEntry> entries,
                               String nextCursor,
                               boolean truncated) {

    public TransferFilePage {
        entries = entries == null ? List.of() : List.copyOf(entries);
    }
}
