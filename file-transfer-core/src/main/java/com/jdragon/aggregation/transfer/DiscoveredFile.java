package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;

public record DiscoveredFile(String relativePath, TransferFileEntry entry) {
}
