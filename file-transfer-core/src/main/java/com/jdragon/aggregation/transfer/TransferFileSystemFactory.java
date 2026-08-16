package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;

@FunctionalInterface
public interface TransferFileSystemFactory {

    TransferFileSystem open() throws Exception;
}
