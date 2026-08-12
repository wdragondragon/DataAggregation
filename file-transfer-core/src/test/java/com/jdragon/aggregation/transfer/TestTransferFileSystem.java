package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.transfer.StorageCapabilities;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.datasource.file.transfer.TransferWriteSession;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

abstract class TestTransferFileSystem implements TransferFileSystem {

    @Override
    public TransferFileEntry stat(String path) throws java.io.IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransferFilePage listPage(String directory, String cursor, int pageSize) throws java.io.IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream openRead(String path, long offset, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransferWriteSession prepareWrite(String temporaryPath, String sessionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long append(TransferWriteSession session, long offset, InputStream source, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(TransferWriteSession session, String targetPath, boolean replaceExisting) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(TransferWriteSession session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void move(String sourcePath, String targetPath, boolean replaceExisting) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(String path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageCapabilities capabilities() {
        return new StorageCapabilities(false, false, false, false, false, false, Set.of(), true);
    }

    @Override
    public boolean exists(String path, String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listFile(String dir, String regex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFile(String dir, String fileName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mkdir(String filePath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rm(String path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean connect(Configuration configuration) {
        return true;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public boolean mv(String from, String to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getInputStream(String path, String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputStream getOutputStream(String path, String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }
}
