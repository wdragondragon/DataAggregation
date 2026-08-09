package com.jdragon.aggregation.datasource.file.s3.minio;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.transfer.StorageCapabilities;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.datasource.file.transfer.TransferWriteSession;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class MinioSourcePlugin extends AbstractPlugin implements TransferFileSystem {

    private TransferFileSystem delegate;

    @Override
    public boolean connect(Configuration configuration) {
        String provider = configuration == null ? null : configuration.getString("storageProvider", "minio");
        this.delegate = "minio".equalsIgnoreCase(StringUtils.trimToEmpty(provider))
                ? new MinioHelper()
                : new AliyunOssHelper();
        return delegate.connect(configuration);
    }

    @Override
    public boolean exists(String path, String name) throws IOException {
        return delegate().exists(normalizeObjectPath(path), name);
    }

    @Override
    public Set<String> listFile(String dir, String regex) throws IOException {
        return delegate().listFile(normalizeObjectPath(dir), regex);
    }

    @Override
    public boolean isFile(String dir, String fileName) throws IOException {
        return delegate().isFile(normalizeObjectPath(dir), fileName);
    }

    @Override
    public void mkdir(String filePath) throws IOException {
        delegate().mkdir(normalizeObjectPath(filePath));
    }

    @Override
    public void rm(String path) throws IOException {
        delegate().rm(normalizeObjectPath(path));
    }

    @Override
    public boolean isConnected() {
        return delegate != null && delegate.isConnected();
    }

    @Override
    public boolean mv(String from, String to) throws Exception {
        return delegate().mv(normalizeObjectPath(from), normalizeObjectPath(to));
    }

    @Override
    public InputStream getInputStream(String path, String name) throws IOException {
        return delegate().getInputStream(normalizeObjectPath(path), name);
    }

    @Override
    public OutputStream getOutputStream(String path, String name) throws IOException {
        return delegate().getOutputStream(normalizeObjectPath(path), name);
    }

    @Override
    public void fresh() {
        delegate().fresh();
    }

    @Override
    public void readFile(String absPath,
                         String fileType,
                         Consumer<Map<String, Object>> row,
                         Configuration options) throws IOException {
        delegate().readFile(absPath, fileType, row, options);
    }

    @Override
    public TransferFileEntry stat(String path) throws IOException {
        return delegate().stat(normalizeObjectPath(path));
    }

    @Override
    public TransferFilePage listPage(String directory, String cursor, int pageSize) throws IOException {
        return delegate().listPage(normalizeObjectPath(directory), cursor, pageSize);
    }

    @Override
    public InputStream openRead(String path, long offset, long length) throws IOException {
        return delegate().openRead(normalizeObjectPath(path), offset, length);
    }

    @Override
    public TransferWriteSession prepareWrite(String temporaryPath, String sessionId) throws IOException {
        return delegate().prepareWrite(normalizeObjectPath(temporaryPath), sessionId);
    }

    @Override
    public long append(TransferWriteSession session, long offset, InputStream source, long length)
            throws IOException {
        return delegate().append(session, offset, source, length);
    }

    @Override
    public void commit(TransferWriteSession session, String targetPath, boolean replaceExisting)
            throws IOException {
        delegate().commit(session, normalizeObjectPath(targetPath), replaceExisting);
    }

    @Override
    public void abort(TransferWriteSession session) throws IOException {
        delegate().abort(session);
    }

    @Override
    public void move(String sourcePath, String targetPath, boolean replaceExisting) throws IOException {
        delegate().move(normalizeObjectPath(sourcePath), normalizeObjectPath(targetPath), replaceExisting);
    }

    @Override
    public void delete(String path) throws IOException {
        delegate().delete(normalizeObjectPath(path));
    }

    @Override
    public StorageCapabilities capabilities() {
        return delegate().capabilities();
    }

    @Override
    public String checksum(String path, String algorithm) throws IOException {
        return delegate().checksum(normalizeObjectPath(path), algorithm);
    }

    @Override
    public boolean transferExists(String path) throws IOException {
        return delegate().transferExists(normalizeObjectPath(path));
    }

    @Override
    public void close() throws Exception {
        if (delegate != null) {
            delegate.close();
        }
    }

    private TransferFileSystem delegate() {
        if (delegate == null) {
            throw new IllegalStateException("MinioSourcePlugin is not connected.");
        }
        return delegate;
    }

    private String normalizeObjectPath(String path) {
        String normalized = StringUtils.trimToEmpty(path).replace('\\', '/');
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        return normalized;
    }
}
