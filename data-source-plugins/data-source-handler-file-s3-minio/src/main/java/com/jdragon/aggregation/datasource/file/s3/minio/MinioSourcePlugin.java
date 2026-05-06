package com.jdragon.aggregation.datasource.file.s3.minio;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

public class MinioSourcePlugin extends AbstractPlugin implements FileHelper {

    private FileHelper delegate;

    @Override
    public boolean connect(Configuration configuration) {
        String provider = configuration == null ? null : configuration.getString("storageProvider", "oss");
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
    public void close() throws Exception {
        if (delegate != null) {
            delegate.close();
        }
    }

    private FileHelper delegate() {
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
