package com.jdragon.aggregation.datasource.file.s3.minio;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.AppendObjectRequest;
import com.aliyun.oss.model.AppendObjectResult;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.transfer.AsyncUploadOutputStream;
import com.jdragon.aggregation.datasource.file.transfer.StorageCapabilities;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.datasource.file.transfer.TransferWriteSession;
import com.jdragon.aggregation.datasource.file.utils.FileParser;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

@Slf4j
public class AliyunOssHelper extends AbstractPlugin implements TransferFileSystem {

    private static final int PIPE_BUFFER_SIZE = 64 * 1024;
    private static final StorageCapabilities CAPABILITIES =
            new StorageCapabilities(true, true, false, true, true, true, Set.of("SHA-256"));

    private OSS ossClient;
    private String bucketName;
    private String rootPrefix;
    private boolean connected;

    @Override
    public boolean connect(Configuration configuration) {
        try {
            String endpoint = configuration.getString("endpoint");
            String accessKeyId = configuration.getString("accessKey");
            String accessKeySecret = configuration.getString("secretKey");
            bucketName = configuration.getString("bucket");
            rootPrefix = normalizeKey(configuration.getString("rootPath"));
            boolean createBucket = configuration.getBool("createBucket", true);
            ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
            if (createBucket && !ossClient.doesBucketExist(bucketName)) {
                log.info("bucket {} does not exist, creating it", bucketName);
                ossClient.createBucket(bucketName);
            }
            connected = true;
            return true;
        } catch (Exception e) {
            log.error("failed to connect to Aliyun OSS: {}", e.getMessage(), e);
            connected = false;
            return false;
        }
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public boolean exists(String path, String name) throws IOException {
        return transferExists(processingPath(path, name));
    }

    @Override
    public Set<String> listFile(String dir, String regex) throws IOException {
        String prefix = directoryPrefix(objectName(dir));
        Pattern pattern = Pattern.compile(regex);
        Set<String> names = new HashSet<>();
        ListObjectsRequest request = new ListObjectsRequest(bucketName).withPrefix(prefix).withMaxKeys(1_000);
        try {
            ObjectListing listing;
            do {
                listing = ossClient.listObjects(request);
                for (OSSObjectSummary summary : listing.getObjectSummaries()) {
                    if (pattern.matcher(summary.getKey()).matches()) {
                        names.add(summary.getKey());
                    }
                }
                request.setMarker(listing.getNextMarker());
            } while (listing.isTruncated());
            return names;
        } catch (RuntimeException e) {
            throw new IOException("failed to list OSS prefix " + prefix, e);
        }
    }

    @Override
    public boolean isFile(String dir, String fileName) throws IOException {
        try {
            return !stat(processingPath(dir, fileName)).directory();
        } catch (NoSuchFileException e) {
            return false;
        }
    }

    @Override
    public void mkdir(String filePath) throws IOException {
        String key = directoryPrefix(objectName(filePath));
        try (InputStream input = new ByteArrayInputStream(new byte[0])) {
            ossClient.putObject(bucketName, key, input);
        } catch (RuntimeException e) {
            throw new IOException("failed to create OSS directory marker " + key, e);
        }
    }

    @Override
    public void rm(String path) throws IOException {
        delete(path);
    }

    @Override
    public boolean mv(String from, String to) throws IOException {
        move(from, to, false);
        return true;
    }

    @Override
    public InputStream getInputStream(String path, String name) throws IOException {
        TransferFileEntry entry = stat(processingPath(path, name));
        return openRead(entry.path(), 0, entry.size());
    }

    @Override
    public OutputStream getOutputStream(String path, String name) throws IOException {
        String key = objectName(processingPath(path, name));
        return new AsyncUploadOutputStream(PIPE_BUFFER_SIZE,
                input -> ossClient.putObject(bucketName, key, input));
    }

    @Override
    public void readFile(String absPath, String fileType,
                         java.util.function.Consumer<Map<String, Object>> row,
                         Configuration options) throws IOException {
        TransferFileEntry entry = stat(absPath);
        try (InputStream input = openRead(entry.path(), 0, entry.size())) {
            FileParser.parseInputStream(input, FileParser.FileFormat.fromString(fileType),
                    "UTF-8", row, options);
        }
    }

    @Override
    public TransferFileEntry stat(String path) throws IOException {
        String key = objectName(path);
        ObjectMetadata metadata = tryMetadata(key);
        if (metadata != null) {
            boolean directory = key.endsWith("/");
            long modified = metadata.getLastModified() == null ? 0L : metadata.getLastModified().getTime();
            return new TransferFileEntry(trimDirectorySuffix(key), entryName(key), directory,
                    directory ? 0L : metadata.getContentLength(), modified, metadata.getETag());
        }
        if (hasPrefix(directoryPrefix(key))) {
            return new TransferFileEntry(trimDirectorySuffix(key), entryName(key), true, 0L, 0L, null);
        }
        throw new NoSuchFileException(path);
    }

    @Override
    public TransferFilePage listPage(String directory, String cursor, int pageSize) throws IOException {
        int limit = Math.max(1, Math.min(pageSize, 1_000));
        String prefix = directoryPrefix(objectName(directory));
        ListObjectsRequest request = new ListObjectsRequest(bucketName).withPrefix(prefix)
                .withDelimiter("/").withMaxKeys(limit);
        if (StringUtils.isNotBlank(cursor)) {
            request.setMarker(cursor);
        }
        try {
            ObjectListing listing = ossClient.listObjects(request);
            List<TransferFileEntry> entries = new ArrayList<>();
            for (String commonPrefix : listing.getCommonPrefixes()) {
                String path = trimDirectorySuffix(commonPrefix);
                entries.add(new TransferFileEntry(path, entryName(path), true, 0L, 0L, null));
            }
            for (OSSObjectSummary summary : listing.getObjectSummaries()) {
                if (summary.getKey().equals(prefix)) {
                    continue;
                }
                boolean directoryMarker = summary.getKey().endsWith("/");
                String path = trimDirectorySuffix(summary.getKey());
                entries.add(new TransferFileEntry(path, entryName(path), directoryMarker,
                        directoryMarker ? 0L : summary.getSize(),
                        summary.getLastModified() == null ? 0L : summary.getLastModified().getTime(),
                        summary.getETag()));
            }
            entries.sort(Comparator.comparing(TransferFileEntry::directory).reversed()
                    .thenComparing(TransferFileEntry::name, String.CASE_INSENSITIVE_ORDER));
            return new TransferFilePage(entries, listing.isTruncated() ? listing.getNextMarker() : null,
                    listing.isTruncated());
        } catch (RuntimeException e) {
            throw new IOException("failed to list OSS directory " + directory, e);
        }
    }

    @Override
    public InputStream openRead(String path, long offset, long length) throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
        String key = objectName(path);
        ObjectMetadata metadata = tryMetadata(key);
        if (metadata == null) {
            throw new NoSuchFileException(path);
        }
        if (offset > metadata.getContentLength()) {
            throw new IOException("read offset exceeds object size: " + offset);
        }
        if (length == 0) {
            return InputStream.nullInputStream();
        }
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, key);
            request.setRange(offset, offset + length - 1);
            OSSObject object = ossClient.getObject(request);
            return object.getObjectContent();
        } catch (RuntimeException e) {
            throw new IOException("failed to open OSS range for " + path, e);
        }
    }

    @Override
    public TransferWriteSession prepareWrite(String temporaryPath, String sessionId) throws IOException {
        String key = objectName(temporaryPath);
        ObjectMetadata metadata = tryMetadata(key);
        long offset;
        if (metadata == null) {
            ObjectMetadata emptyMetadata = new ObjectMetadata();
            emptyMetadata.setContentLength(0);
            AppendObjectRequest request = new AppendObjectRequest(bucketName, key,
                    new ByteArrayInputStream(new byte[0]), emptyMetadata).withPosition(0L);
            try {
                offset = ossClient.appendObject(request).getNextPosition();
            } catch (RuntimeException e) {
                throw new IOException("failed to create OSS appendable temporary object " + key, e);
            }
        } else {
            offset = metadata.getContentLength();
        }
        return new TransferWriteSession(sessionId, key, offset,
                Map.of("writeMode", "append-object", "bucket", bucketName));
    }

    @Override
    public long append(TransferWriteSession session, long offset, InputStream source, long length)
            throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
        String key = objectName(session.temporaryPath());
        ObjectMetadata existing = tryMetadata(key);
        long currentSize = existing == null ? 0L : existing.getContentLength();
        if (currentSize != offset) {
            throw new IOException("temporary object offset mismatch, expected " + offset
                    + " but was " + currentSize);
        }
        CountingBoundedInputStream input = new CountingBoundedInputStream(source, length);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(length);
        AppendObjectRequest request = new AppendObjectRequest(bucketName, key, input, metadata)
                .withPosition(offset);
        try {
            AppendObjectResult result = ossClient.appendObject(request);
            long advanced = result.getNextPosition() - offset;
            if (advanced != input.count()) {
                throw new IOException("OSS append response offset does not match uploaded bytes");
            }
            return input.count();
        } catch (RuntimeException e) {
            throw new IOException("failed to append OSS temporary object at offset " + offset, e);
        }
    }

    @Override
    public void commit(TransferWriteSession session, String targetPath, boolean replaceExisting)
            throws IOException {
        String temporary = objectName(session.temporaryPath());
        String target = objectName(targetPath);
        if (!replaceExisting && transferExists(target)) {
            throw new FileAlreadyExistsException(targetPath);
        }
        try {
            ossClient.copyObject(new CopyObjectRequest(bucketName, temporary, bucketName, target));
            ossClient.deleteObject(bucketName, temporary);
        } catch (RuntimeException e) {
            throw new IOException("failed to commit OSS object " + target, e);
        }
    }

    @Override
    public void abort(TransferWriteSession session) throws IOException {
        deleteExact(objectName(session.temporaryPath()));
    }

    @Override
    public void move(String sourcePath, String targetPath, boolean replaceExisting) throws IOException {
        String source = objectName(sourcePath);
        String target = objectName(targetPath);
        if (!replaceExisting && transferExists(target)) {
            throw new FileAlreadyExistsException(targetPath);
        }
        try {
            ossClient.copyObject(new CopyObjectRequest(bucketName, source, bucketName, target));
            ossClient.deleteObject(bucketName, source);
        } catch (RuntimeException e) {
            throw new IOException("failed to move OSS object " + source + " to " + target, e);
        }
    }

    @Override
    public void delete(String path) throws IOException {
        String key = objectName(path);
        if (tryMetadata(key) != null) {
            deleteExact(key);
        }
        String prefix = directoryPrefix(key);
        ListObjectsRequest request = new ListObjectsRequest(bucketName).withPrefix(prefix).withMaxKeys(1_000);
        try {
            ObjectListing listing;
            do {
                listing = ossClient.listObjects(request);
                for (OSSObjectSummary summary : listing.getObjectSummaries()) {
                    ossClient.deleteObject(bucketName, summary.getKey());
                }
                request.setMarker(listing.getNextMarker());
            } while (listing.isTruncated());
        } catch (RuntimeException e) {
            throw new IOException("failed to delete OSS prefix " + prefix, e);
        }
    }

    @Override
    public StorageCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public boolean transferExists(String path) throws IOException {
        try {
            stat(path);
            return true;
        } catch (NoSuchFileException ignored) {
            return false;
        }
    }

    @Override
    public void close() {
        if (ossClient != null) {
            ossClient.shutdown();
        }
        connected = false;
    }

    private ObjectMetadata tryMetadata(String key) throws IOException {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        try {
            return ossClient.getObjectMetadata(bucketName, key);
        } catch (OSSException e) {
            if ("NoSuchKey".equals(e.getErrorCode()) || "NoSuchObject".equals(e.getErrorCode())) {
                return null;
            }
            throw new IOException("failed to stat OSS object " + key, e);
        } catch (RuntimeException e) {
            throw new IOException("failed to stat OSS object " + key, e);
        }
    }

    private boolean hasPrefix(String prefix) throws IOException {
        try {
            ObjectListing listing = ossClient.listObjects(
                    new ListObjectsRequest(bucketName).withPrefix(prefix).withMaxKeys(1));
            return !listing.getObjectSummaries().isEmpty() || !listing.getCommonPrefixes().isEmpty();
        } catch (RuntimeException e) {
            throw new IOException("failed to inspect OSS prefix " + prefix, e);
        }
    }

    private void deleteExact(String key) throws IOException {
        try {
            ossClient.deleteObject(bucketName, key);
        } catch (RuntimeException e) {
            throw new IOException("failed to delete OSS object " + key, e);
        }
    }

    private String objectName(String path) {
        String key = normalizeKey(path);
        String bucketPrefix = StringUtils.trimToEmpty(bucketName) + "/";
        if (StringUtils.isNotBlank(bucketName) && key.startsWith(bucketPrefix)) {
            key = key.substring(bucketPrefix.length());
        }
        if (StringUtils.isBlank(rootPrefix)) {
            return key;
        }
        if (key.isBlank()) {
            return rootPrefix;
        }
        if (key.equals(rootPrefix) || key.startsWith(rootPrefix + "/")) {
            return key;
        }
        return rootPrefix + "/" + key;
    }

    private String normalizeKey(String path) {
        String value = StringUtils.trimToEmpty(path).replace('\\', '/');
        while (value.startsWith("/")) {
            value = value.substring(1);
        }
        List<String> normalized = new ArrayList<>();
        for (String segment : value.split("/")) {
            if (segment.isBlank() || ".".equals(segment)) {
                continue;
            }
            if ("..".equals(segment)) {
                throw new IllegalArgumentException("object path must not contain '..': " + path);
            }
            normalized.add(segment);
        }
        return String.join("/", normalized);
    }

    private String directoryPrefix(String key) {
        return StringUtils.isBlank(key) ? "" : trimDirectorySuffix(key) + "/";
    }

    private String trimDirectorySuffix(String key) {
        String value = key;
        while (value.length() > 1 && value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    private String entryName(String key) {
        String value = trimDirectorySuffix(key);
        int slash = value.lastIndexOf('/');
        return slash < 0 ? value : value.substring(slash + 1);
    }

    private static final class CountingBoundedInputStream extends FilterInputStream {
        private long remaining;
        private long count;

        private CountingBoundedInputStream(InputStream input, long length) {
            super(input);
            remaining = length;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int value = super.read();
            if (value >= 0) {
                remaining--;
                count++;
            }
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int read = super.read(buffer, offset, (int) Math.min(length, remaining));
            if (read > 0) {
                remaining -= read;
                count += read;
            }
            return read;
        }

        private long count() {
            return count;
        }
    }
}
