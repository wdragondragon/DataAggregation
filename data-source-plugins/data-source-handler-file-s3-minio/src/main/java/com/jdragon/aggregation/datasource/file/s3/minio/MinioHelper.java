package com.jdragon.aggregation.datasource.file.s3.minio;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.transfer.AsyncUploadOutputStream;
import com.jdragon.aggregation.datasource.file.transfer.StorageCapabilities;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.datasource.file.transfer.TransferWriteSession;
import com.jdragon.aggregation.datasource.file.utils.FileParser;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import io.minio.BucketExistsArgs;
import io.minio.ComposeObjectArgs;
import io.minio.ComposeSource;
import io.minio.CopyObjectArgs;
import io.minio.CopySource;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.Item;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

@Slf4j
public class MinioHelper extends AbstractPlugin implements TransferFileSystem {

    private static final long MIN_COMPOSE_PART_SIZE = 5L * 1024 * 1024;
    private static final int MAX_COMPOSE_PARTS = 10_000;
    private static final int PIPE_BUFFER_SIZE = 64 * 1024;
    private static final StorageCapabilities CAPABILITIES =
            new StorageCapabilities(true, true, false, true, true, true, Set.of("SHA-256"));

    private final Set<String> activeTemporaryObjects = ConcurrentHashMap.newKeySet();
    private MinioClient minioClient;
    private String bucketName;
    private String rootPrefix;
    private boolean connected;
    private OkHttpClient httpClient;

    @Override
    public boolean connect(Configuration configuration) {
        try {
            httpClient = new OkHttpClient();
            minioClient = MinioClient.builder()
                    .httpClient(httpClient)
                    .endpoint(configuration.getString("endpoint"))
                    .credentials(configuration.getString("accessKey"), configuration.getString("secretKey"))
                    .build();
            bucketName = configuration.getString("bucket");
            rootPrefix = normalizeKey(configuration.getString("rootPath"));
            boolean createBucket = configuration.getBool("createBucket", true);
            if (createBucket && !minioClient.bucketExists(
                    BucketExistsArgs.builder().bucket(bucketName).build())) {
                log.info("bucket {} does not exist, creating it", bucketName);
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            }
            connected = true;
            return true;
        } catch (Exception e) {
            connected = false;
            throw new IllegalStateException("failed to connect to MinIO", e);
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
        Pattern pattern = Pattern.compile(regex);
        Set<String> names = new HashSet<>();
        String prefix = directoryPrefix(objectName(dir));
        try {
            for (Result<Item> result : minioClient.listObjects(ListObjectsArgs.builder()
                    .bucket(bucketName).prefix(prefix).recursive(true).build())) {
                Item item = result.get();
                if (!item.isDir() && pattern.matcher(item.objectName()).matches()) {
                    names.add(item.objectName());
                }
            }
            return names;
        } catch (Exception e) {
            throw new IOException("failed to list MinIO prefix " + prefix, e);
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
    public void mkdir(String filePath) {
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
        return new AsyncUploadOutputStream(PIPE_BUFFER_SIZE, input -> minioClient.putObject(
                PutObjectArgs.builder().bucket(bucketName).object(key)
                        .stream(input, -1, 10L * 1024 * 1024)
                        .contentType("application/octet-stream").build()));
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
        StatObjectResponse object = tryStatObject(key);
        if (object != null) {
            boolean directory = key.endsWith("/");
            long modified = object.lastModified() == null
                    ? 0L : object.lastModified().toInstant().toEpochMilli();
            return new TransferFileEntry(key, entryName(key), directory,
                    directory ? 0L : object.size(), modified, object.etag());
        }
        List<Part> parts = listParts(key);
        if (!parts.isEmpty() || activeTemporaryObjects.contains(key)) {
            return new TransferFileEntry(key, entryName(key), false,
                    contiguousSize(parts), 0L, null);
        }
        if (hasPrefix(directoryPrefix(key))) {
            return new TransferFileEntry(key, entryName(key), true, 0L, 0L, null);
        }
        throw new NoSuchFileException(path);
    }

    @Override
    public TransferFilePage listPage(String directory, String cursor, int pageSize) throws IOException {
        int limit = Math.max(1, Math.min(pageSize, 1_000));
        String prefix = directoryPrefix(objectName(directory));
        List<TransferFileEntry> entries = new ArrayList<>();
        String nextCursor = null;
        try {
            ListObjectsArgs.Builder builder = ListObjectsArgs.builder().bucket(bucketName)
                    .prefix(prefix).recursive(false).maxKeys(limit + 1);
            if (cursor != null && !cursor.isBlank()) {
                builder.startAfter(cursor);
            }
            for (Result<Item> result : minioClient.listObjects(builder.build())) {
                Item item = result.get();
                if (item.objectName().equals(prefix)) {
                    continue;
                }
                String path = trimDirectorySuffix(item.objectName());
                Instant modified = item.lastModified() == null ? null : item.lastModified().toInstant();
                entries.add(new TransferFileEntry(path, entryName(path), item.isDir(),
                        item.isDir() ? 0L : item.size(), modified == null ? 0L : modified.toEpochMilli(),
                        item.etag()));
                if (entries.size() <= limit) {
                    nextCursor = item.objectName();
                }
                if (entries.size() > limit) {
                    break;
                }
            }
        } catch (Exception e) {
            throw new IOException("failed to list MinIO directory " + directory, e);
        }
        boolean truncated = entries.size() > limit;
        if (truncated) {
            entries = new ArrayList<>(entries.subList(0, limit));
        }
        return new TransferFilePage(entries, truncated ? nextCursor : null, truncated);
    }

    @Override
    public InputStream openRead(String path, long offset, long length) throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
        String key = objectName(path);
        StatObjectResponse object = tryStatObject(key);
        if (object != null) {
            if (offset > object.size()) {
                throw new IOException("read offset exceeds object size: " + offset);
            }
            if (length == 0) {
                return InputStream.nullInputStream();
            }
            try {
                return minioClient.getObject(GetObjectArgs.builder().bucket(bucketName).object(key)
                        .offset(offset).length(length).build());
            } catch (Exception e) {
                throw new IOException("failed to open MinIO range for " + path, e);
            }
        }
        List<Part> parts = listParts(key);
        long size = contiguousSize(parts);
        if (offset > size || (parts.isEmpty() && !activeTemporaryObjects.contains(key))) {
            throw new NoSuchFileException(path);
        }
        return new PartSequenceInputStream(parts, offset, Math.min(length, size - offset));
    }

    @Override
    public TransferWriteSession prepareWrite(String temporaryPath, String sessionId) throws IOException {
        String key = objectName(temporaryPath);
        activeTemporaryObjects.add(key);
        StatObjectResponse existing = tryStatObject(key);
        long offset = existing == null ? contiguousSize(listParts(key)) : existing.size();
        return new TransferWriteSession(sessionId, key, offset,
                Map.of("writeMode", "chunked-compose", "bucket", bucketName));
    }

    @Override
    public long append(TransferWriteSession session, long offset, InputStream source, long length)
            throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
        String temporary = objectName(session.temporaryPath());
        long confirmed = contiguousSize(listParts(temporary));
        if (confirmed != offset) {
            throw new IOException("temporary object offset mismatch, expected " + offset
                    + " but was " + confirmed);
        }
        String partKey = rawPartPrefix(temporary) + String.format("%020d", offset);
        CountingBoundedInputStream input = new CountingBoundedInputStream(source, length);
        try {
            minioClient.putObject(PutObjectArgs.builder().bucket(bucketName).object(partKey)
                    .stream(input, length, -1).contentType("application/octet-stream").build());
        } catch (Exception e) {
            removeObjectQuietly(partKey);
            throw new IOException("failed to upload MinIO transfer part at offset " + offset, e);
        }
        return input.count();
    }

    @Override
    public void commit(TransferWriteSession session, String targetPath, boolean replaceExisting)
            throws IOException {
        String temporary = objectName(session.temporaryPath());
        String target = objectName(targetPath);
        if (!replaceExisting && transferExists(target)) {
            throw new FileAlreadyExistsException(targetPath);
        }
        if (tryStatObject(temporary) == null) {
            composeTemporaryObject(temporary);
        }
        try {
            minioClient.copyObject(CopyObjectArgs.builder().bucket(bucketName).object(target)
                    .source(CopySource.builder().bucket(bucketName).object(temporary).build()).build());
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucketName)
                    .object(temporary).build());
            cleanupParts(temporary);
            activeTemporaryObjects.remove(temporary);
        } catch (Exception e) {
            throw new IOException("failed to commit MinIO object " + target, e);
        }
    }

    @Override
    public void abort(TransferWriteSession session) throws IOException {
        String temporary = objectName(session.temporaryPath());
        removeObjectQuietly(temporary);
        cleanupParts(temporary);
        activeTemporaryObjects.remove(temporary);
    }

    @Override
    public void move(String sourcePath, String targetPath, boolean replaceExisting) throws IOException {
        String source = objectName(sourcePath);
        String target = objectName(targetPath);
        if (!replaceExisting && transferExists(target)) {
            throw new FileAlreadyExistsException(targetPath);
        }
        try {
            minioClient.copyObject(CopyObjectArgs.builder().bucket(bucketName).object(target)
                    .source(CopySource.builder().bucket(bucketName).object(source).build()).build());
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(source).build());
        } catch (Exception e) {
            throw new IOException("failed to move MinIO object " + source + " to " + target, e);
        }
    }

    @Override
    public void delete(String path) throws IOException {
        String key = objectName(path);
        if (tryStatObject(key) != null) {
            removeObject(key);
        }
        String prefix = directoryPrefix(key);
        try {
            for (Result<Item> result : minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName)
                    .prefix(prefix).recursive(true).build())) {
                Item item = result.get();
                if (!item.isDir()) {
                    removeObject(item.objectName());
                }
            }
        } catch (Exception e) {
            throw new IOException("failed to delete MinIO prefix " + prefix, e);
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
        if (httpClient != null && connected) {
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
        connected = false;
        activeTemporaryObjects.clear();
    }

    private void composeTemporaryObject(String temporary) throws IOException {
        List<Part> rawParts = listParts(temporary);
        long totalSize = contiguousSize(rawParts);
        if (rawParts.isEmpty()) {
            try {
                minioClient.putObject(PutObjectArgs.builder().bucket(bucketName).object(temporary)
                        .stream(new ByteArrayInputStream(new byte[0]), 0, -1).build());
                return;
            } catch (Exception e) {
                throw new IOException("failed to create empty MinIO temporary object", e);
            }
        }
        List<Part> composeParts = coalesceParts(temporary, rawParts, totalSize);
        try {
            if (composeParts.size() == 1) {
                minioClient.copyObject(CopyObjectArgs.builder().bucket(bucketName).object(temporary)
                        .source(CopySource.builder().bucket(bucketName)
                                .object(composeParts.get(0).key()).build()).build());
            } else {
                List<ComposeSource> sources = composeParts.stream()
                        .map(part -> ComposeSource.builder().bucket(bucketName).object(part.key()).build())
                        .toList();
                minioClient.composeObject(ComposeObjectArgs.builder().bucket(bucketName)
                        .object(temporary).sources(sources).build());
            }
        } catch (Exception e) {
            throw new IOException("failed to compose MinIO temporary object", e);
        }
    }

    private List<Part> coalesceParts(String temporary, List<Part> rawParts, long totalSize)
            throws IOException {
        long targetPartSize = Math.max(MIN_COMPOSE_PART_SIZE,
                (totalSize + MAX_COMPOSE_PARTS - 1) / MAX_COMPOSE_PARTS);
        List<List<Part>> groups = new ArrayList<>();
        List<Part> current = new ArrayList<>();
        long currentSize = 0L;
        for (Part part : rawParts) {
            current.add(part);
            currentSize += part.size();
            if (currentSize >= targetPartSize) {
                groups.add(current);
                current = new ArrayList<>();
                currentSize = 0L;
            }
        }
        if (!current.isEmpty()) {
            groups.add(current);
        }
        List<Part> result = new ArrayList<>();
        long offset = 0L;
        for (int index = 0; index < groups.size(); index++) {
            List<Part> group = groups.get(index);
            long size = group.stream().mapToLong(Part::size).sum();
            boolean last = index == groups.size() - 1;
            if (group.size() == 1 && (size >= MIN_COMPOSE_PART_SIZE || last)) {
                result.add(new Part(group.get(0).key(), offset, size));
            } else {
                String key = composePartPrefix(temporary) + String.format("%05d", index);
                try (InputStream input = new PartSequenceInputStream(group, 0, size)) {
                    minioClient.putObject(PutObjectArgs.builder().bucket(bucketName).object(key)
                            .stream(input, size, -1).contentType("application/octet-stream").build());
                } catch (Exception e) {
                    throw new IOException("failed to coalesce MinIO transfer parts", e);
                }
                result.add(new Part(key, offset, size));
            }
            offset += size;
        }
        return result;
    }

    private List<Part> listParts(String temporary) throws IOException {
        String prefix = rawPartPrefix(temporary);
        List<Part> parts = new ArrayList<>();
        try {
            for (Result<Item> result : minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName)
                    .prefix(prefix).recursive(true).build())) {
                Item item = result.get();
                if (item.isDir()) {
                    continue;
                }
                String suffix = item.objectName().substring(prefix.length());
                try {
                    parts.add(new Part(item.objectName(), Long.parseLong(suffix), item.size()));
                } catch (NumberFormatException ignored) {
                    throw new IOException("invalid MinIO transfer part key: " + item.objectName());
                }
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("failed to inspect MinIO transfer parts", e);
        }
        parts.sort(Comparator.comparingLong(Part::offset));
        return parts;
    }

    private long contiguousSize(List<Part> parts) throws IOException {
        long offset = 0L;
        for (Part part : parts) {
            if (part.offset() != offset) {
                throw new IOException("MinIO transfer parts are not contiguous at offset " + offset);
            }
            offset += part.size();
        }
        return offset;
    }

    private boolean hasPrefix(String prefix) throws IOException {
        try {
            for (Result<Item> ignored : minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName)
                    .prefix(prefix).recursive(false).maxKeys(1).build())) {
                return true;
            }
            return false;
        } catch (Exception e) {
            throw new IOException("failed to inspect MinIO prefix " + prefix, e);
        }
    }

    private StatObjectResponse tryStatObject(String key) throws IOException {
        if (key == null || key.isBlank()) {
            return null;
        }
        try {
            return minioClient.statObject(StatObjectArgs.builder().bucket(bucketName).object(key).build());
        } catch (ErrorResponseException e) {
            if (isNotFound(e)) {
                return null;
            }
            throw new IOException("failed to stat MinIO object " + key, e);
        } catch (Exception e) {
            throw new IOException("failed to stat MinIO object " + key, e);
        }
    }

    private boolean isNotFound(ErrorResponseException error) {
        String code = error.errorResponse() == null ? null : error.errorResponse().code();
        return "NoSuchKey".equals(code) || "NoSuchObject".equals(code) || "NotFound".equals(code);
    }

    private void cleanupParts(String temporary) throws IOException {
        String prefix = temporary + ".filetransfer-";
        try {
            for (Result<Item> result : minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName)
                    .prefix(prefix).recursive(true).build())) {
                Item item = result.get();
                if (!item.isDir()) {
                    removeObject(item.objectName());
                }
            }
        } catch (Exception e) {
            throw new IOException("failed to clean MinIO transfer parts", e);
        }
    }

    private void removeObject(String key) throws IOException {
        try {
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(key).build());
        } catch (Exception e) {
            throw new IOException("failed to delete MinIO object " + key, e);
        }
    }

    private void removeObjectQuietly(String key) {
        try {
            removeObject(key);
        } catch (IOException ignored) {
        }
    }

    private String objectName(String path) {
        String key = normalizeKey(path);
        String bucketPrefix = bucketName == null ? "" : bucketName.trim() + "/";
        if (!bucketPrefix.equals("/") && key.startsWith(bucketPrefix)) {
            key = key.substring(bucketPrefix.length());
        }
        if (rootPrefix == null || rootPrefix.isBlank()) {
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
        String value = path == null ? "" : path.trim().replace('\\', '/');
        while (value.startsWith("/")) {
            value = value.substring(1);
        }
        String[] segments = value.split("/");
        List<String> normalized = new ArrayList<>();
        for (String segment : segments) {
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
        return key == null || key.isBlank() ? "" : trimDirectorySuffix(key) + "/";
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

    private String rawPartPrefix(String temporary) {
        return temporary + ".filetransfer-parts/";
    }

    private String composePartPrefix(String temporary) {
        return temporary + ".filetransfer-compose/";
    }

    private record Part(String key, long offset, long size) {
    }

    private record Segment(String key, long offset, long length) {
    }

    private final class PartSequenceInputStream extends InputStream {
        private final List<Segment> segments;
        private int index;
        private InputStream current;

        private PartSequenceInputStream(List<Part> parts, long offset, long length) {
            segments = new ArrayList<>();
            long end = offset + length;
            for (Part part : parts) {
                long partEnd = part.offset() + part.size();
                long start = Math.max(offset, part.offset());
                long segmentEnd = Math.min(end, partEnd);
                if (start < segmentEnd) {
                    segments.add(new Segment(part.key(), start - part.offset(), segmentEnd - start));
                }
            }
        }

        @Override
        public int read() throws IOException {
            byte[] single = new byte[1];
            int read = read(single, 0, 1);
            return read < 0 ? -1 : single[0] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            while (true) {
                if (current == null && !openNext()) {
                    return -1;
                }
                int read = current.read(buffer, offset, length);
                if (read >= 0) {
                    return read;
                }
                current.close();
                current = null;
            }
        }

        @Override
        public void close() throws IOException {
            if (current != null) {
                current.close();
                current = null;
            }
            index = segments.size();
        }

        private boolean openNext() throws IOException {
            if (index >= segments.size()) {
                return false;
            }
            Segment segment = segments.get(index++);
            try {
                current = minioClient.getObject(GetObjectArgs.builder().bucket(bucketName)
                        .object(segment.key()).offset(segment.offset()).length(segment.length()).build());
                return true;
            } catch (Exception e) {
                throw new IOException("failed to read MinIO transfer part " + segment.key(), e);
            }
        }
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
