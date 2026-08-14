package com.jdragon.aggregation.datasource.file;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.transfer.StorageCapabilities;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.datasource.file.transfer.TransferWriteSession;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import org.apache.commons.lang3.StringUtils;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class LocalFileHelper extends AbstractPlugin implements TransferFileSystem {

    private static final int COPY_BUFFER_SIZE = 64 * 1024;
    private static final StorageCapabilities CAPABILITIES =
            new StorageCapabilities(true, true, true, true, false, false, Set.of("SHA-256"),
                    !System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win"));

    private List<Path> allowedRoots = List.of();

    @Override
    public boolean connect(Configuration configuration) {
        if (configuration == null) {
            allowedRoots = List.of();
            return true;
        }
        List<Path> configuredRoots = new ArrayList<>();
        String rootPath = configuration.getString("rootPath");
        if (StringUtils.isNotBlank(rootPath)) {
            configuredRoots.add(Paths.get(rootPath).toAbsolutePath().normalize());
        }
        List<String> roots = configuration.getList("allowedRoots", String.class);
        if (roots != null) {
            for (String root : roots) {
                if (StringUtils.isNotBlank(root)) {
                    configuredRoots.add(Paths.get(root).toAbsolutePath().normalize());
                }
            }
        }
        allowedRoots = List.copyOf(configuredRoots);
        return true;
    }

    @Override
    public boolean exists(String path, String name) throws IOException {
        return Files.exists(resolve(processingPath(path, name)));
    }

    @Override
    public Set<String> listFile(String dir, String regex) throws IOException {
        Path directory = resolve(dir);
        if (!Files.isDirectory(directory)) {
            throw new IOException(dir + " is not a directory");
        }
        Pattern pattern = Pattern.compile(regex);
        Set<String> names = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path child : stream) {
                String name = child.getFileName().toString();
                if (!StringUtils.equalsAny(name, ".", "..") && pattern.matcher(name).find()) {
                    names.add(name);
                }
            }
        }
        return names;
    }

    @Override
    public boolean isFile(String dir, String fileName) throws IOException {
        return Files.isRegularFile(resolve(processingPath(dir, fileName)));
    }

    @Override
    public void mkdir(String filePath) throws IOException {
        Files.createDirectories(resolve(filePath));
    }

    @Override
    public void rm(String path) throws IOException {
        delete(path);
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public boolean mv(String from, String to) throws IOException {
        move(from, to, false);
        return true;
    }

    @Override
    public InputStream getInputStream(String path, String name) throws IOException {
        Path source = resolve(processingPath(path, name));
        return Files.exists(source) ? Files.newInputStream(source) : null;
    }

    @Override
    public OutputStream getOutputStream(String path, String name) throws IOException {
        Path target = resolve(processingPath(path, name));
        createParent(target);
        return Files.newOutputStream(target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public TransferFileEntry stat(String path) throws IOException {
        Path resolved = resolve(path);
        if (!Files.exists(resolved)) {
            throw new NoSuchFileException(path);
        }
        BasicFileAttributes attributes = Files.readAttributes(resolved, BasicFileAttributes.class);
        return new TransferFileEntry(toExternalPath(resolved),
                resolved.getFileName() == null ? resolved.toString() : resolved.getFileName().toString(),
                attributes.isDirectory(), attributes.isDirectory() ? 0L : attributes.size(),
                attributes.lastModifiedTime().toMillis(), null);
    }

    @Override
    public TransferFilePage listPage(String directory, String cursor, int pageSize) throws IOException {
        Path resolved = resolve(directory);
        if (!Files.isDirectory(resolved)) {
            throw new IOException(directory + " is not a directory");
        }
        int safePageSize = Math.max(1, Math.min(pageSize, 1000));
        int offset = parseCursor(cursor);
        List<TransferFileEntry> all = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(resolved)) {
            for (Path child : stream) {
                BasicFileAttributes attributes = Files.readAttributes(child, BasicFileAttributes.class);
                all.add(new TransferFileEntry(toExternalPath(child), child.getFileName().toString(),
                        attributes.isDirectory(), attributes.isDirectory() ? 0L : attributes.size(),
                        attributes.lastModifiedTime().toMillis(), null));
            }
        }
        all.sort(Comparator.comparing(TransferFileEntry::directory).reversed()
                .thenComparing(TransferFileEntry::name, String.CASE_INSENSITIVE_ORDER));
        int from = Math.min(offset, all.size());
        int to = Math.min(from + safePageSize, all.size());
        boolean truncated = to < all.size();
        return new TransferFilePage(all.subList(from, to), truncated ? String.valueOf(to) : null, truncated);
    }

    @Override
    public InputStream openRead(String path, long offset, long length) throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
        Path source = resolve(path);
        FileChannel channel = FileChannel.open(source, StandardOpenOption.READ);
        if (offset > channel.size()) {
            channel.close();
            throw new IOException("read offset exceeds file size: " + offset);
        }
        channel.position(offset);
        return new LimitedInputStream(Channels.newInputStream(channel), length);
    }

    @Override
    public TransferWriteSession prepareWrite(String temporaryPath, String sessionId) throws IOException {
        Path target = resolve(temporaryPath);
        createParent(target);
        long offset = Files.exists(target) ? Files.size(target) : 0L;
        if (!Files.exists(target)) {
            Files.createFile(target);
        }
        return new TransferWriteSession(sessionId, toExternalPath(target), offset, Map.of());
    }

    @Override
    public Optional<TransferWriteSession> recoverWriteSession(
            TransferWriteSession session, long confirmedOffset) throws IOException {
        if (confirmedOffset < 0) {
            throw new IllegalArgumentException("confirmedOffset must be non-negative");
        }
        Path target = resolve(session.temporaryPath());
        if (!Files.exists(target)) {
            return Optional.empty();
        }
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.WRITE)) {
            if (channel.size() < confirmedOffset) {
                return Optional.empty();
            }
            if (channel.size() > confirmedOffset) {
                channel.truncate(confirmedOffset);
                channel.force(false);
            }
        }
        return Optional.of(new TransferWriteSession(session.sessionId(),
                session.temporaryPath(), confirmedOffset, session.metadata()));
    }

    @Override
    public long append(TransferWriteSession session, long offset, InputStream source, long length) throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
        Path target = resolve(session.temporaryPath());
        createParent(target);
        OpenOption[] options = {StandardOpenOption.CREATE, StandardOpenOption.WRITE};
        try (FileChannel channel = FileChannel.open(target, options)) {
            long existing = channel.size();
            if (existing != offset) {
                throw new IOException("temporary file offset mismatch, expected " + offset + " but was " + existing);
            }
            channel.position(offset);
            byte[] buffer = new byte[COPY_BUFFER_SIZE];
            long remaining = length;
            long written = 0L;
            while (remaining > 0) {
                int read = source.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (read < 0) {
                    break;
                }
                if (read == 0) {
                    continue;
                }
                channel.write(java.nio.ByteBuffer.wrap(buffer, 0, read));
                written += read;
                remaining -= read;
            }
            channel.force(false);
            return written;
        }
    }

    @Override
    public void commit(TransferWriteSession session, String targetPath, boolean replaceExisting) throws IOException {
        moveInternal(resolve(session.temporaryPath()), resolve(targetPath), replaceExisting);
    }

    @Override
    public void abort(TransferWriteSession session) throws IOException {
        Files.deleteIfExists(resolve(session.temporaryPath()));
    }

    @Override
    public void move(String sourcePath, String targetPath, boolean replaceExisting) throws IOException {
        moveInternal(resolve(sourcePath), resolve(targetPath), replaceExisting);
    }

    @Override
    public void delete(String path) throws IOException {
        Path target = resolve(path);
        if (!Files.exists(target)) {
            return;
        }
        if (Files.isDirectory(target)) {
            try (java.util.stream.Stream<Path> walk = Files.walk(target)) {
                List<Path> entries = walk.sorted(Comparator.reverseOrder()).toList();
                for (Path entry : entries) {
                    Files.deleteIfExists(entry);
                }
            }
        } else {
            Files.deleteIfExists(target);
        }
    }

    @Override
    public StorageCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public void close() {
    }

    protected List<Path> configuredRoots() {
        return allowedRoots;
    }

    private Path resolve(String rawPath) throws IOException {
        if (StringUtils.isBlank(rawPath)) {
            if (allowedRoots.isEmpty()) {
                return Paths.get("").toAbsolutePath().normalize();
            }
            return allowedRoots.get(0);
        }
        Path value = Paths.get(rawPath);
        if (!value.isAbsolute() && !allowedRoots.isEmpty()) {
            value = allowedRoots.get(0).resolve(value);
        }
        Path normalized = value.toAbsolutePath().normalize();
        if (!allowedRoots.isEmpty() && allowedRoots.stream().noneMatch(normalized::startsWith)) {
            throw new AccessDeniedException("path is outside configured local roots: " + rawPath);
        }
        return normalized;
    }

    private String toExternalPath(Path path) {
        return path.toAbsolutePath().normalize().toString();
    }

    private int parseCursor(String cursor) {
        if (StringUtils.isBlank(cursor)) {
            return 0;
        }
        try {
            int parsed = Integer.parseInt(cursor);
            return Math.max(parsed, 0);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid local page cursor: " + cursor, e);
        }
    }

    private void createParent(Path target) throws IOException {
        Path parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    private void moveInternal(Path source, Path target, boolean replaceExisting) throws IOException {
        createParent(target);
        if (!replaceExisting && Files.exists(target)) {
            throw new FileAlreadyExistsException(target.toString());
        }
        List<StandardCopyOption> options = new ArrayList<>();
        options.add(StandardCopyOption.ATOMIC_MOVE);
        if (replaceExisting) {
            options.add(StandardCopyOption.REPLACE_EXISTING);
        }
        try {
            Files.move(source, target, options.toArray(StandardCopyOption[]::new));
        } catch (AtomicMoveNotSupportedException ignored) {
            options.remove(StandardCopyOption.ATOMIC_MOVE);
            Files.move(source, target, options.toArray(StandardCopyOption[]::new));
        }
    }

    private static final class LimitedInputStream extends FilterInputStream {
        private long remaining;

        private LimitedInputStream(InputStream delegate, long remaining) {
            super(delegate);
            this.remaining = remaining;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int value = super.read();
            if (value >= 0) {
                remaining--;
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
            }
            return read;
        }
    }
}
