package com.jdragon.aggregation.core.streaming;

import lombok.Data;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class PartitionedSpillStore implements AutoCloseable {

    private static final OpenOption[] APPEND_OPTIONS = new OpenOption[]{
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND,
            StandardOpenOption.WRITE
    };

    private final Path workingDirectory;
    private final int partitionCount;
    private final boolean keepTempFiles;
    private final SpillGuard spillGuard;
    private final BufferedWriter[] writers;
    private final Object[] locks;

    public PartitionedSpillStore(String jobId, String spillPath, int partitionCount, boolean keepTempFiles) {
        this(jobId, spillPath, partitionCount, keepTempFiles, null);
    }

    public PartitionedSpillStore(String jobId,
                                 String spillPath,
                                 int partitionCount,
                                 boolean keepTempFiles,
                                 SpillGuard spillGuard) {
        this.partitionCount = Math.max(1, partitionCount);
        this.keepTempFiles = keepTempFiles;
        this.spillGuard = spillGuard;
        this.workingDirectory = initDirectory(jobId, spillPath);
        this.writers = new BufferedWriter[this.partitionCount];
        this.locks = new Object[this.partitionCount];
        for (int i = 0; i < this.partitionCount; i++) {
            this.locks[i] = new Object();
        }
    }

    private Path initDirectory(String jobId, String spillPath) {
        try {
            Path baseDir;
            if (spillPath == null || spillPath.trim().isEmpty()) {
                baseDir = Paths.get(System.getProperty("java.io.tmpdir"), "aggregation-stream");
            } else {
                baseDir = Paths.get(spillPath);
            }
            Files.createDirectories(baseDir);
            Path directory = Files.createTempDirectory(baseDir, jobId + "-");
            directory.toFile().deleteOnExit();
            return directory;
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize spill directory", e);
        }
    }

    public void append(String sourceId, CompositeKey key, Map<String, Object> row) {
        PartitionRow partitionRow = new PartitionRow();
        partitionRow.setSourceId(sourceId);
        partitionRow.setKey(key.asString());
        partitionRow.setRow(row != null ? new HashMap<>(row) : new HashMap<String, Object>());
        append(partitionRow);
    }

    public void append(PartitionRow row) {
        int partition = resolvePartition(row.getKey());
        synchronized (locks[partition]) {
            try {
                String encoded = RowCodec.encode(row);
                reserveSpill(encoded);
                BufferedWriter writer = writer(partition);
                writer.write(encoded);
                writer.newLine();
            } catch (IOException e) {
                throw new RuntimeException("Failed to append partition row", e);
            }
        }
    }

    private BufferedWriter writer(int partition) throws IOException {
        BufferedWriter writer = writers[partition];
        if (writer == null) {
            writer = Files.newBufferedWriter(getPartitionPath(partition), StandardCharsets.UTF_8, APPEND_OPTIONS);
            writers[partition] = writer;
        }
        return writer;
    }

    public int resolvePartition(String key) {
        return Math.abs(key != null ? key.hashCode() : 0) % partitionCount;
    }

    public Path getPartitionPath(int partition) {
        return workingDirectory.resolve(String.format("partition-%03d.jsonl", partition));
    }

    public boolean partitionExists(int partition) {
        return Files.exists(getPartitionPath(partition));
    }

    public PartitionReader openPartitionReader(int partition) throws IOException {
        return new PartitionReader(getPartitionPath(partition));
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public Path getWorkingDirectory() {
        return workingDirectory;
    }

    public SpillGuard getSpillGuard() {
        return spillGuard;
    }

    @Override
    public void close() {
        for (BufferedWriter writer : writers) {
            if (writer != null) {
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    public void cleanup() {
        close();
        if (keepTempFiles) {
            return;
        }
        deleteRecursively(workingDirectory, spillGuard);
    }

    public static void cleanupConsumedPartition(Path partitionPath, boolean keepTempFiles, SpillGuard spillGuard) {
        if (keepTempFiles || partitionPath == null || !Files.exists(partitionPath)) {
            return;
        }
        long fileBytes = fileSize(partitionPath);
        try {
            if (Files.deleteIfExists(partitionPath) && spillGuard != null && fileBytes > 0L) {
                spillGuard.release(fileBytes);
            }
        } catch (IOException ignored) {
        }
    }

    private static void deleteRecursively(Path path, SpillGuard spillGuard) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try {
            if (Files.isDirectory(path)) {
                try (Stream<Path> children = Files.list(path)) {
                    children.forEach(child -> deleteRecursively(child, spillGuard));
                }
            }
            long fileBytes = Files.isRegularFile(path) ? fileSize(path) : 0L;
            if (Files.deleteIfExists(path) && spillGuard != null && fileBytes > 0L) {
                spillGuard.release(fileBytes);
            }
        } catch (IOException ignored) {
        }
    }

    private void reserveSpill(String encoded) {
        if (spillGuard == null) {
            return;
        }
        long bytes = encoded.getBytes(StandardCharsets.UTF_8).length
                + System.lineSeparator().getBytes(StandardCharsets.UTF_8).length;
        spillGuard.reserve(workingDirectory, bytes);
    }

    private static long fileSize(Path path) {
        try {
            return Files.exists(path) ? Files.size(path) : 0L;
        } catch (IOException ignored) {
            return 0L;
        }
    }

    @Data
    public static class PartitionRow {
        private String sourceId;
        private String key;
        private Map<String, Object> row = new HashMap<>();
    }
}
