package com.jdragon.aggregation.core.streaming;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.util.LRUMap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple append-only spill-backed list.
 */
public class AppendOnlySpillList<T> extends AbstractList<T> implements AutoCloseable {

    private final ObjectMapper objectMapper;
    private final JavaType itemType;
    private final RandomAccessFile file;
    private final Path filePath;
    private final List<Long> offsets = new ArrayList<>();

    public AppendOnlySpillList(String prefix, Type itemType) {
        this(prefix, itemType, null);
    }

    public AppendOnlySpillList(String prefix, Type itemType, String baseDirectory) {
        TypeFactory typeFactory = TypeFactory.defaultInstance()
                .withCache(new LRUMap<Object, JavaType>(16, 200))
                .withClassLoader(resolveClassLoader(itemType));
        this.objectMapper = new ObjectMapper().setTypeFactory(typeFactory);
        this.itemType = typeFactory.constructType(itemType);
        try {
            Path directory;
            if (baseDirectory == null || baseDirectory.trim().isEmpty()) {
                directory = Paths.get(System.getProperty("java.io.tmpdir"), "aggregation-stream");
            } else {
                directory = Paths.get(baseDirectory);
            }
            Files.createDirectories(directory);
            this.filePath = Files.createTempFile(directory, prefix + "-", ".spill");
            File tempFile = filePath.toFile();
            tempFile.deleteOnExit();
            this.file = new RandomAccessFile(tempFile, "rw");
        } catch (IOException e) {
            throw new RuntimeException("Failed to create spill list", e);
        }
    }

    @Override
    public synchronized T get(int index) {
        if (index < 0 || index >= offsets.size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", size: " + offsets.size());
        }
        try {
            file.seek(offsets.get(index));
            int length = file.readInt();
            byte[] payload = new byte[length];
            file.readFully(payload);
            return objectMapper.readValue(payload, itemType);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read spill element", e);
        }
    }

    @Override
    public synchronized boolean add(T element) {
        try {
            byte[] payload = objectMapper.writeValueAsBytes(element);
            long offset = file.length();
            file.seek(offset);
            file.writeInt(payload.length);
            file.write(payload);
            offsets.add(offset);
            return true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to append spill element", e);
        }
    }

    @Override
    public synchronized int size() {
        return offsets.size();
    }

    public Path getFilePath() {
        return filePath;
    }

    private static ClassLoader resolveClassLoader(Type type) {
        if (type instanceof Class<?>) {
            return ((Class<?>) type).getClassLoader();
        }
        if (type instanceof ParameterizedType) {
            return resolveClassLoader(((ParameterizedType) type).getRawType());
        }
        return Thread.currentThread().getContextClassLoader();
    }

    @Override
    public synchronized void close() {
        try {
            file.close();
        } catch (IOException ignored) {
        }
    }
}
