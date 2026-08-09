package com.jdragon.aggregation.datasource.file.transfer;

import com.jdragon.aggregation.datasource.file.FileHelper;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public interface TransferFileSystem extends FileHelper {

    TransferFileEntry stat(String path) throws IOException;

    TransferFilePage listPage(String directory, String cursor, int pageSize) throws IOException;

    InputStream openRead(String path, long offset, long length) throws IOException;

    TransferWriteSession prepareWrite(String temporaryPath, String sessionId) throws IOException;

    long append(TransferWriteSession session, long offset, InputStream source, long length) throws IOException;

    void commit(TransferWriteSession session, String targetPath, boolean replaceExisting) throws IOException;

    void abort(TransferWriteSession session) throws IOException;

    void move(String sourcePath, String targetPath, boolean replaceExisting) throws IOException;

    void delete(String path) throws IOException;

    StorageCapabilities capabilities();

    default String checksum(String path, String algorithm) throws IOException {
        String normalizedAlgorithm = algorithm == null || algorithm.isBlank() ? "SHA-256" : algorithm;
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(normalizedAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("unsupported checksum algorithm: " + normalizedAlgorithm, e);
        }
        TransferFileEntry entry = stat(path);
        if (entry.directory()) {
            throw new IOException("cannot checksum a directory: " + path);
        }
        try (InputStream input = openRead(path, 0, entry.size())) {
            byte[] buffer = new byte[64 * 1024];
            long remaining = entry.size();
            while (remaining > 0) {
                int read = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (read < 0) {
                    throw new IOException("unexpected EOF while checksumming " + path);
                }
                if (read == 0) {
                    continue;
                }
                digest.update(buffer, 0, read);
                remaining -= read;
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    default boolean transferExists(String path) throws IOException {
        try {
            stat(path);
            return true;
        } catch (java.nio.file.NoSuchFileException ignored) {
            return false;
        }
    }
}
