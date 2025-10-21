package com.jdragon.aggregation.datasource.file;

import com.jdragon.aggregation.commons.util.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @Author JDragon
 * @Date 2022.05.05 上午 11:39
 * @Email 1061917196@qq.com
 * @Des:
 */
public interface FileHelper extends AutoCloseable {
    /**
     * 格式路径
     */
    default String processingPath(String path, String fileName) {
        boolean end = path.endsWith("/");
        boolean start = fileName.startsWith("/");
        if (start && end) {
            return path + fileName.substring(1);
        } else if (start || end) {
            return path + fileName;
        } else {
            return path + "/" + fileName;
        }
    }

    default String processingPath(String path, String... fileNames) {
        for (String fileName : fileNames) {
            path = processingPath(path, fileName);
        }
        return path;
    }

    boolean exists(String path, String name) throws IOException;

    Set<String> listFile(String dir, String regex) throws IOException;

    boolean isFile(String dir, String fileName) throws IOException;

    void mkdir(String filePath) throws IOException;

    void rm(String path) throws IOException;

    boolean connect(Configuration configuration);

    boolean isConnected();

    boolean mv(String from, String to) throws Exception;

    InputStream getInputStream(String path, String name) throws IOException;

    OutputStream getOutputStream(String path, String name) throws IOException;

    default void fresh() {
    }

    default void readFile(String absPath, String fileType, Consumer<Map<String, Object>> row) throws IOException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    default void testWrite(String absPath, String jdbcUrl, String table, Properties connectionProperties, Map<String, String> option) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
