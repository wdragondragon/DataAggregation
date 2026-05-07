package com.jdragon.aggregation.filetablereader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.datasource.file.utils.FileParser;
import com.jdragon.aggregation.unstructuredstorage.ValidateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Slf4j
public class CommonFileTableReader extends Reader.Job {

    private static final int MAX_SCAN_DEPTH = 64;

    private final FileHelper fileHelper;
    private final boolean objectStorageListing;

    private Configuration readerSliceConfig;
    private String rootPath;
    private String partition;
    private String partitionType;
    private FilePartitionMatcher partitionMatcher;
    private String fileType;
    private String encoding;
    private List<FileColumnEntry> columns;

    public CommonFileTableReader(FileHelper fileHelper) {
        this(fileHelper, false);
    }

    public CommonFileTableReader(FileHelper fileHelper, boolean objectStorageListing) {
        this.fileHelper = fileHelper;
        this.objectStorageListing = objectStorageListing;
    }

    @Override
    public void init() {
        this.readerSliceConfig = getPluginJobConf();
        Configuration connectConfig = readerSliceConfig.getConfiguration("connect");
        if (!fileHelper.connect(connectConfig == null ? Configuration.newDefault() : connectConfig)) {
            throw new IllegalStateException("File source connect failed.");
        }
        this.rootPath = normalizePath(resolveDynamicText(readerSliceConfig.getString("rootPath", "/")));
        this.partition = resolveDynamicText(readerSliceConfig.getString("partition", "*"));
        this.partitionType = readerSliceConfig.getString("partitionType", "glob");
        this.partitionMatcher = FilePartitionMatcher.compile(partitionType, partition);
        this.fileType = normalizeFileType(readerSliceConfig.getString("fileType", "csv"));
        this.encoding = readerSliceConfig.getString("encoding", "UTF-8");
        this.columns = loadColumns(readerSliceConfig);
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("File table reader columns are required.");
        }
        log.info("file table reader initialized, rootPath={}, partitionType={}, partition={}, fileType={}, columns={}",
                rootPath, partitionType, partition, fileType, columns.size());
    }

    @Override
    public void startRead(RecordSender recordSender) {
        List<FileRef> files = resolveMatchedFiles();
        log.info("file table reader matched {} file(s)", files.size());
        for (FileRef file : files) {
            readFile(file, recordSender);
        }
    }

    @Override
    public void destroy() {
        try {
            fileHelper.close();
        } catch (Exception e) {
            log.warn("close file helper failed: {}", e.getMessage(), e);
        }
    }

    private List<FileRef> resolveMatchedFiles() {
        try {
            List<FileRef> result = objectStorageListing ? listObjectStorageFiles() : listHierarchicalFiles();
            Collections.sort(result);
            return result;
        } catch (IOException e) {
            throw new RuntimeException("List files failed: " + e.getMessage(), e);
        }
    }

    private List<FileRef> listObjectStorageFiles() throws IOException {
        String root = stripLeadingSlash(trimTrailingSlash(rootPath));
        Set<String> listedFiles = fileHelper.listFile(rootPath, ".*");
        List<FileRef> result = new ArrayList<>();
        for (String listedFile : listedFiles) {
            String objectPath = stripLeadingSlash(normalizePath(listedFile));
            if (objectPath.isEmpty() || objectPath.endsWith("/")) {
                continue;
            }
            String relativePath = toRelativeObjectPath(objectPath, root);
            if (matchesPartition(relativePath)) {
                result.add(new FileRef(objectPath, relativePath));
            }
        }
        return result;
    }

    private List<FileRef> listHierarchicalFiles() throws IOException {
        List<FileRef> result = new ArrayList<>();
        walkDirectory(rootPath, "", null, 0, result);
        return result;
    }

    private void walkDirectory(String currentDir,
                               String relativePrefix,
                               Set<String> knownChildren,
                               int depth,
                               List<FileRef> result) throws IOException {
        if (depth > MAX_SCAN_DEPTH) {
            throw new IOException("File scan depth exceeded " + MAX_SCAN_DEPTH + " at " + currentDir);
        }
        Set<String> children = knownChildren == null ? fileHelper.listFile(currentDir, ".*") : knownChildren;
        List<String> sortedChildren = new ArrayList<>(children);
        Collections.sort(sortedChildren);
        for (String child : sortedChildren) {
            if (StringUtils.isBlank(child) || ".".equals(child) || "..".equals(child)) {
                continue;
            }
            String childName = normalizePath(child);
            String relativePath = joinPath(relativePrefix, childName);
            String absolutePath = joinPath(currentDir, childName);
            boolean fileLike = isFileSafe(currentDir, childName);
            Set<String> nestedChildren = tryListDirectory(absolutePath);
            if (isDirectoryListing(childName, nestedChildren, fileLike)) {
                walkDirectory(absolutePath, relativePath, nestedChildren, depth + 1, result);
            } else if (isReadableFileListing(childName, nestedChildren, fileLike) && matchesPartition(relativePath)) {
                result.add(new FileRef(absolutePath, relativePath));
            }
        }
    }

    private boolean isFileSafe(String dir, String fileName) {
        try {
            return fileHelper.isFile(dir, fileName);
        } catch (Exception ignored) {
            return false;
        }
    }

    private Set<String> tryListDirectory(String dir) {
        try {
            return fileHelper.listFile(dir, ".*");
        } catch (Exception ignored) {
            return null;
        }
    }

    private boolean isDirectoryListing(String childName, Set<String> nestedChildren, boolean fileLike) {
        if (nestedChildren == null) {
            return false;
        }
        if (nestedChildren.isEmpty()) {
            return false;
        }
        if (nestedChildren.size() == 1 && nestedChildren.contains(childName)) {
            return false;
        }
        return true;
    }

    private boolean isReadableFileListing(String childName, Set<String> nestedChildren, boolean fileLike) {
        if (fileLike || nestedChildren == null) {
            return true;
        }
        return nestedChildren.size() == 1 && nestedChildren.contains(childName);
    }

    private boolean matchesPartition(String relativePath) {
        return partitionMatcher.matches(relativePath);
    }

    private void readFile(FileRef file, RecordSender recordSender) {
        log.info("start read file: {}", file.absolutePath);
        try (InputStream inputStream = openInputStream(file.absolutePath)) {
            FileParser.parseInputStream(inputStream, FileParser.FileFormat.fromString(fileType), encoding,
                    row -> sendRow(row, recordSender), readerSliceConfig);
        } catch (Exception e) {
            throw new RuntimeException("Read file failed: " + file.absolutePath + ", " + e.getMessage(), e);
        } finally {
            fileHelper.fresh();
        }
    }

    private InputStream openInputStream(String absolutePath) throws IOException {
        String normalized = normalizePath(absolutePath);
        int lastSlash = normalized.lastIndexOf('/');
        String path = lastSlash >= 0 ? normalized.substring(0, lastSlash) : "/";
        String name = lastSlash >= 0 ? normalized.substring(lastSlash + 1) : normalized;
        if (StringUtils.isBlank(path)) {
            path = "/";
        }
        return fileHelper.getInputStream(path, name);
    }

    private void sendRow(Map<String, Object> row, RecordSender recordSender) {
        List<Object> values = new ArrayList<>(row.values());
        Record record = recordSender.createRecord();
        for (FileColumnEntry columnEntry : columns) {
            Object value = row.containsKey(columnEntry.name)
                    ? row.get(columnEntry.name)
                    : columnEntry.index >= 0 && columnEntry.index < values.size() ? values.get(columnEntry.index) : null;
            Column column = ValidateUtil.validate(columnEntry.name, columnEntry.type, value);
            record.addColumn(column);
        }
        recordSender.sendToWriter(record);
    }

    private List<FileColumnEntry> loadColumns(Configuration configuration) {
        List<Object> configuredColumns = configuration.getList("columns", Object.class);
        if (configuredColumns == null || configuredColumns.isEmpty()) {
            return Collections.emptyList();
        }
        List<FileColumnEntry> result = new ArrayList<>();
        for (int i = 0; i < configuredColumns.size(); i++) {
            Map<String, Object> item = toColumnMap(configuredColumns.get(i));
            String name = stringValue(item.get("name"));
            if (StringUtils.isBlank(name)) {
                continue;
            }
            Integer configuredIndex = integerValue(item.get("index"));
            int index = configuredIndex == null ? i : configuredIndex;
            String type = normalizeColumnType(stringValue(item.get("type")));
            result.add(new FileColumnEntry(name, index, type));
        }
        return result;
    }

    private Map<String, Object> toColumnMap(Object value) {
        if (value instanceof JSONObject) {
            JSONObject object = (JSONObject) value;
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : object.entrySet()) {
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }
        if (value instanceof Map<?, ?>) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (entry.getKey() != null) {
                    result.put(String.valueOf(entry.getKey()), entry.getValue());
                }
            }
            return result;
        }
        return Collections.emptyMap();
    }

    private String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private Integer integerValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value == null || StringUtils.isBlank(String.valueOf(value))) {
            return null;
        }
        return Integer.valueOf(String.valueOf(value).trim());
    }

    private String resolveDynamicText(String value) {
        return QualityDynamicFunctionResolver.replaceAll(value);
    }

    private String normalizeFileType(String value) {
        String normalized = StringUtils.defaultIfBlank(value, "csv").trim().toLowerCase(Locale.ENGLISH);
        if ("e".equals(normalized) || "efile".equals(normalized)) {
            return "efile";
        }
        if ("csv".equals(normalized)) {
            return "csv";
        }
        throw new IllegalArgumentException("Unsupported fileType: " + value + ". Only csv and efile are supported.");
    }

    private String normalizeColumnType(String value) {
        String normalized = StringUtils.defaultIfBlank(value, "STRING").trim().toUpperCase(Locale.ENGLISH);
        if (normalized.contains("CHAR") || "STRING".equals(normalized) || "TEXT".equals(normalized)) {
            return "STRING";
        }
        if ("INT".equals(normalized) || "INTEGER".equals(normalized) || "BIGINT".equals(normalized)
                || "LONG".equals(normalized) || "NUMBER".equals(normalized) || "TIMESTAMP".equals(normalized)) {
            return "LONG";
        }
        if ("DECIMAL".equals(normalized) || "DOUBLE".equals(normalized) || "FLOAT".equals(normalized)) {
            return "DOUBLE";
        }
        if ("BOOL".equals(normalized) || "BOOLEAN".equals(normalized)) {
            return "BOOLEAN";
        }
        if ("DATE".equals(normalized) || "DATETIME".equals(normalized)) {
            return normalized;
        }
        return "STRING";
    }

    private String normalizePath(String value) {
        if (value == null) {
            return "";
        }
        String normalized = value.trim().replace('\\', '/');
        while (normalized.contains("//")) {
            normalized = normalized.replace("//", "/");
        }
        return normalized;
    }

    private String joinPath(String left, String right) {
        String normalizedLeft = normalizePath(left);
        String normalizedRight = normalizePath(right);
        if (StringUtils.isBlank(normalizedLeft)) {
            return normalizedRight;
        }
        if (StringUtils.isBlank(normalizedRight)) {
            return normalizedLeft;
        }
        boolean leftEnds = normalizedLeft.endsWith("/");
        boolean rightStarts = normalizedRight.startsWith("/");
        if (leftEnds && rightStarts) {
            return normalizedLeft + normalizedRight.substring(1);
        }
        if (leftEnds || rightStarts) {
            return normalizedLeft + normalizedRight;
        }
        return normalizedLeft + "/" + normalizedRight;
    }

    private String stripLeadingSlash(String value) {
        String result = normalizePath(value);
        while (result.startsWith("/")) {
            result = result.substring(1);
        }
        return result;
    }

    private String trimTrailingSlash(String value) {
        String result = normalizePath(value);
        while (result.endsWith("/") && result.length() > 1) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private String toRelativeObjectPath(String objectPath, String root) {
        if (StringUtils.isBlank(root)) {
            return objectPath;
        }
        String normalizedRoot = trimTrailingSlash(stripLeadingSlash(root));
        if (objectPath.equals(normalizedRoot)) {
            return "";
        }
        String prefix = normalizedRoot + "/";
        return objectPath.startsWith(prefix) ? objectPath.substring(prefix.length()) : objectPath;
    }

    private static final class FileColumnEntry {
        private final String name;
        private final int index;
        private final String type;

        private FileColumnEntry(String name, int index, String type) {
            this.name = name;
            this.index = index;
            this.type = type;
        }

        @Override
        public String toString() {
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("name", name);
            value.put("index", index);
            value.put("type", type);
            return JSON.toJSONString(value);
        }
    }

    private static final class FileRef implements Comparable<FileRef> {
        private final String absolutePath;
        private final String relativePath;

        private FileRef(String absolutePath, String relativePath) {
            this.absolutePath = absolutePath;
            this.relativePath = relativePath;
        }

        @Override
        public int compareTo(FileRef other) {
            return this.relativePath.compareTo(other.relativePath);
        }
    }
}
