package com.jdragon.aggregation.filetablereader;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.datasource.file.FileHelper;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommonFileTableReaderTest {

    @Test
    public void shouldReadCsvByGlobAndMapColumnsByIndex() {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper()
                .put("/root/dt=20260505/part-1.csv", "id,name,age\n1,Alice,19\n2,Bob,20\n")
                .put("/root/dt=20260505/skip.txt", "id,name,age\n9,Ignored,99\n");
        CapturingRecordSender sender = read(fileHelper, true, config(
                entry("rootPath", "/root"),
                entry("partitionType", "glob"),
                entry("partition", "dt=*/part-*.csv"),
                entry("fileType", "csv"),
                entry("hasHeader", Boolean.TRUE),
                entry("columns", columns(column("age", 2, "LONG"), column("id", 0, "LONG")))));

        assertEquals(2, sender.records.size());
        assertEquals(Long.valueOf(19L), sender.records.get(0).getColumn(0).asLong());
        assertEquals(Long.valueOf(1L), sender.records.get(0).getColumn(1).asLong());
        assertEquals(Long.valueOf(20L), sender.records.get(1).getColumn(0).asLong());
        assertEquals(Long.valueOf(2L), sender.records.get(1).getColumn(1).asLong());
    }

    @Test
    public void shouldResolveDynamicRootAndReadEFileByRegex() {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper()
                .put("/dt=20260505/sample.efile",
                        "<! Entity='demo' dataTime='20260505' type='test' !>\n"
                                + "<T01::Demo planDate='20260505' >\n"
                                + "@ id name\n"
                                + "// ID NAME\n"
                                + "# 7 Carol\n"
                                + "</T01::Demo>\n");
        CapturingRecordSender sender = read(fileHelper, true, config(
                entry("rootPath", "/data/{dyn_timestamp}"),
                entry("partitionType", "regex"),
                entry("partition", "dt=.*/sample\\.efile"),
                entry("fileType", "efile"),
                entry("columns", columns(column("name", 1, "STRING"), column("id", 0, "LONG")))));

        assertEquals(1, sender.records.size());
        assertEquals("Carol", sender.records.get(0).getColumn(0).asString());
        assertEquals(Long.valueOf(7L), sender.records.get(0).getColumn(1).asLong());
        assertTrue("rootPath dynamic function should be resolved before listing",
                Pattern.compile("/data/\\d+").matcher(fileHelper.lastListedDir).matches());
    }

    @Test
    public void shouldReadHierarchicalFtpStyleListingWhenIsFileIsUnavailable() {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper(false, false)
                .put("/root/dt=20260505/part-1.csv", "id,name\n3,Chris\n");
        CapturingRecordSender sender = read(fileHelper, false, config(
                entry("rootPath", "/root"),
                entry("partitionType", "glob"),
                entry("partition", "dt=*/part-*.csv"),
                entry("fileType", "csv"),
                entry("hasHeader", Boolean.TRUE),
                entry("columns", columns(column("id", 0, "LONG"), column("name", 1, "STRING")))));

        assertEquals(1, sender.records.size());
        assertEquals(Long.valueOf(3L), sender.records.get(0).getColumn(0).asLong());
        assertEquals("Chris", sender.records.get(0).getColumn(1).asString());
    }

    @Test
    public void shouldAllowSameSourceColumnToFeedMultipleOutputColumns() {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper(false, false)
                .put("/root/part-1.csv", "id,name\n5,Dana\n");
        CapturingRecordSender sender = read(fileHelper, false, config(
                entry("rootPath", "/root"),
                entry("partitionType", "glob"),
                entry("partition", "part-*.csv"),
                entry("fileType", "csv"),
                entry("hasHeader", Boolean.TRUE),
                entry("columns", columns(column("id", 0, "LONG"), column("id", 0, "STRING")))));

        assertEquals(1, sender.records.size());
        assertEquals(2, sender.records.get(0).getColumnNumber());
        assertEquals(Long.valueOf(5L), sender.records.get(0).getColumn(0).asLong());
        assertEquals("5", sender.records.get(0).getColumn(1).asString());
    }


    private CapturingRecordSender read(InMemoryFileHelper fileHelper,
                                       boolean objectStorageListing,
                                       Configuration configuration) {
        CommonFileTableReader reader = new CommonFileTableReader(fileHelper, objectStorageListing);
        reader.setPluginJobConf(configuration);
        reader.init();
        CapturingRecordSender sender = new CapturingRecordSender();
        reader.startRead(sender);
        reader.destroy();
        return sender;
    }

    private Configuration config(Map.Entry<String, Object>... entries) {
        Configuration configuration = Configuration.newDefault();
        configuration.set("connect", Collections.emptyMap());
        configuration.set("encoding", "UTF-8");
        for (Map.Entry<String, Object> entry : entries) {
            configuration.set(entry.getKey(), entry.getValue());
        }
        return configuration;
    }

    private Map.Entry<String, Object> entry(String key, Object value) {
        return new java.util.AbstractMap.SimpleEntry<String, Object>(key, value);
    }

    private List<Map<String, Object>> columns(Map<String, Object>... columns) {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        Collections.addAll(result, columns);
        return result;
    }

    private Map<String, Object> column(String name, int index, String type) {
        Map<String, Object> column = new LinkedHashMap<String, Object>();
        column.put("name", name);
        column.put("index", Integer.valueOf(index));
        column.put("type", type);
        return column;
    }

    private static final class CapturingRecordSender implements RecordSender {
        private final List<Record> records = new ArrayList<Record>();

        @Override
        public Record createRecord() {
            return new DefaultRecord();
        }

        @Override
        public void sendToWriter(Record record) {
            records.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void terminate() {
        }

        @Override
        public void shutdown() {
        }
    }

    private static final class InMemoryFileHelper implements FileHelper {
        private final Map<String, byte[]> files = new LinkedHashMap<String, byte[]>();
        private final boolean objectStorageListing;
        private final boolean reliableIsFile;
        private String lastListedDir;

        private InMemoryFileHelper() {
            this(true, true);
        }

        private InMemoryFileHelper(boolean objectStorageListing, boolean reliableIsFile) {
            this.objectStorageListing = objectStorageListing;
            this.reliableIsFile = reliableIsFile;
        }

        private InMemoryFileHelper put(String path, String content) {
            files.put(normalize(path), content.getBytes(StandardCharsets.UTF_8));
            return this;
        }

        @Override
        public boolean connect(Configuration configuration) {
            return true;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public boolean exists(String path, String name) {
            return files.containsKey(join(path, name));
        }

        @Override
        public Set<String> listFile(String dir, String regex) {
            this.lastListedDir = normalize(dir);
            if (!objectStorageListing) {
                return listHierarchicalFile(this.lastListedDir, regex);
            }
            String normalizedDir = trimTrailingSlash(this.lastListedDir);
            String normalizedRegex = regex == null || regex.trim().isEmpty() ? ".*" : regex;
            Set<String> result = new LinkedHashSet<String>();
            for (String path : files.keySet()) {
                String normalizedPath = normalize(path);
                String normalizedPathWithoutSlash = stripLeadingSlash(normalizedPath);
                if (normalizedPathWithoutSlash.matches(normalizedRegex)
                        || normalizedPath.matches(normalizedRegex)
                        || ".*".equals(normalizedRegex)) {
                    result.add(normalizedPath);
                }
            }
            return result;
        }

        @Override
        public boolean isFile(String dir, String fileName) {
            if (!reliableIsFile) {
                return false;
            }
            return files.containsKey(join(dir, fileName));
        }

        @Override
        public void mkdir(String filePath) {
        }

        @Override
        public void rm(String path) {
            files.remove(normalize(path));
        }

        @Override
        public boolean mv(String from, String to) {
            byte[] content = files.remove(normalize(from));
            if (content == null) {
                return false;
            }
            files.put(normalize(to), content);
            return true;
        }

        @Override
        public InputStream getInputStream(String path, String name) throws IOException {
            String fullPath = join(path, name);
            byte[] content = files.get(fullPath);
            if (content == null) {
                content = files.get("/" + fullPath);
            }
            if (content == null) {
                throw new IOException("Missing file: " + fullPath);
            }
            return new ByteArrayInputStream(content);
        }

        @Override
        public OutputStream getOutputStream(String path, String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }

        private String join(String path, String name) {
            String normalizedPath = trimTrailingSlash(normalize(path));
            String normalizedName = stripLeadingSlash(normalize(name));
            if (normalizedPath.isEmpty()) {
                return "/" + normalizedName;
            }
            return normalize(normalizedPath + "/" + normalizedName);
        }

        private Set<String> listHierarchicalFile(String dir, String regex) {
            String normalizedDir = trimTrailingSlash(normalize(dir));
            String normalizedRegex = regex == null || regex.trim().isEmpty() ? ".*" : regex;
            Set<String> result = new LinkedHashSet<String>();
            if (files.containsKey(normalizedDir)) {
                String name = lastName(normalizedDir);
                if (name.matches(normalizedRegex)) {
                    result.add(name);
                }
                return result;
            }
            String prefix = normalizedDir.endsWith("/") ? normalizedDir : normalizedDir + "/";
            for (String path : files.keySet()) {
                String normalizedPath = normalize(path);
                if (!normalizedPath.startsWith(prefix)) {
                    continue;
                }
                String relativePath = normalizedPath.substring(prefix.length());
                if (relativePath.isEmpty()) {
                    continue;
                }
                String childName = relativePath.contains("/")
                        ? relativePath.substring(0, relativePath.indexOf('/'))
                        : relativePath;
                if (childName.matches(normalizedRegex)) {
                    result.add(childName);
                }
            }
            return result;
        }

        private String lastName(String path) {
            String normalized = trimTrailingSlash(normalize(path));
            int slash = normalized.lastIndexOf('/');
            return slash >= 0 ? normalized.substring(slash + 1) : normalized;
        }

        private String normalize(String path) {
            if (path == null) {
                return "";
            }
            String normalized = path.trim().replace('\\', '/');
            while (normalized.contains("//")) {
                normalized = normalized.replace("//", "/");
            }
            return normalized;
        }

        private String trimTrailingSlash(String value) {
            String result = normalize(value);
            while (result.endsWith("/") && result.length() > 1) {
                result = result.substring(0, result.length() - 1);
            }
            return result;
        }

        private String stripLeadingSlash(String value) {
            String result = normalize(value);
            while (result.startsWith("/")) {
                result = result.substring(1);
            }
            return result;
        }
    }
}
