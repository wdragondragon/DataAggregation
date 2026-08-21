package com.jdragon.aggregation.filetablewriter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.datasource.file.FileHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommonFileTableWriterTest {

    @Test
    public void shouldWriteCsvWithHeaderAndColumnOrder() {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper();
        write(fileHelper, config(
                entry("rootPath", "/out"),
                entry("fileName", "sample.csv"),
                entry("fileType", "csv"),
                entry("columns", columns(column("name", 1, "STRING"), column("id", 0, "LONG")))),
                records(record("1", "Alice"), record("2", "Bob")));

        String content = normalizeNewLine(fileHelper.text("/out/sample.csv"));
        assertEquals("name,id\nAlice,1\nBob,2\n", content);
    }

    @Test
    public void shouldAppendCsvWithoutDuplicatingHeader() {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper()
                .put("/out/sample.csv", "id,name\n0,Old\n");
        write(fileHelper, config(
                entry("rootPath", "/out"),
                entry("fileName", "sample.csv"),
                entry("fileType", "csv"),
                entry("writeMode", "append"),
                entry("columns", columns(column("id", 0, "LONG"), column("name", 1, "STRING")))),
                records(record("1", "Alice")));

        String content = normalizeNewLine(fileHelper.text("/out/sample.csv"));
        assertEquals("id,name\n0,Old\n1,Alice\n", content);
    }

    @Test
    public void shouldAppendCsvWithLineBreakWhenExistingFileHasNoTrailingNewLine() {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper()
                .put("/out/sample.csv", "id,name\n0,Old");
        write(fileHelper, config(
                entry("rootPath", "/out"),
                entry("fileName", "sample.csv"),
                entry("fileType", "csv"),
                entry("writeMode", "append"),
                entry("columns", columns(column("id", 0, "LONG"), column("name", 1, "STRING")))),
                records(record("1", "Alice")));

        String content = normalizeNewLine(fileHelper.text("/out/sample.csv"));
        assertEquals("id,name\n0,Old\n1,Alice\n", content);
    }

    @Test
    public void shouldWriteJsonArrayAndJsonl() {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper();
        write(fileHelper, config(
                entry("rootPath", "/out"),
                entry("fileName", "sample.json"),
                entry("fileType", "json"),
                entry("columns", columns(column("id", 0, "LONG"), column("name", 1, "STRING")))),
                records(record("1", "Alice"), record("2", "Bob")));

        JSONArray array = JSON.parseArray(fileHelper.text("/out/sample.json"));
        assertEquals(2, array.size());
        assertEquals(Integer.valueOf(1), Integer.valueOf(array.getJSONObject(0).getIntValue("id")));
        assertEquals("Alice", array.getJSONObject(0).getString("name"));

        write(fileHelper, config(
                entry("rootPath", "/out"),
                entry("fileName", "sample.jsonl"),
                entry("fileType", "jsonl"),
                entry("columns", columns(column("id", 0, "LONG"), column("name", 1, "STRING")))),
                records(record("1", "Alice"), record("2", "Bob")));
        String jsonl = normalizeNewLine(fileHelper.text("/out/sample.jsonl"));
        assertEquals("{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}\n", jsonl);
    }

    @Test
    public void shouldWriteEFileAndUseTagFieldsAsEntityTags() {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper();
        write(fileHelper, config(
                entry("rootPath", "/out/$getCurrentTime(yyyyMMdd,0)"),
                entry("fileName", "sample.efile"),
                entry("fileType", "efile"),
                entry("efile", efileOptions()),
                entry("columns", columns(
                        columnWithRemarks("id", 0, "LONG", "Primary key"),
                        columnWithRemarks("name", 1, "STRING", "User name"),
                        tagColumn("areaCode", 2, "STRING")))),
                records(record("1", "Alice", "GD"), record("2", "Bob", "GD")));

        String path = fileHelper.singlePath();
        assertTrue(Pattern.compile("/out/\\d{8}/sample\\.efile").matcher(path).matches());
        String content = normalizeNewLine(fileHelper.text(path));
        assertTrue(content.contains("<! Entity='demo' dataTime='20260507_00:00:00' type='test' !>"));
        assertTrue(content.contains("<T01::Demo planDate='20260507' areaCode='GD' >"));
        assertEquals(1, countOccurrences(content, "planDate="));
        assertTrue(content.contains("@ id name\n"));
        assertTrue(content.contains("// 'Primary key' 'User name'\n"));
        assertFalse(content.contains("@ id name areaCode"));
        assertTrue(content.contains("# 1 Alice\n# 2 Bob\n"));
    }

    @Test
    public void shouldUseTagPlanDateWhenModelPlanDateIsMissing() {
        Map<String, Object> efile = efileOptions();
        efile.remove("planDate");
        InMemoryFileHelper fileHelper = new InMemoryFileHelper();

        write(fileHelper, config(
                entry("rootPath", "/out"),
                entry("fileName", "tag-plan-date.efile"),
                entry("fileType", "efile"),
                entry("efile", efile),
                entry("columns", columns(
                        column("id", 0, "LONG"),
                        tagColumn("planDate", 1, "STRING")))),
                records(record("1", "20260820"), record("2", "20260820")));

        String content = normalizeNewLine(fileHelper.text("/out/tag-plan-date.efile"));
        assertTrue(content.contains("<T01::Demo planDate='20260820' >"));
        assertEquals(1, countOccurrences(content, "planDate="));
        assertFalse(content.contains("@ id planDate"));
    }

    @Test
    public void shouldWritePlanDateOnceWhenModelAndTagValuesMatch() {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper();

        write(fileHelper, config(
                entry("rootPath", "/out"),
                entry("fileName", "matching-plan-date.efile"),
                entry("fileType", "efile"),
                entry("efile", efileOptions()),
                entry("columns", columns(
                        column("id", 0, "LONG"),
                        tagColumn("planDate", 1, "STRING")))),
                records(record("1", " 20260507 ")));

        String content = normalizeNewLine(fileHelper.text("/out/matching-plan-date.efile"));
        assertTrue(content.contains("<T01::Demo planDate='20260507' >"));
        assertEquals(1, countOccurrences(content, "planDate="));
    }

    @Test
    public void shouldRejectConflictingModelAndTagPlanDate() {
        try {
            write(new InMemoryFileHelper(), config(
                    entry("rootPath", "/out"),
                    entry("fileName", "conflicting-plan-date.efile"),
                    entry("fileType", "efile"),
                    entry("efile", efileOptions()),
                    entry("columns", columns(
                            column("id", 0, "LONG"),
                            tagColumn("planDate", 1, "STRING")))),
                    records(record("1", "20260819")));
            fail("Expected conflicting planDate values to fail");
        } catch (IllegalArgumentException e) {
            assertEquals("EFile planDate conflicts between model metadata and TAG field: model=20260507, tag=20260819",
                    e.getMessage());
        }
    }

    @Test
    public void shouldOmitPlanDateWhenModelAndTagValuesAreBlank() {
        Map<String, Object> efile = efileOptions();
        efile.put("planDate", "   ");
        InMemoryFileHelper fileHelper = new InMemoryFileHelper();

        write(fileHelper, config(
                entry("rootPath", "/out"),
                entry("fileName", "blank-plan-date.efile"),
                entry("fileType", "efile"),
                entry("efile", efile),
                entry("columns", columns(
                        column("id", 0, "LONG"),
                        tagColumn("planDate", 1, "STRING")))),
                records(record("1", "")));

        String content = normalizeNewLine(fileHelper.text("/out/blank-plan-date.efile"));
        assertTrue(content.contains("<T01::Demo >"));
        assertFalse(content.contains("planDate="));
    }

    @Test
    public void shouldRejectInconsistentPlanDateTagValues() {
        Map<String, Object> efile = efileOptions();
        efile.remove("planDate");
        try {
            write(new InMemoryFileHelper(), config(
                    entry("rootPath", "/out"),
                    entry("fileName", "inconsistent-plan-date.efile"),
                    entry("fileType", "efile"),
                    entry("efile", efile),
                    entry("columns", columns(
                            column("id", 0, "LONG"),
                            tagColumn("planDate", 1, "STRING")))),
                    records(record("1", "20260820"), record("2", "20260821")));
            fail("Expected inconsistent planDate TAG values to fail");
        } catch (IllegalArgumentException e) {
            assertEquals("EFile tag column has inconsistent values: planDate", e.getMessage());
        }
    }

    @Test
    public void shouldFallBackAfterDynamicModelPlanDateResolvesToBlank() {
        Map<String, Object> efile = efileOptions();
        efile.put("planDate", "$unknown()");
        InMemoryFileHelper withTag = new InMemoryFileHelper();

        write(withTag, config(
                entry("rootPath", "/out"),
                entry("fileName", "dynamic-tag-plan-date.efile"),
                entry("fileType", "efile"),
                entry("efile", efile),
                entry("columns", columns(
                        column("id", 0, "LONG"),
                        tagColumn("planDate", 1, "STRING")))),
                records(record("1", "20260820")));

        String withTagContent = normalizeNewLine(withTag.text("/out/dynamic-tag-plan-date.efile"));
        assertTrue(withTagContent.contains("<T01::Demo planDate='20260820' >"));
        assertEquals(1, countOccurrences(withTagContent, "planDate="));

        InMemoryFileHelper withoutTag = new InMemoryFileHelper();
        write(withoutTag, config(
                entry("rootPath", "/out"),
                entry("fileName", "dynamic-blank-plan-date.efile"),
                entry("fileType", "efile"),
                entry("efile", efile),
                entry("columns", columns(column("id", 0, "LONG")))),
                records(record("1")));

        String withoutTagContent = normalizeNewLine(withoutTag.text("/out/dynamic-blank-plan-date.efile"));
        assertTrue(withoutTagContent.contains("<T01::Demo >"));
        assertFalse(withoutTagContent.contains("planDate="));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectInconsistentEFileTagValues() {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper();
        write(fileHelper, config(
                entry("rootPath", "/out"),
                entry("fileName", "sample.efile"),
                entry("fileType", "efile"),
                entry("efile", efileOptions()),
                entry("columns", columns(
                        column("id", 0, "LONG"),
                        tagColumn("areaCode", 1, "STRING")))),
                records(record("1", "GD"), record("2", "GX")));
    }

    @Test
    public void shouldWriteExcelXlsx() throws Exception {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper();
        write(fileHelper, config(
                entry("rootPath", "/out"),
                entry("fileName", "sample.xlsx"),
                entry("fileType", "excel"),
                entry("sheetName", "Data"),
                entry("columns", columns(column("id", 0, "LONG"), column("name", 1, "STRING")))),
                records(record("1", "Alice")));

        try (XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(fileHelper.bytes("/out/sample.xlsx")))) {
            Sheet sheet = workbook.getSheet("Data");
            Row header = sheet.getRow(0);
            Row row = sheet.getRow(1);
            assertEquals("id", header.getCell(0).getStringCellValue());
            assertEquals("name", header.getCell(1).getStringCellValue());
            assertEquals(1D, row.getCell(0).getNumericCellValue(), 0.001D);
            assertEquals("Alice", row.getCell(1).getStringCellValue());
        }
    }

    @Test
    public void shouldWriteExcelWithoutHeader() throws Exception {
        InMemoryFileHelper fileHelper = new InMemoryFileHelper();
        write(fileHelper, config(
                entry("rootPath", "/out"),
                entry("fileName", "sample.xlsx"),
                entry("fileType", "excel"),
                entry("hasHeader", Boolean.FALSE),
                entry("columns", columns(column("id", 0, "LONG"), column("name", 1, "STRING")))),
                records(record("1", "Alice")));

        try (XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(fileHelper.bytes("/out/sample.xlsx")))) {
            Sheet sheet = workbook.getSheet("Sheet1");
            Row row = sheet.getRow(0);
            assertEquals(1D, row.getCell(0).getNumericCellValue(), 0.001D);
            assertEquals("Alice", row.getCell(1).getStringCellValue());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectAppendForJson() {
        CommonFileTableWriter writer = new CommonFileTableWriter(new InMemoryFileHelper());
        writer.setPluginJobConf(config(
                entry("rootPath", "/out"),
                entry("fileName", "sample.json"),
                entry("fileType", "json"),
                entry("writeMode", "append"),
                entry("columns", columns(column("id", 0, "LONG")))));
        writer.init();
    }

    private void write(InMemoryFileHelper fileHelper, Configuration configuration, List<Record> records) {
        CommonFileTableWriter writer = new CommonFileTableWriter(fileHelper);
        writer.setPluginJobConf(configuration);
        writer.init();
        writer.startWrite(new ListRecordReceiver(records));
        writer.destroy();
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

    private List<Record> records(Record... records) {
        List<Record> result = new ArrayList<Record>();
        Collections.addAll(result, records);
        return result;
    }

    private Record record(String... values) {
        DefaultRecord record = new DefaultRecord();
        for (String value : values) {
            record.addColumn(new StringColumn(value));
        }
        return record;
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

    private Map<String, Object> columnWithRemarks(String name, int index, String type, String remarks) {
        Map<String, Object> column = column(name, index, type);
        column.put("remarks", remarks);
        return column;
    }

    private Map<String, Object> tagColumn(String name, int index, String type) {
        Map<String, Object> column = column(name, index, type);
        column.put("sourceKind", "TAG");
        return column;
    }

    private Map<String, Object> efileOptions() {
        Map<String, Object> efile = new LinkedHashMap<String, Object>();
        efile.put("entity", "demo");
        efile.put("type", "test");
        efile.put("dataTime", "20260507_00:00:00");
        efile.put("tableCode", "T01");
        efile.put("tableName", "Demo");
        efile.put("planDate", "20260507");
        return efile;
    }

    private String normalizeNewLine(String value) {
        return value.replace("\r\n", "\n");
    }

    private int countOccurrences(String value, String token) {
        int count = 0;
        int fromIndex = 0;
        while ((fromIndex = value.indexOf(token, fromIndex)) >= 0) {
            count++;
            fromIndex += token.length();
        }
        return count;
    }

    private static final class ListRecordReceiver implements RecordReceiver {
        private final List<Record> records;
        private int index = 0;

        private ListRecordReceiver(List<Record> records) {
            this.records = records == null ? Collections.<Record>emptyList() : records;
        }

        @Override
        public Record getFromReader() {
            if (index >= records.size()) {
                return null;
            }
            return records.get(index++);
        }

        @Override
        public void shutdown() {
        }
    }

    private static final class InMemoryFileHelper implements FileHelper {
        private final Map<String, byte[]> files = new LinkedHashMap<String, byte[]>();
        private final Set<String> directories = new LinkedHashSet<String>();

        private InMemoryFileHelper put(String path, String content) {
            files.put(normalize(path), content.getBytes(StandardCharsets.UTF_8));
            return this;
        }

        private String singlePath() {
            return files.keySet().iterator().next();
        }

        private String text(String path) {
            return new String(bytes(path), StandardCharsets.UTF_8);
        }

        private byte[] bytes(String path) {
            return files.get(normalize(path));
        }

        @Override
        public boolean exists(String path, String name) {
            return files.containsKey(join(path, name));
        }

        @Override
        public Set<String> listFile(String dir, String regex) {
            return Collections.emptySet();
        }

        @Override
        public boolean isFile(String dir, String fileName) {
            return exists(dir, fileName);
        }

        @Override
        public void mkdir(String filePath) {
            directories.add(normalize(filePath));
        }

        @Override
        public void rm(String path) {
            files.remove(normalize(path));
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
            byte[] content = files.get(join(path, name));
            if (content == null) {
                throw new IOException("Missing file: " + join(path, name));
            }
            return new ByteArrayInputStream(content);
        }

        @Override
        public OutputStream getOutputStream(String path, String name) {
            String target = join(path, name);
            return new ByteArrayOutputStream() {
                @Override
                public void close() throws IOException {
                    files.put(target, toByteArray());
                    super.close();
                }
            };
        }

        @Override
        public void close() {
        }

        private String join(String path, String name) {
            String normalizedPath = normalize(path);
            String normalizedName = normalize(name);
            while (normalizedName.startsWith("/")) {
                normalizedName = normalizedName.substring(1);
            }
            if (normalizedPath.isEmpty() || "/".equals(normalizedPath)) {
                return "/" + normalizedName;
            }
            while (normalizedPath.endsWith("/") && normalizedPath.length() > 1) {
                normalizedPath = normalizedPath.substring(0, normalizedPath.length() - 1);
            }
            return normalize(normalizedPath + "/" + normalizedName);
        }

        private String normalize(String value) {
            if (value == null) {
                return "";
            }
            String normalized = value.trim().replace('\\', '/');
            while (normalized.contains("//")) {
                normalized = normalized.replace("//", "/");
            }
            if (!normalized.startsWith("/")) {
                normalized = "/" + normalized;
            }
            return normalized;
        }
    }
}
