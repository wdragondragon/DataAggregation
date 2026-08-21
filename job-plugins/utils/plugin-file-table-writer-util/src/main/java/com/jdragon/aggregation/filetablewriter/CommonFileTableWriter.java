package com.jdragon.aggregation.filetablewriter;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.filetablereader.QualityDynamicFunctionResolver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Slf4j
public class CommonFileTableWriter extends Writer.Job {

    private static final String FILE_TYPE_CSV = "csv";
    private static final String FILE_TYPE_JSON = "json";
    private static final String FILE_TYPE_JSONL = "jsonl";
    private static final String FILE_TYPE_EFILE = "efile";
    private static final String FILE_TYPE_EXCEL = "excel";

    private static final String WRITE_MODE_OVERWRITE = "overwrite";
    private static final String WRITE_MODE_APPEND = "append";
    private static final String WRITE_MODE_FAIL_IF_EXISTS = "failifexists";
    private static final String SOURCE_KIND_TAG = "TAG";

    private final FileHelper fileHelper;

    private Configuration writerConfig;
    private String rootPath;
    private String fileName;
    private String fileType;
    private String encoding;
    private String writeMode;
    private List<FileColumnEntry> columns;

    public CommonFileTableWriter(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    @Override
    public void init() {
        this.writerConfig = getPluginJobConf();
        Configuration connectConfig = writerConfig.getConfiguration("connect");
        if (!fileHelper.connect(connectConfig == null ? Configuration.newDefault() : connectConfig)) {
            throw new IllegalStateException("File target connect failed.");
        }
        this.rootPath = normalizePath(resolveDynamicText(writerConfig.getString("rootPath", "/")));
        this.fileName = normalizePath(resolveDynamicText(writerConfig.getString("fileName")));
        if (StringUtils.isBlank(fileName)) {
            throw new IllegalArgumentException("File table writer fileName is required.");
        }
        this.fileType = normalizeFileType(writerConfig.getString("fileType", fileName));
        this.encoding = writerConfig.getString("encoding", StandardCharsets.UTF_8.name());
        this.writeMode = normalizeWriteMode(writerConfig.getString("writeMode", WRITE_MODE_OVERWRITE));
        this.columns = loadColumns(writerConfig);
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("File table writer columns are required.");
        }
        validateWriteMode();
        log.info("file table writer initialized, rootPath={}, fileName={}, fileType={}, writeMode={}, columns={}",
                rootPath, fileName, fileType, writeMode, columns.size());
    }

    @Override
    public void startWrite(RecordReceiver recordReceiver) {
        List<Map<String, Object>> rows = readRows(recordReceiver);
        try {
            byte[] output = serialize(rows);
            writeBytes(output);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Write file failed: " + targetPath() + ", " + e.getMessage(), e);
        } finally {
            fileHelper.fresh();
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

    private List<Map<String, Object>> readRows(RecordReceiver recordReceiver) {
        List<Map<String, Object>> rows = new ArrayList<>();
        Record record;
        while ((record = recordReceiver.getFromReader()) != null) {
            rows.add(recordToRow(record));
        }
        return rows;
    }

    private Map<String, Object> recordToRow(Record record) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (FileColumnEntry entry : columns) {
            Column column = record == null ? null : record.getColumn(entry.index);
            row.put(entry.name, convertColumn(column, entry.type));
        }
        return row;
    }

    private Object convertColumn(Column column, String type) {
        if (column == null || column.getRawData() == null) {
            return null;
        }
        String normalizedType = normalizeColumnType(type);
        if ("LONG".equals(normalizedType)) {
            return column.asLong();
        }
        if ("DOUBLE".equals(normalizedType)) {
            BigDecimal value = column.asBigDecimal();
            return value == null ? null : value;
        }
        if ("BOOLEAN".equals(normalizedType)) {
            return column.asBoolean();
        }
        return column.asString();
    }

    private byte[] serialize(List<Map<String, Object>> rows) throws IOException {
        if (FILE_TYPE_CSV.equals(fileType)) {
            return writeCsv(rows);
        }
        if (FILE_TYPE_JSON.equals(fileType)) {
            return writeJson(rows);
        }
        if (FILE_TYPE_JSONL.equals(fileType)) {
            return writeJsonl(rows);
        }
        if (FILE_TYPE_EFILE.equals(fileType)) {
            return writeEFile(rows);
        }
        if (FILE_TYPE_EXCEL.equals(fileType)) {
            return writeExcel(rows);
        }
        throw new IllegalArgumentException("Unsupported fileType: " + fileType);
    }

    private byte[] writeCsv(List<Map<String, Object>> rows) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Charset charset = Charset.forName(encoding);
        boolean hasHeader = writerConfig.getBool("hasHeader", true);
        String delimiter = writerConfig.getString("delimiter", ",");
        String nullFormat = writerConfig.getString("nullFormat", "\\N");
        Character fieldQuote = writerConfig.getChar("fieldQuote", '"');
        CSVFormat format = CSVFormat.DEFAULT;
        if (StringUtils.isNotEmpty(delimiter)) {
            format = format.withDelimiter("\\t".equals(delimiter) ? '\t' : delimiter.charAt(0));
        }
        if (fieldQuote != null) {
            format = format.withQuote(fieldQuote.charValue());
        }
        if (nullFormat != null) {
            format = format.withNullString(nullFormat);
        }
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, charset));
             CSVPrinter printer = new CSVPrinter(writer, format)) {
            List<FileColumnEntry> writableColumns = dataColumns(false);
            if (hasHeader && !(WRITE_MODE_APPEND.equals(writeMode) && safeExists())) {
                List<String> headers = new ArrayList<>();
                for (FileColumnEntry column : writableColumns) {
                    headers.add(column.name);
                }
                printer.printRecord(headers);
            }
            for (Map<String, Object> row : rows) {
                printer.printRecord(values(row, writableColumns));
            }
        }
        return outputStream.toByteArray();
    }

    private boolean safeExists() {
        try {
            return fileHelper.exists(rootPath, fileName);
        } catch (Exception e) {
            return false;
        }
    }

    private byte[] writeJson(List<Map<String, Object>> rows) {
        JSONArray array = new JSONArray();
        List<FileColumnEntry> writableColumns = dataColumns(false);
        for (Map<String, Object> row : rows) {
            array.add(rowToJson(row, writableColumns));
        }
        return array.toJSONString().getBytes(Charset.forName(encoding));
    }

    private byte[] writeJsonl(List<Map<String, Object>> rows) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Charset charset = Charset.forName(encoding);
        List<FileColumnEntry> writableColumns = dataColumns(false);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, charset))) {
            for (Map<String, Object> row : rows) {
                writer.write(rowToJson(row, writableColumns).toJSONString());
                writer.newLine();
            }
        }
        return outputStream.toByteArray();
    }

    private byte[] writeExcel(List<Map<String, Object>> rows) throws IOException {
        String sheetName = writerConfig.getString("sheetName", "Sheet1");
        boolean hasHeader = writerConfig.getBool("hasHeader", true);
        try (XSSFWorkbook workbook = new XSSFWorkbook();
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            Sheet sheet = workbook.createSheet(StringUtils.defaultIfBlank(sheetName, "Sheet1"));
            List<FileColumnEntry> writableColumns = dataColumns(false);
            int rowIndex = 0;
            if (hasHeader) {
                Row header = sheet.createRow(rowIndex++);
                for (int columnIndex = 0; columnIndex < writableColumns.size(); columnIndex++) {
                    header.createCell(columnIndex).setCellValue(writableColumns.get(columnIndex).name);
                }
            }
            for (Map<String, Object> item : rows) {
                Row row = sheet.createRow(rowIndex++);
                List<Object> values = values(item, writableColumns);
                for (int columnIndex = 0; columnIndex < values.size(); columnIndex++) {
                    setCellValue(row.createCell(columnIndex), values.get(columnIndex));
                }
            }
            workbook.write(outputStream);
            return outputStream.toByteArray();
        }
    }

    private void setCellValue(Cell cell, Object value) {
        if (value == null) {
            cell.setBlank();
            return;
        }
        if (value instanceof Number) {
            cell.setCellValue(((Number) value).doubleValue());
            return;
        }
        if (value instanceof Boolean) {
            cell.setCellValue((Boolean) value);
            return;
        }
        cell.setCellValue(String.valueOf(value));
    }

    private byte[] writeEFile(List<Map<String, Object>> rows) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Charset charset = Charset.forName(encoding);
        List<FileColumnEntry> dataColumns = dataColumns(true);
        Map<String, String> tagValues = resolveEFileTagValues(rows);
        EFileOptions options = efileOptions(tagValues);
        tagValues.remove("planDate");
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, charset))) {
            writer.write("<! Entity='" + options.entity + "' dataTime='" + options.dataTime + "' type='" + options.type + "' !>");
            writer.newLine();
            writer.write("<" + options.tableCode + "::" + options.tableName);
            if (StringUtils.isNotBlank(options.planDate)) {
                writer.write(" planDate='" + escapeEFileTagValue(options.planDate) + "'");
            }
            for (Map.Entry<String, String> entry : tagValues.entrySet()) {
                writer.write(" " + entry.getKey() + "='" + escapeEFileTagValue(entry.getValue()) + "'");
            }
            writer.write(" >");
            writer.newLine();
            writer.write("@");
            for (FileColumnEntry column : dataColumns) {
                writer.write(" " + column.name);
            }
            writer.newLine();
            writer.write("//");
            for (FileColumnEntry column : dataColumns) {
                String description = StringUtils.isBlank(column.remarks) ? column.name : column.remarks;
                writer.write(" " + formatEFileDataValue(description));
            }
            writer.newLine();
            for (Map<String, Object> row : rows) {
                writer.write("#");
                for (FileColumnEntry column : dataColumns) {
                    writer.write(" " + formatEFileDataValue(row.get(column.name)));
                }
                writer.newLine();
            }
            writer.write("</" + options.tableCode + "::" + options.tableName + ">");
            writer.newLine();
        }
        return outputStream.toByteArray();
    }

    private Map<String, String> resolveEFileTagValues(List<Map<String, Object>> rows) {
        Map<String, String> values = new LinkedHashMap<>();
        for (FileColumnEntry column : columns) {
            if (!column.tagColumn) {
                continue;
            }
            String firstValue = null;
            for (Map<String, Object> row : rows) {
                Object value = row.get(column.name);
                if (value == null || StringUtils.isBlank(String.valueOf(value))) {
                    continue;
                }
                String text = String.valueOf(value);
                if (firstValue == null) {
                    firstValue = text;
                } else if (!firstValue.equals(text)) {
                    throw new IllegalArgumentException("EFile tag column has inconsistent values: " + column.name);
                }
            }
            if (firstValue != null) {
                values.put(column.name, firstValue);
            }
        }
        return values;
    }

    private EFileOptions efileOptions(Map<String, String> tagValues) {
        Configuration efile = writerConfig.getConfiguration("efile");
        if (efile == null) {
            efile = Configuration.newDefault();
        }
        EFileOptions options = new EFileOptions();
        options.entity = resolveDynamicText(firstNonBlank(efile.getString("entity"), tagValues.get("entity"), "demo"));
        options.type = resolveDynamicText(firstNonBlank(efile.getString("type"), tagValues.get("type"), "test"));
        options.dataTime = resolveDynamicText(firstNonBlank(efile.getString("dataTime"), tagValues.get("dataTime"),
                "$getCurrentTime(yyyyMMdd_HH:mm:ss,0)"));
        options.tableName = resolveDynamicText(firstNonBlank(efile.getString("tableName"), tagValues.get("tableName"), "Data"));
        options.tableCode = resolveDynamicText(firstNonBlank(efile.getString("tableCode"), tagValues.get("tableCode"), "T01"));
        String modelPlanDate = StringUtils.trimToNull(resolveDynamicText(efile.getString("planDate")));
        String tagPlanDate = StringUtils.trimToNull(tagValues.get("planDate"));
        if (modelPlanDate != null && tagPlanDate != null && !modelPlanDate.equals(tagPlanDate)) {
            throw new IllegalArgumentException("EFile planDate conflicts between model metadata and TAG field: model="
                    + modelPlanDate + ", tag=" + tagPlanDate);
        }
        options.planDate = firstNonBlank(modelPlanDate, tagPlanDate);
        return options;
    }

    private JSONObject rowToJson(Map<String, Object> row, List<FileColumnEntry> writableColumns) {
        JSONObject object = new JSONObject(true);
        for (FileColumnEntry column : writableColumns) {
            object.put(column.name, row.get(column.name));
        }
        return object;
    }

    private List<Object> values(Map<String, Object> row, List<FileColumnEntry> writableColumns) {
        List<Object> values = new ArrayList<>(writableColumns.size());
        for (FileColumnEntry column : writableColumns) {
            values.add(row.get(column.name));
        }
        return values;
    }

    private List<FileColumnEntry> dataColumns(boolean excludeTags) {
        if (!excludeTags) {
            return columns;
        }
        List<FileColumnEntry> result = new ArrayList<>();
        for (FileColumnEntry column : columns) {
            if (!column.tagColumn) {
                result.add(column);
            }
        }
        return result;
    }

    private void writeBytes(byte[] output) throws IOException {
        boolean exists = safeExists();
        byte[] finalOutput = output;
        if (WRITE_MODE_FAIL_IF_EXISTS.equals(writeMode) && exists) {
            throw new IOException("Target file already exists: " + targetPath());
        }
        if (WRITE_MODE_APPEND.equals(writeMode) && exists) {
            finalOutput = appendBytes(readExistingBytes(), output);
        }
        if (exists) {
            fileHelper.rm(targetPath());
        }
        if (StringUtils.isNotBlank(rootPath)) {
            fileHelper.mkdir(rootPath);
        }
        try (OutputStream outputStream = fileHelper.getOutputStream(rootPath, fileName)) {
            outputStream.write(finalOutput);
        }
    }

    private byte[] readExistingBytes() throws IOException {
        try (InputStream inputStream = fileHelper.getInputStream(rootPath, fileName);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = inputStream.read(buffer)) >= 0) {
                outputStream.write(buffer, 0, read);
            }
            return outputStream.toByteArray();
        }
    }

    private byte[] appendBytes(byte[] existing, byte[] appended) {
        byte[] left = existing == null ? new byte[0] : existing;
        byte[] right = appended == null ? new byte[0] : appended;
        byte[] separator = needsAppendLineSeparator(left, right) ? "\n".getBytes(Charset.forName(encoding)) : new byte[0];
        byte[] result = new byte[left.length + separator.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(separator, 0, result, left.length, separator.length);
        System.arraycopy(right, 0, result, left.length + separator.length, right.length);
        return result;
    }

    private boolean needsAppendLineSeparator(byte[] existing, byte[] appended) {
        if (existing == null || existing.length == 0 || appended == null || appended.length == 0) {
            return false;
        }
        byte last = existing[existing.length - 1];
        return last != '\n' && last != '\r';
    }

    private void validateWriteMode() {
        if (!WRITE_MODE_OVERWRITE.equals(writeMode)
                && !WRITE_MODE_APPEND.equals(writeMode)
                && !WRITE_MODE_FAIL_IF_EXISTS.equals(writeMode)) {
            throw new IllegalArgumentException("Unsupported writeMode: " + writeMode);
        }
        if (WRITE_MODE_APPEND.equals(writeMode)
                && !(FILE_TYPE_CSV.equals(fileType) || FILE_TYPE_JSONL.equals(fileType))) {
            throw new IllegalArgumentException("Append writeMode only supports csv and jsonl.");
        }
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
            boolean tagColumn = SOURCE_KIND_TAG.equalsIgnoreCase(StringUtils.trimToEmpty(stringValue(item.get("sourceKind"))));
            String remarks = stringValue(item.get("remarks"));
            result.add(new FileColumnEntry(name, index, type, tagColumn, remarks));
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

    private String targetPath() {
        return joinPath(rootPath, fileName);
    }

    private String resolveDynamicText(String value) {
        return QualityDynamicFunctionResolver.replaceAll(value);
    }

    private String normalizeFileType(String value) {
        String normalized = StringUtils.defaultIfBlank(value, FILE_TYPE_CSV).trim().toLowerCase(Locale.ENGLISH);
        if (normalized.endsWith(".csv") || FILE_TYPE_CSV.equals(normalized)) {
            return FILE_TYPE_CSV;
        }
        if (normalized.endsWith(".json") || FILE_TYPE_JSON.equals(normalized)) {
            return FILE_TYPE_JSON;
        }
        if (normalized.endsWith(".jsonl") || normalized.endsWith(".ndjson") || FILE_TYPE_JSONL.equals(normalized)) {
            return FILE_TYPE_JSONL;
        }
        if (normalized.endsWith(".efile") || "e".equals(normalized) || FILE_TYPE_EFILE.equals(normalized)) {
            return FILE_TYPE_EFILE;
        }
        if (normalized.endsWith(".xlsx") || FILE_TYPE_EXCEL.equals(normalized) || "xlsx".equals(normalized)) {
            return FILE_TYPE_EXCEL;
        }
        throw new IllegalArgumentException("Unsupported fileType: " + value);
    }

    private String normalizeWriteMode(String value) {
        String normalized = StringUtils.defaultIfBlank(value, WRITE_MODE_OVERWRITE).trim().toLowerCase(Locale.ENGLISH);
        if ("failIfExists".equals(value)) {
            return WRITE_MODE_FAIL_IF_EXISTS;
        }
        return normalized;
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

    private String formatEFileDataValue(Object value) {
        if (value == null) {
            return "";
        }
        String text = String.valueOf(value);
        if (text.contains(" ") || text.contains("\t") || text.contains("'") || text.contains("\"")) {
            return "'" + text.replace("'", "") + "'";
        }
        return text;
    }

    private String escapeEFileTagValue(String value) {
        return value == null ? "" : value.replace("'", "");
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (StringUtils.isNotBlank(value)) {
                return value;
            }
        }
        return null;
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

    private static final class FileColumnEntry {
        private final String name;
        private final int index;
        private final String type;
        private final boolean tagColumn;
        private final String remarks;

        private FileColumnEntry(String name, int index, String type, boolean tagColumn, String remarks) {
            this.name = name;
            this.index = index;
            this.type = type;
            this.tagColumn = tagColumn;
            this.remarks = remarks;
        }
    }

    private static final class EFileOptions {
        private String entity;
        private String type;
        private String dataTime;
        private String tableName;
        private String tableCode;
        private String planDate;
    }
}
