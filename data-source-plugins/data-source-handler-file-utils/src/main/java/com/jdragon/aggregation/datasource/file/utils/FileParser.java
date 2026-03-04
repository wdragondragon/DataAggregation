package com.jdragon.aggregation.datasource.file.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 通用文件解析器
 * 支持CSV、JSON、JSONL格式的文件解析
 */
@Slf4j
public class FileParser {

    public enum FileFormat {
        CSV("csv"), JSON("json"), JSONL("jsonl"), PARQUET("parquet"), AVRO("avro"), XML("xml");

        private final String value;

        FileFormat(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static FileFormat fromString(String format) {
            if (format == null) {
                return CSV; // 默认格式
            }
            String lower = format.toLowerCase();
            for (FileFormat f : values()) {
                if (f.value.equals(lower)) {
                    return f;
                }
            }
            // 根据文件扩展名推断
            if (lower.endsWith(".csv")) {
                return CSV;
            } else if (lower.endsWith(".json")) {
                return JSON;
            } else if (lower.endsWith(".jsonl") || lower.endsWith(".ndjson")) {
                return JSONL;
            } else if (lower.endsWith(".parquet")) {
                return PARQUET;
            } else if (lower.endsWith(".avro")) {
                return AVRO;
            } else if (lower.endsWith(".xml")) {
                return XML;
            }
            return CSV; // 默认CSV格式
        }
    }

    /**
     * 解析输入流中的文件数据
     */
    public static void parseInputStream(InputStream is, FileFormat format, String encoding, Consumer<Map<String, Object>> rowConsumer) throws IOException {
        parseInputStream(is, format, encoding, rowConsumer, new LinkedHashMap<>());
    }

    /**
     * 解析输入流中的文件数据（带配置选项）
     */
    public static void parseInputStream(InputStream is, FileFormat format, String encoding, Consumer<Map<String, Object>> rowConsumer, Map<String, Object> options) throws IOException {
        if (is == null) {
            throw new IllegalArgumentException("InputStream cannot be null");
        }

        switch (format) {
            case CSV:
                parseCsvStream(is, encoding, rowConsumer, options);
                break;
            case JSON:
                parseJsonStream(is, encoding, rowConsumer, options);
                break;
            case JSONL:
                parseJsonLinesStream(is, encoding, rowConsumer, options);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported file format: " + format);
        }
    }

    /**
     * 解析本地文件
     */
    public static void parseFile(File file, FileFormat format, String encoding, Consumer<Map<String, Object>> rowConsumer) throws IOException {
        parseFile(file, format, encoding, rowConsumer, null);
    }

    /**
     * 解析本地文件（带配置选项）
     */
    public static void parseFile(File file, FileFormat format, String encoding, Consumer<Map<String, Object>> rowConsumer, Map<String, Object> options) throws IOException {
        if (!file.exists() || !file.isFile()) {
            throw new IOException("File does not exist or is not a file: " + file.getAbsolutePath());
        }

        try (InputStream is = Files.newInputStream(file.toPath())) {
            parseInputStream(is, format, encoding, rowConsumer, options);
        }
    }

    /**
     * 解析CSV流
     */
    private static void parseCsvStream(InputStream is, String encoding, Consumer<Map<String, Object>> rowConsumer, Map<String, Object> options) throws IOException {
        boolean hasHeader = Boolean.parseBoolean(String.valueOf(options.getOrDefault("hasHeader", "true")));
        String delimiter = options.getOrDefault("delimiter", ",").toString();
        String nullFormat = options.getOrDefault("nullFormat", "\\N").toString();
        Charset charset = Charset.forName(encoding != null ? encoding : "UTF-8");
        Character fieldQuote = CharUtils.toChar(options.getOrDefault("fieldQuote", "\"").toString());

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset))) {
            CSVParser csvParser;
            CSVFormat build = CSVFormat.DEFAULT.builder().setNullString(nullFormat).setDelimiter(delimiter).setQuote(fieldQuote).build();
            csvParser = new CSVParser(reader, build);
            List<String> header = new ArrayList<>();
            if (hasHeader) {
                CSVRecord next = csvParser.iterator().next();
                header = next.stream().collect(Collectors.toList());
                log.info("Header line {} has been skiped.", JSONObject.toJSONString(header));
            }
            for (CSVRecord record : csvParser) {
                Map<String, Object> data = new LinkedHashMap<>(record.size());
                for (int i = 0; i < record.size(); i++) {
                    String columnStr = record.get(i);
                    if (hasHeader) {
                        data.put(header.get(i), columnStr);
                    } else {
                        data.put("field_" + i, columnStr);
                    }
                }
                rowConsumer.accept(data);
            }
        }
    }

    /**
     * 解析JSON流
     */
    private static void parseJsonStream(InputStream is, String encoding, Consumer<Map<String, Object>> rowConsumer, Map<String, Object> options) throws IOException {
        Charset charset = Charset.forName(encoding != null ? encoding : "UTF-8");

        String jsonContent = IOUtils.toString(is, charset).trim();
        if (StringUtils.isBlank(jsonContent)) {
            return;
        }

        try {
            if (jsonContent.startsWith("[")) {
                // JSON数组格式
                JSONArray array = JSON.parseArray(jsonContent);
                for (Object item : array) {
                    rowConsumer.accept((JSONObject) item);
                }
            } else {
                // 单个JSON对象
                JSONObject jsonObject = JSON.parseObject(jsonContent);
                rowConsumer.accept(jsonObject);
            }
        } catch (Exception e) {
            throw new IOException("JSON解析失败: " + e.getMessage(), e);
        }
    }

    /**
     * 解析JSON Lines流
     */
    private static void parseJsonLinesStream(InputStream is, String encoding, Consumer<Map<String, Object>> rowConsumer, Map<String, Object> options) throws IOException {
        Charset charset = Charset.forName(encoding != null ? encoding : "UTF-8");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset))) {
            String line;
            int lineNum = 0;

            while ((line = reader.readLine()) != null) {
                lineNum++;
                if (StringUtils.isBlank(line)) {
                    continue;
                }

                try {
                    JSONObject jsonObject = JSON.parseObject(line);
                    rowConsumer.accept(jsonObjectToMap(jsonObject));
                } catch (Exception e) {
                    log.warn("第{}行JSON解析失败: {}", lineNum, line, e);
                }
            }
        }
    }

    /**
     * 将JSONObject转换为Map
     */
    private static Map<String, Object> jsonObjectToMap(JSONObject jsonObject) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            Object value = entry.getValue();
            // 处理嵌套JSON对象和数组
            if (value instanceof JSONObject) {
                value = jsonObjectToMap((JSONObject) value);
            } else if (value instanceof JSONArray) {
                value = convertJsonArrayToList((JSONArray) value);
            }
            map.put(entry.getKey(), value);
        }
        return map;
    }

    /**
     * 将JSONArray转换为List
     */
    private static List<Object> convertJsonArrayToList(JSONArray jsonArray) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            Object value = jsonArray.get(i);
            if (value instanceof JSONObject) {
                list.add(jsonObjectToMap((JSONObject) value));
            } else if (value instanceof JSONArray) {
                list.add(convertJsonArrayToList((JSONArray) value));
            } else {
                list.add(value);
            }
        }
        return list;
    }
}