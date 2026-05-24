package com.jdragon.aggregation.writer.http;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.util.FastJsonMemory;
import com.jdragon.aggregation.commons.util.RetryUtil;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.plugin.httpdyn.HttpDynColumnExecuteHandler;
import com.jdragon.aggregation.plugin.httpdyn.util.HttpUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class HttpWriter extends Writer.Job {

    private static final Logger logger = LoggerFactory.getLogger(HttpWriter.class);

    private Configuration conf;
    private String url;
    private String mode;
    private String contentType;
    private String headerStr;
    private String paramsStr;
    private String requestBodyTemplate;
    private PayloadMode payloadMode;
    private String dataNodePath;
    private boolean includeTotal;
    private String totalNodePath;
    private int batchSize;
    private Map<String, Object> responseStatus;
    private int retryTimes;
    private long retryIntervalMs;
    private int connectTimeoutMs;
    private int socketTimeoutMs;
    private List<ColumnEntry> columns;
    private long sentCount;
    private int requestSequence;

    @Override
    public void init() {
        this.conf = super.getPluginJobConf();
        this.url = conf.getNecessaryValue(Key.URL, HttpWriterErrorCode.REQUIRED_VALUE);
        this.mode = conf.getUnnecessaryValue(Key.MODE, "POST", null).toUpperCase(Locale.ENGLISH);
        this.contentType = conf.getUnnecessaryValue(Key.CONTENT_TYPE, "application/json;charset=utf-8", null);
        this.headerStr = conf.getString(Key.HEADER, "{}");
        this.paramsStr = conf.getUnnecessaryValue(Key.PARAMS, "{}", null);
        this.requestBodyTemplate = conf.getString(Key.REQUEST_BODY, "");
        this.payloadMode = resolvePayloadMode(conf.getString(Key.PAYLOAD_MODE, "object"));
        this.dataNodePath = trimToEmpty(conf.getString(Key.DATA_NODE_PATH, ""));
        this.includeTotal = conf.getBool(Key.INCLUDE_TOTAL, false);
        this.totalNodePath = trimToEmpty(conf.getString(Key.TOTAL_NODE_PATH, ""));
        this.batchSize = Math.max(1, conf.getInt(Key.BATCH_SIZE, 500));
        this.responseStatus = conf.getMap(Key.RESPONSE_STATUS, new HashMap<>(), Object.class);
        this.retryTimes = Math.max(1, conf.getInt(Key.RETRY_TIMES, 3));
        this.retryIntervalMs = Math.max(0L, conf.getLong(Key.RETRY_INTERVAL_MS, 1000L));
        this.connectTimeoutMs = Math.max(1, conf.getInt(Key.CONNECT_TIMEOUT_MS, 3000));
        this.socketTimeoutMs = Math.max(1, conf.getInt(Key.SOCKET_TIMEOUT_MS, 3000));
        this.columns = ColumnEntry.getListColumnEntry(conf, Key.COLUMNS);
        validateConfig();
    }

    @Override
    public void startWrite(RecordReceiver lineReceiver) {
        logger.info("start to writer http url={}, mode={}, payloadMode={}", url, mode, payloadMode);
        if (PayloadMode.ARRAY == payloadMode) {
            writeArray(lineReceiver);
            return;
        }
        writeObject(lineReceiver);
    }

    private void writeObject(RecordReceiver lineReceiver) {
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            JSONObject payload = recordToJson(record);
            sendPayload(payload, 1);
        }
    }

    private void writeArray(RecordReceiver lineReceiver) {
        List<JSONObject> batch = new ArrayList<>(batchSize);
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            batch.add(recordToJson(record));
            if (batch.size() >= batchSize) {
                sendBatch(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            sendBatch(batch);
        }
    }

    private void sendBatch(List<JSONObject> batch) {
        JSONArray payload = new JSONArray();
        payload.addAll(batch);
        sendPayload(payload, batch.size());
    }

    private void sendPayload(Object payload, int payloadSize) {
        long currentOffset = sentCount;
        int currentRequest = ++requestSequence;
        String body = buildBody(payload, payloadSize);
        Map<String, String> httpContent = new LinkedHashMap<>();
        httpContent.put(Key.HEADER, headerStr);
        httpContent.put(Key.PARAMS, paramsStr);
        httpContent.put(Key.REQUEST_BODY, body);
        HttpDynColumnExecuteHandler.replacePage(httpContent, String.valueOf(currentRequest));
        HttpDynColumnExecuteHandler.replacePageSize(httpContent, String.valueOf(payloadSize));
        HttpDynColumnExecuteHandler.replaceOffset(httpContent, String.valueOf(currentOffset));
        HttpDynColumnExecuteHandler.replaceDynVal(httpContent);
        executeWithRetry(httpContent);
        sentCount += payloadSize;
    }

    private void executeWithRetry(Map<String, String> httpContent) {
        try {
            RetryUtil.executeWithRetry(() -> {
                Map<String, String> exec = executeHttp(httpContent);
                validateResponse(exec);
                return exec;
            }, retryTimes, retryIntervalMs, false);
        } catch (Exception e) {
            throw AggregationException.asException(HttpWriterErrorCode.REQUEST_ERROR, e);
        }
    }

    private Map<String, String> executeHttp(Map<String, String> httpContent) {
        Map<String, String> headerMap = parseJsonObjectString(httpContent.get(Key.HEADER), Key.HEADER);
        Map<String, String> paramMap = parseJsonObjectString(httpContent.get(Key.PARAMS), Key.PARAMS);
        HttpUtils httpUtils = HttpUtils.init()
                .setUrl(url)
                .setMethod(mode)
                .setConnectTimeout(connectTimeoutMs)
                .setSocketTimeout(socketTimeoutMs)
                .setHeader("Content-Type", contentType)
                .setHeaderMap(headerMap)
                .setParamMap(paramMap)
                .setRawBody(httpContent.get(Key.REQUEST_BODY));
        try {
            return httpUtils.exec();
        } catch (Exception e) {
            throw AggregationException.asException(HttpWriterErrorCode.REQUEST_ERROR, e);
        }
    }

    private void validateResponse(Map<String, String> exec) {
        if (!"200".equals(exec.get("statusCode"))) {
            throw AggregationException.asException(HttpWriterErrorCode.REQUEST_ERROR,
                    "HTTP status code is " + exec.get("statusCode") + ", response=" + exec.get("result"));
        }
        if (responseStatus == null || responseStatus.isEmpty()) {
            return;
        }
        Object path = responseStatus.get("path");
        if (isBlankValue(path)) {
            return;
        }
        String code = isBlankValue(responseStatus.get("code")) ? "200" : String.valueOf(responseStatus.get("code")).trim();
        String result = exec.get("result");
        if (StringUtils.isBlank(result)) {
            throw AggregationException.asException(HttpWriterErrorCode.RESPONSE_STATUS_ERROR, "响应为空，无法校验业务状态码");
        }
        FastJsonMemory resultObj = new FastJsonMemory(result);
        Object actual = resultObj.get(String.valueOf(path).trim());
        if (actual == null || !code.equalsIgnoreCase(String.valueOf(actual))) {
            throw AggregationException.asException(HttpWriterErrorCode.RESPONSE_STATUS_ERROR,
                    String.format("业务成功状态码：[%s],请求返回状态码：[%s]", code, actual));
        }
    }

    private String buildBody(Object payload, int payloadSize) {
        Object bodyRoot;
        if (StringUtils.isBlank(requestBodyTemplate)) {
            bodyRoot = payload;
        } else {
            JSONObject template = parseTemplateObject(requestBodyTemplate);
            if (StringUtils.isNotBlank(dataNodePath)) {
                putByPath(template, dataNodePath, payload);
                bodyRoot = template;
            } else if (payload instanceof JSONObject) {
                template.putAll((JSONObject) payload);
                bodyRoot = template;
            } else {
                throw AggregationException.asException(HttpWriterErrorCode.ILLEGAL_VALUE,
                        "array payload with requestBody template requires dataNodePath");
            }
        }
        if (StringUtils.isNotBlank(dataNodePath) && StringUtils.isBlank(requestBodyTemplate)) {
            JSONObject wrapper = new JSONObject(true);
            putByPath(wrapper, dataNodePath, payload);
            bodyRoot = wrapper;
        }
        if (includeTotal) {
            if (!(bodyRoot instanceof JSONObject)) {
                throw AggregationException.asException(HttpWriterErrorCode.ILLEGAL_VALUE,
                        "includeTotal requires request body root to be a JSON object");
            }
            putByPath((JSONObject) bodyRoot, totalNodePath, Integer.valueOf(payloadSize));
        }
        return JSON.toJSONString(bodyRoot);
    }

    private JSONObject recordToJson(Record record) {
        JSONObject jsonObject = new JSONObject(true);
        int fallbackIndex = 0;
        for (ColumnEntry columnEntry : columns) {
            int index = columnEntry.getIndex() == null ? fallbackIndex : columnEntry.getIndex();
            String name = columnEntry.getName();
            if (StringUtils.isBlank(name)) {
                throw AggregationException.asException(HttpWriterErrorCode.ILLEGAL_VALUE, "HTTP writer column name is required");
            }
            if (index < 0 || index >= record.getColumnNumber()) {
                throw AggregationException.asException(HttpWriterErrorCode.ILLEGAL_VALUE,
                        "HTTP writer column index out of range: " + index);
            }
            jsonObject.put(name, columnValue(record.getColumn(index)));
            fallbackIndex++;
        }
        return jsonObject;
    }

    private Object columnValue(Column column) {
        if (column == null || column.getRawData() == null) {
            return null;
        }
        return column.getRawData();
    }

    private void putByPath(JSONObject root, String path, Object value) {
        if (StringUtils.isBlank(path)) {
            throw AggregationException.asException(HttpWriterErrorCode.ILLEGAL_VALUE, "JSON path is required");
        }
        String[] nodes = path.split("\\.");
        JSONObject current = root;
        for (int i = 0; i < nodes.length; i++) {
            String node = nodes[i].trim();
            if (StringUtils.isBlank(node)) {
                throw AggregationException.asException(HttpWriterErrorCode.ILLEGAL_VALUE, "JSON path contains blank node: " + path);
            }
            if (i == nodes.length - 1) {
                current.put(node, value);
                return;
            }
            Object child = current.get(node);
            if (!(child instanceof JSONObject)) {
                child = new JSONObject(true);
                current.put(node, child);
            }
            current = (JSONObject) child;
        }
    }

    private JSONObject parseTemplateObject(String value) {
        try {
            Object parsed = JSON.parse(value);
            if (parsed instanceof JSONObject) {
                return (JSONObject) parsed;
            }
        } catch (JSONException e) {
            throw AggregationException.asException(HttpWriterErrorCode.ILLEGAL_VALUE, "requestBody must be a JSON object string", e);
        }
        throw AggregationException.asException(HttpWriterErrorCode.ILLEGAL_VALUE, "requestBody must be a JSON object string");
    }

    private Map<String, String> parseJsonObjectString(String value, String key) {
        String text = StringUtils.isBlank(value) ? "{}" : value.trim();
        try {
            JSONObject object = JSONObject.parseObject(text);
            Map<String, String> result = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : object.entrySet()) {
                result.put(entry.getKey(), entry.getValue() == null ? null : String.valueOf(entry.getValue()));
            }
            return result;
        } catch (Exception e) {
            throw AggregationException.asException(HttpWriterErrorCode.ILLEGAL_VALUE,
                    "HTTP writer " + key + " must be a JSON object string", e);
        }
    }

    private PayloadMode resolvePayloadMode(String value) {
        if ("array".equalsIgnoreCase(value)) {
            return PayloadMode.ARRAY;
        }
        return PayloadMode.OBJECT;
    }

    private void validateConfig() {
        if (columns == null || columns.isEmpty()) {
            throw AggregationException.asException(HttpWriterErrorCode.REQUIRED_VALUE, "HTTP writer columns are required");
        }
        parseJsonObjectString(headerStr, Key.HEADER);
        parseJsonObjectString(paramsStr, Key.PARAMS);
        if (includeTotal && StringUtils.isBlank(totalNodePath)) {
            throw AggregationException.asException(HttpWriterErrorCode.REQUIRED_VALUE,
                    "totalNodePath is required when includeTotal is true");
        }
        if (StringUtils.isNotBlank(requestBodyTemplate) && PayloadMode.ARRAY == payloadMode && StringUtils.isBlank(dataNodePath)) {
            throw AggregationException.asException(HttpWriterErrorCode.REQUIRED_VALUE,
                    "dataNodePath is required when array payload uses requestBody template");
        }
    }

    private String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
    }

    private boolean isBlankValue(Object value) {
        return value == null || String.valueOf(value).trim().isEmpty();
    }
}
