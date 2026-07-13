package com.jdragon.aggregation.datasource.http;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.unstructuredstorage.ColumnEntry;
import com.jdragon.aggregation.unstructuredstorage.UnstructuredStorageReaderUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpSourceConfig {
    private static final Pattern CHARSET_PARAMETER = Pattern.compile(
            "(?i);\\s*charset\\s*=\\s*(?:\"([^\"]+)\"|([^;\\s]+))");
    private String url;
    private String mode = "GET";
    private String protocolMode = "REST_JSON";
    private String soapVersion = "SOAP_11";
    private String soapAction = "";
    private boolean soapFaultFail = true;
    private String contentType;
    private String header = "{}";
    private String params = "{}";
    private String requestBody = "";
    private String resultType = "json";
    private Map<String, Object> responseStatus;
    private String totalCodePath;
    private boolean pageRead;
    private int pageSize = 500;
    private int maxRepeatedPages = 2;
    private int retryTimes = 3;
    private long retryIntervalMs = 1000L;
    private boolean retryNonIdempotent;
    private List<ColumnEntry> columns;
    private final Map<String, Map<String, Object>> pushedRequestContext = new LinkedHashMap<>();
    private final Map<String, String> pushedLiteralReplacements = new LinkedHashMap<>();

    public static HttpSourceConfig fromConfiguration(Configuration configuration) {
        HttpSourceConfig config = new HttpSourceConfig();
        config.url = configuration.getNecessaryValue(HttpSourceKeys.URL, HttpSourceErrorCode.REQUIRED_VALUE);
        config.mode = configuration.getNecessaryValue(HttpSourceKeys.MODE, HttpSourceErrorCode.REQUIRED_VALUE);
        config.protocolMode = configuration.getUnnecessaryValue(HttpSourceKeys.PROTOCOL_MODE, "REST_JSON", null);
        config.soapVersion = configuration.getUnnecessaryValue(HttpSourceKeys.SOAP_VERSION, "SOAP_11", null);
        config.soapAction = configuration.getUnnecessaryValue(HttpSourceKeys.SOAP_ACTION, "", null);
        config.soapFaultFail = configuration.getBool(HttpSourceKeys.SOAP_FAULT_FAIL, true);
        config.resultType = resolveResultType(configuration, config.protocolMode);
        if (configuration.get(HttpSourceKeys.PROTOCOL_MODE) == null) {
            if ("soap".equalsIgnoreCase(config.resultType)) {
                config.protocolMode = "SOAP";
            } else if ("xml".equalsIgnoreCase(config.resultType)) {
                config.protocolMode = "REST_XML";
            }
        }
        config.contentType = resolveContentType(configuration, config);
        config.header = configuration.getString(HttpSourceKeys.HEADER, "{}");
        config.params = configuration.getUnnecessaryValue(HttpSourceKeys.PARAMS, "{}", null);
        config.requestBody = configuration.getString(HttpSourceKeys.REQUEST_BODY, "");
        config.responseStatus = configuration.getMap(HttpSourceKeys.RESPONSE_STATUS);
        config.totalCodePath = configuration.getUnnecessaryValue(HttpSourceKeys.TOTAL_CODE_PATH, null, null);
        config.pageRead = configuration.getBool(HttpSourceKeys.PAGE_READ, false);
        config.pageSize = configuration.getInt(HttpSourceKeys.PAGE_SIZE, 500);
        config.maxRepeatedPages = configuration.getInt(HttpSourceKeys.MAX_REPEATED_PAGES, 2);
        config.retryTimes = configuration.getInt(HttpSourceKeys.RETRY_TIMES, 3);
        config.retryIntervalMs = configuration.getLong(HttpSourceKeys.RETRY_INTERVAL_MS, 1000L);
        config.retryNonIdempotent = configuration.getBool(HttpSourceKeys.RETRY_NON_IDEMPOTENT, false);
        config.columns = UnstructuredStorageReaderUtil.getListColumnEntry(configuration, HttpSourceKeys.COLUMNS);
        return config;
    }

    public static HttpSourceConfig fromJson(String json) {
        Map<String, Object> map = JSON.parseObject(json, LinkedHashMap.class);
        return fromConfiguration(Configuration.from(map == null ? new LinkedHashMap<String, Object>() : map));
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(HttpSourceKeys.URL, url);
        map.put(HttpSourceKeys.MODE, mode);
        map.put(HttpSourceKeys.PROTOCOL_MODE, protocolMode);
        map.put(HttpSourceKeys.SOAP_VERSION, soapVersion);
        map.put(HttpSourceKeys.SOAP_ACTION, soapAction);
        map.put(HttpSourceKeys.SOAP_FAULT_FAIL, soapFaultFail);
        map.put(HttpSourceKeys.CONTENT_TYPE, contentType);
        map.put(HttpSourceKeys.HEADER, header);
        map.put(HttpSourceKeys.PARAMS, params);
        map.put(HttpSourceKeys.REQUEST_BODY, requestBody);
        map.put(HttpSourceKeys.RESULT_TYPE, resultType);
        map.put(HttpSourceKeys.RESPONSE_STATUS, responseStatus);
        map.put(HttpSourceKeys.TOTAL_CODE_PATH, totalCodePath);
        map.put(HttpSourceKeys.PAGE_READ, pageRead);
        map.put(HttpSourceKeys.PAGE_SIZE, pageSize);
        map.put(HttpSourceKeys.MAX_REPEATED_PAGES, maxRepeatedPages);
        map.put(HttpSourceKeys.RETRY_TIMES, retryTimes);
        map.put(HttpSourceKeys.RETRY_INTERVAL_MS, retryIntervalMs);
        map.put(HttpSourceKeys.RETRY_NON_IDEMPOTENT, retryNonIdempotent);
        map.put(HttpSourceKeys.COLUMNS, columns == null ? new JSONArray() : JSON.parseArray(JSON.toJSONString(columns)));
        return map;
    }

    boolean isSoapMode() {
        return "SOAP".equalsIgnoreCase(protocolMode) || "soap".equalsIgnoreCase(resultType);
    }

    boolean isSoap12() {
        return isSoapMode() && "SOAP_12".equalsIgnoreCase(soapVersion);
    }

    private static String resolveContentType(Configuration configuration, HttpSourceConfig config) {
        String configured = configuration.getString(HttpSourceKeys.CONTENT_TYPE, null);
        String defaultValue = config.isSoapMode()
                ? ("SOAP_12".equalsIgnoreCase(config.soapVersion)
                ? "application/soap+xml;charset=UTF-8"
                : "text/xml;charset=UTF-8")
                : ("REST_XML".equalsIgnoreCase(config.protocolMode)
                ? "application/xml;charset=UTF-8"
                : "application/json;charset=utf-8");
        if (StringUtils.isBlank(configured)) {
            return defaultValue;
        }
        String mediaType = configured.substring(0,
                configured.indexOf(';') < 0 ? configured.length() : configured.indexOf(';'))
                .trim().toLowerCase(Locale.ENGLISH);
        boolean compatible = config.isSoapMode()
                ? (config.isSoap12() ? "application/soap+xml".equals(mediaType) : "text/xml".equals(mediaType))
                : ("REST_XML".equalsIgnoreCase(config.protocolMode)
                ? mediaType.contains("xml") && !mediaType.contains("soap")
                : !mediaType.contains("xml") && !mediaType.contains("soap"));
        if (compatible) {
            return configured.trim();
        }
        Matcher charsetMatcher = CHARSET_PARAMETER.matcher(configured);
        if (charsetMatcher.find()) {
            String charset = StringUtils.defaultIfBlank(charsetMatcher.group(1), charsetMatcher.group(2));
            return defaultValue.replaceFirst("(?i)charset=[^;\\s]+", "charset=" + charset);
        }
        return defaultValue;
    }

    private static String resolveResultType(Configuration configuration, String protocolMode) {
        String expected = defaultResultType(protocolMode);
        if (configuration.get(HttpSourceKeys.PROTOCOL_MODE) != null) {
            return expected;
        }
        return configuration.getUnnecessaryValue(HttpSourceKeys.RESULT_TYPE, expected, null);
    }

    private static String defaultResultType(String protocolMode) {
        if ("SOAP".equalsIgnoreCase(protocolMode)) {
            return "soap";
        }
        if ("REST_XML".equalsIgnoreCase(protocolMode)) {
            return "xml";
        }
        return "json";
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getMode() {
        return mode;
    }

    public String getProtocolMode() {
        return protocolMode;
    }

    public String getSoapVersion() {
        return soapVersion;
    }

    public String getSoapAction() {
        return soapAction;
    }

    public boolean isSoapFaultFail() {
        return soapFaultFail;
    }

    public String getContentType() {
        return contentType;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public String getRequestBody() {
        return requestBody;
    }

    public void setRequestBody(String requestBody) {
        this.requestBody = requestBody;
    }

    public String getResultType() {
        return resultType;
    }

    public Map<String, Object> getResponseStatus() {
        return responseStatus;
    }

    public String getTotalCodePath() {
        return totalCodePath;
    }

    public boolean isPageRead() {
        return pageRead;
    }

    public void setPageRead(boolean pageRead) {
        this.pageRead = pageRead;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getMaxRepeatedPages() {
        return maxRepeatedPages;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public long getRetryIntervalMs() {
        return retryIntervalMs;
    }

    public boolean isRetryNonIdempotent() {
        return retryNonIdempotent;
    }

    public List<ColumnEntry> getColumns() {
        return columns;
    }

    public Map<String, Map<String, Object>> getPushedRequestContext() {
        Map<String, Map<String, Object>> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : pushedRequestContext.entrySet()) {
            copy.put(entry.getKey(), new LinkedHashMap<>(entry.getValue()));
        }
        return copy;
    }

    public void recordPushedValue(String location, String field, Object value) {
        pushedRequestContext.computeIfAbsent(location, ignored -> new LinkedHashMap<>()).put(field, value);
    }

    public Object protectJsonPushedValue(Object value) {
        if (!(value instanceof String)) {
            return value;
        }
        String serialized = JSON.toJSONString(value);
        String replacement = serialized.length() >= 2
                ? serialized.substring(1, serialized.length() - 1)
                : "";
        return registerPushedLiteral(replacement);
    }

    public Object protectXmlPushedValue(Object value) {
        if (!(value instanceof String)) {
            return value;
        }
        String replacement = String.valueOf(value)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
        return registerPushedLiteral(replacement);
    }

    public String restorePushedLiterals(String value) {
        String restored = value;
        if (restored == null || pushedLiteralReplacements.isEmpty()) {
            return restored;
        }
        for (Map.Entry<String, String> entry : pushedLiteralReplacements.entrySet()) {
            restored = restored.replace(entry.getKey(), entry.getValue());
        }
        return restored;
    }

    private String registerPushedLiteral(String replacement) {
        String placeholder;
        do {
            placeholder = "__AGG_HTTP_PUSHED_LITERAL_" + UUID.randomUUID().toString().replace("-", "") + "__";
        } while (pushedLiteralReplacements.containsKey(placeholder));
        pushedLiteralReplacements.put(placeholder, replacement);
        return placeholder;
    }
}
