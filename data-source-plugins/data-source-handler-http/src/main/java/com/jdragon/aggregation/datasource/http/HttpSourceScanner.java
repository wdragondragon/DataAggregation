package com.jdragon.aggregation.datasource.http;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.FastJsonMemory;
import com.jdragon.aggregation.commons.util.json.JsonExtractionResult;
import com.jdragon.aggregation.plugin.httpdyn.HttpDynColumnExecuteHandler;
import com.jdragon.aggregation.plugin.httpdyn.exception.HttpRequestConfigurationException;
import com.jdragon.aggregation.plugin.httpdyn.util.HttpUtils;
import com.jdragon.aggregation.plugin.httpdyn.util.HttpReadTimeoutSession;
import com.jdragon.aggregation.unstructuredstorage.ColumnEntry;
import com.jdragon.aggregation.unstructuredstorage.SoapToJSON;
import com.jdragon.aggregation.unstructuredstorage.UnstructuredStorageReaderUtil;
import com.jdragon.aggregation.unstructuredstorage.XmlToJSON;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpSourceScanner {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSourceScanner.class);
    private static final Pattern CURLY_PATH_VARIABLE = Pattern.compile("\\{([^{}]+)}");
    private static final Pattern COLON_PATH_VARIABLE = Pattern.compile("(?:^|/):([A-Za-z_][A-Za-z0-9_]*)");
    private static final Pattern SOAP_ACTION_PARAMETER = Pattern.compile(
            "(?i);\\s*action\\s*=\\s*(?:\"(?:\\\\.|[^\"])*\"|[^;]*)");
    private static final Pattern CONTENT_TYPE_CHARSET = Pattern.compile(
            "(?i);\\s*charset\\s*=\\s*(?:\"([^\"]+)\"|([^;\\s]+))");

    public void scan(HttpSourceConfig config, Consumer<Map<String, Object>> rowConsumer) {
        scan(config, rowConsumer, 0);
    }

    public void scan(HttpSourceConfig config, Consumer<Map<String, Object>> rowConsumer, int maxRows) {
        if (config.getColumns() == null || config.getColumns().isEmpty()) {
            return;
        }
        validatePaginationConfig(config);
        Map<String, String> httpContent = newHttpContent(config);
        HttpDynColumnExecuteHandler.replaceStaticVal(httpContent);
        int pageNum = 1;
        long offset = 0L;
        int emittedRows = 0;
        HttpReadTimeoutSession timeoutSession = new HttpReadTimeoutSession(config.getReadTimeout());
        PageRepeatGuard pageRepeatGuard = new PageRepeatGuard(config.getMaxRepeatedPages());
        Map<String, String> requestContent = new LinkedHashMap<>(httpContent);
        String resultStr = connectToGetData(config, requestContent, pageNum, offset, timeoutSession);
        if (StringUtils.isBlank(resultStr)) {
            if (StringUtils.isNotBlank(config.getTotalCodePath())) {
                throw AggregationException.asException(HttpSourceErrorCode.RESPONSE_PAGE_ERROR,
                        "HTTP 分页响应提前结束: page=1, 已配置总量节点但首页响应为空");
            }
            return;
        }
        FastJsonMemory resultJson = parseResponseJson(resultStr);
        ScanPageResult scanPageResult = emitRows(config, resultJson, rowConsumer,
                remainingRows(maxRows, emittedRows), requestContext(config), pageRepeatGuard, pageNum, resultStr);
        emittedRows += scanPageResult.getEmittedRows();
        while (config.isPageRead()
                && hasRemainingRows(maxRows, emittedRows)
                && checkPage(config, pageNum, scanPageResult.getFetchedRows(), resultJson)) {
            pageNum++;
            offset += config.getPageSize();
            requestContent = new LinkedHashMap<>(httpContent);
            resultStr = connectToGetData(config, requestContent, pageNum, offset, timeoutSession);
            if (StringUtils.isBlank(resultStr)) {
                if (StringUtils.isNotBlank(config.getTotalCodePath())) {
                    throw AggregationException.asException(HttpSourceErrorCode.RESPONSE_PAGE_ERROR,
                            "HTTP 分页响应提前结束: page=" + pageNum + ", 已知仍有未读取数据");
                }
                break;
            }
            resultJson = parseResponseJson(resultStr);
            scanPageResult = emitRows(config, resultJson, rowConsumer,
                    remainingRows(maxRows, emittedRows), requestContext(config), pageRepeatGuard, pageNum, resultStr);
            emittedRows += scanPageResult.getEmittedRows();
        }
    }

    public String connectToGetData(HttpSourceConfig config, Map<String, String> httpContent, Integer pageNum) {
        return connectToGetData(config, httpContent, pageNum, 0L);
    }

    public String connectToGetData(HttpSourceConfig config, Map<String, String> httpContent, Integer pageNum, Long offset) {
        return connectToGetData(config, httpContent, pageNum, offset,
                new HttpReadTimeoutSession(config.getReadTimeout()));
    }

    public String connectToGetData(HttpSourceConfig config, Map<String, String> httpContent,
                                   Integer pageNum, Long offset, HttpReadTimeoutSession timeoutSession) {
        HttpDynColumnExecuteHandler.replacePage(httpContent, String.valueOf(pageNum));
        HttpDynColumnExecuteHandler.replacePageSize(httpContent, String.valueOf(config.getPageSize()));
        HttpDynColumnExecuteHandler.replaceOffset(httpContent, String.valueOf(offset == null ? 0L : offset));
        HttpDynColumnExecuteHandler.replaceDynVal(httpContent);
        restorePushedLiterals(config, httpContent);
        validatePathVariablesBound(config.getUrl());

        Map<String, String> headerMap = JSONObject.parseObject(httpContent.get(HttpSourceKeys.HEADER),
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> paramMap = JSONObject.parseObject(httpContent.get(HttpSourceKeys.PARAMS),
                new TypeReference<Map<String, String>>() {
                });

        HttpUtils httpUtils = HttpUtils.init();
        httpUtils.setUrl(config.getUrl());
        httpUtils.setMethod(config.getMode());
        httpUtils.setHeaderMap(headerMap);
        configureProtocolHeaders(httpUtils, config, headerMap);
        httpUtils.setParamMap(paramMap);
        httpUtils.setRawBody(httpContent.get(HttpSourceKeys.REQUEST_BODY));

        int requestAttempts = requestAttempts(config);
        try {
            String resultStr = executeWithRetry(config, httpUtils, pageNum, requestAttempts, timeoutSession);
            resultStr = convertResponseIfNeeded(config, resultStr);
            validateResponseStatus(config, resultStr);
            return resultStr;
        } catch (Exception e) {
            if (e instanceof AggregationException) {
                throw (AggregationException) e;
            }
            if (e instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) e;
            }
            throw new RuntimeException("HTTP request failed", e);
        }
    }

    private String executeWithRetry(HttpSourceConfig config, HttpUtils httpUtils, Integer pageNum,
                                    int requestAttempts, HttpReadTimeoutSession timeoutSession) {
        int attempt = 0;
        int otherFailures = 0;
        while (true) {
            ensureNotInterrupted();
            int currentTimeoutMs = timeoutSession.getReadTimeoutMs();
            httpUtils.setSocketTimeout(currentTimeoutMs);
            attempt++;
            try {
                return executeRequest(config, httpUtils);
            } catch (RuntimeException failure) {
                ensureNotInterrupted();
                if (HttpReadTimeoutSession.isReadTimeout(failure)) {
                    if (!timeoutSession.increaseAfterReadTimeout()) {
                        LOG.error("HTTP reader read timeout limit reached: page={}, attempt={}, errorType={}, readTimeoutMs={}; stopping retries",
                                pageNum, attempt, failureType(failure), currentTimeoutMs);
                        throw failure;
                    }
                } else {
                    otherFailures++;
                    if (!(failure instanceof RetryableHttpRequestException)) {
                        throw failure;
                    }
                    if (otherFailures >= requestAttempts) {
                        if (requestAttempts > 1) {
                            throw new RuntimeException(
                                    "HTTP request failed after " + requestAttempts + " attempts", failure);
                        }
                        throw failure;
                    }
                }
                LOG.warn("HTTP reader request retry: page={}, attempt={}, errorType={}, currentTimeoutMs={}, nextTimeoutMs={}",
                        pageNum, attempt, failureType(failure), currentTimeoutMs,
                        timeoutSession.getReadTimeoutMs());
                waitForRetry(config.getRetryIntervalMs());
            }
        }
    }

    private void waitForRetry(long retryIntervalMs) {
        ensureNotInterrupted();
        if (retryIntervalMs <= 0) {
            return;
        }
        try {
            Thread.sleep(retryIntervalMs);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("HTTP request interrupted during retry delay", interrupted);
        }
        ensureNotInterrupted();
    }

    private void ensureNotInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new IllegalStateException("HTTP request interrupted");
        }
    }

    private String failureType(Throwable failure) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable lastCause = failure;
        for (Throwable cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
            lastCause = cause;
        }
        return lastCause.getClass().getSimpleName();
    }

    public HttpSourceConfig applyPushdown(HttpSourceConfig config, List<HttpPushdownPredicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return config;
        }
        JSONObject headers = parseObject(config.getHeader(), HttpSourceKeys.HEADER);
        JSONObject params = parseObject(config.getParams(), HttpSourceKeys.PARAMS);
        String bodyText = config.getRequestBody();
        boolean bodyChanged = false;
        String url = config.getUrl();
        Map<String, Object> assignedTargets = new LinkedHashMap<>();
        for (HttpPushdownPredicate predicate : predicates) {
            Object value = valueFor(predicate);
            String location = normalizeLocation(predicate.getLocation());
            String target = pushdownTarget(predicate, location);
            if (assignedTargets.containsKey(target)) {
                if (!pushdownValuesEqual(assignedTargets.get(target), value)) {
                    throw new IllegalArgumentException("HTTP 下推参数 " + target + " 收到冲突条件");
                }
                continue;
            }
            assignedTargets.put(target, value);
            config.recordPushedValue(location, predicate.getField(), value);
            if ("param".equals(location) || "query".equals(location)) {
                String name = firstText(predicate.getRequestParamName(), predicate.getField());
                applyPaginationOverride(config, predicate, params.get(name), value, target);
                params.put(name, config.protectJsonPushedValue(value));
            } else if ("header".equals(location)) {
                String name = firstText(predicate.getHeaderName(), predicate.getField());
                String configuredName = findHeaderNameIgnoreCase(headers, name);
                applyPaginationOverride(config, predicate,
                        configuredName == null ? null : headers.get(configuredName), value, target);
                putHeaderIgnoreCase(headers, name, config.protectJsonPushedValue(value));
            } else if ("body".equals(location)) {
                if (!HttpUtils.supportsRequestBody(config.getMode())) {
                    throw new IllegalArgumentException("HTTP " + config.getMode()
                            + " 请求不会发送实体，不能进行 body 下推");
                }
                String path = firstText(predicate.getBodyPath(), predicate.getField());
                BodyUpdateResult updated = applyBodyValue(config, bodyText, path, value);
                applyPaginationOverride(config, predicate, updated.getConfiguredValue(), value, target);
                bodyText = updated.getBody();
                bodyChanged = true;
            } else if ("path".equals(location)) {
                String name = firstText(predicate.getPathVariable(), predicate.getField());
                if (!containsPathVariable(url, name)) {
                    throw new IllegalArgumentException("HTTP path 下推字段 " + name + " 在请求 URL 中没有对应占位符");
                }
                url = replacePathVariable(url, name, value == null ? "" : String.valueOf(value));
            } else {
                throw new IllegalArgumentException("Unsupported HTTP pushdown location: " + location);
            }
        }
        config.setUrl(url);
        config.setHeader(headers.toJSONString());
        config.setParams(params.toJSONString());
        if (bodyChanged) {
            config.setRequestBody(bodyText);
        }
        return config;
    }

    private boolean pushdownValuesEqual(Object left, Object right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        if (left instanceof Number && right instanceof Number) {
            try {
                return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString())) == 0;
            } catch (NumberFormatException ignored) {
                return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue()) == 0;
            }
        }
        if (left instanceof Collection<?> && right instanceof Collection<?>) {
            Collection<?> leftValues = (Collection<?>) left;
            Collection<?> rightValues = (Collection<?>) right;
            if (leftValues.size() != rightValues.size()) {
                return false;
            }
            java.util.Iterator<?> leftIterator = leftValues.iterator();
            java.util.Iterator<?> rightIterator = rightValues.iterator();
            while (leftIterator.hasNext()) {
                if (!pushdownValuesEqual(leftIterator.next(), rightIterator.next())) {
                    return false;
                }
            }
            return true;
        }
        if (left.getClass().isArray() && right.getClass().isArray()) {
            int length = Array.getLength(left);
            if (length != Array.getLength(right)) {
                return false;
            }
            for (int index = 0; index < length; index++) {
                if (!pushdownValuesEqual(Array.get(left, index), Array.get(right, index))) {
                    return false;
                }
            }
            return true;
        }
        return Objects.deepEquals(left, right);
    }

    public void validatePaginationConfig(HttpSourceConfig config) {
        if (config == null) {
            return;
        }
        if (config.getRetryTimes() <= 0) {
            throw new IllegalArgumentException("HTTP retryTimes 必须是正整数: " + config.getRetryTimes());
        }
        if (config.getRetryIntervalMs() < 0) {
            throw new IllegalArgumentException("HTTP retryIntervalMs 不能小于 0: " + config.getRetryIntervalMs());
        }
        if (!config.isPageRead()) {
            return;
        }
        if (config.getPageSize() <= 0) {
            throw new IllegalArgumentException("HTTP 分页读取 pageSize 必须是正整数: " + config.getPageSize());
        }
        if (config.getMaxRepeatedPages() < 0) {
            throw new IllegalArgumentException("HTTP 分页读取 maxRepeatedPages 不能小于 0: "
                    + config.getMaxRepeatedPages());
        }
        String paginationTemplate = StringUtils.defaultString(config.getHeader())
                + '\n' + StringUtils.defaultString(config.getParams())
                + '\n' + StringUtils.defaultString(config.getRequestBody());
        if (!paginationTemplate.contains("{dyn_page}") && !paginationTemplate.contains("{dyn_offset}")) {
            throw new IllegalArgumentException(
                    "HTTP 分页读取已启用，但 header、params、requestBody 中未配置 {dyn_page} 或 {dyn_offset}");
        }
    }

    private String pushdownTarget(HttpPushdownPredicate predicate, String location) {
        if ("param".equals(location) || "query".equals(location)) {
            return "query." + firstText(predicate.getRequestParamName(), predicate.getField());
        }
        if ("header".equals(location)) {
            return "header." + firstText(predicate.getHeaderName(), predicate.getField()).toLowerCase(Locale.ENGLISH);
        }
        if ("body".equals(location)) {
            return "body." + firstText(predicate.getBodyPath(), predicate.getField());
        }
        if ("path".equals(location)) {
            return "path." + firstText(predicate.getPathVariable(), predicate.getField());
        }
        return location + "." + firstText(null, predicate.getField());
    }

    private void applyPaginationOverride(HttpSourceConfig config,
                                         HttpPushdownPredicate predicate,
                                         Object configuredValue,
                                         Object pushedValue,
                                         String target) {
        String template = configuredValue == null ? "" : String.valueOf(configuredValue);
        List<String> tokens = predicate == null || predicate.getPaginationTokens() == null
                ? Collections.emptyList()
                : predicate.getPaginationTokens();
        if (tokens.contains("PAGE_SIZE") || template.contains("{dyn_pageSize}")) {
            config.setPageSize(positiveInteger(pushedValue, target));
        }
        boolean pageOverride = tokens.contains("PAGE") || template.contains("{dyn_page}");
        boolean offsetOverride = tokens.contains("OFFSET") || template.contains("{dyn_offset}");
        if (pageOverride) {
            positiveInteger(pushedValue, target);
        }
        if (offsetOverride) {
            nonNegativeInteger(pushedValue, target);
        }
        if (pageOverride || offsetOverride) {
            config.setPageRead(false);
        }
    }

    private int positiveInteger(Object value, String target) {
        try {
            int result = strictInteger(value);
            if (result <= 0) {
                throw new NumberFormatException("not positive");
            }
            return result;
        } catch (Exception ex) {
            throw new IllegalArgumentException("HTTP 分页参数 " + target + " 必须是正整数: " + value, ex);
        }
    }

    private int nonNegativeInteger(Object value, String target) {
        try {
            int result = strictInteger(value);
            if (result < 0) {
                throw new NumberFormatException("negative");
            }
            return result;
        } catch (Exception ex) {
            throw new IllegalArgumentException("HTTP 分页参数 " + target + " 必须是非负整数: " + value, ex);
        }
    }

    private int strictInteger(Object value) {
        if (value instanceof Number) {
            return new BigDecimal(String.valueOf(value)).intValueExact();
        }
        return Integer.parseInt(String.valueOf(value).trim());
    }

    private Map<String, String> newHttpContent(HttpSourceConfig config) {
        Map<String, String> httpContent = new LinkedHashMap<>();
        httpContent.put(HttpSourceKeys.HEADER, StringUtils.defaultIfBlank(config.getHeader(), "{}"));
        httpContent.put(HttpSourceKeys.PARAMS, StringUtils.defaultIfBlank(config.getParams(), "{}"));
        httpContent.put(HttpSourceKeys.REQUEST_BODY, StringUtils.defaultString(config.getRequestBody()));
        return httpContent;
    }

    private void restorePushedLiterals(HttpSourceConfig config, Map<String, String> httpContent) {
        if (config == null || httpContent == null) {
            return;
        }
        for (Map.Entry<String, String> entry : httpContent.entrySet()) {
            entry.setValue(config.restorePushedLiterals(entry.getValue()));
        }
    }

    private ScanPageResult emitRows(HttpSourceConfig config,
                                    FastJsonMemory resultJson,
                                    Consumer<Map<String, Object>> rowConsumer,
                                    int remainingRows,
                                    Map<String, Object> requestContext,
                                    PageRepeatGuard pageRepeatGuard,
                                    int pageNum,
                                    String pageContent) {
        JsonExtractionResult extracted = detailRows(resultJson, config, remainingRows);
        JSONArray rows = extracted.rows();
        if (config.isPageRead() && pageRepeatGuard.observe(pageContent)) {
            throw AggregationException.asException(HttpSourceErrorCode.RESPONSE_PAGE_ERROR,
                    "HTTP 分页检测到重复的完整页或短页，已停止读取: page=" + pageNum
                            + ", maxRepeatedPages=" + config.getMaxRepeatedPages());
        }
        int emittedRows = 0;
        Set<String> physicalColumns = physicalColumnNames(config);
        for (Object row : rows) {
            if (remainingRows > 0 && emittedRows >= remainingRows) {
                break;
            }
            if (row instanceof JSONObject && rowConsumer != null) {
                Map<String, Object> emittedRow = new LinkedHashMap<>(((JSONObject) row).getInnerMap());
                if (requestContext != null) {
                    for (Map.Entry<String, Object> entry : requestContext.entrySet()) {
                        if (!physicalColumns.contains(entry.getKey())) {
                            emittedRow.putIfAbsent(entry.getKey(), entry.getValue());
                        }
                    }
                }
                rowConsumer.accept(emittedRow);
                emittedRows++;
            }
        }
        return new ScanPageResult(extracted.sourceRowCount(), emittedRows);
    }

    private Set<String> physicalColumnNames(HttpSourceConfig config) {
        Set<String> names = new LinkedHashSet<>();
        if (config != null && config.getColumns() != null) {
            for (ColumnEntry column : config.getColumns()) {
                if (column != null && StringUtils.isNotBlank(column.getName())) {
                    names.add(column.getName());
                }
            }
        }
        return names;
    }

    private Map<String, Object> requestContext(HttpSourceConfig config) {
        Map<String, Object> context = new LinkedHashMap<>();
        Map<String, Map<String, Object>> pushed = config.getPushedRequestContext();
        Map<String, Object> params = new LinkedHashMap<>();
        Map<String, Object> headers = new LinkedHashMap<>();
        Map<String, Object> body = new LinkedHashMap<>();

        mergeLocationValues(params, pushed.get("param"));
        mergeLocationValues(params, pushed.get("query"));
        mergeLocationValues(headers, pushed.get("header"));
        mergeLocationValues(body, pushed.get("body"));
        Map<String, Object> path = copyMap(pushed.get("path"));

        putLocationContext(context, "param", params);
        putLocationContext(context, "query", params);
        putLocationContext(context, "header", headers);
        putLocationContext(context, "body", body);
        putLocationContext(context, "path", path);
        addUniqueLeafValues(context, params, headers, body, path);
        return context;
    }

    private void putLocationContext(Map<String, Object> context, String location, Map<String, Object> values) {
        if (values != null && !values.isEmpty()) {
            context.put(location, new LinkedHashMap<>(values));
        }
    }

    private void mergeLocationValues(Map<String, Object> target, Map<String, Object> values) {
        if (values == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            putNestedValue(target, entry.getKey(), entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    private void putNestedValue(Map<String, Object> target, String path, Object value) {
        if (target == null || StringUtils.isBlank(path)) {
            return;
        }
        String[] parts = path.split("\\.");
        Map<String, Object> current = target;
        for (int index = 0; index < parts.length; index++) {
            String part = parts[index].trim();
            if (part.isEmpty()) {
                continue;
            }
            if (index == parts.length - 1) {
                current.put(part, value);
                return;
            }
            Object child = current.get(part);
            if (!(child instanceof Map<?, ?>)) {
                child = new LinkedHashMap<String, Object>();
                current.put(part, child);
            }
            current = (Map<String, Object>) child;
        }
    }

    private void addUniqueLeafValues(Map<String, Object> context, Map<String, Object>... locations) {
        Map<String, List<Object>> valuesByLeaf = new LinkedHashMap<>();
        for (Map<String, Object> location : locations) {
            collectLeafValues(location, valuesByLeaf);
        }
        for (Map.Entry<String, List<Object>> entry : valuesByLeaf.entrySet()) {
            if (entry.getValue().size() == 1) {
                context.putIfAbsent(entry.getKey(), entry.getValue().get(0));
            }
        }
    }

    private void collectLeafValues(Map<String, Object> source, Map<String, List<Object>> valuesByLeaf) {
        if (source == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map<?, ?>) {
                collectLeafValues(copyMap((Map<?, ?>) value), valuesByLeaf);
            } else {
                valuesByLeaf.computeIfAbsent(entry.getKey(), ignored -> new ArrayList<>()).add(value);
            }
        }
    }

    private Map<String, Object> deepCopyMap(Map<?, ?> source) {
        Map<String, Object> copy = new LinkedHashMap<>();
        if (source == null) {
            return copy;
        }
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            Object value = entry.getValue();
            copy.put(String.valueOf(entry.getKey()), value instanceof Map<?, ?>
                    ? deepCopyMap((Map<?, ?>) value)
                    : value);
        }
        return copy;
    }

    private Map<String, Object> copyMap(Map<?, ?> source) {
        return deepCopyMap(source);
    }

    private JsonExtractionResult detailRows(FastJsonMemory json, HttpSourceConfig config, int maxRows) {
        if (config.getColumns() == null || config.getColumns().isEmpty()) {
            return new JsonExtractionResult(new JSONArray(), 0);
        }
        try {
            return UnstructuredStorageReaderUtil.extractJson(json, config.getColumns(), config.getExtractionPlan(), maxRows);
        } catch (IllegalArgumentException ex) {
            if (config.getExtractionPlan() != null) {
                throw AggregationException.asException(HttpSourceErrorCode.RESULT_TO_JSON_ERROR,
                        "HTTP response data extraction failed: " + ex.getMessage());
            }
            throw AggregationException.asException(HttpSourceErrorCode.RESULT_TO_JSON_ERROR,
                    "HTTP response data extraction failed");
        } catch (Exception ex) {
            throw AggregationException.asException(HttpSourceErrorCode.RESULT_TO_JSON_ERROR,
                    "HTTP response data extraction failed");
        }
    }

    private FastJsonMemory parseResponseJson(String resultStr) {
        try {
            return new FastJsonMemory(resultStr);
        } catch (Exception ex) {
            throw AggregationException.asException(HttpSourceErrorCode.RESULT_TO_JSON_ERROR,
                    "HTTP response JSON parse failed");
        }
    }

    private boolean hasRemainingRows(int maxRows, int emittedRows) {
        return maxRows <= 0 || emittedRows < maxRows;
    }

    private int remainingRows(int maxRows, int emittedRows) {
        return maxRows <= 0 ? 0 : Math.max(maxRows - emittedRows, 0);
    }

    private boolean checkPage(HttpSourceConfig config, Integer pageNum, Integer recordNum, FastJsonMemory jsonUtil) {
        if (StringUtils.isNotBlank(config.getTotalCodePath())) {
            Object totalObj = jsonUtil.get(config.getTotalCodePath());
            if (totalObj == null) {
                throw AggregationException.asException(HttpSourceErrorCode.RESPONSE_PAGE_ERROR,
                        "获取返回数据总量值失败:获取路径[" + config.getTotalCodePath() + "],实际返回:[null]");
            }
            long total;
            try {
                total = parseTotalCount(totalObj);
            } catch (IllegalArgumentException ex) {
                throw AggregationException.asException(HttpSourceErrorCode.RESPONSE_PAGE_ERROR,
                        "HTTP 分页总量配置无效: path=" + config.getTotalCodePath() + ", " + ex.getMessage());
            }
            long totalPage = total % config.getPageSize() == 0 ? total / config.getPageSize() : total / config.getPageSize() + 1;
            return pageNum < totalPage;
        }
        return recordNum.equals(config.getPageSize());
    }

    String executeRequest(HttpSourceConfig config, HttpUtils httpUtils) {
        final Map<String, String> result;
        try {
            result = httpUtils.exec();
        } catch (HttpRequestConfigurationException ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        } catch (Exception ex) {
            throw new RetryableHttpRequestException("HTTP request execution failed", ex);
        }
        String responseBody = result.get("result");
        int statusCode = parseStatusCode(result.get("statusCode"));
        if (statusCode >= 200 && statusCode < 300) {
            return responseBody;
        }
        String faultMessage = config.isSoapMode() ? SoapToJSON.faultMessage(responseBody) : null;
        if (StringUtils.isNotBlank(faultMessage)) {
            throw AggregationException.asException(HttpSourceErrorCode.RESPONSE_STATUS_ERROR,
                    "SOAP Fault returned by remote service");
        }
        if (isRetryableStatus(statusCode)) {
            throw new RetryableHttpRequestException("HTTP request failed with status " + statusCode);
        }
        throw AggregationException.asException(HttpSourceErrorCode.RESPONSE_STATUS_ERROR,
                "HTTP request failed with status " + statusCode);
    }

    private int requestAttempts(HttpSourceConfig config) {
        if (config.getRetryTimes() <= 1) {
            return 1;
        }
        if (config.isRetryNonIdempotent()) {
            return config.getRetryTimes();
        }
        if (config.isSoapMode()) {
            return 1;
        }
        String method = StringUtils.defaultString(config.getMode()).trim().toUpperCase(Locale.ENGLISH);
        return "GET".equals(method)
                || "HEAD".equals(method)
                || "OPTIONS".equals(method)
                || "TRACE".equals(method)
                || "PUT".equals(method)
                || "DELETE".equals(method)
                ? config.getRetryTimes()
                : 1;
    }

    private void configureProtocolHeaders(HttpUtils httpUtils,
                                          HttpSourceConfig config,
                                          Map<String, String> headerMap) {
        if (!config.isSoapMode()) {
            httpUtils.setHeader("Content-Type", config.getContentType());
            return;
        }
        if (config.isSoap12()) {
            httpUtils.setHeader("Content-Type", soap12ContentType(config.getContentType(), config.getSoapAction()));
            httpUtils.removeHeader("SOAPAction");
            return;
        }
        httpUtils.setHeader("Content-Type", config.getContentType());
        if (StringUtils.isNotBlank(config.getSoapAction())
                && !containsHeaderIgnoreCase(headerMap, "SOAPAction")) {
            httpUtils.setHeader("SOAPAction", config.getSoapAction());
        }
    }

    private String soap12ContentType(String configuredContentType, String soapAction) {
        String contentType = StringUtils.defaultIfBlank(configuredContentType,
                "application/soap+xml;charset=UTF-8").trim();
        if (!contentType.toLowerCase(Locale.ENGLISH).startsWith("application/soap+xml")) {
            contentType = "application/soap+xml;charset=UTF-8";
        }
        if (StringUtils.isBlank(soapAction)) {
            return contentType;
        }
        String withoutAction = SOAP_ACTION_PARAMETER.matcher(contentType).replaceAll("").trim();
        String quotedAction = soapAction.replace("\\", "\\\\").replace("\"", "\\\"");
        return withoutAction + "; action=\"" + quotedAction + "\"";
    }

    private String headerValueIgnoreCase(Map<String, String> headers, String headerName) {
        String existingName = findHeaderNameIgnoreCase(headers, headerName);
        return existingName == null ? null : headers.get(existingName);
    }

    public void validatePathVariablesBound(String url) {
        String path = pathPart(url);
        Set<String> unboundVariables = new LinkedHashSet<>();
        Matcher curlyMatcher = CURLY_PATH_VARIABLE.matcher(path);
        while (curlyMatcher.find()) {
            unboundVariables.add(curlyMatcher.group(1));
        }
        Matcher colonMatcher = COLON_PATH_VARIABLE.matcher(path);
        while (colonMatcher.find()) {
            unboundVariables.add(colonMatcher.group(1));
        }
        if (!unboundVariables.isEmpty()) {
            throw new IllegalArgumentException("HTTP 请求路径变量未绑定: "
                    + String.join(", ", unboundVariables));
        }
    }

    private String pathPart(String url) {
        return splitUrlPath(url).path;
    }

    private String convertResponseIfNeeded(HttpSourceConfig config, String resultStr) {
        if (StringUtils.isBlank(resultStr)) {
            return resultStr;
        }
        if (config.isSoapMode() && config.isSoapFaultFail()) {
            String faultMessage = SoapToJSON.faultMessage(resultStr);
            if (StringUtils.isNotBlank(faultMessage)) {
                throw AggregationException.asException(HttpSourceErrorCode.RESPONSE_STATUS_ERROR,
                        "SOAP Fault returned by remote service");
            }
        }
        if ("xml".equalsIgnoreCase(config.getResultType())) {
            try {
                return JSONObject.toJSONString(XmlToJSON.toJson(resultStr));
            } catch (Exception e) {
                throw AggregationException.asException(HttpSourceErrorCode.RESULT_TO_JSON_ERROR,
                        "XML response parse failed");
            }
        }
        if ("soap".equalsIgnoreCase(config.getResultType())) {
            try {
                return JSONObject.toJSONString(SoapToJSON.toJson(resultStr));
            } catch (Exception e) {
                throw AggregationException.asException(HttpSourceErrorCode.RESULT_TO_JSON_ERROR,
                        "SOAP response parse failed");
            }
        }
        return resultStr;
    }

    private void validateResponseStatus(HttpSourceConfig config, String resultStr) {
        Map<String, Object> responseStatus = config.getResponseStatus();
        if (responseStatus == null || responseStatus.isEmpty()) {
            return;
        }
        FastJsonMemory resultObj = parseResponseJson(resultStr);
        Object pathValue = resultObj.get(String.valueOf(responseStatus.get("path")));
        Object codeValue = responseStatus.get("code");
        if (pathValue == null || codeValue == null) {
            throw AggregationException.asException(HttpSourceErrorCode.REQUIRED_VALUE, "状态码path和code为不能为空！");
        }
        String resultCode = String.valueOf(pathValue);
        if (!resultCode.equalsIgnoreCase(String.valueOf(codeValue))) {
            throw AggregationException.asException(HttpSourceErrorCode.RESPONSE_STATUS_ERROR,
                    "业务成功状态码：[" + codeValue + "],请求返回状态码：[" + resultCode + "]");
        }
    }

    private JSONObject parseObject(String value, String label) {
        if (StringUtils.isBlank(value)) {
            return new JSONObject(true);
        }
        Object parsed = JSONObject.parse(value);
        if (parsed instanceof JSONObject) {
            return (JSONObject) parsed;
        }
        throw new IllegalArgumentException("HTTP " + label + " must be a JSON object string");
    }

    private JSONObject parseBodyObject(String value) {
        if (StringUtils.isBlank(value)) {
            return new JSONObject(true);
        }
        final Object parsed;
        try {
            parsed = JSONObject.parse(value);
        } catch (Exception ex) {
            throw new IllegalArgumentException("HTTP body 下推仅支持 JSON Object 请求体", ex);
        }
        if (parsed instanceof JSONObject) {
            return (JSONObject) parsed;
        }
        throw new IllegalArgumentException("HTTP body 下推仅支持 JSON Object 请求体");
    }

    private BodyUpdateResult applyBodyValue(HttpSourceConfig config, String body, String path, Object value) {
        String text = StringUtils.defaultString(body).trim();
        if (text.isEmpty() || text.startsWith("{")) {
            JSONObject object = parseBodyObject(text);
            List<JsonPathSegment> segments = jsonPathSegments(path);
            Object configuredValue = getPath(object, segments, path);
            putPath(object, segments, path, config.protectJsonPushedValue(value));
            return new BodyUpdateResult(object.toJSONString(), configuredValue);
        }
        if (text.startsWith("<")) {
            return applyXmlBodyValue(config, text, path, value);
        }
        throw new IllegalArgumentException("HTTP body 下推仅支持 JSON Object 或 XML 请求体");
    }

    private BodyUpdateResult applyXmlBodyValue(HttpSourceConfig config, String xml, String path, Object value) {
        try {
            DocumentBuilderFactory factory = secureDocumentBuilderFactory();
            Document document = factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
            List<Element> matches = findXmlElements(document.getDocumentElement(), path);
            if (matches.isEmpty()) {
                throw new IllegalArgumentException("HTTP XML body 下推路径 " + path + " 不存在");
            }
            if (matches.size() > 1) {
                throw new IllegalArgumentException("HTTP XML body 下推路径 " + path
                        + " 匹配到多个节点，请使用完整 XML 节点路径");
            }
            Element target = matches.get(0);
            String configuredValue = target.getTextContent();
            Object protectedValue = config.protectXmlPushedValue(value);
            target.setTextContent(protectedValue == null ? "" : String.valueOf(protectedValue));
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
                    xml.startsWith("<?xml") ? "no" : "yes");
            transformer.setOutputProperty(OutputKeys.ENCODING, requestBodyCharset(config).name());
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(document), new StreamResult(writer));
            return new BodyUpdateResult(writer.toString(), configuredValue);
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalArgumentException("HTTP XML body 下推失败: " + ex.getMessage(), ex);
        }
    }

    private DocumentBuilderFactory secureDocumentBuilderFactory() throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setExpandEntityReferences(false);
        return factory;
    }

    private Charset requestBodyCharset(HttpSourceConfig config) {
        String contentType = config == null ? null : config.getContentType();
        Matcher matcher = CONTENT_TYPE_CHARSET.matcher(StringUtils.defaultString(contentType));
        if (!matcher.find()) {
            return StandardCharsets.UTF_8;
        }
        String charset = StringUtils.defaultIfBlank(matcher.group(1), matcher.group(2));
        try {
            return Charset.forName(charset);
        } catch (Exception ex) {
            throw new IllegalArgumentException("HTTP Content-Type charset 配置无效", ex);
        }
    }

    private List<Element> findXmlElements(Element root, String path) {
        List<XmlPathSegment> expectedPath = pathSegments(path);
        if (expectedPath.isEmpty()) {
            return Collections.emptyList();
        }
        List<Element> matches = new ArrayList<>();
        collectXmlMatches(root, new ArrayList<>(), expectedPath, matches);
        return matches;
    }

    private void collectXmlMatches(Element element,
                                   List<Element> parentPath,
                                   List<XmlPathSegment> expectedPath,
                                   List<Element> matches) {
        List<Element> currentPath = new ArrayList<>(parentPath);
        currentPath.add(element);
        if (endsWithPath(currentPath, expectedPath)) {
            matches.add(element);
        }
        NodeList nodes = element.getChildNodes();
        for (int index = 0; index < nodes.getLength(); index++) {
            Node node = nodes.item(index);
            if (node instanceof Element) {
                collectXmlMatches((Element) node, currentPath, expectedPath, matches);
            }
        }
    }

    private boolean endsWithPath(List<Element> actual, List<XmlPathSegment> expected) {
        if (actual.size() < expected.size()) {
            return false;
        }
        int offset = actual.size() - expected.size();
        for (int index = 0; index < expected.size(); index++) {
            if (!expected.get(index).matches(actual.get(offset + index))) {
                return false;
            }
        }
        return true;
    }

    private List<XmlPathSegment> pathSegments(String path) {
        List<XmlPathSegment> result = new ArrayList<>();
        String text = StringUtils.defaultString(path);
        StringBuilder segment = new StringBuilder();
        boolean inClarkNamespace = false;
        for (int index = 0; index <= text.length(); index++) {
            char current = index == text.length() ? '.' : text.charAt(index);
            if (current == '{' && segment.length() == 0) {
                inClarkNamespace = true;
            } else if (current == '}' && inClarkNamespace) {
                inClarkNamespace = false;
            }
            if (current == '.' && !inClarkNamespace) {
                String value = segment.toString().trim();
                if (!value.isEmpty()) {
                    result.add(XmlPathSegment.parse(value));
                }
                segment.setLength(0);
            } else {
                segment.append(current);
            }
        }
        return result;
    }

    private String xmlLocalName(Element element) {
        String local = element.getLocalName();
        return StringUtils.isNotBlank(local) ? local : element.getTagName().replaceFirst("^.*:", "");
    }

    private List<JsonPathSegment> jsonPathSegments(String path) {
        String text = StringUtils.defaultString(path).trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException("HTTP JSON body 下推路径不能为空");
        }
        List<JsonPathSegment> result = new ArrayList<>();
        int index = 0;
        while (index < text.length()) {
            char current = text.charAt(index);
            if (current == '.') {
                index++;
                continue;
            }
            if (current == '[') {
                int close = text.indexOf(']', index + 1);
                if (close < 0) {
                    throw invalidJsonArrayPath(path);
                }
                String value = text.substring(index + 1, close).trim();
                if (!isNonNegativeInteger(value)) {
                    throw invalidJsonArrayPath(path);
                }
                result.add(JsonPathSegment.index(Integer.parseInt(value)));
                index = close + 1;
                continue;
            }
            int end = index;
            while (end < text.length() && text.charAt(end) != '.' && text.charAt(end) != '[') {
                if (text.charAt(end) == ']') {
                    throw invalidJsonArrayPath(path);
                }
                end++;
            }
            String value = text.substring(index, end).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException("HTTP JSON body 下推路径格式无效: " + path);
            }
            result.add(JsonPathSegment.property(value));
            index = end;
        }
        if (result.isEmpty() || result.get(0).isIndex()) {
            throw new IllegalArgumentException("HTTP JSON body 下推仅支持 JSON Object 根节点: " + path);
        }
        return result;
    }

    private boolean isNonNegativeInteger(String value) {
        if (StringUtils.isBlank(value)) {
            return false;
        }
        for (int index = 0; index < value.length(); index++) {
            if (!Character.isDigit(value.charAt(index))) {
                return false;
            }
        }
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private IllegalArgumentException invalidJsonArrayPath(String path) {
        return new IllegalArgumentException("HTTP JSON body 数组路径必须使用非负整数索引，例如 filters[0].customer_id: " + path);
    }

    private Object getPath(JSONObject root, List<JsonPathSegment> segments, String path) {
        if (root == null || segments.isEmpty()) {
            return null;
        }
        Object current = root;
        for (JsonPathSegment segment : segments) {
            if (current instanceof JSONObject) {
                if (segment.isIndex()) {
                    throw jsonPathTypeMismatch(path, "对象节点后不能直接使用数组索引");
                }
                current = ((JSONObject) current).get(segment.getProperty());
            } else if (current instanceof JSONArray) {
                int index = segment.arrayIndex(path);
                JSONArray array = (JSONArray) current;
                if (index >= array.size()) {
                    throw jsonArrayIndexOutOfBounds(path, index, array.size());
                }
                current = array.get(index);
            } else {
                return null;
            }
        }
        return current;
    }

    private void putPath(JSONObject root, List<JsonPathSegment> segments, String path, Object value) {
        Object current = root;
        for (int index = 0; index < segments.size(); index++) {
            JsonPathSegment segment = segments.get(index);
            boolean last = index == segments.size() - 1;
            if (current instanceof JSONObject) {
                if (segment.isIndex()) {
                    throw jsonPathTypeMismatch(path, "对象节点后不能直接使用数组索引");
                }
                JSONObject object = (JSONObject) current;
                if (last) {
                    object.put(segment.getProperty(), value);
                    return;
                }
                Object child = object.get(segment.getProperty());
                if (child == null) {
                    JsonPathSegment next = segments.get(index + 1);
                    if (next.isIndex()) {
                        throw new IllegalArgumentException("HTTP JSON body 下推数组节点不存在，不能自动创建数组: " + path);
                    }
                    child = new JSONObject(true);
                    object.put(segment.getProperty(), child);
                } else if (!(child instanceof JSONObject) && !(child instanceof JSONArray)) {
                    throw jsonPathTypeMismatch(path, "中间节点不是对象或数组");
                }
                current = child;
            } else if (current instanceof JSONArray) {
                JSONArray array = (JSONArray) current;
                int arrayIndex = segment.arrayIndex(path);
                if (arrayIndex >= array.size()) {
                    throw jsonArrayIndexOutOfBounds(path, arrayIndex, array.size());
                }
                if (last) {
                    array.set(arrayIndex, value);
                    return;
                }
                Object child = array.get(arrayIndex);
                if (!(child instanceof JSONObject) && !(child instanceof JSONArray)) {
                    throw jsonPathTypeMismatch(path, "数组元素不是对象或数组");
                }
                current = child;
            } else {
                throw jsonPathTypeMismatch(path, "中间节点不是对象或数组");
            }
        }
    }

    private IllegalArgumentException jsonPathTypeMismatch(String path, String reason) {
        return new IllegalArgumentException("HTTP JSON body 下推路径类型不匹配: " + path + "，" + reason);
    }

    private IllegalArgumentException jsonArrayIndexOutOfBounds(String path, int index, int size) {
        return new IllegalArgumentException("HTTP JSON body 下推数组索引越界: " + path
                + "，index=" + index + "，size=" + size);
    }

    public long parseTotalCount(Object totalObj) {
        if (totalObj == null) {
            throw new IllegalArgumentException("总量节点不存在");
        }
        try {
            long total = new BigDecimal(String.valueOf(totalObj).trim()).longValueExact();
            if (total < 0L) {
                throw new ArithmeticException("negative total");
            }
            return total;
        } catch (NumberFormatException | ArithmeticException ex) {
            throw new IllegalArgumentException("总量必须是非负整数且不能超过 long 范围", ex);
        }
    }

    private static final class JsonPathSegment {
        private final String property;
        private final Integer index;

        private JsonPathSegment(String property, Integer index) {
            this.property = property;
            this.index = index;
        }

        private static JsonPathSegment property(String property) {
            return new JsonPathSegment(property, null);
        }

        private static JsonPathSegment index(int index) {
            return new JsonPathSegment(null, index);
        }

        private boolean isIndex() {
            return index != null;
        }

        private String getProperty() {
            return property;
        }

        private int arrayIndex(String path) {
            if (index != null) {
                return index;
            }
            if (StringUtils.isNotBlank(property)) {
                for (int offset = 0; offset < property.length(); offset++) {
                    if (!Character.isDigit(property.charAt(offset))) {
                        throw new IllegalArgumentException("HTTP JSON body 数组索引必须是非负整数: " + path);
                    }
                }
                try {
                    return Integer.parseInt(property);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("HTTP JSON body 数组索引超出整数范围: " + path, ex);
                }
            }
            throw new IllegalArgumentException("HTTP JSON body 数组索引不能为空: " + path);
        }
    }

    private Object valueFor(HttpPushdownPredicate predicate) {
        List<Object> values = predicate.getValues();
        if (values == null || values.isEmpty()) {
            return null;
        }
        if (values.size() == 1 && !"IN".equalsIgnoreCase(predicate.getOperator())) {
            return values.get(0);
        }
        JSONArray array = new JSONArray();
        array.addAll(values);
        return array;
    }

    private String replacePathVariable(String url, String name, String value) {
        if (StringUtils.isBlank(url) || StringUtils.isBlank(name)) {
            return url;
        }
        UrlPathParts parts = splitUrlPath(url);
        String encodedValue = encodePathSegment(value);
        String replaced = parts.path.replace("{" + name + "}", encodedValue);
        Matcher matcher = pathVariablePattern(name).matcher(replaced);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group(1) + encodedValue));
        }
        matcher.appendTail(result);
        return parts.prefix + result + parts.suffix;
    }

    private String encodePathSegment(String value) {
        byte[] bytes = StringUtils.defaultString(value).getBytes(StandardCharsets.UTF_8);
        StringBuilder encoded = new StringBuilder(bytes.length);
        for (byte item : bytes) {
            int current = item & 0xff;
            if ((current >= 'a' && current <= 'z')
                    || (current >= 'A' && current <= 'Z')
                    || (current >= '0' && current <= '9')
                    || current == '-' || current == '.' || current == '_' || current == '~') {
                encoded.append((char) current);
            } else {
                encoded.append('%');
                String hex = Integer.toHexString(current).toUpperCase(Locale.ENGLISH);
                if (hex.length() == 1) {
                    encoded.append('0');
                }
                encoded.append(hex);
            }
        }
        return encoded.toString();
    }

    private boolean containsPathVariable(String url, String name) {
        String path = splitUrlPath(url).path;
        return StringUtils.isNotBlank(path)
                && StringUtils.isNotBlank(name)
                && (path.contains("{" + name + "}") || pathVariablePattern(name).matcher(path).find());
    }

    private Pattern pathVariablePattern(String name) {
        return Pattern.compile("(^|/)" + Pattern.quote(":" + name) + "(?![A-Za-z0-9_])");
    }

    private UrlPathParts splitUrlPath(String url) {
        String value = StringUtils.defaultString(url);
        int suffixStart = value.length();
        int queryIndex = value.indexOf('?');
        int fragmentIndex = value.indexOf('#');
        if (queryIndex >= 0) {
            suffixStart = Math.min(suffixStart, queryIndex);
        }
        if (fragmentIndex >= 0) {
            suffixStart = Math.min(suffixStart, fragmentIndex);
        }
        int schemeIndex = value.indexOf("://");
        int pathStart = 0;
        if (schemeIndex >= 0) {
            pathStart = value.indexOf('/', schemeIndex + 3);
            if (pathStart < 0 || pathStart > suffixStart) {
                pathStart = suffixStart;
            }
        }
        return new UrlPathParts(value.substring(0, pathStart),
                value.substring(pathStart, suffixStart), value.substring(suffixStart));
    }

    private String normalizeLocation(String value) {
        return value == null ? "" : value.trim().toLowerCase();
    }

    private String firstText(String first, String second) {
        return StringUtils.isNotBlank(first) ? first.trim() : StringUtils.defaultString(second).trim();
    }

    private boolean containsHeaderIgnoreCase(Map<String, String> headerMap, String headerName) {
        if (headerMap == null || headerName == null) {
            return false;
        }
        for (String key : headerMap.keySet()) {
            if (headerName.equalsIgnoreCase(key)) {
                return true;
            }
        }
        return false;
    }

    private String findHeaderNameIgnoreCase(Map<String, ?> headers, String headerName) {
        if (headers == null || headerName == null) {
            return null;
        }
        for (String key : headers.keySet()) {
            if (headerName.equalsIgnoreCase(key)) {
                return key;
            }
        }
        return null;
    }

    private void putHeaderIgnoreCase(JSONObject headers, String headerName, Object value) {
        String existingName = findHeaderNameIgnoreCase(headers, headerName);
        List<String> duplicateNames = new ArrayList<>();
        for (String key : headers.keySet()) {
            if (headerName.equalsIgnoreCase(key)) {
                duplicateNames.add(key);
            }
        }
        for (String duplicateName : duplicateNames) {
            headers.remove(duplicateName);
        }
        headers.put(existingName == null ? headerName : existingName, value);
    }

    private int parseStatusCode(String value) {
        try {
            return Integer.parseInt(StringUtils.defaultString(value).trim());
        } catch (NumberFormatException ex) {
            throw new RetryableHttpRequestException("HTTP response status is invalid");
        }
    }

    private boolean isRetryableStatus(int statusCode) {
        return statusCode == 408 || statusCode == 425 || statusCode == 429 || statusCode >= 500;
    }

    private static final class XmlPathSegment {
        private final String namespaceUri;
        private final String prefix;
        private final String localName;

        private XmlPathSegment(String namespaceUri, String prefix, String localName) {
            this.namespaceUri = namespaceUri;
            this.prefix = prefix;
            this.localName = localName;
        }

        private static XmlPathSegment parse(String value) {
            if (value.startsWith("{")) {
                int closeBrace = value.indexOf('}');
                if (closeBrace <= 1 || closeBrace == value.length() - 1) {
                    throw new IllegalArgumentException("Invalid Clark notation XML path segment: " + value);
                }
                return new XmlPathSegment(value.substring(1, closeBrace), null,
                        value.substring(closeBrace + 1));
            }
            int colon = value.indexOf(':');
            if (colon > 0 && colon < value.length() - 1) {
                return new XmlPathSegment(null, value.substring(0, colon), value.substring(colon + 1));
            }
            return new XmlPathSegment(null, null, value);
        }

        private boolean matches(Element element) {
            String actualLocalName = StringUtils.defaultIfBlank(element.getLocalName(),
                    element.getTagName().replaceFirst("^.*:", ""));
            if (!localName.equals(actualLocalName)) {
                return false;
            }
            String actualNamespace = StringUtils.defaultString(element.getNamespaceURI());
            if (namespaceUri != null) {
                return namespaceUri.equals(actualNamespace);
            }
            if (prefix != null) {
                String resolvedNamespace = element.lookupNamespaceURI(prefix);
                return resolvedNamespace != null && resolvedNamespace.equals(actualNamespace);
            }
            return true;
        }
    }

    private static final class RetryableHttpRequestException extends RuntimeException {
        private RetryableHttpRequestException(String message) {
            super(message);
        }

        private RetryableHttpRequestException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final class UrlPathParts {
        private final String prefix;
        private final String path;
        private final String suffix;

        private UrlPathParts(String prefix, String path, String suffix) {
            this.prefix = prefix;
            this.path = path;
            this.suffix = suffix;
        }
    }

    private static final class ScanPageResult {
        private final int fetchedRows;
        private final int emittedRows;

        private ScanPageResult(int fetchedRows, int emittedRows) {
            this.fetchedRows = fetchedRows;
            this.emittedRows = emittedRows;
        }

        private int getFetchedRows() {
            return fetchedRows;
        }

        private int getEmittedRows() {
            return emittedRows;
        }
    }

    private static final class BodyUpdateResult {
        private final String body;
        private final Object configuredValue;

        private BodyUpdateResult(String body, Object configuredValue) {
            this.body = body;
            this.configuredValue = configuredValue;
        }

        private String getBody() {
            return body;
        }

        private Object getConfiguredValue() {
            return configuredValue;
        }
    }
}
