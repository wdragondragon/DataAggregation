package com.jdragon.aggregation.datasource.http;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.plugin.httpdyn.util.HttpReadTimeoutSession;
import com.jdragon.aggregation.plugin.httpdyn.util.HttpUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class HttpSourceScannerTest {
    private HttpServer server;
    private ExecutorService executor;

    @After
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void readTimeoutShouldKeepExistingNamesAndNormalizeTheConfiguredValue() {
        Map<String, Object> rawConfig = offlineConfig("/timeout-config", "GET");
        assertEquals(10000, HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)).getReadTimeout());

        rawConfig.put("socketTimeout", 25000);
        HttpSourceConfig alias = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));
        assertEquals(25000, alias.getReadTimeout());
        assertEquals(25000, alias.toMap().get("readTimeout"));
        assertEquals(25000, HttpSourceConfig.fromConfiguration(Configuration.from(alias.toMap())).getReadTimeout());

        rawConfig.put("readTimeout", 40000);
        assertEquals(40000, HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)).getReadTimeout());
        rawConfig.put("readTimeout", 0);
        assertEquals(10000, HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)).getReadTimeout());
        rawConfig.put("readTimeout", 90000);
        assertEquals(60000, HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)).getReadTimeout());
    }

    @Test
    public void readTimeoutShouldRetryPostIndependentlyOfFixedRetryCountAndKeepSuccessfulTimeout() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            if (requests.incrementAndGet() == 1) {
                Thread.sleep(150L);
            }
            writeJson(exchange, "{\"data\":{\"customer_id\":\"C100\",\"risk_score\":100}}");
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/timeout-post", "data");
        rawConfig.put(HttpSourceKeys.MODE, "POST");
        rawConfig.put(HttpSourceKeys.RETRY_TIMES, 1);
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));
        HttpReadTimeoutSession session = new HttpReadTimeoutSession(40);
        HttpSourceScanner scanner = new HttpSourceScanner();

        String first = scanner.connectToGetData(config, requestContent(), 1, 0L, session);
        assertTrue(first.contains("C100"));
        assertEquals(2, requests.get());
        assertEquals(10040, session.getReadTimeoutMs());

        String second = scanner.connectToGetData(config, requestContent(), 2, 2L, session);
        assertTrue(second.contains("C100"));
        assertEquals(3, requests.get());
        assertEquals(10040, session.getReadTimeoutMs());
    }

    @Test
    public void scanShouldReuseTimeoutAcrossPagesAndResetItForEachScan() {
        List<Integer> seenTimeouts = new ArrayList<>();
        HttpSourceScanner scanner = new HttpSourceScanner() {
            @Override
            public String connectToGetData(HttpSourceConfig config, Map<String, String> httpContent,
                                            Integer pageNum, Long offset, HttpReadTimeoutSession session) {
                seenTimeouts.add(session.getReadTimeoutMs());
                if (pageNum == 1) {
                    session.increaseAfterReadTimeout();
                    return "{\"code\":\"200\",\"data\":{\"total\":3,\"items\":["
                            + "{\"customer_id\":\"C101\",\"risk_score\":1},"
                            + "{\"customer_id\":\"C102\",\"risk_score\":2}]}}";
                }
                return "{\"code\":\"200\",\"data\":{\"total\":3,\"items\":["
                        + "{\"customer_id\":\"C103\",\"risk_score\":3}]}}";
            }
        };
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(offlineConfig("/pages", "GET")));
        List<Map<String, Object>> firstRows = new ArrayList<>();
        List<Map<String, Object>> secondRows = new ArrayList<>();

        scanner.scan(config, firstRows::add);
        scanner.scan(config, secondRows::add);

        assertEquals(Arrays.asList(10000, 20000, 10000, 20000), seenTimeouts);
        assertEquals(3, firstRows.size());
        assertEquals(3, secondRows.size());
    }

    @Test
    public void readTimeoutAtSixtySecondsShouldStopWithoutRestartingFixedRetries() {
        HttpReadTimeoutSession session = new HttpReadTimeoutSession(10000);
        List<Integer> attemptedTimeouts = new ArrayList<>();
        HttpSourceScanner scanner = new HttpSourceScanner() {
            @Override
            String executeRequest(HttpSourceConfig config, HttpUtils httpUtils) {
                attemptedTimeouts.add(session.getReadTimeoutMs());
                throw new IllegalStateException("secret-response", new SocketTimeoutException("Read timed out secret-response"));
            }
        };
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(
                offlineConfig("/timeout-limit?token=secret-token", "GET")));
        Map<String, String> content = requestContent();
        content.put(HttpSourceKeys.HEADER, "{\"Authorization\":\"Bearer secret-header\"}");
        content.put(HttpSourceKeys.REQUEST_BODY, "secret-body");
        Logger logger = (Logger) LoggerFactory.getLogger(HttpSourceScanner.class);
        Level previousLevel = logger.getLevel();
        logger.setLevel(Level.WARN);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            RuntimeException error = assertThrows(RuntimeException.class,
                    () -> scanner.connectToGetData(config, content, 1, 0L, session));

            assertEquals(Arrays.asList(10000, 20000, 30000, 40000, 50000, 60000), attemptedTimeouts);
            assertEquals(60000, session.getReadTimeoutMs());
            assertTrue(exceptionMessages(error).contains("Read timed out"));
            assertEquals(6, appender.list.size());
            String firstRetry = appender.list.get(0).getFormattedMessage();
            assertTrue(firstRetry.contains("errorType=SocketTimeoutException"));
            assertTrue(firstRetry.contains("currentTimeoutMs=10000"));
            assertTrue(firstRetry.contains("nextTimeoutMs=20000"));
            assertTrue(appender.list.get(5).getFormattedMessage().contains("readTimeoutMs=60000; stopping retries"));
            for (ILoggingEvent event : appender.list) {
                assertFalse(event.getFormattedMessage().contains("secret-"));
                assertTrue(event.getThrowableProxy() == null);
            }
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previousLevel);
            appender.stop();
        }
    }

    @Test
    public void interruptedReadFailureShouldStopBeforeIncreasingTimeoutOrRetrying() {
        AtomicInteger requests = new AtomicInteger();
        HttpReadTimeoutSession session = new HttpReadTimeoutSession(10000);
        HttpSourceScanner scanner = new HttpSourceScanner() {
            @Override
            String executeRequest(HttpSourceConfig config, HttpUtils httpUtils) {
                requests.incrementAndGet();
                Thread.currentThread().interrupt();
                throw new IllegalStateException("synthetic read failure", new SocketTimeoutException("Read timed out"));
            }
        };
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(offlineConfig("/interrupted", "GET")));

        try {
            assertThrows(RuntimeException.class,
                    () -> scanner.connectToGetData(config, requestContent(), 1, 0L, session));
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(1, requests.get());
            assertEquals(10000, session.getReadTimeoutMs());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void mixedFailuresShouldKeepFixedNonTimeoutBudgetWithoutResettingTimeout() throws Exception {
        AtomicInteger httpRequests = new AtomicInteger();
        startServer(exchange -> {
            httpRequests.incrementAndGet();
            write(exchange, 503, "temporary");
        });
        HttpReadTimeoutSession session = new HttpReadTimeoutSession(10000);
        List<Integer> attemptedTimeouts = new ArrayList<>();
        HttpSourceScanner scanner = new HttpSourceScanner() {
            @Override
            String executeRequest(HttpSourceConfig config, HttpUtils httpUtils) {
                attemptedTimeouts.add(session.getReadTimeoutMs());
                if (attemptedTimeouts.size() == 1 || attemptedTimeouts.size() == 3) {
                    throw new IllegalStateException("synthetic read failure", new SocketTimeoutException("Read timed out"));
                }
                return super.executeRequest(config, httpUtils);
            }
        };
        Map<String, Object> rawConfig = jsonShapeConfig("/mixed-failures", "data");
        rawConfig.put(HttpSourceKeys.RETRY_TIMES, 3);
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));

        RuntimeException error = assertThrows(RuntimeException.class,
                () -> scanner.connectToGetData(config, requestContent(), 1, 0L, session));

        assertEquals(Arrays.asList(10000, 20000, 20000, 30000, 30000), attemptedTimeouts);
        assertEquals(3, httpRequests.get());
        assertEquals(30000, session.getReadTimeoutMs());
        assertTrue(exceptionMessages(error).contains("status 503"));
    }

    private Map<String, String> requestContent() {
        Map<String, String> content = new LinkedHashMap<>();
        content.put(HttpSourceKeys.HEADER, "{}");
        content.put(HttpSourceKeys.PARAMS, "{}");
        content.put(HttpSourceKeys.REQUEST_BODY, "{}");
        return content;
    }

    @Test
    public void scanShouldApplyQueryHeaderBodyAndPathPushdownAcrossPages() throws Exception {
        List<String> seenRequests = new ArrayList<>();
        startServer(exchange -> {
            String path = exchange.getRequestURI().getPath();
            String query = exchange.getRequestURI().getRawQuery();
            String header = exchange.getRequestHeaders().getFirst("X-Customer");
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            seenRequests.add(path + "?" + query + "|" + header + "|" + body);
            if (query.contains("page=1")) {
                writeJson(exchange, "{\"code\":\"200\",\"data\":{\"total\":3,\"items\":[{\"customer_id\":\"C001\",\"risk_score\":91},{\"customer_id\":\"C001\",\"risk_score\":87}]}}");
            } else {
                writeJson(exchange, "{\"code\":\"200\",\"data\":{\"total\":3,\"items\":[{\"customer_id\":\"C001\",\"risk_score\":83}]}}");
            }
        });
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(baseConfig("/customers/{customer_id}", "POST")));

        scanner.applyPushdown(config, Arrays.asList(
                predicate("param", "region", "region", null, null, null, "EAST"),
                predicate("header", "customer_id", null, null, "X-Customer", null, "C001"),
                predicate("body", "customer_id", null, "filter.customer_id", null, null, "C001"),
                predicate("path", "customer_id", null, null, null, "customer_id", "C001")));
        List<Map<String, Object>> rows = new ArrayList<>();

        scanner.scan(config, rows::add);

        assertEquals(3, rows.size());
        assertEquals("C001", rows.get(0).get("customer_id"));
        assertEquals(91, rows.get(0).get("risk_score"));
        assertEquals(2, seenRequests.size());
        assertTrue(seenRequests.get(0).contains("/customers/C001?"));
        assertTrue(seenRequests.get(0).contains("region=EAST"));
        assertTrue(seenRequests.get(0).contains("page=1"));
        assertTrue(seenRequests.get(0).contains("size=2"));
        assertTrue(seenRequests.get(0).contains("|C001|"));
        assertTrue(seenRequests.get(0).contains("\"filter\":{\"customer_id\":\"C001\"}"));
        assertTrue(seenRequests.get(1).contains("page=2"));
    }

    @Test
    public void scanShouldConvertRestXmlAndSoapResponses() throws Exception {
        startServer(exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (path.endsWith("/xml")) {
                writeXml(exchange, "<root><code>200</code><data><items><customer_id>C010</customer_id><risk_score>70</risk_score></items></data></root>");
            } else {
                writeXml(exchange, "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                        + "<soap:Body><ns:QueryRowsResponse xmlns:ns=\"urn:test\"><code>200</code>"
                        + "<items><customer_id>C011</customer_id><risk_score>71</risk_score></items>"
                        + "</ns:QueryRowsResponse></soap:Body></soap:Envelope>");
            }
        });

        List<Map<String, Object>> xmlRows = new ArrayList<>();
        new HttpSourceScanner().scan(HttpSourceConfig.fromConfiguration(Configuration.from(xmlConfig("/xml"))), xmlRows::add);
        assertEquals(1, xmlRows.size());
        assertEquals("C010", xmlRows.get(0).get("customer_id"));

        List<Map<String, Object>> soapRows = new ArrayList<>();
        new HttpSourceScanner().scan(HttpSourceConfig.fromConfiguration(Configuration.from(soapConfig("/soap"))), soapRows::add);
        assertEquals(1, soapRows.size());
        assertEquals("C011", soapRows.get(0).get("customer_id"));
    }

    @Test
    public void legacySoapResultTypeShouldDeriveSoapContentType() {
        Map<String, Object> rawConfig = offlineConfig("/soap", "POST");
        rawConfig.remove(HttpSourceKeys.PROTOCOL_MODE);
        rawConfig.remove(HttpSourceKeys.CONTENT_TYPE);
        rawConfig.put(HttpSourceKeys.RESULT_TYPE, "soap");

        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));

        assertEquals("text/xml;charset=UTF-8", config.getContentType());
    }

    @Test
    public void restXmlProtocolShouldDefaultToXmlResultType() {
        Map<String, Object> values = baseConfigForUrl("http://127.0.0.1/test", "GET");
        values.put("protocolMode", "REST_XML");
        values.remove("resultType");

        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(values));

        assertEquals("xml", config.getResultType());
        assertEquals("application/xml;charset=UTF-8", config.getContentType());
    }

    @Test
    public void explicitRestJsonProtocolShouldOverrideConflictingLegacyResultAndContentTypes() {
        Map<String, Object> values = baseConfigForUrl("http://127.0.0.1/test", "POST");
        values.put(HttpSourceKeys.PROTOCOL_MODE, "REST_JSON");
        values.put(HttpSourceKeys.RESULT_TYPE, "xml");
        values.put(HttpSourceKeys.CONTENT_TYPE, "application/xml;charset=GBK");

        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(values));

        assertEquals("json", config.getResultType());
        assertEquals("application/json;charset=GBK", config.getContentType());
    }

    @Test
    public void scanShouldDecodeResponseUsingDeclaredCharset() throws Exception {
        startServer(exchange -> {
            String body = "{\"data\":{\"customer_id\":\"客户甲\",\"risk_score\":88}}";
            byte[] bytes = body.getBytes(Charset.forName("GBK"));
            exchange.getResponseHeaders().set("Content-Type", "application/json;charset=GBK");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/gbk", "data");
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));
        List<Map<String, Object>> rows = new ArrayList<>();

        new HttpSourceScanner().scan(config, rows::add);

        assertEquals(1, rows.size());
        assertEquals("客户甲", rows.get(0).get("customer_id"));
    }

    @Test
    public void scanShouldConvertCommonJsonShapes() throws Exception {
        startServer(exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (path.endsWith("/root-array")) {
                writeJson(exchange, "[{\"customer_id\":\"C020\",\"risk_score\":72},{\"customer_id\":\"C021\",\"risk_score\":73}]");
            } else if (path.endsWith("/deep-nested-array")) {
                writeJson(exchange, "{\"data\":{\"items\":[{\"customer\":{\"name\":\"A\"}},{\"customer\":{\"name\":\"B\"}}]}}");
            } else if (path.endsWith("/nested-array")) {
                writeJson(exchange, "{\"code\":\"200\",\"data\":{\"items\":[{\"customer_id\":\"C022\",\"risk_score\":74},{\"customer_id\":\"C023\",\"risk_score\":75}]}}");
            } else {
                writeJson(exchange, "{\"code\":\"200\",\"data\":{\"customer_id\":\"C024\",\"risk_score\":76}}");
            }
        });

        List<Map<String, Object>> rootArrayRows = new ArrayList<>();
        new HttpSourceScanner().scan(HttpSourceConfig.fromConfiguration(Configuration.from(jsonShapeConfig("/root-array", ""))), rootArrayRows::add);
        assertEquals(2, rootArrayRows.size());
        assertEquals("C020", rootArrayRows.get(0).get("customer_id"));

        List<Map<String, Object>> nestedArrayRows = new ArrayList<>();
        new HttpSourceScanner().scan(HttpSourceConfig.fromConfiguration(Configuration.from(jsonShapeConfig("/nested-array", "data.items"))), nestedArrayRows::add);
        assertEquals(2, nestedArrayRows.size());
        assertEquals("C022", nestedArrayRows.get(0).get("customer_id"));

        Map<String, Object> deepNestedConfig = jsonShapeConfig("/deep-nested-array", "data.items.customer");
        deepNestedConfig.put(HttpSourceKeys.COLUMNS,
                Arrays.asList(column("data.items.customer", "name", "STRING")));
        List<Map<String, Object>> deepNestedRows = new ArrayList<>();
        new HttpSourceScanner().scan(
                HttpSourceConfig.fromConfiguration(Configuration.from(deepNestedConfig)), deepNestedRows::add);
        assertEquals(2, deepNestedRows.size());
        assertEquals("A", deepNestedRows.get(0).get("name"));
        assertEquals("B", deepNestedRows.get(1).get("name"));

        List<Map<String, Object>> singleObjectRows = new ArrayList<>();
        new HttpSourceScanner().scan(HttpSourceConfig.fromConfiguration(Configuration.from(jsonShapeConfig("/single-object", "data"))), singleObjectRows::add);
        assertEquals(1, singleObjectRows.size());
        assertEquals("C024", singleObjectRows.get(0).get("customer_id"));
        assertEquals(76, singleObjectRows.get(0).get("risk_score"));
    }

    @Test
    public void dataModelPreviewShouldStopAfterLimitRows() throws Exception {
        List<String> seenRequests = new ArrayList<>();
        startServer(exchange -> {
            String query = exchange.getRequestURI().getRawQuery();
            seenRequests.add(query);
            if (query.contains("page=1")) {
                writeJson(exchange, "{\"code\":\"200\",\"data\":{\"total\":3,\"items\":[{\"customer_id\":\"C030\",\"risk_score\":80},{\"customer_id\":\"C031\",\"risk_score\":81}]}}");
            } else {
                writeJson(exchange, "{\"code\":\"200\",\"data\":{\"total\":3,\"items\":[{\"customer_id\":\"C032\",\"risk_score\":82}]}}");
            }
        });
        BaseDataSourceDTO dto = new BaseDataSourceDTO();
        Map<String, String> extraParams = new LinkedHashMap<>();
        extraParams.put(HttpSourceKeys.STUDIO_READER_CONFIG,
                JSONObject.toJSONString(baseConfig("/preview", "GET")));
        dto.setExtraParams(extraParams);

        Table<Map<String, Object>> table = new HttpSourcePlugin().dataModelPreview(dto, "preview", "1");

        assertEquals(1, table.getBodies().size());
        assertEquals("C030", table.getBodies().get(0).get("customer_id"));
        assertEquals(1, seenRequests.size());
        assertTrue(seenRequests.get(0).contains("page=1"));
    }

    @Test
    public void studioScanMaxRowsShouldStopAtExactFullPageBoundary() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            int request = requests.incrementAndGet();
            if (request == 1) {
                writeJson(exchange, "{\"code\":\"200\",\"data\":{\"total\":3,\"items\":["
                        + "{\"customer_id\":\"C030\",\"risk_score\":80},"
                        + "{\"customer_id\":\"C031\",\"risk_score\":81}]}}");
            } else {
                writeJson(exchange, "{\"code\":\"200\",\"data\":{\"total\":3,\"items\":["
                        + "{\"customer_id\":\"C032\",\"risk_score\":82}]}}");
            }
        });
        BaseDataSourceDTO dto = new BaseDataSourceDTO();
        Map<String, String> extraParams = new LinkedHashMap<>();
        extraParams.put(HttpSourceKeys.STUDIO_READER_CONFIG,
                JSONObject.toJSONString(baseConfig("/studio-max-rows", "GET")));
        extraParams.put(HttpSourceKeys.STUDIO_MAX_ROWS, "2");
        dto.setExtraParams(extraParams);
        List<Map<String, Object>> rows = new ArrayList<>();

        new HttpSourcePlugin().scanQuery(dto, "SELECT * FROM http", true, rows::add);

        assertEquals(2, rows.size());
        assertEquals(1, requests.get());
    }

    @Test
    public void scanShouldStopAutoPaginationWhenSqlOverridesDynamicPage() throws Exception {
        List<String> seenRequests = new ArrayList<>();
        startServer(exchange -> {
            seenRequests.add(exchange.getRequestURI().getRawQuery());
            writeJson(exchange, "{\"code\":\"200\",\"data\":{\"total\":100,\"items\":[{\"customer_id\":\"C040\",\"risk_score\":84}]}}");
        });
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(baseConfig("/page", "GET")));

        scanner.applyPushdown(config, Arrays.asList(
                predicate("param", "page", "page", null, null, null, 5)));
        List<Map<String, Object>> rows = new ArrayList<>();
        scanner.scan(config, rows::add);

        assertEquals(1, rows.size());
        assertEquals(1, seenRequests.size());
        assertTrue(seenRequests.get(0).contains("page=5"));
    }

    @Test
    public void scanShouldUsePushedPageSizeForFullPagination() throws Exception {
        List<String> seenRequests = new ArrayList<>();
        startServer(exchange -> {
            String query = exchange.getRequestURI().getRawQuery();
            seenRequests.add(query);
            String page = query.contains("page=2") ? "2" : query.contains("page=3") ? "3" : "1";
            writeJson(exchange, "{\"code\":\"200\",\"data\":{\"total\":3,\"items\":[{\"customer_id\":\"C05"
                    + page + "\",\"risk_score\":85}]}}");
        });
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(baseConfig("/page-size", "GET")));

        scanner.applyPushdown(config, Arrays.asList(
                predicate("param", "size", "size", null, null, null, 1)));
        List<Map<String, Object>> rows = new ArrayList<>();
        scanner.scan(config, rows::add);

        assertEquals(3, rows.size());
        assertEquals(3, seenRequests.size());
        assertTrue(seenRequests.get(0).contains("size=1"));
        assertTrue(seenRequests.get(2).contains("page=3"));
    }

    @Test
    public void offsetOverrideShouldAllowZeroWhilePageOverrideRequiresPositiveValue() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        Map<String, Object> offsetConfig = offlineConfig("/offset", "GET");
        offsetConfig.put(HttpSourceKeys.PARAMS, "{\"offset\":\"{dyn_offset}\"}");
        HttpSourceConfig offset = HttpSourceConfig.fromConfiguration(Configuration.from(offsetConfig));

        scanner.applyPushdown(offset, Arrays.asList(
                predicate("param", "offset", "offset", null, null, null, 0)));

        assertFalse(offset.isPageRead());

        Map<String, Object> pageConfig = offlineConfig("/page", "GET");
        pageConfig.put(HttpSourceKeys.PARAMS, "{\"page\":\"{dyn_page}\"}");
        HttpSourceConfig page = HttpSourceConfig.fromConfiguration(Configuration.from(pageConfig));
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                scanner.applyPushdown(page, Arrays.asList(
                        predicate("param", "page", "page", null, null, null, 0))));

        assertTrue(error.getMessage().contains("必须是正整数"));
    }

    @Test
    public void applyPushdownShouldRejectConflictingValuesForSameRequestTarget() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(offlineConfig("/conflict", "GET")));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> scanner.applyPushdown(config,
                Arrays.asList(
                        predicate("param", "customer_id", "customer", null, null, null, "C001"),
                        predicate("query", "customer_id", "customer", null, null, null, "C002"))));

        assertTrue(error.getMessage().contains("收到冲突条件"));
        assertFalse(error.getMessage().contains("C001"));
        assertFalse(error.getMessage().contains("C002"));
    }

    @Test
    public void applyPushdownShouldTreatEquivalentNumberRepresentationsAsTheSameTargetValue() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(
                offlineConfig("/numeric-equivalence", "GET")));

        scanner.applyPushdown(config, Arrays.asList(
                predicate("param", "amount", "amount", null, null, null, 1.0d),
                predicate("query", "amount", "amount", null, null, null, new BigDecimal("1.00"))));

        assertEquals(Double.valueOf(1.0d), config.getPushedRequestContext().get("param").get("amount"));
    }

    @Test
    public void queryPushdownShouldPreserveNonJsonRequestBody() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(offlineConfig("/soap-query", "POST")));
        String requestBody = "<Envelope><Body><customer>C001</customer></Body></Envelope>";
        config.setRequestBody(requestBody);

        scanner.applyPushdown(config, Arrays.asList(
                predicate("param", "region", "region", null, null, null, "EAST")));

        assertEquals(requestBody, config.getRequestBody());
        assertFalse(config.getParams().contains("EAST"));
        assertEquals("EAST", config.getPushedRequestContext().get("param").get("region"));
    }

    @Test
    public void bodyPushdownShouldUpdateXmlRequestBody() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(offlineConfig("/soap-body", "POST")));
        config.setRequestBody("<Envelope><Body><customer_id>OLD</customer_id></Body></Envelope>");

        scanner.applyPushdown(config, Arrays.asList(
                predicate("body", "customer_id", null, "Envelope.Body.customer_id", null, null, "C001")));

        assertFalse(config.getRequestBody().contains("<customer_id>C001</customer_id>"));
        assertEquals("C001", config.getPushedRequestContext().get("body").get("customer_id"));
    }

    @Test
    public void jsonBodyPushdownShouldPreserveExistingArraysForDotAndBracketIndexes() {
        for (String path : Arrays.asList("filters.0.customer_id", "filters[0].customer_id")) {
            HttpSourceScanner scanner = new HttpSourceScanner();
            HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(
                    offlineConfig("/json-array-body", "POST")));
            config.setRequestBody("{\"filters\":[{\"customer_id\":\"OLD\",\"keep\":1}],\"enabled\":true}");

            scanner.applyPushdown(config, Arrays.asList(
                    predicate("body", "customer_id", null, path, null, null, "C001")));

            JSONObject body = JSONObject.parseObject(config.restorePushedLiterals(config.getRequestBody()));
            JSONArray filters = body.getJSONArray("filters");
            assertEquals(1, filters.size());
            assertEquals("C001", filters.getJSONObject(0).getString("customer_id"));
            assertEquals(1, filters.getJSONObject(0).getIntValue("keep"));
            assertTrue(body.getBooleanValue("enabled"));
        }
    }

    @Test
    public void jsonBodyPushdownShouldRejectInvalidArrayPathsWithoutChangingTheBody() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        for (String path : Arrays.asList("filters[1].customer_id", "filters[abc].customer_id")) {
            HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(
                    offlineConfig("/json-array-body", "POST")));
            String originalBody = "{\"filters\":[{\"customer_id\":\"OLD\"}]}";
            config.setRequestBody(originalBody);

            IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                    scanner.applyPushdown(config, Arrays.asList(
                            predicate("body", "customer_id", null, path, null, null, "C001"))));

            assertTrue(error.getMessage().contains("数组"));
            assertEquals(originalBody, config.getRequestBody());
        }

        HttpSourceConfig scalarConfig = HttpSourceConfig.fromConfiguration(Configuration.from(
                offlineConfig("/json-array-body", "POST")));
        String scalarBody = "{\"filters\":\"not-an-array\"}";
        scalarConfig.setRequestBody(scalarBody);
        IllegalArgumentException scalarError = assertThrows(IllegalArgumentException.class, () ->
                scanner.applyPushdown(scalarConfig, Arrays.asList(
                        predicate("body", "customer_id", null, "filters.0.customer_id", null, null, "C001"))));
        assertTrue(scalarError.getMessage().contains("类型不匹配"));
        assertEquals(scalarBody, scalarConfig.getRequestBody());
    }

    @Test
    public void bodyPushdownShouldRequireFullXmlPathForDuplicateLeafNames() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(offlineConfig("/soap-body", "POST")));
        config.setRequestBody("<Envelope><Body><first><id>1</id></first><second><id>2</id></second></Body></Envelope>");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> scanner.applyPushdown(config,
                Arrays.asList(predicate("body", "id", null, "id", null, null, "3"))));

        assertTrue(error.getMessage().contains("匹配到多个节点"));
        scanner.applyPushdown(config, Arrays.asList(
                predicate("body", "id", null, "Envelope.Body.second.id", null, null, "3")));
        assertTrue(config.getRequestBody().contains("<first><id>1</id></first>"));
        assertFalse(config.getRequestBody().contains("<second><id>3</id></second>"));
    }

    @Test
    public void pushedStringLiteralsShouldNotExecuteDynamicTemplates() throws Exception {
        AtomicReference<String> query = new AtomicReference<>();
        AtomicReference<String> header = new AtomicReference<>();
        AtomicReference<String> body = new AtomicReference<>();
        startServer(exchange -> {
            query.set(exchange.getRequestURI().getRawQuery());
            header.set(exchange.getRequestHeaders().getFirst("X-Literal"));
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            writeJson(exchange, "{\"data\":{\"customer_id\":\"C001\",\"risk_score\":90}}");
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/literal-pushdown", "data");
        rawConfig.put(HttpSourceKeys.MODE, "POST");
        rawConfig.put(HttpSourceKeys.PARAMS, "{\"customer\":\"\"}");
        rawConfig.put(HttpSourceKeys.HEADER, "{\"X-Literal\":\"\"}");
        rawConfig.put(HttpSourceKeys.REQUEST_BODY, "{\"filter\":{\"customer_id\":\"\"}}");
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));
        HttpSourceScanner scanner = new HttpSourceScanner();
        scanner.applyPushdown(config, Arrays.asList(
                predicate("param", "customer_id", "customer", null, null, null, "{dyn_page}"),
                predicate("header", "trace", null, null, "X-Literal", null, "<dyn_timestamp>"),
                predicate("body", "customer_id", null, "filter.customer_id", null, null,
                        "{dyn_from_http_token(GET,http://127.0.0.1:1,,,data.token)}")));

        List<Map<String, Object>> rows = new ArrayList<>();
        scanner.scan(config, rows::add);

        assertEquals(1, rows.size());
        assertTrue(query.get().contains("customer=%7Bdyn_page%7D"));
        assertEquals("<dyn_timestamp>", header.get());
        assertTrue(body.get().contains("{dyn_from_http_token(GET,http://127.0.0.1:1,,,data.token)}"));
    }

    @Test
    public void scanShouldExposePushedRequestContextWithoutOverwritingResponseFields() throws Exception {
        startServer(exchange -> writeJson(exchange,
                "{\"data\":{\"customer_id\":\"RESPONSE\",\"risk_score\":90}}"));
        Map<String, Object> rawConfig = jsonShapeConfig("/request-context", "data");
        rawConfig.put(HttpSourceKeys.PARAMS, "{}");
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));
        HttpSourceScanner scanner = new HttpSourceScanner();
        scanner.applyPushdown(config, Arrays.asList(
                predicate("param", "customer_id", "customer", null, null, null, "C001")));
        List<Map<String, Object>> rows = new ArrayList<>();

        scanner.scan(config, rows::add);

        assertEquals(1, rows.size());
        assertEquals("RESPONSE", rows.get(0).get("customer_id"));
        assertEquals("C001", ((Map<?, ?>) rows.get(0).get("param")).get("customer_id"));
        assertEquals("C001", ((Map<?, ?>) rows.get(0).get("query")).get("customer_id"));
    }

    @Test
    public void scanShouldNotExposeStaticRequestSecrets() throws Exception {
        startServer(exchange -> writeJson(exchange,
                "{\"data\":{\"customer_id\":\"C070\",\"risk_score\":90}}"));
        Map<String, Object> rawConfig = jsonShapeConfig("/request-secrets", "data");
        rawConfig.put(HttpSourceKeys.HEADER, "{\"Authorization\":\"Bearer secret\"}");
        rawConfig.put(HttpSourceKeys.PARAMS, "{\"tenant\":\"acme\"}");
        rawConfig.put(HttpSourceKeys.REQUEST_BODY, "{\"password\":\"p@ss\"}");
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));
        List<Map<String, Object>> rows = new ArrayList<>();

        new HttpSourceScanner().scan(config, rows::add);

        assertEquals(1, rows.size());
        Map<String, Object> row = rows.get(0);
        assertFalse(row.containsKey("param"));
        assertFalse(row.containsKey("query"));
        assertFalse(row.containsKey("header"));
        assertFalse(row.containsKey("body"));
        assertFalse(row.containsKey("Authorization"));
        assertFalse(row.containsKey("tenant"));
        assertFalse(row.containsKey("password"));
    }

    @Test
    public void serializedPaginationTokenShouldStopAutoPagination() throws Exception {
        List<String> seenRequests = new ArrayList<>();
        startServer(exchange -> {
            seenRequests.add(exchange.getRequestURI().getRawQuery());
            writeJson(exchange, "{\"code\":\"200\",\"data\":{\"total\":100,\"items\":[{\"customer_id\":\"C060\",\"risk_score\":86}]}}");
        });
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(baseConfig("/serialized-page", "GET")));
        HttpPushdownPredicate pushedPage = predicate("param", "page", "page", null, null, null, 7);
        pushedPage.setPaginationTokens(Arrays.asList("PAGE"));
        List<HttpPushdownPredicate> serialized = HttpPushdownPredicate.parseList(
                JSONObject.toJSONString(Arrays.asList(pushedPage)));

        scanner.applyPushdown(config, serialized);
        scanner.scan(config, row -> {
        });

        assertEquals(1, seenRequests.size());
        assertTrue(seenRequests.get(0).contains("page=7"));
    }

    @Test
    public void paginationPushdownShouldRejectFractionalIntegerValues() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(
                offlineConfig("/fractional-page-size", "GET")));
        HttpPushdownPredicate predicate = predicate("param", "size", "size", null, null, null, 1.5d);
        predicate.setPaginationTokens(Arrays.asList("PAGE_SIZE"));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> scanner.applyPushdown(config, Arrays.asList(predicate)));

        assertTrue(error.getMessage().contains("必须是正整数"));
    }

    @Test
    public void pathPushdownShouldRejectMissingUrlPlaceholder() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(offlineConfig("/customers", "GET")));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> scanner.applyPushdown(config,
                Arrays.asList(predicate("path", "customer_id", null, null, null, "customer_id", "C001"))));

        assertTrue(error.getMessage().contains("没有对应占位符"));
    }

    @Test
    public void scanShouldRejectPaginationWithoutPageOrOffsetTemplate() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        Map<String, Object> rawConfig = offlineConfig("/customers", "GET");
        rawConfig.put(HttpSourceKeys.PARAMS, "{\"size\":\"{dyn_pageSize}\"}");
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> scanner.scan(config, row -> {
                }));

        assertTrue(error.getMessage().contains("未配置 {dyn_page} 或 {dyn_offset}"));
    }

    @Test
    public void scanShouldRejectNonPositivePageSize() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        Map<String, Object> rawConfig = offlineConfig("/customers", "GET");
        rawConfig.put(HttpSourceKeys.PAGE_SIZE, 0);
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> scanner.scan(config, row -> {
                }));

        assertTrue(error.getMessage().contains("pageSize 必须是正整数"));
    }

    @Test
    public void pathPushdownShouldEncodePathSegmentValue() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(
                offlineConfig("/customers/{customer_id}", "GET")));

        scanner.applyPushdown(config, Arrays.asList(
                predicate("path", "customer_id", null, null, null, "customer_id", "客户 A/1?#")));

        assertTrue(config.getUrl().endsWith("/customers/%E5%AE%A2%E6%88%B7%20A%2F1%3F%23"));
    }

    @Test
    public void pathPushdownShouldOnlyInspectAndReplaceUriPath() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        String url = "http://reader:secret@127.0.0.1/items/{id}?redirect=:id&literal={id}#fragment-:id";
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(
                baseConfigForUrl(url, "GET")));

        scanner.applyPushdown(config, Arrays.asList(
                predicate("path", "id", null, null, null, "id", "A/B")));

        assertEquals("http://reader:secret@127.0.0.1/items/A%2FB?redirect=:id&literal={id}#fragment-:id",
                config.getUrl());

        HttpSourceConfig queryOnly = HttpSourceConfig.fromConfiguration(Configuration.from(
                baseConfigForUrl("http://127.0.0.1/items?redirect={id}", "GET")));
        assertThrows(IllegalArgumentException.class, () -> scanner.applyPushdown(queryOnly, Arrays.asList(
                predicate("path", "id", null, null, null, "id", "A"))));
    }

    @Test
    public void headerPushdownShouldOverrideExistingHeaderIgnoringCase() throws Exception {
        AtomicReference<List<String>> headerValues = new AtomicReference<>();
        startServer(exchange -> {
            headerValues.set(exchange.getRequestHeaders().get("X-Token"));
            writeJson(exchange, "{\"data\":{\"customer_id\":\"C080\",\"risk_score\":90}}");
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/headers", "data");
        rawConfig.put(HttpSourceKeys.HEADER, "{\"X-Token\":\"old\"}");
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));
        HttpSourceScanner scanner = new HttpSourceScanner();

        scanner.applyPushdown(config, Arrays.asList(
                predicate("header", "token", null, null, "x-token", null, "new")));
        scanner.scan(config, row -> {
        });

        JSONObject headers = JSONObject.parseObject(config.restorePushedLiterals(config.getHeader()));
        assertEquals(1, headers.size());
        assertEquals("new", headers.getString("X-Token"));
        assertEquals(Arrays.asList("new"), headerValues.get());
    }

    @Test
    public void requestParamsShouldPreserveExistingUrlQuery() throws Exception {
        AtomicReference<String> query = new AtomicReference<>();
        startServer(exchange -> {
            query.set(exchange.getRequestURI().getRawQuery());
            writeJson(exchange, "{\"data\":{\"customer_id\":\"C081\",\"risk_score\":91}}");
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/query?fixed=keep", "data");

        new HttpSourceScanner().scan(
                HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), row -> {
                });

        assertTrue(query.get().contains("fixed=keep"));
        assertTrue(query.get().contains("page=1"));
        assertTrue(query.get().contains("size=2"));
    }

    @Test
    public void requestParamsShouldOverrideSameNameExistingUrlQuery() throws Exception {
        AtomicReference<String> query = new AtomicReference<>();
        startServer(exchange -> {
            query.set(exchange.getRequestURI().getRawQuery());
            writeJson(exchange, "{\"data\":{\"customer_id\":\"C082\",\"risk_score\":92}}");
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/query?page=99&fixed=keep", "data");

        new HttpSourceScanner().scan(
                HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), row -> {
                });

        assertTrue(query.get().contains("fixed=keep"));
        assertTrue(query.get().contains("page=1"));
        assertFalse(query.get().contains("page=99"));
    }

    @Test
    public void colonPathVariableShouldNotMatchLongerVariableName() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(
                offlineConfig("/customers/:id2/orders/:id", "GET")));

        scanner.applyPushdown(config, Arrays.asList(
                predicate("path", "id", null, null, null, "id", "42")));

        assertTrue(config.getUrl().endsWith("/customers/:id2/orders/42"));
    }

    @Test
    public void xmlBodyPushdownShouldDistinguishNamespaceByPrefixAndClarkNotation() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(
                offlineConfig("/soap-namespace", "POST")));
        config.setRequestBody("<Envelope xmlns:a=\"urn:first\" xmlns:b=\"urn:test.v2\"><Body>"
                + "<a:id>1</a:id><b:id>2</b:id></Body></Envelope>");

        scanner.applyPushdown(config, Arrays.asList(
                predicate("body", "id", null, "Envelope.Body.b:id", null, null, "3")));
        assertTrue(config.restorePushedLiterals(config.getRequestBody()).contains("<a:id>1</a:id>"));
        assertTrue(config.restorePushedLiterals(config.getRequestBody()).contains("<b:id>3</b:id>"));

        scanner.applyPushdown(config, Arrays.asList(
                predicate("body", "id", null, "Envelope.Body.{urn:test.v2}id", null, null, "4")));
        assertTrue(config.restorePushedLiterals(config.getRequestBody()).contains("<a:id>1</a:id>"));
        assertTrue(config.restorePushedLiterals(config.getRequestBody()).contains("<b:id>4</b:id>"));
    }

    @Test
    public void soapHttp500FaultShouldBeParsedWithoutRetry() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            requests.incrementAndGet();
            writeXml(exchange, 500,
                    "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                            + "<soap:Body><soap:Fault><faultcode>soap:Client</faultcode>"
                            + "<faultstring>invalid customer</faultstring></soap:Fault></soap:Body>"
                            + "</soap:Envelope>");
        });

        AggregationException exception = assertThrows(AggregationException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(soapConfig("/soap-fault-500"))),
                        row -> {
                        }));

        assertEquals(1, requests.get());
        assertTrue(exception.getMessage().contains("SOAP Fault"));
        assertFalse(exception.getMessage().contains("invalid customer"));
    }

    @Test
    public void nonSoapHttpErrorShouldNotExposeResponseBodyInException() throws Exception {
        String secretBody = "sensitive-response-body-token";
        startServer(exchange -> write(exchange, 500, secretBody));
        Map<String, Object> rawConfig = jsonShapeConfig("/http-error", "data");

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), row -> {
                        }));

        assertFalse(exceptionMessages(exception).contains(secretBody));
        assertTrue(exceptionMessages(exception).contains("status 500"));
    }

    @Test
    public void networkFailureShouldRetainCauseWithoutExposingRequestSecrets() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        Map<String, Object> rawConfig = baseConfigForUrl(
                "http://127.0.0.1:" + port + "/private?access_token=secret-query", "PUT");
        rawConfig.put(HttpSourceKeys.HEADER, "{\"Authorization\":\"Bearer secret-header\"}");
        rawConfig.put(HttpSourceKeys.PARAMS, "{\"api_key\":\"secret-param\"}");
        rawConfig.put(HttpSourceKeys.REQUEST_BODY, "{\"password\":\"secret-body\"}");
        rawConfig.put(HttpSourceKeys.PAGE_READ, false);
        rawConfig.put(HttpSourceKeys.TOTAL_CODE_PATH, "");
        rawConfig.put(HttpSourceKeys.RETRY_TIMES, 2);
        rawConfig.put(HttpSourceKeys.RETRY_INTERVAL_MS, 0L);

        RuntimeException error = assertThrows(RuntimeException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), row -> {
                        }));

        String messages = exceptionMessages(error);
        assertTrue(messages.contains("after 2 attempts"));
        assertTrue(hasCause(error, IOException.class));
        assertFalse(messages.contains("secret-query"));
        assertFalse(messages.contains("secret-header"));
        assertFalse(messages.contains("secret-param"));
        assertFalse(messages.contains("secret-body"));
    }

    @Test
    public void clientHttpErrorShouldNotRetry() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            requests.incrementAndGet();
            write(exchange, 400, "invalid request with private-token");
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/http-client-error", "data");

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), row -> {
                        }));

        assertEquals(1, requests.get());
        assertTrue(exceptionMessages(exception).contains("status 400"));
        assertFalse(exceptionMessages(exception).contains("private-token"));
    }

    @Test
    public void bodyPushdownShouldRejectMethodsThatDoNotSendEntities() {
        HttpSourceScanner scanner = new HttpSourceScanner();
        for (String method : Arrays.asList("GET", "HEAD", "OPTIONS", "TRACE", "DELETE")) {
            HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(
                    offlineConfig("/body-pushdown", method)));

            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> scanner.applyPushdown(config, Arrays.asList(
                            predicate("body", "customer_id", null, "customer_id", null, null, "C001"))));

            assertTrue(error.getMessage().contains(method));
            assertTrue(error.getMessage().contains("不能进行 body 下推"));
        }
    }

    @Test
    public void requestShouldRejectEveryUnboundPathVariableBeforeConnecting() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            requests.incrementAndGet();
            writeJson(exchange, "{\"data\":{\"customer_id\":\"C090\",\"risk_score\":90}}");
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/tenants/{tenant}/orders/:orderId", "GET");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), row -> {
                        }));

        assertEquals(0, requests.get());
        assertTrue(error.getMessage().contains("tenant"));
        assertTrue(error.getMessage().contains("orderId"));
    }

    @Test
    public void soap12ActionShouldUseContentTypeWhileSoap11KeepsSoapAction() throws Exception {
        AtomicReference<String> soap12ContentType = new AtomicReference<>();
        AtomicReference<String> soap12Action = new AtomicReference<>();
        AtomicReference<String> soap11ContentType = new AtomicReference<>();
        AtomicReference<String> soap11Action = new AtomicReference<>();
        startServer(exchange -> {
            if (exchange.getRequestURI().getPath().endsWith("soap12")) {
                soap12ContentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
                soap12Action.set(exchange.getRequestHeaders().getFirst("SOAPAction"));
            } else {
                soap11ContentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
                soap11Action.set(exchange.getRequestHeaders().getFirst("SOAPAction"));
            }
            writeXml(exchange, "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                    + "<soap:Body><QueryRowsResponse><code>200</code><items>"
                    + "<customer_id>C091</customer_id><risk_score>91</risk_score>"
                    + "</items></QueryRowsResponse></soap:Body></soap:Envelope>");
        });

        Map<String, Object> soap12 = soapConfig("/soap12");
        soap12.remove(HttpSourceKeys.CONTENT_TYPE);
        soap12.put(HttpSourceKeys.HEADER, "{\"Content-Type\":\"application/json\"}");
        soap12.put(HttpSourceKeys.SOAP_VERSION, "SOAP_12");
        soap12.put(HttpSourceKeys.SOAP_ACTION, "urn:queryRows12");
        new HttpSourceScanner().scan(HttpSourceConfig.fromConfiguration(Configuration.from(soap12)), row -> {
        });

        Map<String, Object> soap11 = soapConfig("/soap11");
        soap11.remove(HttpSourceKeys.CONTENT_TYPE);
        soap11.put(HttpSourceKeys.HEADER, "{\"content-type\":\"application/json\"}");
        soap11.put(HttpSourceKeys.SOAP_VERSION, "SOAP_11");
        soap11.put(HttpSourceKeys.SOAP_ACTION, "urn:queryRows11");
        new HttpSourceScanner().scan(HttpSourceConfig.fromConfiguration(Configuration.from(soap11)), row -> {
        });

        assertTrue(soap12ContentType.get().startsWith("application/soap+xml"));
        assertTrue(soap12ContentType.get().contains("action=\"urn:queryRows12\""));
        assertTrue(soap12Action.get() == null);
        assertTrue(soap11ContentType.get().startsWith("text/xml"));
        assertEquals("urn:queryRows11", soap11Action.get());
    }

    @Test
    public void protocolContentTypeShouldOverrideConflictingHeaderAndControlBodyCharset() throws Exception {
        AtomicReference<String> contentType = new AtomicReference<>();
        AtomicReference<String> requestBody = new AtomicReference<>();
        startServer(exchange -> {
            contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            requestBody.set(new String(exchange.getRequestBody().readAllBytes(), Charset.forName("GBK")));
            writeXml(exchange, "<root><data><customer_id>C100</customer_id><risk_score>100</risk_score></data></root>");
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/xml-gbk", "root.data");
        rawConfig.put(HttpSourceKeys.MODE, "POST");
        rawConfig.put(HttpSourceKeys.PROTOCOL_MODE, "REST_XML");
        rawConfig.put(HttpSourceKeys.RESULT_TYPE, "json");
        rawConfig.put(HttpSourceKeys.CONTENT_TYPE, "application/xml;charset=GBK");
        rawConfig.put(HttpSourceKeys.HEADER, "{\"content-type\":\"application/json;charset=UTF-8\"}");
        rawConfig.put(HttpSourceKeys.REQUEST_BODY,
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><request><name>客户甲</name></request>");
        List<Map<String, Object>> rows = new ArrayList<>();

        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));
        new HttpSourceScanner().scan(config, rows::add);

        assertEquals("xml", config.getResultType());
        assertTrue(contentType.get().toLowerCase().startsWith("application/xml"));
        assertTrue(contentType.get().toLowerCase().contains("charset=gbk"));
        assertTrue(requestBody.get().contains("encoding=\"GBK\""));
        assertTrue(requestBody.get().contains("<name>客户甲</name>"));
        assertEquals(1, rows.size());
    }

    @Test
    public void restJsonShouldPreserveCustomRequestContentType() throws Exception {
        AtomicReference<String> contentType = new AtomicReference<>();
        AtomicReference<String> requestBody = new AtomicReference<>();
        startServer(exchange -> {
            contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            requestBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            writeJson(exchange, "{\"data\":{\"customer_id\":\"C001\",\"risk_score\":88}}");
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/form-json", "data");
        rawConfig.put(HttpSourceKeys.MODE, "POST");
        rawConfig.put(HttpSourceKeys.CONTENT_TYPE, "application/x-www-form-urlencoded;charset=UTF-8");
        rawConfig.put(HttpSourceKeys.REQUEST_BODY, "customer_id=C001");

        List<Map<String, Object>> rows = new ArrayList<>();
        new HttpSourceScanner().scan(
                HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), rows::add);

        assertEquals("application/x-www-form-urlencoded;charset=UTF-8", contentType.get());
        assertEquals("customer_id=C001", requestBody.get());
        assertEquals(1, rows.size());
    }

    @Test
    public void xmlBodyPushdownShouldParseJavaStringAndKeepDeclarationAlignedWithRequestCharset() throws Exception {
        Map<String, String> receivedBodies = new LinkedHashMap<>();
        startServer(exchange -> {
            String path = exchange.getRequestURI().getPath();
            Charset charset = path.endsWith("gbk") ? Charset.forName("GBK") : StandardCharsets.UTF_8;
            receivedBodies.put(path, new String(exchange.getRequestBody().readAllBytes(), charset));
            writeXml(exchange, "<root><data><customer_id>C107</customer_id><risk_score>107</risk_score></data></root>");
        });

        for (String charsetName : Arrays.asList("GBK", "UTF-8")) {
            String suffix = charsetName.equals("GBK") ? "gbk" : "utf8";
            Map<String, Object> rawConfig = jsonShapeConfig("/xml-pushdown-" + suffix, "root.data");
            rawConfig.put(HttpSourceKeys.MODE, "POST");
            rawConfig.put(HttpSourceKeys.PROTOCOL_MODE, "REST_XML");
            rawConfig.put(HttpSourceKeys.CONTENT_TYPE, "application/xml;charset=" + charsetName);
            rawConfig.put(HttpSourceKeys.REQUEST_BODY,
                    "<?xml version=\"1.0\" encoding=\"GBK\"?><request><name>旧客户</name></request>");
            HttpSourceScanner scanner = new HttpSourceScanner();
            HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));

            scanner.applyPushdown(config, Arrays.asList(
                    predicate("body", "name", null, "request.name", null, null, "新客户")));
            scanner.scan(config, row -> {
            });

            String received = receivedBodies.get("/xml-pushdown-" + suffix);
            assertTrue(received.contains("encoding=\"" + charsetName + "\""));
            assertTrue(received.contains("<name>新客户</name>"));
            assertFalse(received.contains("旧客户"));
        }
    }

    @Test
    public void deterministicRequestConfigurationErrorsShouldNotRetry() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            requests.incrementAndGet();
            writeJson(exchange, "{}");
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/invalid-method", "data");
        rawConfig.put(HttpSourceKeys.MODE, "INVALID");
        rawConfig.put(HttpSourceKeys.RETRY_TIMES, 5);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), row -> {
                        }));

        assertTrue(error.getMessage().contains("不支持的请求方法"));
        assertEquals(0, requests.get());
    }

    @Test
    public void postShouldNotRetryUnlessReaderExplicitlyAllowsIt() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            int request = requests.incrementAndGet();
            if (request == 1) {
                write(exchange, 503, "temporary");
            } else {
                writeJson(exchange, "{\"data\":{\"customer_id\":\"C092\",\"risk_score\":92}}");
            }
        });
        Map<String, Object> defaultConfig = jsonShapeConfig("/post-default", "data");
        defaultConfig.put(HttpSourceKeys.MODE, "POST");

        RuntimeException defaultError = assertThrows(RuntimeException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(defaultConfig)), row -> {
                        }));
        assertTrue(exceptionMessages(defaultError).contains("status 503"));
        assertEquals(1, requests.get());

        requests.set(0);
        Map<String, Object> explicitConfig = jsonShapeConfig("/post-explicit", "data");
        explicitConfig.put(HttpSourceKeys.MODE, "POST");
        explicitConfig.put(HttpSourceKeys.RETRY_NON_IDEMPOTENT, true);
        List<Map<String, Object>> rows = new ArrayList<>();
        new HttpSourceScanner().scan(
                HttpSourceConfig.fromConfiguration(Configuration.from(explicitConfig)), rows::add);

        assertEquals(2, requests.get());
        assertEquals(1, rows.size());
    }

    @Test
    public void soapShouldNotRetryByDefaultEvenWithGetMethod() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            requests.incrementAndGet();
            writeXml(exchange, 503,
                    "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                            + "<soap:Body><Notice>temporary soap outage</Notice></soap:Body>"
                            + "</soap:Envelope>");
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/soap-no-retry", "data");
        rawConfig.put(HttpSourceKeys.MODE, "GET");
        rawConfig.put(HttpSourceKeys.PROTOCOL_MODE, "SOAP");
        rawConfig.put(HttpSourceKeys.RESULT_TYPE, "json");

        RuntimeException error = assertThrows(RuntimeException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), row -> {
                        }));

        assertEquals(1, requests.get());
        assertTrue(exceptionMessages(error).contains("status 503"));
    }

    @Test
    public void noTotalPaginationShouldStopOnConfiguredRepeatedFullPageLimit() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            requests.incrementAndGet();
            writeJson(exchange, "{\"data\":{\"items\":["
                    + "{\"customer_id\":\"C093\",\"risk_score\":93},"
                    + "{\"customer_id\":\"C094\",\"risk_score\":94}]}}");
        });
        Map<String, Object> rawConfig = baseConfig("/repeat-page", "GET");
        rawConfig.put(HttpSourceKeys.TOTAL_CODE_PATH, "");
        rawConfig.put(HttpSourceKeys.RESPONSE_STATUS, new LinkedHashMap<>());
        rawConfig.put(HttpSourceKeys.MAX_REPEATED_PAGES, 1);
        List<Map<String, Object>> rows = new ArrayList<>();

        AggregationException error = assertThrows(AggregationException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), rows::add));

        assertEquals(2, requests.get());
        assertEquals(2, rows.size());
        assertTrue(error.getMessage().contains("重复的完整页"));
    }

    @Test
    public void noTotalPaginationShouldStopAlternatingFullPageLoop() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            int request = requests.incrementAndGet();
            String prefix = request % 2 == 1 ? "A" : "B";
            writeJson(exchange, "{\"data\":{\"items\":["
                    + "{\"customer_id\":\"" + prefix + "1\",\"risk_score\":91},"
                    + "{\"customer_id\":\"" + prefix + "2\",\"risk_score\":92}]}}");
        });
        Map<String, Object> rawConfig = baseConfig("/alternating-pages", "GET");
        rawConfig.put(HttpSourceKeys.TOTAL_CODE_PATH, "");
        rawConfig.put(HttpSourceKeys.RESPONSE_STATUS, new LinkedHashMap<>());
        List<Map<String, Object>> rows = new ArrayList<>();

        AggregationException error = assertThrows(AggregationException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), rows::add));

        assertEquals(4, requests.get());
        assertEquals(6, rows.size());
        assertTrue(error.getMessage().contains("重复的完整页"));
    }

    @Test
    public void totalPaginationShouldAlsoStopOnConfiguredRepeatedFullPageLimit() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            requests.incrementAndGet();
            writeJson(exchange, "{\"data\":{\"total\":100,\"items\":["
                    + "{\"customer_id\":\"C093\",\"risk_score\":93},"
                    + "{\"customer_id\":\"C094\",\"risk_score\":94}]}}");
        });
        Map<String, Object> rawConfig = baseConfig("/repeat-page-with-total", "GET");
        rawConfig.put(HttpSourceKeys.RESPONSE_STATUS, new LinkedHashMap<>());
        rawConfig.put(HttpSourceKeys.MAX_REPEATED_PAGES, 1);
        List<Map<String, Object>> rows = new ArrayList<>();

        AggregationException error = assertThrows(AggregationException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), rows::add));

        assertEquals(2, requests.get());
        assertEquals(2, rows.size());
        assertTrue(error.getMessage().contains("重复的完整页"));
    }

    @Test
    public void totalPaginationShouldStopWhenEndpointRepeatsShortPage() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            requests.incrementAndGet();
            writeJson(exchange, "{\"data\":{\"total\":100,\"items\":["
                    + "{\"customer_id\":\"C093\",\"risk_score\":93}]}}");
        });
        Map<String, Object> rawConfig = baseConfig("/repeat-short-page-with-total", "GET");
        rawConfig.put(HttpSourceKeys.RESPONSE_STATUS, new LinkedHashMap<>());
        rawConfig.put(HttpSourceKeys.MAX_REPEATED_PAGES, 1);
        List<Map<String, Object>> rows = new ArrayList<>();

        AggregationException error = assertThrows(AggregationException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), rows::add));

        assertEquals(2, requests.get());
        assertEquals(1, rows.size());
        assertTrue(error.getMessage().contains("重复的完整页或短页"));
    }

    @Test
    public void integralDecimalTotalShouldBeAccepted() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            int page = requests.incrementAndGet();
            String items = page < 3
                    ? "[{\"customer_id\":\"C" + page + "1\",\"risk_score\":91},"
                    + "{\"customer_id\":\"C" + page + "2\",\"risk_score\":92}]"
                    : "[{\"customer_id\":\"C31\",\"risk_score\":93}]";
            writeJson(exchange, "{\"data\":{\"total\":5.0,\"items\":" + items + "}}");
        });
        Map<String, Object> rawConfig = baseConfig("/decimal-total", "GET");
        rawConfig.put(HttpSourceKeys.RESPONSE_STATUS, new LinkedHashMap<>());
        List<Map<String, Object>> rows = new ArrayList<>();

        new HttpSourceScanner().scan(
                HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), rows::add);

        assertEquals(3, requests.get());
        assertEquals(5, rows.size());
    }

    @Test
    public void fractionalAndOverflowTotalsShouldFailClearly() throws Exception {
        startServer(exchange -> {
            String total = exchange.getRequestURI().getPath().contains("overflow")
                    ? "9223372036854775808"
                    : "5.5";
            writeJson(exchange, "{\"data\":{\"total\":" + total + ",\"items\":["
                    + "{\"customer_id\":\"C1\",\"risk_score\":91},"
                    + "{\"customer_id\":\"C2\",\"risk_score\":92}]}}");
        });

        for (String path : Arrays.asList("/fractional-total", "/overflow-total")) {
            Map<String, Object> rawConfig = baseConfig(path, "GET");
            rawConfig.put(HttpSourceKeys.RESPONSE_STATUS, new LinkedHashMap<>());

            AggregationException error = assertThrows(AggregationException.class, () ->
                    new HttpSourceScanner().scan(
                            HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), row -> {
                            }));

            assertTrue(error.getMessage().contains("总量必须是非负整数"));
            assertTrue(error.getMessage().contains("long 范围"));
        }
    }

    @Test
    public void sourcePluginShouldSendNoEntityWhenRequestBodyIsMissing() throws Exception {
        AtomicReference<String> requestBody = new AtomicReference<>();
        startServer(exchange -> {
            requestBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            writeJson(exchange, "{\"data\":{\"customer_id\":\"C1\",\"risk_score\":91}}");
        });
        Map<String, Object> rawConfig = jsonShapeConfig("/missing-body", "data");
        rawConfig.put(HttpSourceKeys.MODE, "POST");
        rawConfig.remove(HttpSourceKeys.REQUEST_BODY);

        new HttpSourceScanner().scan(
                HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), row -> {
                });

        assertEquals("", requestBody.get());
    }

    @Test
    public void noContentSuccessShouldReturnAnEmptyResult() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            requests.incrementAndGet();
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        Map<String, Object> rawConfig = baseConfig("/no-content", "GET");
        rawConfig.put(HttpSourceKeys.PAGE_READ, false);
        rawConfig.put(HttpSourceKeys.TOTAL_CODE_PATH, "");
        rawConfig.put(HttpSourceKeys.RESPONSE_STATUS, new LinkedHashMap<>());
        List<Map<String, Object>> rows = new ArrayList<>();

        new HttpSourceScanner().scan(
                HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), rows::add);

        assertEquals(1, requests.get());
        assertTrue(rows.isEmpty());
    }

    @Test
    public void knownTotalPaginationShouldFailWhenFirstPageHasEmptyBody() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            requests.incrementAndGet();
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        Map<String, Object> rawConfig = baseConfig("/empty-first-page", "GET");
        rawConfig.put(HttpSourceKeys.RESPONSE_STATUS, new LinkedHashMap<>());

        AggregationException error = assertThrows(AggregationException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), row -> {
                        }));

        assertEquals(1, requests.get());
        assertTrue(error.getMessage().contains("首页响应为空"));
    }

    @Test
    public void knownTotalPaginationShouldFailWhenLaterPageHasEmptyBody() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            if (requests.incrementAndGet() == 1) {
                writeJson(exchange, "{\"data\":{\"total\":3,\"items\":["
                        + "{\"customer_id\":\"C101\",\"risk_score\":101},"
                        + "{\"customer_id\":\"C102\",\"risk_score\":102}]}}");
            } else {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
            }
        });
        Map<String, Object> rawConfig = baseConfig("/empty-later-page", "GET");
        rawConfig.put(HttpSourceKeys.RESPONSE_STATUS, new LinkedHashMap<>());

        AggregationException error = assertThrows(AggregationException.class, () ->
                new HttpSourceScanner().scan(
                        HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), row -> {
                        }));

        assertEquals(2, requests.get());
        assertTrue(error.getMessage().contains("已知仍有未读取数据"));
    }

    @Test
    public void missingParentNodeShouldEmitNoRows() throws Exception {
        startServer(exchange -> writeJson(exchange, "{}"));
        Map<String, Object> rawConfig = jsonShapeConfig("/missing-parent", "data.items");
        List<Map<String, Object>> rows = new ArrayList<>();

        new HttpSourceScanner().scan(
                HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), rows::add);

        assertTrue(rows.isEmpty());
    }

    @Test
    public void pushedRequestContextShouldNotFillMissingOrNullPhysicalColumns() throws Exception {
        startServer(exchange -> writeJson(exchange, "{\"data\":{\"items\":["
                + "{\"risk_score\":103},{\"customer_id\":null,\"risk_score\":104}]}}"));
        Map<String, Object> rawConfig = jsonShapeConfig("/physical-null", "data.items");
        rawConfig.put(HttpSourceKeys.PARAMS, "{\"customer_id\":\"\",\"region\":\"\"}");
        HttpSourceScanner scanner = new HttpSourceScanner();
        HttpSourceConfig config = HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig));
        scanner.applyPushdown(config, Arrays.asList(
                predicate("param", "customer_id", "customer_id", null, null, null, "C001"),
                predicate("param", "region", "region", null, null, null, "EAST")));
        List<Map<String, Object>> rows = new ArrayList<>();

        scanner.scan(config, rows::add);

        assertEquals(2, rows.size());
        assertTrue(rows.get(0).containsKey("customer_id"));
        assertTrue(rows.get(0).get("customer_id") == null);
        assertTrue(rows.get(1).get("customer_id") == null);
        assertEquals("EAST", rows.get(0).get("region"));
        assertEquals("EAST", rows.get(1).get("region"));
    }

    @Test
    public void studioMaxRowsShouldStopBeforeRequestingAnotherFullPage() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            requests.incrementAndGet();
            writeJson(exchange, "{\"code\":\"200\",\"data\":{\"total\":4,\"items\":["
                    + "{\"customer_id\":\"C105\",\"risk_score\":105},"
                    + "{\"customer_id\":\"C106\",\"risk_score\":106}]}}");
        });
        BaseDataSourceDTO dto = new BaseDataSourceDTO();
        Map<String, String> extraParams = new LinkedHashMap<>();
        extraParams.put(HttpSourceKeys.STUDIO_READER_CONFIG,
                JSONObject.toJSONString(baseConfig("/max-rows", "GET")));
        extraParams.put(HttpSourceKeys.STUDIO_MAX_ROWS, "2");
        dto.setExtraParams(extraParams);
        List<Map<String, Object>> rows = new ArrayList<>();

        new HttpSourcePlugin().scanQuery(dto, "", true, rows::add);

        assertEquals(2, rows.size());
        assertEquals(1, requests.get());
    }

    @Test
    public void noTotalPaginationShouldUseRawPageContentForRepeatDetection() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        startServer(exchange -> {
            int page = requests.incrementAndGet();
            if (page < 4) {
                writeJson(exchange, "{\"data\":{\"items\":["
                        + "{\"customer_id\":\"C" + page + "1\",\"risk_score\":91},"
                        + "{\"customer_id\":\"C" + page + "2\",\"risk_score\":91}]}}");
            } else {
                writeJson(exchange, "{\"data\":{\"items\":["
                        + "{\"customer_id\":\"C41\",\"risk_score\":91}]}}");
            }
        });
        Map<String, Object> rawConfig = baseConfig("/distinct-pages", "GET");
        rawConfig.put(HttpSourceKeys.TOTAL_CODE_PATH, "");
        rawConfig.put(HttpSourceKeys.RESPONSE_STATUS, new LinkedHashMap<>());
        rawConfig.put(HttpSourceKeys.COLUMNS,
                Collections.singletonList(column("data.items", "risk_score", "LONG")));
        List<Map<String, Object>> rows = new ArrayList<>();

        new HttpSourceScanner().scan(
                HttpSourceConfig.fromConfiguration(Configuration.from(rawConfig)), rows::add);

        assertEquals(4, requests.get());
        assertEquals(7, rows.size());
    }

    private void startServer(ExchangeHandler handler) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                handler.handle(exchange);
            } catch (Exception ex) {
                byte[] body = String.valueOf(ex.getMessage()).getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(500, body.length);
                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(body);
                }
            }
        });
        executor = Executors.newSingleThreadExecutor();
        server.setExecutor(executor);
        server.start();
    }

    private Map<String, Object> baseConfig(String path, String method) {
        return baseConfigForUrl(baseUrl() + path, method);
    }

    private Map<String, Object> offlineConfig(String path, String method) {
        return baseConfigForUrl("http://127.0.0.1" + path, method);
    }

    private Map<String, Object> baseConfigForUrl(String url, String method) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(HttpSourceKeys.URL, url);
        config.put(HttpSourceKeys.MODE, method);
        config.put(HttpSourceKeys.HEADER, "{}");
        config.put(HttpSourceKeys.PARAMS, "{\"page\":\"{dyn_page}\",\"size\":\"{dyn_pageSize}\"}");
        config.put(HttpSourceKeys.REQUEST_BODY, "{}");
        config.put(HttpSourceKeys.RESULT_TYPE, "json");
        config.put(HttpSourceKeys.SOAP_FAULT_FAIL, false);
        config.put(HttpSourceKeys.RESPONSE_STATUS, responseStatus("code"));
        config.put(HttpSourceKeys.PAGE_READ, true);
        config.put(HttpSourceKeys.PAGE_SIZE, 2);
        config.put(HttpSourceKeys.RETRY_INTERVAL_MS, 0L);
        config.put(HttpSourceKeys.TOTAL_CODE_PATH, "data.total");
        config.put(HttpSourceKeys.COLUMNS, columns("data.items"));
        return config;
    }

    private Map<String, Object> xmlConfig(String path) {
        Map<String, Object> config = baseConfig(path, "GET");
        config.put(HttpSourceKeys.RESULT_TYPE, "xml");
        config.put(HttpSourceKeys.RESPONSE_STATUS, responseStatus("root.code"));
        config.put(HttpSourceKeys.PAGE_READ, false);
        config.put(HttpSourceKeys.TOTAL_CODE_PATH, "");
        config.put(HttpSourceKeys.COLUMNS, columns("root.data.items"));
        return config;
    }

    private Map<String, Object> soapConfig(String path) {
        Map<String, Object> config = baseConfig(path, "POST");
        config.put(HttpSourceKeys.PROTOCOL_MODE, "SOAP");
        config.put(HttpSourceKeys.RESULT_TYPE, "soap");
        config.put(HttpSourceKeys.RESPONSE_STATUS, responseStatus("QueryRowsResponse.code"));
        config.put(HttpSourceKeys.PAGE_READ, false);
        config.put(HttpSourceKeys.TOTAL_CODE_PATH, "");
        config.put(HttpSourceKeys.COLUMNS, columns("QueryRowsResponse.items"));
        return config;
    }

    private Map<String, Object> jsonShapeConfig(String path, String parentNode) {
        Map<String, Object> config = baseConfig(path, "GET");
        config.put(HttpSourceKeys.RESPONSE_STATUS, new LinkedHashMap<>());
        config.put(HttpSourceKeys.PAGE_READ, false);
        config.put(HttpSourceKeys.TOTAL_CODE_PATH, "");
        config.put(HttpSourceKeys.COLUMNS, columns(parentNode));
        return config;
    }

    private Map<String, Object> responseStatus(String path) {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("path", path);
        status.put("code", "200");
        return status;
    }

    private List<Map<String, Object>> columns(String parentNode) {
        List<Map<String, Object>> columns = new ArrayList<>();
        columns.add(column(parentNode, "customer_id", "STRING"));
        columns.add(column(parentNode, "risk_score", "LONG"));
        return columns;
    }

    private Map<String, Object> column(String parentNode, String name, String type) {
        Map<String, Object> column = new LinkedHashMap<>();
        column.put("parentNode", parentNode);
        column.put("name", name);
        column.put("type", type);
        return column;
    }

    private HttpPushdownPredicate predicate(String location,
                                            String field,
                                            String requestParamName,
                                            String bodyPath,
                                            String headerName,
                                            String pathVariable,
                                            Object value) {
        HttpPushdownPredicate predicate = new HttpPushdownPredicate();
        predicate.setLocation(location);
        predicate.setField(field);
        predicate.setRequestParamName(requestParamName);
        predicate.setBodyPath(bodyPath);
        predicate.setHeaderName(headerName);
        predicate.setPathVariable(pathVariable);
        predicate.setOperator("=");
        predicate.setValues(Arrays.asList(value));
        return predicate;
    }

    private String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private void writeJson(HttpExchange exchange, String body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json;charset=utf-8");
        write(exchange, body);
    }

    private void writeXml(HttpExchange exchange, String body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/xml;charset=utf-8");
        write(exchange, body);
    }

    private void writeXml(HttpExchange exchange, int statusCode, String body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/xml;charset=utf-8");
        write(exchange, statusCode, body);
    }

    private void write(HttpExchange exchange, String body) throws IOException {
        write(exchange, 200, body);
    }

    private void write(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    private String exceptionMessages(Throwable throwable) {
        StringBuilder messages = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null) {
                messages.append(current.getMessage()).append('\n');
            }
            current = current.getCause();
        }
        return messages.toString();
    }

    private boolean hasCause(Throwable throwable, Class<? extends Throwable> expectedType) {
        Throwable current = throwable;
        while (current != null) {
            if (expectedType.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private interface ExchangeHandler {
        void handle(HttpExchange exchange) throws Exception;
    }
}
