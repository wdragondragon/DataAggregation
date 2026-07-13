package com.jdragon.aggregation.httpreader;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.plugin.httpdyn.exception.HttpDynException;
import com.jdragon.aggregation.plugin.httpdyn.impl.FromHttpToken;
import com.jdragon.aggregation.unstructuredstorage.ValidateUtil;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class HttpReaderRequestBodyDynamicTest {

    private HttpServer server;

    @After
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void connectToGetDataUsesDynamicReplacedRequestBody() throws Exception {
        AtomicReference<String> capturedBody = new AtomicReference<>("");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/token", exchange -> respond(exchange, "{\"data\":{\"token\":\"TOKEN-OK\"}}"));
        server.createContext("/data", exchange -> {
            capturedBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, "{\"data\":[{\"id\":1}],\"total\":1}");
        });
        server.start();

        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        String requestBody = "{\"page\":\"{dyn_page}\","
                + "\"pageSize\":\"{dyn_pageSize}\","
                + "\"offset\":\"{dyn_offset}\","
                + "\"token\":\"{dyn_from_http_token(GET," + baseUrl + "/token,,,data.token)}\"}";

        HttpReader reader = new HttpReader();
        reader.setPluginJobConf(Configuration.from(readerConfig(baseUrl + "/data", requestBody)));
        reader.init();

        Map<String, String> httpContent = new HashMap<>();
        httpContent.put(Key.HEADER, "{}");
        httpContent.put(Key.PARAM_STR, "{}");
        httpContent.put(Key.REQUEST_BODY, requestBody);

        reader.connectToGetData(httpContent, Integer.valueOf(1));

        String body = capturedBody.get();
        assertFalse(body.contains("dyn_page"));
        assertFalse(body.contains("dyn_pageSize"));
        assertFalse(body.contains("dyn_offset"));
        assertFalse(body.contains("dyn_from_http_token"));
        assertTrue(body.contains("TOKEN-OK"));
        assertTrue(body.contains("page"));
        assertTrue(body.contains("1"));
        assertTrue(body.contains("pageSize"));
        assertTrue(body.contains("25"));
        assertTrue(body.contains("offset"));
        assertTrue(body.contains("0"));
    }

    @Test
    public void soapResponseShouldParseByLocalNameAndIgnoreNamespacePrefix() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/soap", exchange -> respond(exchange,
                "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ns=\"urn:test\">"
                        + "<soap:Body><ns:QueryResponse><ns:items><ns:id>1</ns:id><ns:name>A</ns:name></ns:items>"
                        + "<ns:items><ns:id>2</ns:id><ns:name>B</ns:name></ns:items></ns:QueryResponse></soap:Body>"
                        + "</soap:Envelope>"));
        server.start();

        HttpReader reader = new HttpReader();
        Map<String, Object> config = readerConfig("http://127.0.0.1:" + server.getAddress().getPort() + "/soap", "");
        config.put(Key.PROTOCOL_MODE, "SOAP");
        config.put(Key.RESULT_DATA_TYPE, "soap");
        config.remove(Key.CONTENT_TYPE);
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();

        String result = reader.connectToGetData(httpContent(""), Integer.valueOf(1));

        assertTrue(result.contains("\"QueryResponse\""));
        assertTrue(result.contains("\"items\""));
        assertTrue(result.contains("\"id\":\"1\""));
        assertFalse(result.contains("soap:"));
        assertFalse(result.contains("ns:"));
    }

    @Test
    public void soapFaultShouldFailReader() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/fault", exchange -> respond(exchange,
                "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                        + "<soap:Body><soap:Fault><faultcode>soap:Client</faultcode><faultstring>bad request</faultstring></soap:Fault></soap:Body>"
                        + "</soap:Envelope>"));
        server.start();

        HttpReader reader = new HttpReader();
        Map<String, Object> config = readerConfig("http://127.0.0.1:" + server.getAddress().getPort() + "/fault", "");
        config.put(Key.PROTOCOL_MODE, "SOAP");
        config.put(Key.RESULT_DATA_TYPE, "soap");
        config.remove(Key.CONTENT_TYPE);
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();

        AggregationException exception = assertThrows(AggregationException.class,
                () -> reader.connectToGetData(httpContent(""), Integer.valueOf(1)));
        assertTrue(exception.getMessage().contains("SOAP Fault"));
        assertFalse(exception.getMessage().contains("bad request"));
    }

    @Test
    public void invalidXmlShouldFailReaderWithClearError() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/broken", exchange -> respond(exchange, "<root><item>1</root>"));
        server.start();

        HttpReader reader = new HttpReader();
        Map<String, Object> config = readerConfig("http://127.0.0.1:" + server.getAddress().getPort() + "/broken", "");
        config.put(Key.RESULT_DATA_TYPE, "xml");
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();

        AggregationException exception = assertThrows(AggregationException.class,
                () -> reader.connectToGetData(httpContent(""), Integer.valueOf(1)));
        assertTrue(exception.getMessage().contains("XML response parse failed"));
    }

    @Test
    public void tokenHttpErrorShouldNotExposeResponseBodyInException() throws Exception {
        String secretBody = "token-response-secret";
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/token-error", exchange -> respond(exchange, 500, secretBody));
        server.start();

        HttpDynException exception = assertThrows(HttpDynException.class, () -> new FromHttpToken().execute(
                "GET",
                "http://127.0.0.1:" + server.getAddress().getPort() + "/token-error",
                "",
                "",
                "data.token"));

        assertFalse(exception.getMessage().contains(secretBody));
        assertTrue(exception.getMessage().contains("HTTP status 500"));
    }

    @Test
    public void readerShouldStopBeforeEmittingConfiguredRepeatedFullPage() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/repeat", exchange -> {
            requests.incrementAndGet();
            respond(exchange, "{\"data\":{\"items\":["
                    + "{\"id\":1},{\"id\":2}]}}");
        });
        server.start();

        Map<String, Object> config = readerConfig(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/repeat", "");
        config.put(Key.MODE, "GET");
        config.put(Key.PARAM_STR, "{\"page\":\"{dyn_page}\",\"size\":\"{dyn_pageSize}\"}");
        config.put(Key.PAGE_READ, true);
        config.put(Key.PAGE_SIZE, 2);
        config.put(Key.TOTAL_CODE_PATH, "");
        config.put(Key.MAX_REPEATED_PAGES, 1);
        config.put(Key.COLUMN, columns("data.items"));
        HttpReader reader = new HttpReader();
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();
        CapturingRecordSender sender = new CapturingRecordSender();

        AggregationException error = assertThrows(AggregationException.class, () -> reader.startRead(sender));

        assertEquals(2, requests.get());
        assertEquals(2, sender.records.size());
        assertTrue(error.getMessage().contains("重复的完整页"));
    }

    @Test
    public void readerShouldStopAlternatingFullPageLoopWithoutTotal() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/alternating", exchange -> {
            int request = requests.incrementAndGet();
            int base = request % 2 == 1 ? 10 : 20;
            respond(exchange, "{\"data\":{\"items\":["
                    + "{\"id\":" + (base + 1) + "},{\"id\":" + (base + 2) + "}]}}");
        });
        server.start();

        Map<String, Object> config = readerConfig(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/alternating", "");
        config.put(Key.MODE, "GET");
        config.put(Key.PARAM_STR, "{\"page\":\"{dyn_page}\",\"size\":\"{dyn_pageSize}\"}");
        config.put(Key.PAGE_READ, true);
        config.put(Key.PAGE_SIZE, 2);
        config.put(Key.TOTAL_CODE_PATH, "");
        config.put(Key.COLUMN, columns("data.items"));
        HttpReader reader = new HttpReader();
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();
        CapturingRecordSender sender = new CapturingRecordSender();

        AggregationException error = assertThrows(AggregationException.class, () -> reader.startRead(sender));

        assertEquals(4, requests.get());
        assertEquals(6, sender.records.size());
        assertTrue(error.getMessage().contains("重复的完整页"));
    }

    @Test
    public void readerShouldStopRepeatedFullPageEvenWhenTotalPathIsConfigured() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/repeat-with-total", exchange -> {
            requests.incrementAndGet();
            respond(exchange, "{\"data\":{\"total\":100,\"items\":["
                    + "{\"id\":1},{\"id\":2}]}}");
        });
        server.start();

        Map<String, Object> config = readerConfig(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/repeat-with-total", "");
        config.put(Key.MODE, "GET");
        config.put(Key.PARAM_STR, "{\"page\":\"{dyn_page}\",\"size\":\"{dyn_pageSize}\"}");
        config.put(Key.PAGE_READ, true);
        config.put(Key.PAGE_SIZE, 2);
        config.put(Key.TOTAL_CODE_PATH, "data.total");
        config.put(Key.MAX_REPEATED_PAGES, 1);
        config.put(Key.COLUMN, columns("data.items"));
        HttpReader reader = new HttpReader();
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();
        CapturingRecordSender sender = new CapturingRecordSender();

        AggregationException error = assertThrows(AggregationException.class, () -> reader.startRead(sender));

        assertEquals(2, requests.get());
        assertEquals(2, sender.records.size());
        assertTrue(error.getMessage().contains("重复的完整页"));
    }

    @Test
    public void readerShouldStopRepeatedShortPageWhenTotalPathIsConfigured() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/repeat-short-with-total", exchange -> {
            requests.incrementAndGet();
            respond(exchange, "{\"data\":{\"total\":100,\"items\":[{\"id\":1}]}}");
        });
        server.start();

        Map<String, Object> config = readerConfig(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/repeat-short-with-total", "");
        config.put(Key.MODE, "GET");
        config.put(Key.PARAM_STR, "{\"page\":\"{dyn_page}\",\"size\":\"{dyn_pageSize}\"}");
        config.put(Key.PAGE_READ, true);
        config.put(Key.PAGE_SIZE, 2);
        config.put(Key.TOTAL_CODE_PATH, "data.total");
        config.put(Key.MAX_REPEATED_PAGES, 1);
        config.put(Key.COLUMN, columns("data.items"));
        HttpReader reader = new HttpReader();
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();
        CapturingRecordSender sender = new CapturingRecordSender();

        AggregationException error = assertThrows(AggregationException.class, () -> reader.startRead(sender));

        assertEquals(2, requests.get());
        assertEquals(1, sender.records.size());
        assertTrue(error.getMessage().contains("重复的完整页或短页"));
    }

    @Test
    public void readerShouldAcceptIntegralDecimalTotal() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/decimal-total", exchange -> {
            int page = requests.incrementAndGet();
            String items = page < 3
                    ? "[{\"id\":" + (page * 10 + 1) + "},{\"id\":" + (page * 10 + 2) + "}]"
                    : "[{\"id\":31}]";
            respond(exchange, "{\"data\":{\"total\":5.0,\"items\":" + items + "}}");
        });
        server.start();

        Map<String, Object> config = readerConfig(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/decimal-total", "");
        config.put(Key.MODE, "GET");
        config.put(Key.PARAM_STR, "{\"page\":\"{dyn_page}\",\"size\":\"{dyn_pageSize}\"}");
        config.put(Key.PAGE_READ, true);
        config.put(Key.PAGE_SIZE, 2);
        config.put(Key.TOTAL_CODE_PATH, "data.total");
        config.put(Key.COLUMN, columns("data.items"));
        HttpReader reader = new HttpReader();
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();
        CapturingRecordSender sender = new CapturingRecordSender();

        reader.startRead(sender);

        assertEquals(3, requests.get());
        assertEquals(5, sender.records.size());
    }

    @Test
    public void readerShouldRejectFractionalAndOverflowTotalsClearly() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/fractional-total", exchange -> respond(exchange,
                "{\"data\":{\"total\":5.5,\"items\":[{\"id\":1},{\"id\":2}]}}"));
        server.createContext("/overflow-total", exchange -> respond(exchange,
                "{\"data\":{\"total\":9223372036854775808,\"items\":[{\"id\":1},{\"id\":2}]}}"));
        server.start();

        for (String path : new String[]{"/fractional-total", "/overflow-total"}) {
            Map<String, Object> config = readerConfig(
                    "http://127.0.0.1:" + server.getAddress().getPort() + path, "");
            config.put(Key.MODE, "GET");
            config.put(Key.PARAM_STR, "{\"page\":\"{dyn_page}\",\"size\":\"{dyn_pageSize}\"}");
            config.put(Key.PAGE_READ, true);
            config.put(Key.PAGE_SIZE, 2);
            config.put(Key.TOTAL_CODE_PATH, "data.total");
            config.put(Key.COLUMN, columns("data.items"));
            HttpReader reader = new HttpReader();
            reader.setPluginJobConf(Configuration.from(config));
            reader.init();

            AggregationException error = assertThrows(AggregationException.class,
                    () -> reader.startRead(new CapturingRecordSender()));

            assertTrue(error.getMessage().contains("总量必须是非负整数"));
            assertTrue(error.getMessage().contains("long 范围"));
        }
    }

    @Test
    public void readerShouldKeepLegacyEmptyJsonBodyWhenRequestBodyIsMissing() throws Exception {
        AtomicReference<String> requestBody = new AtomicReference<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/missing-body", exchange -> {
            requestBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, "{\"data\":{\"items\":[]}}");
        });
        server.start();

        Map<String, Object> config = readerConfig(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/missing-body", "ignored");
        config.remove(Key.REQUEST_BODY);
        config.put(Key.COLUMN, columns("data.items"));
        HttpReader reader = new HttpReader();
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();

        reader.startRead(new CapturingRecordSender());

        assertEquals("{}", requestBody.get());
    }

    @Test
    public void readerNetworkFailureShouldRetainCauseWithoutExposingRequestSecrets() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        Map<String, Object> config = readerConfig(
                "http://127.0.0.1:" + port + "/private?access_token=secret-query",
                "{\"password\":\"secret-body\"}");
        config.put(Key.MODE, "PUT");
        config.put(Key.HEADER, "{\"Authorization\":\"Bearer secret-header\"}");
        config.put(Key.PARAM_STR, "{\"api_key\":\"secret-param\"}");
        config.put(Key.RETRY_TIMES, 2);
        config.put(Key.RETRY_INTERVAL_MS, 0L);
        config.put(Key.COLUMN, columns("data.items"));
        HttpReader reader = new HttpReader();
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();

        RuntimeException error = assertThrows(RuntimeException.class,
                () -> reader.startRead(new CapturingRecordSender()));

        String messages = exceptionMessages(error);
        assertTrue(messages.contains("after 2 attempts"));
        assertTrue(hasCause(error, IOException.class));
        assertFalse(messages.contains("secret-query"));
        assertFalse(messages.contains("secret-header"));
        assertFalse(messages.contains("secret-param"));
        assertFalse(messages.contains("secret-body"));
    }

    @Test
    public void readerShouldTreatNoContentAsAnEmptySuccessfulResult() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/no-content", exchange -> {
            requests.incrementAndGet();
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        server.start();

        Map<String, Object> config = readerConfig(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/no-content", "");
        config.put(Key.MODE, "GET");
        config.put(Key.PAGE_READ, false);
        config.put(Key.COLUMN, columns("data.items"));
        HttpReader reader = new HttpReader();
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();
        CapturingRecordSender sender = new CapturingRecordSender();

        reader.startRead(sender);

        assertEquals(1, requests.get());
        assertTrue(sender.records.isEmpty());
    }

    @Test
    public void readerShouldFailWhenKnownTotalFirstPageReturnsNoContent() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/empty-first-page", exchange -> {
            requests.incrementAndGet();
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        server.start();

        Map<String, Object> config = readerConfig(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/empty-first-page", "");
        config.put(Key.MODE, "GET");
        config.put(Key.PARAM_STR, "{\"page\":\"{dyn_page}\",\"size\":\"{dyn_pageSize}\"}");
        config.put(Key.PAGE_READ, true);
        config.put(Key.TOTAL_CODE_PATH, "data.total");
        config.put(Key.COLUMN, columns("data.items"));
        HttpReader reader = new HttpReader();
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();

        AggregationException error = assertThrows(AggregationException.class,
                () -> reader.startRead(new CapturingRecordSender()));

        assertEquals(1, requests.get());
        assertTrue(error.getMessage().contains("首页响应为空"));
    }

    @Test
    public void readerShouldFailWhenKnownRemainingPageReturnsNoContent() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/early-end", exchange -> {
            if (requests.incrementAndGet() == 1) {
                respond(exchange, "{\"data\":{\"total\":4,\"items\":[{\"id\":1},{\"id\":2}]}}");
                return;
            }
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        server.start();

        Map<String, Object> config = readerConfig(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/early-end", "");
        config.put(Key.MODE, "GET");
        config.put(Key.PARAM_STR, "{\"page\":\"{dyn_page}\",\"size\":\"{dyn_pageSize}\"}");
        config.put(Key.PAGE_READ, true);
        config.put(Key.PAGE_SIZE, 2);
        config.put(Key.TOTAL_CODE_PATH, "data.total");
        config.put(Key.COLUMN, columns("data.items"));
        HttpReader reader = new HttpReader();
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();
        CapturingRecordSender sender = new CapturingRecordSender();

        AggregationException error = assertThrows(AggregationException.class, () -> reader.startRead(sender));

        assertEquals(2, requests.get());
        assertEquals(2, sender.records.size());
        assertTrue(error.getMessage().contains("分页响应提前结束"));
    }

    @Test
    public void validateUtilShouldAcceptSoapDateTimeAndNumericBooleanValues() {
        Column date = ValidateUtil.validate("date_val", "DATE", "2026-06-02");
        Column datetime = ValidateUtil.validate("datetime_val", "DATETIME", "2026-06-02 12:34:56");
        Column timestamp = ValidateUtil.validate("timestamp_val", "TIMESTAMP", "2026-06-02T12:34:56.123");
        Column trueValue = ValidateUtil.validate("bool_flag", "BOOLEAN", "1");
        Column falseValue = ValidateUtil.validate("bool_flag", "BOOLEAN", "0");

        assertEquals(Column.Type.DATE, date.getType());
        assertEquals(Column.Type.DATE, datetime.getType());
        assertEquals(Column.Type.DATE, timestamp.getType());
        assertTrue(date.asDate() != null);
        assertTrue(datetime.asDate() != null);
        assertTrue(timestamp.asDate() != null);
        assertEquals(Boolean.TRUE, trueValue.asBoolean());
        assertEquals(Boolean.FALSE, falseValue.asBoolean());
        assertEquals(Long.valueOf(1L), trueValue.asLong());
        assertEquals(Long.valueOf(0L), falseValue.asLong());
    }

    private static Map<String, Object> readerConfig(String url, String requestBody) {
        Map<String, Object> config = new HashMap<>();
        config.put(Key.URL, url);
        config.put(Key.MODE, "POST");
        config.put(Key.CONTENT_TYPE, "application/json;charset=utf-8");
        config.put(Key.HEADER, "{}");
        config.put(Key.PARAM_STR, "{}");
        config.put(Key.REQUEST_BODY, requestBody);
        config.put(Key.RESULT_DATA_TYPE, "json");
        config.put(Key.PAGE_SIZE, Integer.valueOf(25));
        config.put(Key.PAGE_READ, Boolean.FALSE);
        return config;
    }

    private static Map<String, String> httpContent(String requestBody) {
        Map<String, String> content = new HashMap<>();
        content.put(Key.HEADER, "{}");
        content.put(Key.PARAM_STR, "{}");
        content.put(Key.REQUEST_BODY, requestBody);
        return content;
    }

    private static List<Map<String, Object>> columns(String parentNode) {
        List<Map<String, Object>> columns = new ArrayList<>();
        Map<String, Object> id = new HashMap<>();
        id.put("parentNode", parentNode);
        id.put("name", "id");
        id.put("type", "LONG");
        columns.add(id);
        return columns;
    }

    private static void respond(HttpExchange exchange, String body) throws IOException {
        respond(exchange, 200, body);
    }

    private static void respond(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json;charset=utf-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
        }
    }

    private static String exceptionMessages(Throwable throwable) {
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

    private static boolean hasCause(Throwable throwable, Class<? extends Throwable> expectedType) {
        Throwable current = throwable;
        while (current != null) {
            if (expectedType.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static final class CapturingRecordSender implements RecordSender {
        private final List<Record> records = new ArrayList<>();

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
}
