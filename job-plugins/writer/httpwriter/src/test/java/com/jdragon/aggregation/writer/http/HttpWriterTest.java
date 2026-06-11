package com.jdragon.aggregation.writer.http;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.DateColumn;
import com.jdragon.aggregation.commons.element.LongColumn;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.element.TimestampColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class HttpWriterTest {

    private HttpServer server;

    @After
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void arrayPayloadShouldSendBatchesWithTemplateTotalAndDynamicValues() throws Exception {
        List<CapturedRequest> requests = new ArrayList<>();
        AtomicInteger tokenCount = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/token", exchange -> {
            tokenCount.incrementAndGet();
            respond(exchange, 200, "{\"data\":{\"token\":\"TOKEN-OK\"}}");
        });
        server.createContext("/write", exchange -> {
            requests.add(capture(exchange));
            respond(exchange, 200, "{\"code\":\"200\"}");
        });
        server.start();

        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        HttpWriter writer = new HttpWriter();
        writer.setPluginJobConf(Configuration.from(writerConfig(baseUrl + "/write",
                "array",
                "{\"meta\":{\"page\":\"{dyn_page}\",\"pageSize\":\"{dyn_pageSize}\",\"offset\":\"{dyn_offset}\"}}",
                "data.items",
                true,
                "data.total",
                "{\"Authorization\":\"Bearer {dyn_from_http_token(GET," + baseUrl + "/token,,,data.token)}\"}",
                "{\"page\":\"{dyn_page}\"}",
                2,
                responseStatus())));
        writer.init();

        writer.startWrite(new ListRecordReceiver(Arrays.asList(record(1L, "a"), record(2L, "b"), record(3L, "c"))));

        assertEquals(2, requests.size());
        assertTrue(tokenCount.get() >= 2);

        CapturedRequest first = requests.get(0);
        assertEquals("Bearer TOKEN-OK", first.headers.get("Authorization"));
        assertEquals("page=1", first.query);
        JSONObject firstBody = JSONObject.parseObject(first.body);
        assertEquals(Integer.valueOf(1), firstBody.getJSONObject("meta").getInteger("page"));
        assertEquals(Integer.valueOf(2), firstBody.getJSONObject("meta").getInteger("pageSize"));
        assertEquals(Integer.valueOf(0), firstBody.getJSONObject("meta").getInteger("offset"));
        assertEquals(Integer.valueOf(2), firstBody.getJSONObject("data").getInteger("total"));
        JSONArray firstItems = firstBody.getJSONObject("data").getJSONArray("items");
        assertEquals(2, firstItems.size());
        assertEquals(Long.valueOf(1L), firstItems.getJSONObject(0).getLong("id"));
        assertEquals("a", firstItems.getJSONObject(0).getString("name"));

        CapturedRequest second = requests.get(1);
        assertEquals("page=2", second.query);
        JSONObject secondBody = JSONObject.parseObject(second.body);
        assertEquals(Integer.valueOf(2), secondBody.getJSONObject("meta").getInteger("page"));
        assertEquals(Integer.valueOf(1), secondBody.getJSONObject("meta").getInteger("pageSize"));
        assertEquals(Integer.valueOf(2), secondBody.getJSONObject("meta").getInteger("offset"));
        assertEquals(Integer.valueOf(1), secondBody.getJSONObject("data").getInteger("total"));
        assertEquals(1, secondBody.getJSONObject("data").getJSONArray("items").size());
    }

    @Test
    public void objectPayloadShouldSendRawJsonBodyWithoutDoubleEncoding() throws Exception {
        List<CapturedRequest> requests = new ArrayList<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/write", exchange -> {
            requests.add(capture(exchange));
            respond(exchange, 200, "{\"code\":\"200\"}");
        });
        server.start();

        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        HttpWriter writer = new HttpWriter();
        writer.setPluginJobConf(Configuration.from(writerConfig(baseUrl + "/write",
                "object",
                "",
                "",
                false,
                "",
                "{}",
                "{}",
                500,
                new HashMap<>())));
        writer.init();

        writer.startWrite(new ListRecordReceiver(Arrays.asList(record(10L, ""))));

        assertEquals(1, requests.size());
        assertEquals("{\"id\":10,\"name\":\"\"}", requests.get(0).body);
    }

    @Test
    public void nonSuccessfulHttpStatusShouldFailJob() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/write", exchange -> respond(exchange, 500, "{\"code\":\"500\"}"));
        server.start();

        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        HttpWriter writer = new HttpWriter();
        writer.setPluginJobConf(Configuration.from(writerConfig(baseUrl + "/write",
                "object",
                "",
                "",
                false,
                "",
                "{}",
                "{}",
                500,
                new HashMap<>())));
        writer.init();

        assertThrows(AggregationException.class, () -> writer.startWrite(new ListRecordReceiver(Arrays.asList(record(1L, "a")))));
    }

    @Test
    public void soapPayloadShouldApplyTemplateEscapeXmlAndValidateSoapResponse() throws Exception {
        List<CapturedRequest> requests = new ArrayList<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/soap", exchange -> {
            requests.add(capture(exchange));
            respondXml(exchange, 200,
                    "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ns=\"urn:test\">"
                            + "<soap:Body><ns:WriteResponse><ns:code>200</ns:code></ns:WriteResponse></soap:Body>"
                            + "</soap:Envelope>");
        });
        server.start();

        Map<String, Object> config = writerConfig("http://127.0.0.1:" + server.getAddress().getPort() + "/soap",
                "object",
                "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tns=\"urn:test\">"
                        + "<soap:Body><tns:Write><tns:id>{{id}}</tns:id><tns:name>{{name}}</tns:name></tns:Write></soap:Body>"
                        + "</soap:Envelope>",
                "",
                false,
                "",
                "{}",
                "{}",
                500,
                soapResponseStatus());
        config.remove(Key.CONTENT_TYPE);
        config.put(Key.PROTOCOL_MODE, "SOAP");
        config.put(Key.SOAP_ACTION, "urn:test/Write");
        config.put(Key.PAYLOAD_FORMAT, "soap");
        config.put(Key.RESPONSE_TYPE, "soap");

        HttpWriter writer = new HttpWriter();
        writer.setPluginJobConf(Configuration.from(config));
        writer.init();

        writer.startWrite(new ListRecordReceiver(Arrays.asList(record(7L, "a&<b"))));

        assertEquals(1, requests.size());
        CapturedRequest request = requests.get(0);
        assertTrue(headerValue(request, "Content-Type").contains("text/xml"));
        assertEquals("urn:test/Write", headerValue(request, "SOAPAction"));
        assertTrue(request.body.contains("<tns:id>7</tns:id>"));
        assertTrue(request.body.contains("<tns:name>a&amp;&lt;b</tns:name>"));
    }

    @Test
    public void soapFaultShouldFailWriter() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/soap", exchange -> respondXml(exchange, 200,
                "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                        + "<soap:Body><soap:Fault><faultcode>soap:Server</faultcode><faultstring>write rejected</faultstring></soap:Fault></soap:Body>"
                        + "</soap:Envelope>"));
        server.start();

        Map<String, Object> config = writerConfig("http://127.0.0.1:" + server.getAddress().getPort() + "/soap",
                "object",
                "<Envelope><Body><Write><id>{{id}}</id></Write></Body></Envelope>",
                "",
                false,
                "",
                "{}",
                "{}",
                500,
                new HashMap<>());
        config.put(Key.PROTOCOL_MODE, "SOAP");
        config.put(Key.PAYLOAD_FORMAT, "soap");

        HttpWriter writer = new HttpWriter();
        writer.setPluginJobConf(Configuration.from(config));
        writer.init();

        AggregationException exception = assertThrows(AggregationException.class,
                () -> writer.startWrite(new ListRecordReceiver(Arrays.asList(record(1L, "a")))));
        assertTrue(exception.getMessage().contains("SOAP Fault"));
        assertTrue(exception.getMessage().contains("write rejected"));
    }

    @Test
    public void soapArrayPayloadShouldSendBatchesWithRepeatBlock() throws Exception {
        List<CapturedRequest> requests = new ArrayList<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/soap", exchange -> {
            requests.add(capture(exchange));
            respondXml(exchange, 200,
                    "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ns=\"urn:test\">"
                            + "<soap:Body><ns:WriteBatchResponse><ns:code>200</ns:code></ns:WriteBatchResponse></soap:Body>"
                            + "</soap:Envelope>");
        });
        server.start();

        Map<String, Object> config = writerConfig("http://127.0.0.1/soap",
                "array",
                "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tns=\"urn:test\">"
                        + "<soap:Body><tns:WriteBatch><tns:batchSize>{dyn_pageSize}</tns:batchSize><tns:records>"
                        + "{{#records}}<tns:record><tns:id>{{id}}</tns:id><tns:name>{{name}}</tns:name></tns:record>{{/records}}"
                        + "</tns:records></tns:WriteBatch></soap:Body></soap:Envelope>",
                "",
                false,
                "",
                "{}",
                "{}",
                2,
                soapBatchResponseStatus());
        config.put(Key.URL, "http://127.0.0.1:" + server.getAddress().getPort() + "/soap");
        config.remove(Key.CONTENT_TYPE);
        config.put(Key.PROTOCOL_MODE, "SOAP");
        config.put(Key.SOAP_ACTION, "urn:test/WriteBatch");
        config.put(Key.PAYLOAD_FORMAT, "soap");
        config.put(Key.RESPONSE_TYPE, "soap");

        HttpWriter writer = new HttpWriter();
        writer.setPluginJobConf(Configuration.from(config));
        writer.init();

        writer.startWrite(new ListRecordReceiver(Arrays.asList(record(1L, "a&<b"), record(2L, "c"), record(3L, null))));

        assertEquals(2, requests.size());
        CapturedRequest first = requests.get(0);
        assertEquals("urn:test/WriteBatch", headerValue(first, "SOAPAction"));
        assertTrue(headerValue(first, "Content-Type").contains("text/xml"));
        assertTrue(first.body.contains("<tns:batchSize>2</tns:batchSize>"));
        assertTrue(first.body.contains("<tns:id>1</tns:id>"));
        assertTrue(first.body.contains("<tns:name>a&amp;&lt;b</tns:name>"));
        assertTrue(first.body.contains("<tns:id>2</tns:id>"));
        assertEquals(2, countOccurrences(first.body, "<tns:record>"));

        CapturedRequest second = requests.get(1);
        assertTrue(second.body.contains("<tns:batchSize>1</tns:batchSize>"));
        assertTrue(second.body.contains("<tns:id>3</tns:id>"));
        assertTrue(second.body.contains("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""));
        assertTrue(second.body.contains("<tns:name xsi:nil=\"true\"/>"));
        assertEquals(1, countOccurrences(second.body, "<tns:record>"));
    }

    @Test
    public void soapArrayPayloadShouldRejectMissingRepeatBlock() {
        Map<String, Object> config = writerConfig("http://127.0.0.1/soap",
                "array",
                "<Envelope><Body><Write><id>{{id}}</id></Write></Body></Envelope>",
                "",
                false,
                "",
                "{}",
                "{}",
                500,
                new HashMap<>());
        config.put(Key.PROTOCOL_MODE, "SOAP");
        config.put(Key.PAYLOAD_FORMAT, "soap");

        HttpWriter writer = new HttpWriter();
        writer.setPluginJobConf(Configuration.from(config));

        AggregationException exception = assertThrows(AggregationException.class, writer::init);
        assertTrue(exception.getMessage().contains("{{#records}}"));
    }

    @Test
    public void soapPayloadShouldRenderDateColumnsAsTextInsteadOfEpochMillis() throws Exception {
        List<CapturedRequest> requests = new ArrayList<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/soap", exchange -> {
            requests.add(capture(exchange));
            respondXml(exchange, 200,
                    "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ns=\"urn:test\">"
                            + "<soap:Body><ns:WriteResponse><ns:code>200</ns:code></ns:WriteResponse></soap:Body>"
                            + "</soap:Envelope>");
        });
        server.start();

        Map<String, Object> config = writerConfig("http://127.0.0.1:" + server.getAddress().getPort() + "/soap",
                "object",
                "<Envelope><Body><Write><dateVal>{{date_val}}</dateVal><timestampVal>{{timestamp_val}}</timestampVal></Write></Body></Envelope>",
                "",
                false,
                "",
                "{}",
                "{}",
                500,
                soapResponseStatus());
        config.put(Key.PROTOCOL_MODE, "SOAP");
        config.put(Key.PAYLOAD_FORMAT, "soap");
        config.put(Key.RESPONSE_TYPE, "soap");
        config.put(Key.COLUMNS, Arrays.asList(column(0, "date_val"), column(1, "timestamp_val")));

        HttpWriter writer = new HttpWriter();
        writer.setPluginJobConf(Configuration.from(config));
        writer.init();

        DefaultRecord record = new DefaultRecord();
        record.addColumn(new DateColumn(java.sql.Date.valueOf("2026-06-11")));
        record.addColumn(new TimestampColumn(Timestamp.valueOf("2026-06-11 15:48:40")));
        writer.startWrite(new ListRecordReceiver(Arrays.asList(record)));

        assertEquals(1, requests.size());
        assertTrue(requests.get(0).body.contains("<dateVal>2026-06-11</dateVal>"));
        assertTrue(requests.get(0).body.contains("<timestampVal>2026-06-11 15:48:40</timestampVal>"));
        assertTrue(!requests.get(0).body.contains("178"));
    }

    @Test
    public void soapPayloadShouldRenderNullColumnAsXsiNilElement() throws Exception {
        List<CapturedRequest> requests = new ArrayList<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/soap", exchange -> {
            requests.add(capture(exchange));
            respondXml(exchange, 200,
                    "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ns=\"urn:test\">"
                            + "<soap:Body><ns:WriteResponse><ns:code>200</ns:code></ns:WriteResponse></soap:Body>"
                            + "</soap:Envelope>");
        });
        server.start();

        Map<String, Object> config = writerConfig("http://127.0.0.1:" + server.getAddress().getPort() + "/soap",
                "object",
                "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tns=\"urn:test\">"
                        + "<soap:Body><tns:Write><tns:id>{{id}}</tns:id><tns:name>{{name}}</tns:name></tns:Write></soap:Body>"
                        + "</soap:Envelope>",
                "",
                false,
                "",
                "{}",
                "{}",
                500,
                soapResponseStatus());
        config.put(Key.PROTOCOL_MODE, "SOAP");
        config.put(Key.PAYLOAD_FORMAT, "soap");
        config.put(Key.RESPONSE_TYPE, "soap");

        HttpWriter writer = new HttpWriter();
        writer.setPluginJobConf(Configuration.from(config));
        writer.init();

        writer.startWrite(new ListRecordReceiver(Arrays.asList(record(7L, null))));

        assertEquals(1, requests.size());
        assertTrue(requests.get(0).body.contains("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""));
        assertTrue(requests.get(0).body.contains("<tns:name xsi:nil=\"true\"/>"));
        assertTrue(!requests.get(0).body.contains("{{name}}"));
    }

    @Test
    public void xmlPayloadShouldDefaultToXmlContentType() throws Exception {
        List<CapturedRequest> requests = new ArrayList<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/xml", exchange -> {
            requests.add(capture(exchange));
            respond(exchange, 200, "{\"code\":\"200\"}");
        });
        server.start();

        Map<String, Object> config = writerConfig("http://127.0.0.1:" + server.getAddress().getPort() + "/xml",
                "object",
                "<item><id>{{id}}</id><name>{{name}}</name></item>",
                "",
                false,
                "",
                "{}",
                "{}",
                500,
                new HashMap<>());
        config.remove(Key.CONTENT_TYPE);
        config.put(Key.PAYLOAD_FORMAT, "xml");
        config.put(Key.RESPONSE_TYPE, "json");

        HttpWriter writer = new HttpWriter();
        writer.setPluginJobConf(Configuration.from(config));
        writer.init();

        writer.startWrite(new ListRecordReceiver(Arrays.asList(record(9L, "xml"))));

        assertEquals(1, requests.size());
        assertTrue(headerValue(requests.get(0), "Content-Type").contains("application/xml"));
        assertTrue(requests.get(0).body.contains("<id>9</id>"));
    }

    private static Map<String, Object> writerConfig(String url,
                                                    String payloadMode,
                                                    String requestBody,
                                                    String dataNodePath,
                                                    boolean includeTotal,
                                                    String totalNodePath,
                                                    String header,
                                                    String params,
                                                    int batchSize,
                                                    Map<String, Object> responseStatus) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(Key.URL, url);
        config.put(Key.MODE, "POST");
        config.put(Key.CONTENT_TYPE, "application/json;charset=utf-8");
        config.put(Key.HEADER, header);
        config.put(Key.PARAMS, params);
        config.put(Key.REQUEST_BODY, requestBody);
        config.put(Key.PAYLOAD_MODE, payloadMode);
        config.put(Key.DATA_NODE_PATH, dataNodePath);
        config.put(Key.INCLUDE_TOTAL, Boolean.valueOf(includeTotal));
        config.put(Key.TOTAL_NODE_PATH, totalNodePath);
        config.put(Key.BATCH_SIZE, Integer.valueOf(batchSize));
        config.put(Key.RETRY_TIMES, Integer.valueOf(1));
        config.put(Key.RETRY_INTERVAL_MS, Long.valueOf(0L));
        config.put(Key.RESPONSE_STATUS, responseStatus);
        config.put(Key.COLUMNS, Arrays.asList(column(0, "id"), column(1, "name")));
        return config;
    }

    private static Map<String, Object> column(int index, String name) {
        Map<String, Object> column = new LinkedHashMap<>();
        column.put("index", Integer.valueOf(index));
        column.put("name", name);
        column.put("type", "STRING");
        return column;
    }

    private static Map<String, Object> responseStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("path", "code");
        status.put("code", "200");
        return status;
    }

    private static Map<String, Object> soapResponseStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("path", "WriteResponse.code");
        status.put("code", "200");
        return status;
    }

    private static Map<String, Object> soapBatchResponseStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("path", "WriteBatchResponse.code");
        status.put("code", "200");
        return status;
    }

    private static Record record(Long id, String name) {
        DefaultRecord record = new DefaultRecord();
        record.addColumn(new LongColumn(id));
        record.addColumn(new StringColumn(name));
        return record;
    }

    private static CapturedRequest capture(HttpExchange exchange) throws IOException {
        CapturedRequest request = new CapturedRequest();
        request.query = exchange.getRequestURI().getRawQuery();
        request.body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        request.headers = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : exchange.getRequestHeaders().entrySet()) {
            request.headers.put(entry.getKey(), entry.getValue().isEmpty() ? "" : entry.getValue().get(0));
        }
        return request;
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json;charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
        }
    }

    private static void respondXml(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/xml;charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
        }
    }

    private static String headerValue(CapturedRequest request, String name) {
        for (Map.Entry<String, String> entry : request.headers.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(name)) {
                return entry.getValue();
            }
        }
        return "";
    }

    private static int countOccurrences(String value, String fragment) {
        int count = 0;
        int index = 0;
        while ((index = value.indexOf(fragment, index)) >= 0) {
            count++;
            index += fragment.length();
        }
        return count;
    }

    private static final class CapturedRequest {
        private String query;
        private String body;
        private Map<String, String> headers;
    }

    private static final class ListRecordReceiver implements RecordReceiver {
        private final Iterator<Record> iterator;

        private ListRecordReceiver(List<Record> records) {
            this.iterator = records.iterator();
        }

        @Override
        public Record getFromReader() {
            return iterator.hasNext() ? iterator.next() : null;
        }

        @Override
        public void shutdown() {
        }
    }
}
