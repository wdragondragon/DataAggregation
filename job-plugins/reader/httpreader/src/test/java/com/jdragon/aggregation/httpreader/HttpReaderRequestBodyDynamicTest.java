package com.jdragon.aggregation.httpreader;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
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
        assertTrue(exception.getMessage().contains("bad request"));
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

    private static void respond(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json;charset=utf-8");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
        }
    }
}
