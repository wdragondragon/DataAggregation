package com.jdragon.aggregation.plugin.httpdyn.impl;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.plugin.httpdyn.exception.HttpDynException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FromHttpTokenTest {
    private HttpServer server;

    @After
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void invalidArityShouldReturnSanitizedHttpDynException() {
        HttpDynException error = assertThrows(HttpDynException.class,
                () -> new FromHttpToken().execute("GET"));

        assertEquals("参数数量异常", error.getMessage());
    }

    @Test
    public void keyValueArgumentsShouldPreserveEqualsCharacters() throws Exception {
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/token", exchange -> handleTokenRequest(exchange, serverFailure));
        server.start();

        String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/token?signature=query%26tail==";
        String token = new FromHttpToken().execute(
                "POST",
                url,
                "{\"X-Signature\":\"header,a&b==\"}",
                "{\"payload\":\"body&a,b==\"}",
                "token");

        assertEquals("resolved-token", token);
        if (serverFailure.get() != null) {
            throw new AssertionError(serverFailure.get());
        }
    }

    @Test
    public void jsonBodyAndUrlShouldPreserveTypesEscapesEncodedPathAndDuplicateQuery() throws Exception {
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                assertEquals("/token/a%2Fb", exchange.getRequestURI().getRawPath());
                assertEquals(Arrays.asList("read", "write"),
                        queryParameterValues(exchange.getRequestURI().getRawQuery(), "scope"));
                String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                JSONObject body = JSONObject.parseObject(requestBody);
                assertEquals(Integer.valueOf(7), body.getInteger("count"));
                assertEquals(Boolean.TRUE, body.getBoolean("enabled"));
                assertEquals("a\\b\"c\nline", body.getString("text"));
                assertEquals("C001", body.getJSONObject("filter").getString("customer"));
                assertEquals(2, body.getJSONArray("ids").size());
                byte[] response = "{\"token\":\"typed-token\"}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            } catch (Throwable error) {
                serverFailure.set(error);
                byte[] response = "{\"error\":\"invalid request\"}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(400, response.length);
                exchange.getResponseBody().write(response);
            } finally {
                exchange.close();
            }
        });
        server.start();

        String url = "http://127.0.0.1:" + server.getAddress().getPort()
                + "/token/a%2Fb?scope=read&scope=write";
        String body = "{\"count\":7,\"enabled\":true,\"text\":\"a\\\\b\\\"c\\nline\","
                + "\"filter\":{\"customer\":\"C001\"},\"ids\":[1,2]}";

        String token = new FromHttpToken().execute("POST", url, "{}", body, "token");

        assertEquals("typed-token", token);
        if (serverFailure.get() != null) {
            throw new AssertionError(serverFailure.get());
        }
    }

    private void handleTokenRequest(HttpExchange exchange, AtomicReference<Throwable> serverFailure) throws IOException {
        try {
            assertEquals("query&tail==", queryParameters(exchange.getRequestURI().getRawQuery()).get("signature"));
            assertEquals("header,a&b==", exchange.getRequestHeaders().getFirst("X-Signature"));
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals("body&a,b==", JSONObject.parseObject(requestBody).getString("payload"));
            byte[] body = "{\"token\":\"resolved-token\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
        } catch (Throwable error) {
            serverFailure.set(error);
            byte[] body = "{\"error\":\"invalid request\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(400, body.length);
            exchange.getResponseBody().write(body);
        } finally {
            exchange.close();
        }
    }

    private Map<String, String> queryParameters(String rawQuery) {
        Map<String, String> result = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return result;
        }
        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length == 1 ? "" : URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
            result.put(key, value);
        }
        return result;
    }

    private List<String> queryParameterValues(String rawQuery, String expectedName) {
        List<String> values = new ArrayList<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return values;
        }
        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);
            String name = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            if (expectedName.equals(name)) {
                values.add(parts.length == 1 ? "" : URLDecoder.decode(parts[1], StandardCharsets.UTF_8));
            }
        }
        return values;
    }
}
