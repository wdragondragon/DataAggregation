package com.jdragon.aggregation.plugin.httpdyn;

import com.alibaba.fastjson.JSONObject;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HttpDynColumnExecuteHandlerTest {
    private HttpServer server;

    @After
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void dynamicReplacementShouldPreserveDollarCharacters() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/token", exchange -> {
            byte[] body = "{\"token\":\"price$5\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        String tokenUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/token";
        Map<String, String> content = new LinkedHashMap<>();
        content.put("header", "{\"Authorization\":\"{dyn_from_http_token(GET," + tokenUrl + ",,,token)}\"}");
        content.put("params", "{}");
        content.put("body", "");

        HttpDynColumnExecuteHandler.replaceDynVal(content);

        assertEquals("price$5", JSONObject.parseObject(content.get("header")).getString("Authorization"));
    }

    @Test
    public void dependencyReplacementShouldPreserveDollarAndBackslashCharacters() {
        Map<String, String> content = new LinkedHashMap<>();
        content.put("header", "{\"X-Pattern\":\"$1\\\\folder\"}");
        content.put("params", "{dyn_MD5({%header%})}");
        content.put("body", "{}");

        HttpDynColumnExecuteHandler.replaceDynVal(content);

        assertFalse(content.get("params").contains("{dyn_MD5"));
    }

    @Test
    public void functionParameterParserShouldSupportQuotedCommasJsonAndEscapes() {
        String[] parameters = HttpDynColumnExecuteHandler.splitFunctionParameters(
                "POST,http://example.test,\"Accept=a,b\",{\"value\":\"x,y\"},data.token\\,primary");

        assertEquals(5, parameters.length);
        assertEquals("Accept=a,b", parameters[2]);
        assertEquals("{\"value\":\"x,y\"}", parameters[3]);
        assertEquals("data.token,primary", parameters[4]);
    }

    @Test
    public void functionParameterParserShouldPreserveJsonBackslashEscapes() {
        String jsonBody = "{\"text\":\"a\\\\b\\\"c\\nline\",\"path\":\"C:\\\\temp\"}";

        String[] parameters = HttpDynColumnExecuteHandler.splitFunctionParameters(
                "POST,http://example.test,{}," + jsonBody + ",token");

        assertEquals(jsonBody, parameters[3]);
    }
}
