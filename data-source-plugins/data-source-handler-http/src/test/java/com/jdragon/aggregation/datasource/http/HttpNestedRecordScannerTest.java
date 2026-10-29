package com.jdragon.aggregation.datasource.http;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class HttpNestedRecordScannerTest {
    private HttpServer server;
    private final AtomicInteger requests = new AtomicInteger();

    @After
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void paginationShouldCountParentsWhileOutputAndProjectionFollowChildren() throws Exception {
        startPages("{\"data1\":[{\"id\":\"P1\",\"data2\":[{\"id\":\"D1\"},{\"id\":\"D2\"}]},"
                + "{\"id\":\"P2\",\"data2\":[{\"id\":\"D3\"}]}]}",
                "{\"data1\":[{\"id\":\"P3\",\"data2\":[{\"id\":\"D4\"}]}]}");
        HttpSourceConfig config = config(false);
        List<Map<String, Object>> rows = new ArrayList<>();
        new HttpSourceScanner().scan(config, rows::add);

        assertEquals(2, requests.get());
        assertEquals(4, rows.size());
        assertEquals("P1", rows.get(0).get("parent_id"));
        assertEquals("P1", rows.get(1).get("parent_id"));
        assertEquals("P2", rows.get(2).get("parent_id"));
        assertEquals("D4", rows.get(3).get("detail_id"));

        requests.set(0);
        List<Map<String, Object>> parentsOnly = new ArrayList<>();
        new HttpSourceScanner().scan(config(true), parentsOnly::add);
        assertEquals(4, parentsOnly.size());
        assertEquals(Arrays.asList("parent_id"), new ArrayList<>(parentsOnly.get(0).keySet()));
    }

    @Test
    public void fullSourcePageWithEmptyChildrenShouldStillReadNextPage() throws Exception {
        startPages("{\"data1\":[{\"id\":\"P1\",\"data2\":[]},{\"id\":\"P2\",\"data2\":null}]}",
                "{\"data1\":[{\"id\":\"P3\",\"data2\":[{\"id\":\"D3\"}]}]}");
        List<Map<String, Object>> rows = new ArrayList<>();
        new HttpSourceScanner().scan(config(false), rows::add);

        assertEquals(2, requests.get());
        assertEquals(1, rows.size());
        assertEquals("P3", rows.get(0).get("parent_id"));
        assertEquals("D3", rows.get(0).get("detail_id"));
    }

    @Test
    public void previewShouldPreserveNullColumnsAndStopWithinExpandedPage() throws Exception {
        startPages("{\"data1\":[{\"id\":\"P1\",\"data2\":[{}, {\"id\":\"D2\"}]},"
                + "{\"id\":\"P2\",\"data2\":[{\"id\":\"D3\"}]}]}", "{\"data1\":[]}");
        BaseDataSourceDTO dto = new BaseDataSourceDTO();
        Map<String, String> extra = new LinkedHashMap<>();
        extra.put(HttpSourceKeys.STUDIO_READER_CONFIG, JSONObject.toJSONString(config(false).toMap()));
        dto.setExtraParams(extra);

        Table<Map<String, Object>> preview = new HttpSourcePlugin().dataModelPreview(dto, "nested", "1");

        assertEquals(1, requests.get());
        assertEquals(1, preview.getBodies().size());
        assertTrue(preview.getBodies().get(0).containsKey("detail_id"));
        assertNull(preview.getBodies().get(0).get("detail_id"));
    }

    @Test
    public void configurationShouldRoundTripAndRejectUnboundWildcardsBeforeRequest() throws Exception {
        startPages("{}", "{}");
        HttpSourceConfig original = config(false);
        HttpSourceConfig restored = HttpSourceConfig.fromJson(JSONObject.toJSONString(original.toMap()));
        assertEquals("data1[*].data2[*]", restored.getRecordPath());
        assertEquals("id", restored.getColumns().get(0).getSourceName());

        Map<String, Object> invalid = original.toMap();
        invalid.put(HttpSourceKeys.COLUMNS, Arrays.asList(column("bad", "id", "data1[*].other[*]")));
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> HttpSourceConfig.fromConfiguration(Configuration.from(invalid)));
        assertTrue(error.getMessage().contains("bad"));
        assertEquals(0, requests.get());
    }

    @Test
    public void explicitRootArrayRecordPathShouldReadFieldsFromEachArrayElement() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/root-array", exchange -> respond(exchange,
                "[{\"id\":\"A\"},{\"id\":\"B\"}]"));
        server.start();
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("url", "http://127.0.0.1:" + server.getAddress().getPort() + "/root-array");
        config.put("mode", "GET");
        config.put("recordPath", "$[*]");
        config.put("columns", Arrays.asList(column("id", "id", "$[*]")));
        List<Map<String, Object>> rows = new ArrayList<>();

        new HttpSourceScanner().scan(HttpSourceConfig.fromConfiguration(Configuration.from(config)), rows::add);

        assertEquals(2, rows.size());
        assertEquals("A", rows.get(0).get("id"));
        assertEquals("B", rows.get(1).get("id"));
    }

    private HttpSourceConfig config(boolean parentOnly) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("url", "http://127.0.0.1:" + server.getAddress().getPort() + "/items");
        config.put("mode", "GET");
        config.put("params", "{\"page\":\"{dyn_page}\"}");
        config.put("pageRead", true);
        config.put("pageSize", 2);
        config.put("recordPath", "data1[*].data2[*]");
        List<Map<String, Object>> columns = new ArrayList<>();
        columns.add(column("parent_id", "id", "data1[*]"));
        if (!parentOnly) {
            columns.add(column("detail_id", "id", "data1[*].data2[*]"));
        }
        config.put("columns", columns);
        return HttpSourceConfig.fromConfiguration(Configuration.from(config));
    }

    private static Map<String, Object> column(String name, String sourceName, String parentNode) {
        Map<String, Object> column = new LinkedHashMap<>();
        column.put("name", name);
        column.put("sourceName", sourceName);
        column.put("parentNode", parentNode);
        column.put("type", "STRING");
        return column;
    }

    private void startPages(String firstPage, String lastPage) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/items", exchange -> {
            requests.incrementAndGet();
            respond(exchange, exchange.getRequestURI().getQuery().contains("page=1") ? firstPage : lastPage);
        });
        server.start();
    }

    private static void respond(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }
}
