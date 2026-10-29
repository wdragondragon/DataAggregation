package com.jdragon.aggregation.httpreader;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.sun.net.httpserver.HttpServer;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class HttpNestedRecordReaderTest {
    @Test
    public void readerShouldPreserveDuplicateMappedColumnsAndUseParentPaginationCount() throws Exception {
        runPages(false);
    }

    @Test
    public void readerShouldContinueAfterFullPageWithNoOutputRecords() throws Exception {
        runPages(true);
    }

    private void runPages(boolean emptyFirstPage) throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/items", exchange -> {
            requests.incrementAndGet();
            String firstPage = emptyFirstPage
                    ? "{\"data1\":[{\"id\":\"P1\",\"data2\":[]},{\"id\":\"P2\",\"data2\":[]}]}"
                    : "{\"data1\":[{\"id\":\"P1\",\"data2\":[{\"id\":\"D1\",\"amount\":10},{\"id\":\"D2\",\"amount\":20}]},"
                      + "{\"id\":\"P2\",\"data2\":[{\"id\":\"D3\",\"amount\":30}]}]}";
            String body = exchange.getRequestURI().getQuery().contains("page=1") ? firstPage
                    : "{\"data1\":[{\"id\":\"P3\",\"data2\":[{\"id\":\"D4\",\"amount\":40}]}]}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (java.io.OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        });
        server.start();
        try {
            Map<String, Object> config = new LinkedHashMap<>();
            config.put("url", "http://127.0.0.1:" + server.getAddress().getPort() + "/items");
            config.put("mode", "GET");
            config.put("params", "{\"page\":\"{dyn_page}\"}");
            config.put("pageRead", true);
            config.put("pageSize", 2);
            config.put("recordPath", "data1[*].data2[*]");
            Map<String, Object> parent = column("parent_id", "id", "data1[*]", "STRING");
            config.put("columns", Arrays.asList(parent,
                    column("detail_id", "id", "data1[*].data2[*]", "STRING"),
                    column("amount", "amount", "data1[*].data2[*]", "DECIMAL"), parent));

            HttpReader reader = new HttpReader();
            reader.setPluginJobConf(Configuration.from(config));
            reader.init();
            CapturingSender sender = new CapturingSender();
            reader.startRead(sender);

            assertEquals(2, requests.get());
            assertEquals(emptyFirstPage ? 1 : 4, sender.records.size());
            Record last = sender.records.get(sender.records.size() - 1);
            assertEquals("P3", last.getColumn(0).asString());
            assertEquals("D4", last.getColumn(1).asString());
            assertEquals(Double.valueOf(40), last.getColumn(2).asDouble());
            assertEquals("P3", last.getColumn(3).asString());
            if (!emptyFirstPage) {
                assertEquals("P1", sender.records.get(1).getColumn(0).asString());
                assertEquals("D2", sender.records.get(1).getColumn(1).asString());
            }
        } finally {
            server.stop(0);
        }
    }

    private static Map<String, Object> column(String name, String sourceName, String parentNode, String type) {
        Map<String, Object> column = new LinkedHashMap<>();
        column.put("name", name);
        column.put("sourceName", sourceName);
        column.put("parentNode", parentNode);
        column.put("type", type);
        return column;
    }

    private static final class CapturingSender implements RecordSender {
        private final List<Record> records = new ArrayList<>();
        public Record createRecord() { return new DefaultRecord(); }
        public void sendToWriter(Record record) { records.add(record); }
        public void flush() { }
        public void terminate() { }
        public void shutdown() { }
    }
}
