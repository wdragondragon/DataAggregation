package com.jdragon.aggregation.httpreader;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.sun.net.httpserver.HttpServer;
import org.junit.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class HttpReaderTimeoutSessionTest {

    @Test
    public void successfulRetryTimeoutShouldCarryAcrossPagesAndResetForNextRead() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        ExecutorService executor = Executors.newCachedThreadPool();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/items", exchange -> {
            if (requests.incrementAndGet() == 1) {
                try {
                    Thread.sleep(600L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    exchange.close();
                    return;
                }
            }
            boolean firstPage = exchange.getRequestURI().getQuery().contains("page=1");
            byte[] response = (firstPage ? "{\"data\":[{\"id\":\"P1\"}]}" : "{\"data\":[]}")
                    .getBytes(StandardCharsets.UTF_8);
            try {
                exchange.sendResponseHeaders(200, response.length);
                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(response);
                }
            } finally {
                exchange.close();
            }
        });
        server.start();
        try {
            Map<String, Object> config = readerConfig("http://127.0.0.1:" + server.getAddress().getPort() + "/items");
            config.put("readTimeout", 200);
            // Timeout escalation has its own budget, independent of ordinary retries.
            config.put("retryTimes", 1);
            TrackingReader reader = initializedReader(config);
            CapturingSender sender = new CapturingSender();
            reader.startRead(sender);

            assertEquals(Arrays.asList(200, 10200), reader.pageTimeouts);
            assertEquals(10200, reader.currentTimeout());
            assertEquals(3, requests.get());
            assertEquals(1, sender.records.size());
            assertEquals("P1", sender.records.get(0).getColumn(0).asString());

            TrackingReader separateReader = initializedReader(config);
            assertEquals(200, separateReader.currentTimeout());
            assertNotSame(reader.readTimeoutSession, separateReader.readTimeoutSession);

            reader.pageTimeouts.clear();
            reader.startRead(new CapturingSender());
            assertEquals(Arrays.asList(200, 200), reader.pageTimeouts);
            assertEquals(200, reader.currentTimeout());
            assertEquals(5, requests.get());
        } finally {
            server.stop(0);
            executor.shutdownNow();
        }
    }

    @Test
    public void readerShouldKeepTimeoutAliasesAndApplyTheSharedBounds() {
        Map<String, Object> config = readerConfig("http://127.0.0.1/items");
        config.put("socketTimeout", 25000);
        assertEquals(25000, initializedReader(config).currentTimeout());

        config.put("readTimeout", 35000);
        assertEquals(35000, initializedReader(config).currentTimeout());

        config.put("readTimeout", 90000);
        assertEquals(60000, initializedReader(config).currentTimeout());

        config.put("readTimeout", 0);
        assertEquals(10000, initializedReader(config).currentTimeout());

        config.remove("readTimeout");
        config.remove("socketTimeout");
        assertEquals(10000, initializedReader(config).currentTimeout());
    }

    private static TrackingReader initializedReader(Map<String, Object> config) {
        TrackingReader reader = new TrackingReader();
        reader.setPluginJobConf(Configuration.from(config));
        reader.init();
        return reader;
    }

    private static Map<String, Object> readerConfig(String url) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("url", url);
        config.put("mode", "GET");
        config.put("params", "{\"page\":\"{dyn_page}\"}");
        config.put("pageRead", true);
        config.put("pageSize", 1);
        config.put("recordPath", "data[*]");
        config.put("retryIntervalMs", 0);
        Map<String, Object> column = new LinkedHashMap<>();
        column.put("name", "id");
        column.put("parentNode", "data[*]");
        column.put("type", "STRING");
        config.put("columns", Collections.singletonList(column));
        return config;
    }

    private static class TrackingReader extends HttpReader {
        private final List<Integer> pageTimeouts = new ArrayList<>();

        @Override
        public String connectToGetData(Map<String, String> content, Integer pageNum) {
            pageTimeouts.add(currentTimeout());
            return super.connectToGetData(content, pageNum);
        }

        private int currentTimeout() {
            return readTimeoutSession.getReadTimeoutMs();
        }
    }

    private static class CapturingSender implements RecordSender {
        private final List<Record> records = new ArrayList<>();
        public Record createRecord() { return new DefaultRecord(); }
        public void sendToWriter(Record record) { records.add(record); }
        public void flush() { }
        public void terminate() { }
        public void shutdown() { }
    }
}
