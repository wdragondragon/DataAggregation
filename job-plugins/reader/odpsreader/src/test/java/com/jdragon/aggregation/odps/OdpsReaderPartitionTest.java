package com.jdragon.aggregation.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.datasource.odps.OdpsUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

/**
 * Uses the real ODPS SDK against a loopback HTTP fixture. Only the record stream is replaced,
 * so table metadata, SDK marker pagination and Tunnel session creation use their production paths.
 */
public class OdpsReaderPartitionTest {

    @Test
    public void sdkPartitionIteratorShouldReadAllPagesBeyondTenThousandAndSortInSchemaOrder() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 10007)) {
            TestReader reader = reader(server, true);
            List<PartitionSpec> partitions = reader.resolveTunnelPartitions(odps(server));

            assertEquals(10007, partitions.size());
            assertEquals(11, server.markers.size());
            assertEquals("", server.markers.get(0));
            assertEquals("10000", server.markers.get(10));
            for (int i = 0; i < partitions.size(); i++) {
                assertEquals(spec(i).toString(), partitions.get(i).toString());
                assertEquals(Arrays.asList("pt", "region"), new ArrayList<>(partitions.get(i).keys()));
            }
            assertTrue(server.downloads.isEmpty());
            server.assertHealthy();
        }
    }

    @Test
    public void partitionedTableShouldRequireOptInWhenFlagMissingOrFalse() throws Exception {
        for (Boolean enabled : Arrays.asList(null, Boolean.FALSE)) {
            try (OdpsServer server = new OdpsServer(true, 2)) {
                TestReader reader = reader(server, enabled);
                try {
                    reader.resolveTunnelPartitions(odps(server));
                    fail("Partitioned tables must require explicit opt-in");
                } catch (IllegalArgumentException expected) {
                    assertTrue(expected.getMessage().contains("partitionSpec"));
                    assertTrue(expected.getMessage().contains("autoPartitionScan"));
                }
                assertEquals(1, server.metadataRequests);
                assertTrue(server.markers.isEmpty());
                assertTrue(server.downloads.isEmpty());
                server.assertHealthy();
            }
        }
    }

    @Test
    public void ordinaryTableShouldKeepSingleUnpartitionedDownloadWithEveryFlagSetting() throws Exception {
        for (Boolean enabled : Arrays.asList(null, Boolean.FALSE, Boolean.TRUE)) {
            try (OdpsServer server = new OdpsServer(false, 0)) {
                server.defaultRows = 2;
                TestReader reader = reader(server, enabled);
                CollectingSender sender = new CollectingSender();
                reader.startRead(sender);

                assertEquals(Collections.singletonList(""), server.downloads);
                assertTrue(server.markers.isEmpty());
                assertEquals(2, sender.records.size());
                assertEquals(2, sender.records.get(0).getColumnNumber());
                assertEquals(1, reader.readers.get(0).closeCalls);
                assertEquals(3, reader.readers.get(0).readCalls);
                server.assertHealthy();
            }
        }
    }

    @Test
    public void explicitPartitionShouldBypassDiscoveryWithEveryFlagSetting() throws Exception {
        for (Boolean enabled : Arrays.asList(null, Boolean.FALSE, Boolean.TRUE)) {
            try (OdpsServer server = new OdpsServer(true, 5)) {
                Configuration config = config(server, enabled);
                config.set("partitionSpec", "pt='00003',region='cn'");
                TestReader reader = reader(server, config);
                reader.startRead(new CollectingSender());

                assertEquals(0, server.metadataRequests);
                assertTrue(server.markers.isEmpty());
                assertEquals(Collections.singletonList(spec(3).toString()), server.downloads);
                server.assertHealthy();
            }
        }
    }

    @Test
    public void sqlModeAndAutomaticSqlSelectionShouldKeepTheirExistingPreparation() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 5)) {
            for (Boolean enabled : Arrays.asList(null, Boolean.FALSE, Boolean.TRUE)) {
                Configuration config = config(server, enabled);
                config.set("selectSql", " select id from orders ");
                TestReader reader = reader(server, config);
                assertEquals(OdpsReader.READ_MODE_SQL, field(reader, "readMode"));
                assertEquals("select id from orders;", field(reader, "selectSql"));

                config.set("readMode", "sql");
                config.set("selectSql", "");
                config.set("partitionSpec", "pt='00003'");
                config.set("columns", Arrays.asList("name", "id"));
                reader = reader(server, config);
                assertEquals(OdpsReader.READ_MODE_SQL, field(reader, "readMode"));
                assertEquals("select name, id from orders where pt = '00003';", field(reader, "selectSql"));
            }
            assertEquals(0, server.metadataRequests);
            assertTrue(server.markers.isEmpty());
            assertTrue(server.downloads.isEmpty());
        }
    }

    @Test
    public void dynamicPartitionShouldReachSdkWithoutDiscoveryInTunnelAndAutoModes() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 5)) {
            LocalDateTime executionTime = LocalDateTime.of(2026, 11, 12, 10, 0, 0);
            server.defaultRows = 0;
            for (String mode : Arrays.asList("tunnel", "auto")) {
                Configuration config = config(server, true);
                config.set("readMode", mode);
                String partition = "pt='$getCurrentTime(\"yyyyMMdd\", \"-1d\")',region='$toLower(\"CN\")'";
                config.set("partitionSpec", partition);
                TestReader reader = new TestReader();
                reader.setPluginJobConf(config);
                reader.init();
                reader.prepare(executionTime);
                reader.startRead(new CollectingSender());

                assertEquals("tunnel", field(reader, "readMode"));
                assertEquals(partition, config.getString("partitionSpec"));
            }
            assertEquals(Arrays.asList("pt='20261111',region='cn'", "pt='20261111',region='cn'"), server.downloads);
            assertEquals(0, server.metadataRequests);
            assertTrue(server.markers.isEmpty());
            server.assertHealthy();
        }
    }

    @Test
    public void offsetAndMaxRowsShouldApplyAcrossEmptyAndNonEmptyPartitions() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 5)) {
            server.rows.put(0, 2L);
            server.rows.put(1, 0L);
            server.rows.put(2, 3L);
            server.rows.put(3, 3L);
            Configuration config = config(server, true);
            config.set("offset", 3L);
            config.set("maxRows", 3L);
            TestReader reader = reader(server, config);
            CollectingSender sender = new CollectingSender();
            reader.startRead(sender);

            assertEquals(Arrays.asList(201L, 202L, 300L), sender.ids());
            assertEquals(Arrays.asList(spec(0).toString(), spec(1).toString(), spec(2).toString(), spec(3).toString()), server.downloads);
            assertEquals(2, reader.readers.size());
            assertEquals(1L, reader.readers.get(0).start);
            assertEquals(2L, reader.readers.get(0).count);
            assertEquals(0L, reader.readers.get(1).start);
            assertEquals(1L, reader.readers.get(1).count);
            assertClosedThroughEof(reader);
            server.assertHealthy();
        }
    }

    @Test
    public void zeroMaxRowsShouldReadAllRemainingRowsAcrossPartitions() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 3)) {
            server.defaultRows = 2;
            Configuration config = config(server, true);
            config.set("offset", 1L);
            TestReader reader = reader(server, config);
            CollectingSender sender = new CollectingSender();
            reader.startRead(sender);

            assertEquals(Arrays.asList(1L, 100L, 101L, 200L, 201L), sender.ids());
            assertEquals(3, server.downloads.size());
            assertClosedThroughEof(reader);
            server.assertHealthy();
        }
    }

    @Test
    public void offsetBeyondEntireTableShouldNotOpenAnyRecordStream() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 3)) {
            server.defaultRows = 2;
            Configuration config = config(server, true);
            config.set("offset", 100L);
            TestReader reader = reader(server, config);
            CollectingSender sender = new CollectingSender();
            reader.startRead(sender);

            assertEquals(3, server.downloads.size());
            assertTrue(reader.readers.isEmpty());
            assertTrue(sender.records.isEmpty());
            server.assertHealthy();
        }
    }

    @Test
    public void emptyPartitionCollectionShouldCompleteWithoutDownloads() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 0)) {
            TestReader reader = reader(server, true);
            CollectingSender sender = new CollectingSender();
            reader.startRead(sender);

            assertEquals(Collections.singletonList(""), server.markers);
            assertTrue(server.downloads.isEmpty());
            assertTrue(reader.readers.isEmpty());
            assertTrue(sender.records.isEmpty());
            server.assertHealthy();
        }
    }

    @Test
    public void explicitColumnsShouldKeepOrderAndUsePartitionValuesWithoutIncludeFlag() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 1)) {
            Configuration config = config(server, true);
            config.set("columns", Arrays.asList("region", "name", "pt", "id", "region"));
            TestReader reader = reader(server, config);
            CollectingSender sender = new CollectingSender();
            reader.startRead(sender);

            assertEquals(Arrays.asList("cn", "row-0", "00000", "0"), values(sender.records.get(0)));
            server.assertHealthy();
        }
    }

    @Test
    public void wildcardShouldAppendPartitionValuesOnlyWhenRequested() throws Exception {
        for (boolean include : Arrays.asList(false, true)) {
            try (OdpsServer server = new OdpsServer(true, 1)) {
                Configuration config = config(server, true);
                config.set("columns", Collections.singletonList("*"));
                config.set("includePartitionColumns", include);
                TestReader reader = reader(server, config);
                CollectingSender sender = new CollectingSender();
                reader.startRead(sender);

                List<String> expected = include ? Arrays.asList("0", "row-0", "00000", "cn") : Arrays.asList("0", "row-0");
                assertEquals(expected, values(sender.records.get(0)));
                server.assertHealthy();
            }
        }
    }

    @Test
    public void streamFailureShouldCloseCurrentStreamAndNeverOpenLaterPartitions() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 4)) {
            server.defaultRows = 2;
            TestReader reader = reader(server, true);
            reader.failPartition = 1;
            reader.failAfterRows = 1;
            CollectingSender sender = new CollectingSender();
            RuntimeException failure = readFailure(reader, sender);

            assertTrue(failure.getMessage().contains("partition=" + spec(1)));
            assertTrue(failure.getMessage().contains("emitted=3"));
            assertTrue(failure.getCause() instanceof IOException);
            assertEquals(Arrays.asList(0L, 1L, 100L), sender.ids());
            assertEquals(Arrays.asList(spec(0).toString(), spec(1).toString()), server.downloads);
            assertEquals(2, reader.readers.size());
            for (StubRecordReader stream : reader.readers) {
                assertEquals(1, stream.closeCalls);
            }
            server.assertHealthy();
        }
    }

    @Test
    public void eofValidationFailureShouldPropagateEvenWhenMaxRowsWasReached() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 2)) {
            Configuration config = config(server, true);
            config.set("maxRows", 1L);
            TestReader reader = reader(server, config);
            reader.failPartition = 0;
            reader.failAfterRows = 1;
            CollectingSender sender = new CollectingSender();
            RuntimeException failure = readFailure(reader, sender);

            assertTrue(failure.getCause() instanceof IOException);
            assertEquals(1, sender.records.size());
            assertEquals(Collections.singletonList(spec(0).toString()), server.downloads);
            assertEquals(2, reader.readers.get(0).readCalls);
            assertEquals(1, reader.readers.get(0).closeCalls);
            server.assertHealthy();
        }
    }

    @Test
    public void sessionCreationFailureShouldRetainPartitionAndStopTheScan() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 3)) {
            server.failDownloadPartition = 1;
            TestReader reader = reader(server, true);
            CollectingSender sender = new CollectingSender();
            RuntimeException failure = readFailure(reader, sender);

            assertTrue(failure.getMessage().contains("partition=" + spec(1)));
            assertTrue(failure.getMessage().contains("emitted=1"));
            assertEquals(Arrays.asList(spec(0).toString(), spec(1).toString()), server.downloads);
            assertEquals(1, reader.readers.size());
            assertClosedThroughEof(reader);
            server.assertHealthy();
        }
    }

    @Test
    public void writerFailureShouldCloseCurrentStreamAndStopTheScan() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 3)) {
            TestReader reader = reader(server, true);
            CollectingSender sender = new CollectingSender() {
                @Override
                public void sendToWriter(com.jdragon.aggregation.commons.element.Record record) {
                    throw new IllegalStateException("writer failed");
                }
            };
            RuntimeException failure = readFailure(reader, sender);

            assertEquals("writer failed", failure.getCause().getMessage());
            assertTrue(failure.getMessage().contains("emitted=0"));
            assertEquals(Collections.singletonList(spec(0).toString()), server.downloads);
            assertEquals(1, reader.readers.get(0).closeCalls);
            server.assertHealthy();
        }
    }

    @Test
    public void interruptionAfterReadShouldDropPendingRecordCloseStreamAndStopTheScan() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 3)) {
            TestReader reader = reader(server, true);
            reader.onRead = () -> Thread.currentThread().interrupt();
            CollectingSender sender = new CollectingSender();
            try {
                RuntimeException failure = readFailure(reader, sender);
                assertTrue(failure.getCause() instanceof InterruptedException);
                assertTrue(Thread.currentThread().isInterrupted());
                assertTrue(sender.records.isEmpty());
                assertEquals(Collections.singletonList(spec(0).toString()), server.downloads);
                assertEquals(1, reader.readers.get(0).closeCalls);
            } finally {
                Thread.interrupted();
            }
            server.assertHealthy();
        }
    }

    @Test
    public void destroyDuringReadShouldSurviveClearedThreadInterruptAndStopTheScan() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 3)) {
            TestReader reader = reader(server, true);
            reader.onRead = () -> {
                reader.destroy();
                Thread.currentThread().interrupt();
                Thread.interrupted(); // Simulates an SDK retry consuming the interrupt.
            };
            CollectingSender sender = new CollectingSender();
            try {
                RuntimeException failure = readFailure(reader, sender);
                assertTrue(failure.getCause() instanceof InterruptedException);
                assertTrue(Thread.currentThread().isInterrupted());
                assertTrue(sender.records.isEmpty());
                assertEquals(Collections.singletonList(spec(0).toString()), server.downloads);
                assertEquals(1, reader.readers.get(0).closeCalls);
            } finally {
                Thread.interrupted();
            }
            server.assertHealthy();
        }
    }

    @Test
    public void destroyDuringEmptyDownloadShouldFailInsteadOfReportingSuccessfulCompletion() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 1)) {
            server.defaultRows = 0;
            TestReader reader = reader(server, true);
            server.onDownload = reader::destroy;
            try {
                RuntimeException failure = readFailure(reader, new CollectingSender());
                assertTrue(failure.getCause() instanceof InterruptedException);
                assertTrue(Thread.currentThread().isInterrupted());
                assertTrue(reader.readers.isEmpty());
                assertEquals(Collections.singletonList(spec(0).toString()), server.downloads);
            } finally {
                Thread.interrupted();
            }
            server.assertHealthy();
        }
    }

    @Test
    public void destroyDuringSdkPartitionPageShouldPreventFurtherPagesAndDownloads() throws Exception {
        try (OdpsServer server = new OdpsServer(true, 1001)) {
            TestReader reader = reader(server, true);
            server.onList = reader::destroy;
            try {
                RuntimeException failure = readFailure(reader, new CollectingSender());
                assertTrue(failure.getCause() instanceof InterruptedException);
                assertEquals(Collections.singletonList(""), server.markers);
                assertTrue(server.downloads.isEmpty());
            } finally {
                Thread.interrupted();
            }
            server.assertHealthy();
        }
    }

    private static RuntimeException readFailure(TestReader reader, CollectingSender sender) {
        try {
            reader.startRead(sender);
            fail("Expected reader failure");
            throw new AssertionError();
        } catch (RuntimeException expected) {
            return expected;
        }
    }

    private static void assertClosedThroughEof(TestReader reader) {
        for (StubRecordReader stream : reader.readers) {
            assertEquals("Every stream must be closed once", 1, stream.closeCalls);
            assertEquals("Read EOF to let the SDK validate its stream", stream.count + 1, stream.readCalls);
        }
    }

    private static List<String> values(com.jdragon.aggregation.commons.element.Record record) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < record.getColumnNumber(); i++) {
            result.add(record.getColumn(i).asString());
        }
        return result;
    }

    private static Object field(OdpsReader reader, String name) throws Exception {
        Field field = OdpsReader.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(reader);
    }

    private static TestReader reader(OdpsServer server, Boolean enabled) {
        return reader(server, config(server, enabled));
    }

    private static TestReader reader(OdpsServer server, Configuration config) {
        TestReader reader = new TestReader();
        reader.setPluginJobConf(config);
        reader.init();
        reader.prepare();
        return reader;
    }

    private static Configuration config(OdpsServer server, Boolean enabled) {
        Configuration config = Configuration.newDefault();
        config.set("connect.type", "odps");
        config.set("connect.host", server.endpoint());
        config.set("connect.database", "project");
        config.set("connect.userName", "dummy-access-id");
        config.set("connect.password", "dummy-access-key");
        config.setMapValue("connect.extraParams", "tunnelEndpoint", server.endpoint());
        config.set("table", "orders");
        if (enabled != null) {
            config.set("autoPartitionScan", enabled);
        }
        return config;
    }

    private static Odps odps(OdpsServer server) {
        return OdpsUtils.createOdps(OdpsReader.parseDataSource(config(server, true).getConfiguration("connect")));
    }

    private static PartitionSpec spec(int index) {
        return new PartitionSpec("pt='" + String.format("%05d", index) + "',region='cn'");
    }

    private static int partitionIndex(String partition) {
        return partition == null || partition.isEmpty() ? 0 : Integer.parseInt(new PartitionSpec(partition).get("pt"));
    }

    private static class CollectingSender implements RecordSender {
        final List<com.jdragon.aggregation.commons.element.Record> records = new ArrayList<>();

        @Override
        public com.jdragon.aggregation.commons.element.Record createRecord() {
            return new DefaultRecord();
        }

        @Override
        public void sendToWriter(com.jdragon.aggregation.commons.element.Record record) {
            records.add(record);
        }

        List<Long> ids() {
            List<Long> ids = new ArrayList<>();
            for (com.jdragon.aggregation.commons.element.Record record : records) {
                ids.add(record.getColumn(0).asLong());
            }
            return ids;
        }

        @Override public void flush() { }
        @Override public void terminate() { }
        @Override public void shutdown() { }
    }

    private static final class TestReader extends OdpsReader {
        final List<StubRecordReader> readers = new ArrayList<>();
        int failPartition = -1;
        long failAfterRows;
        Runnable onRead;

        @Override
        RecordReader openTunnelRecordReader(TableTunnel.DownloadSession session, long start, long count) {
            StubRecordReader stream = new StubRecordReader(session, start, count, this);
            readers.add(stream);
            return stream;
        }
    }

    private static final class StubRecordReader implements RecordReader {
        final TableTunnel.DownloadSession session;
        final long start;
        final long count;
        final TestReader owner;
        long position;
        long readCalls;
        int closeCalls;

        StubRecordReader(TableTunnel.DownloadSession session, long start, long count, TestReader owner) {
            this.session = session;
            this.start = start;
            this.count = count;
            this.owner = owner;
        }

        @Override
        public Record read() throws IOException {
            readCalls++;
            if (owner.onRead != null) {
                owner.onRead.run();
            }
            int partition = partitionIndex(session.getPartitionSpec());
            if (owner.failPartition == partition && position == owner.failAfterRows) {
                throw new IOException("simulated stream or EOF checksum failure");
            }
            if (position == count) {
                return null;
            }
            long id = partition * 100L + start + position++;
            Record record = new ArrayRecord(session.getSchema());
            record.set("id", id);
            record.set("name", "row-" + id);
            return record;
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }

    private static final class OdpsServer implements AutoCloseable {
        final HttpServer server;
        final boolean partitioned;
        final int partitionCount;
        final Map<Integer, Long> rows = new LinkedHashMap<>();
        final List<String> markers = new CopyOnWriteArrayList<>();
        final List<String> downloads = new CopyOnWriteArrayList<>();
        final List<String> unexpectedRequests = new CopyOnWriteArrayList<>();
        volatile int metadataRequests;
        long defaultRows = 1;
        int failDownloadPartition = -1;
        Runnable onList;
        Runnable onDownload;

        OdpsServer(boolean partitioned, int partitionCount) throws IOException {
            this.partitioned = partitioned;
            this.partitionCount = partitionCount;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        String endpoint() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private void handle(HttpExchange exchange) throws IOException {
            Map<String, String> query = query(exchange.getRequestURI().getRawQuery());
            String path = exchange.getRequestURI().getPath();
            if (!"/projects/project/tables/orders".equals(path)) {
                unexpectedRequests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
                respond(exchange, 400, "<Error><Code>InvalidParameter</Code><Message>Unexpected request</Message></Error>");
            } else if (query.containsKey("partitions")) {
                String marker = query.getOrDefault("marker", "");
                markers.add(marker);
                if (!"1000".equals(query.get("maxitems")) || !"true".equals(query.get("expectmarker"))) {
                    unexpectedRequests.add("SDK pagination query: " + query);
                }
                int begin = marker.isEmpty() ? 0 : Integer.parseInt(marker);
                int end = Math.min(begin + 1000, partitionCount);
                StringBuilder xml = new StringBuilder("<Partitions>");
                for (int i = begin; i < end; i++) {
                    int index = partitionCount - 1 - i; // Deliberately return reverse order and reversed key order.
                    xml.append("<Partition><Column Name=\"region\" Value=\"cn\"/><Column Name=\"pt\" Value=\"")
                            .append(String.format("%05d", index)).append("\"/></Partition>");
                }
                xml.append("<Marker>").append(end < partitionCount ? end : "").append("</Marker><MaxItems>1000</MaxItems></Partitions>");
                if (onList != null) {
                    onList.run();
                }
                respond(exchange, 200, xml.toString());
            } else if (query.containsKey("downloads") && "POST".equals(exchange.getRequestMethod())) {
                String partition = query.getOrDefault("partition", "");
                downloads.add(partition.isEmpty() ? "" : new PartitionSpec(partition).toString());
                int index = partitionIndex(partition);
                if (onDownload != null) {
                    onDownload.run();
                }
                if (index == failDownloadPartition) {
                    respond(exchange, 400, "{\"Code\":\"InvalidArgument\",\"Message\":\"simulated session failure\"}");
                } else {
                    respond(exchange, 200, "{\"DownloadID\":\"download-" + index + "\",\"Status\":\"Normal\",\"RecordCount\":"
                            + rows.getOrDefault(index, defaultRows) + ",\"Schema\":" + schema() + "}");
                }
            } else if ("GET".equals(exchange.getRequestMethod())) {
                metadataRequests++;
                respond(exchange, 200, "<Table><Name>orders</Name><Project>project</Project><Schema><![CDATA["
                        + schema() + "]]></Schema></Table>");
            } else {
                unexpectedRequests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
                respond(exchange, 400, "<Error><Code>InvalidParameter</Code><Message>Unexpected request</Message></Error>");
            }
        }

        private String schema() {
            return "{\"isVirtualView\":false,\"columns\":[{\"name\":\"id\",\"type\":\"bigint\"},{\"name\":\"name\",\"type\":\"string\"}],\"partitionKeys\":"
                    + (partitioned ? "[{\"name\":\"pt\",\"type\":\"string\"},{\"name\":\"region\",\"type\":\"string\"}]" : "[]") + "}";
        }

        private static Map<String, String> query(String raw) {
            Map<String, String> result = new LinkedHashMap<>();
            if (raw != null) {
                for (String parameter : raw.split("&")) {
                    String[] pair = parameter.split("=", 2);
                    result.put(URLDecoder.decode(pair[0], StandardCharsets.UTF_8),
                            pair.length == 1 ? "" : URLDecoder.decode(pair[1], StandardCharsets.UTF_8));
                }
            }
            return result;
        }

        private static void respond(HttpExchange exchange, int status, String body) throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", body.startsWith("{") ? "application/json" : "application/xml");
            exchange.getResponseHeaders().set("x-odps-request-id", "local-test-request");
            exchange.sendResponseHeaders(status, bytes.length);
            try {
                exchange.getResponseBody().write(bytes);
            } finally {
                exchange.close();
            }
        }

        void assertHealthy() {
            assertEquals(Collections.emptyList(), unexpectedRequests);
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
