package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.enums.State;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.core.plugin.spi.collector.AbstractTaskPluginCollector;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import com.jdragon.aggregation.core.statistics.communication.CommunicationTool;
import com.jdragon.aggregation.core.streaming.job.*;
import com.jdragon.aggregation.core.transformer.TransformerExecution;
import com.jdragon.aggregation.core.transformer.TransformerRegistry;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.core.utils.TransformerUtil;
import com.jdragon.aggregation.pluginloader.ConfigParser;
import com.jdragon.aggregation.pluginloader.JarLoader;
import com.jdragon.aggregation.pluginloader.JarLoaderCenter;
import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeResolvers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipFile;

import static org.junit.Assert.*;

/** Uses the assembled JAR through the actual plugin loader, never target/classes. */
public class Base64StrPackagedIT {

    private final String originalPluginHome = SystemConstants.PLUGIN_HOME;
    private final String originalCoreConfig = SystemConstants.CORE_CONFIG;
    private final String originalTransformerHome = TransformerRegistry.TRANSFORMER_HOME;

    @Before
    public void configurePackagedPluginDirectory() throws Exception {
        Path home = Path.of("target", "aggregation").toAbsolutePath();
        Path configuration = Path.of("target", "packaged-it-core.json").toAbsolutePath();
        Files.writeString(configuration, "{}", StandardCharsets.UTF_8);
        SystemConstants.PLUGIN_HOME = home.resolve("plugin").toString();
        SystemConstants.CORE_CONFIG = configuration.toString();
        TransformerRegistry.TRANSFORMER_HOME = home.resolve("plugin/transformer").toString();
        clearLoaderCaches();
        RecordingCollector.dirtyRecords.clear();
        RecordingCollector.errors.clear();
    }

    @After
    public void restorePluginDirectory() {
        SystemConstants.PLUGIN_HOME = originalPluginHome;
        SystemConstants.CORE_CONFIG = originalCoreConfig;
        TransformerRegistry.TRANSFORMER_HOME = originalTransformerHome;
        clearLoaderCaches();
    }

    @Test
    public void loadsTheActualPackagedJarAndChecksTheZipContract() throws Exception {
        try {
            Class.forName("com.jdragon.aggregation.transformer.impl.Base64Str", false, getClass().getClassLoader());
            fail("The implementation must not be present on the integration test application classpath");
        } catch (ClassNotFoundException expected) {
            // The only implementation available is the packaged plugin JAR.
        }

        List<TransformerExecution> executions = TransformerUtil.buildTransformerInfo(configuration(), Collections.emptyList());
        assertEquals(2, executions.size());
        Class<?> loaded = executions.get(0).getTransformer().getClass();
        assertTrue(loaded.getClassLoader() instanceof JarLoader);
        Path loadedFrom = Path.of(loaded.getProtectionDomain().getCodeSource().getLocation().toURI());
        assertEquals(Path.of("target/aggregation/plugin/transformer/base64_str/base64_str-1.0_jdk17-SNAPSHOT.jar")
                .toAbsolutePath().normalize(), loadedFrom.toAbsolutePath().normalize());
        assertArrayEquals(new Object[]{1, "decode", "standard", "UTF-8"}, executions.get(0).getFinalParas());
        Record transformed = executions.get(0).getTransformer().evaluate(row("5Lit5paH"), executions.get(0).getFinalParas());
        assertEquals("中文", transformed.getColumn(1).asString());

        Path metadata = loadedFrom.getParent().resolve("transformer.json");
        Configuration description = Configuration.from(metadata.toFile());
        assertEquals("base64_str", description.getString("name"));
        assertEquals(loaded.getName(), description.getString("class"));
        Configuration template = Configuration.from(loadedFrom.getParent().resolve("plugin_job_template.json").toFile());
        assertEquals("base64_str", template.getString("name"));
        assertEquals(List.of("encode", "standard", "UTF-8"), template.getList("parameter.paras", String.class));
        try (ZipFile archive = new ZipFile("target/base64_str.zip")) {
            Set<String> entries = new HashSet<>();
            archive.stream().filter(entry -> !entry.isDirectory()).forEach(entry -> entries.add(entry.getName()));
            assertEquals(Set.of("transformer.json", "plugin_job_template.json", "README.md",
                    "base64_str-1.0_jdk17-SNAPSHOT.jar"), entries);
        }
    }

    @Test(timeout = 20000)
    public void batchJobRunsThePackagedConversionChainAndCollectsDirtyRows() {
        List<Record> input = Arrays.asList(row("5Lit5paH"), row("77+/"), row("%%%"), row("/w=="),
                row(null), row(""), row("IA=="));
        List<String> output = Collections.synchronizedList(new ArrayList<>());
        JobContainer container = new JobContainer(configuration());
        container.addConsumerPlugin(PluginType.READER, (plugin, peer) -> new Reader.Job() {
            @Override
            public void startRead(RecordSender sender) {
                input.forEach(sender::sendToWriter);
            }
        });
        container.addConsumerPlugin(PluginType.WRITER, (plugin, peer) -> writer(output));
        container.start();

        assertEquals(State.SUCCEEDED, container.getJobCommunication().getState());
        assertEquals(Arrays.asList("5Lit5paH", "77-_", null, "", "IA=="), output);
        assertStatistics(container.getJobCommunication(), 5, 2);
        assertEquals(2, RecordingCollector.dirtyRecords.size());
        assertEquals("%%%", RecordingCollector.dirtyRecords.get(0).getColumn(1).asString());
        assertEquals("/w==", RecordingCollector.dirtyRecords.get(1).getColumn(1).asString());
        assertTrue(RecordingCollector.errors.stream().allMatch(message -> message.contains("TransformerErrorCode-06")));
        assertEquals("77+/", input.get(1).getColumn(1).asString());
    }

    @Test(timeout = 20000)
    public void streamingMicroBatchesUseThePackagedPluginAndCommitAfterWriting() {
        List<StreamingBatch> batches = List.of(batch("one", "77+/", "%%%"), batch("two", "5Lit5paH", "IA=="));
        List<String> output = Collections.synchronizedList(new ArrayList<>());
        List<String> committed = new ArrayList<>();
        AtomicInteger next = new AtomicInteger();
        AtomicInteger aborted = new AtomicInteger();
        StreamingSource source = context -> new StreamingSession() {
            @Override
            public StreamingBatch pollBatch(StreamingPollRequest request) {
                int index = next.getAndIncrement();
                return index < batches.size() ? batches.get(index) : StreamingBatch.empty();
            }

            @Override
            public void commit(StreamingCheckpoint checkpoint) {
                assertEquals("one".equals(checkpoint.getBatchId()) ? 1 : 3, output.size());
                committed.add(checkpoint.getBatchId());
            }

            @Override
            public void abort(StreamingCheckpoint checkpoint, Throwable failure) {
                aborted.incrementAndGet();
            }

            @Override
            public void close() {
            }
        };
        Configuration configuration = configuration();
        configuration.set("streaming.batchRetryCount", 0);
        StreamingJobContainer container = new StreamingJobContainer(configuration, source);
        container.addConsumerPlugin(PluginType.WRITER, (plugin, peer) -> writer(output));
        List<Communication> statistics = new ArrayList<>();
        container.setListener(new StreamingJobListener() {
            @Override
            public void onBatchCommitted(StreamingBatch batch, Communication communication, long elapsed) {
                statistics.add(communication);
                if (statistics.size() == batches.size()) {
                    container.requestStop();
                }
            }
        });
        try {
            container.start();
        } finally {
            container.cancel();
        }

        assertEquals(List.of("77-_", "5Lit5paH", "IA=="), output);
        assertEquals(List.of("one", "two"), committed);
        assertEquals(0, aborted.get());
        assertEquals("two", container.getLastCommittedCheckpoint().getBatchId());
        assertStatistics(statistics.get(0), 1, 1);
        assertStatistics(statistics.get(1), 2, 0);
        assertEquals(1, RecordingCollector.dirtyRecords.size());
        assertEquals("77+/", batches.get(0).getRecords().get(0).getColumn(1).asString());
    }

    private static void assertStatistics(Communication communication, long success, long failed) {
        assertEquals(success, communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS).longValue());
        assertEquals(failed, communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS).longValue());
        assertEquals(0L, communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS).longValue());
        assertEquals(failed, communication.getLongCounter(CommunicationTool.READ_FAILED_RECORDS).longValue());
    }

    private static Configuration configuration() {
        Configuration configuration = Configuration.from("{"
                + "\"core\":{\"plugin\":{\"reporter\":[]},"
                + "\"container\":{\"taskGroup\":{\"reportInterval\":20,\"sleepInterval\":5}}},"
                + "\"reader\":{\"type\":\"test\",\"config\":{}},"
                + "\"writer\":{\"type\":\"test\",\"config\":{}},"
                + "\"transformer\":["
                + "{\"name\":\"base64_str\",\"parameter\":{\"columnIndex\":1,\"paras\":[\"decode\",\"standard\",\"UTF-8\"]}},"
                + "{\"name\":\"base64_str\",\"parameter\":{\"columnIndex\":1,\"paras\":[\"encode\",\"url_safe\",\"UTF-8\"]}}]}");
        configuration.set("core.plugin.collector", RecordingCollector.class.getName());
        return configuration;
    }

    private static Writer.Job writer(List<String> output) {
        return new Writer.Job() {
            @Override
            public void startWrite(RecordReceiver receiver) {
                Record record;
                while ((record = receiver.getFromReader()) != null) {
                    assertEquals("before", record.getColumn(0).asString());
                    assertEquals("after", record.getColumn(2).asString());
                    output.add(record.getColumn(1).asString());
                }
            }
        };
    }

    private static StreamingBatch batch(String id, String... values) {
        List<Record> records = new ArrayList<>();
        for (String value : values) {
            records.add(row(value));
        }
        return new StreamingBatch(records, new StreamingCheckpoint(id, Map.of("offset", values.length)),
                1_700_000_000_000L, 0L);
    }

    private static DefaultRecord row(String value) {
        DefaultRecord record = new DefaultRecord();
        record.addColumn(new StringColumn("before"));
        record.addColumn(new StringColumn(value));
        record.addColumn(new StringColumn("after"));
        return record;
    }

    private static void clearLoaderCaches() {
        PluginRuntimeResolvers.reset();
        ConfigParser.clearCache();
        JarLoaderCenter.clearJarLoader();
    }

    public static final class RecordingCollector extends AbstractTaskPluginCollector {
        private static final List<Record> dirtyRecords = new CopyOnWriteArrayList<>();
        private static final List<String> errors = new CopyOnWriteArrayList<>();

        public RecordingCollector(Configuration configuration, Communication communication, PluginType type) {
            super(configuration, communication, type);
        }

        @Override
        public void collectDirtyRecord(Record record, Throwable failure, String message) {
            dirtyRecords.add(record);
            errors.add(message);
            super.collectDirtyRecord(record, failure, message);
        }
    }
}
