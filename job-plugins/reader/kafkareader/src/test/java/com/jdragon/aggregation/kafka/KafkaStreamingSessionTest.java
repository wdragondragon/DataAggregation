package com.jdragon.aggregation.kafka;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.streaming.job.StreamingBatch;
import com.jdragon.aggregation.core.streaming.job.StreamingCheckpoint;
import com.jdragon.aggregation.core.streaming.job.StreamingPollRequest;
import com.jdragon.aggregation.core.streaming.job.StreamingSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaStreamingSessionTest {

    private static final String TOPIC = "NativeStreaming-M2-Unit";
    private static final TopicPartition PARTITION_ZERO = new TopicPartition(TOPIC, 0);

    private KafkaStreamingSession session;

    @After
    public void closeSession() {
        if (session != null) {
            session.close();
        }
    }

    @Test
    public void batchIdIsStableAcrossRecordOrder() {
        List<ConsumerRecord<String, String>> records = List.of(
                record(1, 8L, "{\"id\":\"8\"}"),
                record(0, 3L, "{\"id\":\"3\"}"),
                record(0, 4L, "{\"id\":\"4\"}"));
        List<ConsumerRecord<String, String>> reversed = new ArrayList<ConsumerRecord<String, String>>(records);
        Collections.reverse(reversed);

        StreamingCheckpoint first = KafkaStreamingSession.checkpointFor(records);
        StreamingCheckpoint second = KafkaStreamingSession.checkpointFor(reversed);

        assertEquals(first.getBatchId(), second.getBatchId());
        assertTrue(first.getBatchId().startsWith("kafka-"));
        assertEquals(Map.of(TOPIC + ":0", 3L, TOPIC + ":1", 8L),
                first.getState().get("startOffsets"));
        assertEquals(Map.of(TOPIC + ":0", 5L, TOPIC + ":1", 9L),
                first.getState().get("nextOffsets"));
    }

    @Test
    public void commitAdvancesNextOffsetOnlyAfterExplicitCommitAndKeepsHeartbeats() throws Exception {
        TrackingMockConsumer consumer = consumerWithRecords(List.of(
                record(0, 0L, "{\"id\":\"1\"}"),
                record(0, 1L, "{\"id\":\"2\"}")));
        session = new KafkaStreamingSession(TOPIC, jsonParser(), () -> consumer);

        StreamingBatch batch = session.pollBatch(new StreamingPollRequest(2_000L, 10, 1_024L));

        assertFalse(batch.isEmpty());
        assertEquals(2, batch.getRecords().size());
        assertNull(consumer.committed(PARTITION_ZERO));
        await(() -> consumer.paused().contains(PARTITION_ZERO), 2_000L);
        int pollCountAfterDelivery = consumer.pollCount.get();
        await(() -> consumer.pollCount.get() > pollCountAfterDelivery, 2_000L);

        session.commit(batch.getCheckpoint());

        assertEquals(2L, consumer.committed(PARTITION_ZERO).offset());
        assertFalse(consumer.paused().contains(PARTITION_ZERO));
        assertEquals(Long.valueOf(1L), session.metrics().getCounters().get("commits"));
    }

    @Test
    public void commitFailureDoesNotAdvanceAndRetainsInFlightCheckpoint() throws Exception {
        TrackingMockConsumer consumer = consumerWithRecords(
                List.of(record(0, 0L, "{\"id\":\"1\"}")));
        consumer.commitFailuresRemaining.set(1);
        session = new KafkaStreamingSession(TOPIC, jsonParser(), () -> consumer);
        StreamingBatch batch = session.pollBatch(new StreamingPollRequest(2_000L, 10, 1_024L));

        try {
            session.commit(batch.getCheckpoint());
            fail("expected Kafka commit failure");
        } catch (KafkaException expected) {
            assertTrue(expected.getMessage().contains("expected commit failure"));
        }

        assertNull(consumer.committed(PARTITION_ZERO));
        assertEquals(batch.getBatchId(), session.snapshotState().get("batchId"));
        assertTrue(consumer.paused().contains(PARTITION_ZERO));
        assertEquals(Long.valueOf(1L), session.metrics().getCounters().get("commitFailures"));

        session.commit(batch.getCheckpoint());
        assertEquals(1L, consumer.committed(PARTITION_ZERO).offset());
    }

    @Test
    public void abortSeeksToBatchStartWithoutCommitting() throws Exception {
        TrackingMockConsumer consumer = consumerWithRecords(List.of(
                record(0, 0L, "{\"id\":\"1\"}"),
                record(0, 1L, "{\"id\":\"2\"}")));
        session = new KafkaStreamingSession(TOPIC, jsonParser(), () -> consumer);
        StreamingBatch batch = session.pollBatch(new StreamingPollRequest(2_000L, 10, 1_024L));
        assertEquals(2L, consumer.position(PARTITION_ZERO));

        session.abort(batch.getCheckpoint(), new IllegalStateException("expected writer failure"));

        assertEquals(0L, consumer.position(PARTITION_ZERO));
        assertNull(consumer.committed(PARTITION_ZERO));
        assertFalse(consumer.paused().contains(PARTITION_ZERO));
        assertEquals(Long.valueOf(1L), session.metrics().getCounters().get("aborts"));
    }

    @Test
    public void allDirtyRecordsStillProduceCommittableBatch() throws Exception {
        TrackingMockConsumer consumer = consumerWithRecords(
                List.of(record(0, 0L, "only-one-field")));
        session = new KafkaStreamingSession(TOPIC, splitParser(), () -> consumer);

        StreamingBatch batch = session.pollBatch(new StreamingPollRequest(2_000L, 10, 1_024L));

        assertFalse(batch.isEmpty());
        assertTrue(batch.getRecords().isEmpty());
        assertNotNull(batch.getCheckpoint());
        assertEquals(Long.valueOf(1L), session.metrics().getCounters().get("dirtyRecords"));
        session.commit(batch.getCheckpoint());
        assertEquals(1L, consumer.committed(PARTITION_ZERO).offset());
    }

    @Test
    public void restoreBeforeAssignmentIsAppliedWhenPartitionArrives() throws Exception {
        TrackingMockConsumer consumer = new TrackingMockConsumer();
        consumer.updateBeginningOffsets(Map.of(PARTITION_ZERO, 0L));
        consumer.updateEndOffsets(Map.of(PARTITION_ZERO, 10L));
        session = new KafkaStreamingSession(TOPIC, jsonParser(), () -> consumer);

        session.restoreState(Map.of("nextOffsets", Map.of(TOPIC + ":0", 7L)));
        consumer.schedulePollTask(() -> consumer.rebalance(List.of(PARTITION_ZERO)));

        await(() -> consumer.assignment().contains(PARTITION_ZERO), 2_000L);
        assertEquals(7L, consumer.position(PARTITION_ZERO));
    }

    @Test
    public void resetOffsetCommitsBeginningOnlyOnFirstAssignment() throws Exception {
        TrackingMockConsumer consumer = new TrackingMockConsumer();
        consumer.updateBeginningOffsets(Map.of(PARTITION_ZERO, 3L));
        consumer.updateEndOffsets(Map.of(PARTITION_ZERO, 10L));
        consumer.schedulePollTask(() -> consumer.rebalance(List.of(PARTITION_ZERO)));
        session = new KafkaStreamingSession(TOPIC, jsonParser(), () -> consumer, true);

        session.pollBatch(new StreamingPollRequest(20L, 10, 1_024L));
        await(() -> consumer.committed(PARTITION_ZERO) != null, 2_000L);
        assertEquals(3L, consumer.committed(PARTITION_ZERO).offset());

        consumer.commitSync(Map.of(PARTITION_ZERO, new OffsetAndMetadata(6L)));
        int pollsBeforeRebalance = consumer.pollCount.get();
        consumer.schedulePollTask(() -> consumer.rebalance(List.of(PARTITION_ZERO)));
        await(() -> consumer.pollCount.get() > pollsBeforeRebalance, 2_000L);
        assertEquals(6L, consumer.committed(PARTITION_ZERO).offset());
    }

    @Test
    public void wakeupAndCloseAreIdempotent() throws Exception {
        TrackingMockConsumer consumer = new TrackingMockConsumer();
        session = new KafkaStreamingSession(TOPIC, jsonParser(), () -> consumer);
        session.pollBatch(new StreamingPollRequest(20L, 10, 1_024L));

        session.wakeup();
        session.close();
        session.close();

        assertTrue(consumer.closed());
    }

    @Test
    public void optionsForceManualCommitAndKafkaReaderRetainsBatchContract() {
        Configuration configuration = readerConfiguration();
        configuration.set("otherProperties.enable.auto.commit", "true");
        configuration.set("otherProperties.bootstrap.servers", "invalid:9092");
        KafkaReaderOptions options = KafkaReaderOptions.from(configuration);
        Properties properties = options.consumerProperties();

        assertEquals("false", properties.getProperty("enable.auto.commit"));
        assertEquals("127.0.0.1:9092", properties.getProperty("bootstrap.servers"));
        assertTrue(new KafkaReader() instanceof StreamingSource);
        assertNotNull(org.slf4j.event.Level.INFO);
    }

    @Test
    public void datasourceSecurityPropertiesArePassedToKafkaConsumer() {
        Configuration configuration = readerConfiguration();
        configuration.set("security.protocol", "SSL");
        configuration.set("ssl.truststore.location", "truststore.jks");
        configuration.set("otherProperties.security.protocol", "PLAINTEXT");
        configuration.set("otherProperties.ssl.truststore.location", "wrong.jks");

        Properties properties = KafkaReaderOptions.from(configuration).consumerProperties();

        assertEquals("SSL", properties.getProperty("security.protocol"));
        assertEquals("truststore.jks", properties.getProperty("ssl.truststore.location"));
    }

    @Test
    public void splitParserPreservesTrailingEmptyColumn() throws Exception {
        assertEquals("", splitParser().parse("alpha,").getColumn(1).asString());
    }

    @Test
    public void jsonParserErrorDoesNotExposeMessageBody() {
        String sensitiveBody = "{invalid-json-NativeStreaming-secret";
        try {
            jsonParser().parse(sensitiveBody);
            fail("expected JSON parse failure");
        } catch (Exception expected) {
            assertEquals("Kafka JSON message could not be parsed", expected.getMessage());
            assertFalse(expected.getMessage().contains("NativeStreaming-secret"));
        }
    }

    @Test
    public void requiredStreamingOptionsAreValidated() {
        Configuration configuration = readerConfiguration();
        configuration.set("groupId", " ");
        try {
            KafkaReaderOptions.from(configuration);
            fail("expected groupId validation failure");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("groupId"));
        }
    }

    @Test
    public void sourcePollTimeoutUsesReaderValueUpToKafkaClientLimit() {
        assertEquals(5_000L, KafkaStreamingSession.normalizeSourcePollTimeout(5_000L));
        assertEquals(60_000L, KafkaStreamingSession.normalizeSourcePollTimeout(120_000L));
        assertEquals(1L, KafkaStreamingSession.normalizeSourcePollTimeout(0L));
    }

    @Test
    public void streamingSessionPollsKafkaWithReaderTimeout() throws Exception {
        TrackingMockConsumer consumer = new TrackingMockConsumer();
        session = new KafkaStreamingSession(TOPIC, jsonParser(), () -> consumer, false, 5_000L);

        session.pollBatch(new StreamingPollRequest(20L, 10, 1_024L));

        await(() -> consumer.lastPollTimeoutMillis.get() == 5_000L, 2_000L);
    }

    private TrackingMockConsumer consumerWithRecords(List<ConsumerRecord<String, String>> records) {
        TrackingMockConsumer consumer = new TrackingMockConsumer();
        Map<TopicPartition, Long> beginnings = new LinkedHashMap<TopicPartition, Long>();
        Map<TopicPartition, Long> ends = new LinkedHashMap<TopicPartition, Long>();
        for (ConsumerRecord<String, String> record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            beginnings.putIfAbsent(partition, record.offset());
            ends.merge(partition, record.offset() + 1L, Math::max);
        }
        consumer.updateBeginningOffsets(beginnings);
        consumer.updateEndOffsets(ends);
        consumer.schedulePollTask(() -> {
            consumer.rebalance(beginnings.keySet());
            records.forEach(consumer::addRecord);
        });
        return consumer;
    }

    private ConsumerRecord<String, String> record(int partition, long offset, String value) {
        return new ConsumerRecord<String, String>(TOPIC, partition, offset, null, value);
    }

    private KafkaRecordParser jsonParser() {
        ColumnEntry id = new ColumnEntry();
        id.setIndex(0);
        id.setName("id");
        return new KafkaRecordParser("json", null, List.of(id));
    }

    private KafkaRecordParser splitParser() {
        ColumnEntry first = new ColumnEntry();
        first.setIndex(0);
        first.setName("first");
        ColumnEntry second = new ColumnEntry();
        second.setIndex(1);
        second.setName("second");
        return new KafkaRecordParser("split", ",", List.of(first, second));
    }

    private Configuration readerConfiguration() {
        return Configuration.from("{"
                + "\"bootstrapServers\":\"127.0.0.1:9092\","
                + "\"groupId\":\"NativeStreaming-M2-Unit-Group\","
                + "\"topic\":\"" + TOPIC + "\","
                + "\"offsetReset\":\"earliest\","
                + "\"parsingRules\":\"json\","
                + "\"columns\":[{\"index\":0,\"name\":\"id\"}]} ");
    }

    private void await(BooleanSupplier condition, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + Duration.ofMillis(timeoutMillis).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue("condition was not met within " + timeoutMillis + "ms", condition.getAsBoolean());
    }

    private static final class TrackingMockConsumer extends MockConsumer<String, String> {

        private final AtomicInteger pollCount = new AtomicInteger();
        private final AtomicInteger commitFailuresRemaining = new AtomicInteger();
        private final AtomicLong lastPollTimeoutMillis = new AtomicLong();

        private TrackingMockConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
            pollCount.incrementAndGet();
            lastPollTimeoutMillis.set(timeout.toMillis());
            return super.poll(timeout);
        }

        @Override
        public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets,
                                            Duration timeout) {
            if (commitFailuresRemaining.getAndUpdate(value -> Math.max(0, value - 1)) > 0) {
                throw new KafkaException("expected commit failure");
            }
            super.commitSync(offsets, timeout);
        }
    }
}
