package com.jdragon.aggregation.kafka;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.streaming.job.StreamingBatch;
import com.jdragon.aggregation.core.streaming.job.StreamingPollRequest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KafkaStreamingSessionIT {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC = "NativeStreaming-M2-20260827";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final DateTimeFormatter RUN_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").withZone(ZoneOffset.UTC);

    private KafkaStreamingSession session;

    @After
    public void closeSession() {
        if (session != null) {
            session.close();
        }
    }

    @Test
    public void realKafkaCommitAbortAndRestartPreserveAtLeastOnce() throws Exception {
        Assume.assumeTrue("enable with -Dkafka.it.enabled=true",
                Boolean.getBoolean("kafka.it.enabled"));
        String runTag = RUN_FORMAT.format(Instant.now());
        String groupId = "NativeStreaming-M2-" + runTag;
        Properties adminProperties = new Properties();
        adminProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(adminProperties)) {
            ensureTopic(admin);
            long startOffset = latestOffset(admin);
            admin.alterConsumerGroupOffsets(groupId,
                    Map.of(PARTITION, new OffsetAndMetadata(startOffset))).all().get(30, TimeUnit.SECONDS);

            List<Long> initialOffsets = produce(runTag, "commit", 3);
            session = open(groupId);
            StreamingBatch committedBatch = pollNonEmpty(session, 30_000L);
            assertEquals(3, committedBatch.getRecords().size());
            assertEquals(startOffset, initialOffsets.get(0).longValue());
            assertEquals(startOffset, checkpointOffset(committedBatch, "startOffsets"));
            assertEquals(startOffset + 3L, checkpointOffset(committedBatch, "nextOffsets"));
            assertEquals(startOffset, committedOffset(admin, groupId));

            session.commit(committedBatch.getCheckpoint());
            await(() -> committedOffsetUnchecked(admin, groupId) == startOffset + 3L, 10_000L);

            long abortOffset = produce(runTag, "abort", 1).get(0);
            StreamingBatch abortedBatch = pollNonEmpty(session, 30_000L);
            assertEquals(abortOffset, checkpointOffset(abortedBatch, "startOffsets"));
            session.abort(abortedBatch.getCheckpoint(), new IllegalStateException("injected writer failure"));
            assertEquals(startOffset + 3L, committedOffset(admin, groupId));

            StreamingBatch replayedAfterAbort = pollNonEmpty(session, 30_000L);
            assertEquals(abortedBatch.getBatchId(), replayedAfterAbort.getBatchId());
            session.commit(replayedAfterAbort.getCheckpoint());
            await(() -> committedOffsetUnchecked(admin, groupId) == abortOffset + 1L, 10_000L);

            long cancelOffset = produce(runTag, "cancel", 1).get(0);
            StreamingBatch uncommittedBatch = pollNonEmpty(session, 30_000L);
            assertEquals(cancelOffset, checkpointOffset(uncommittedBatch, "startOffsets"));
            String uncommittedBatchId = uncommittedBatch.getBatchId();
            session.close();
            assertEquals(cancelOffset, committedOffset(admin, groupId));

            session = open(groupId);
            StreamingBatch replayedAfterRestart = pollNonEmpty(session, 30_000L);
            assertEquals(uncommittedBatchId, replayedAfterRestart.getBatchId());
            session.commit(replayedAfterRestart.getCheckpoint());
            await(() -> committedOffsetUnchecked(admin, groupId) == cancelOffset + 1L, 10_000L);

            System.out.println("M2_KAFKA_IT topic=" + TOPIC
                    + " group=" + groupId
                    + " startOffset=" + startOffset
                    + " committedOffset=" + committedOffset(admin, groupId)
                    + " commitBatchId=" + committedBatch.getBatchId()
                    + " abortBatchId=" + abortedBatch.getBatchId()
                    + " restartBatchId=" + replayedAfterRestart.getBatchId());
        }
    }

    @Test
    public void realKafkaFencedCommitDoesNotAdvanceOffsetAndReplaysBatch() throws Exception {
        Assume.assumeTrue("enable with -Dkafka.it.enabled=true",
                Boolean.getBoolean("kafka.it.enabled"));
        String runTag = RUN_FORMAT.format(Instant.now());
        String groupId = "NativeStreaming-M2-CommitFailure-" + runTag;
        Properties adminProperties = new Properties();
        adminProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(adminProperties)) {
            ensureTopic(admin);
            long startOffset = latestOffset(admin);
            admin.alterConsumerGroupOffsets(groupId,
                    Map.of(PARTITION, new OffsetAndMetadata(startOffset))).all().get(30, TimeUnit.SECONDS);
            produce(runTag, "commit-failure", 1);

            KafkaReaderOptions options = KafkaReaderOptions.from(readerConfiguration(groupId));
            Properties consumerProperties = options.consumerProperties();
            consumerProperties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupId + "-instance");
            AtomicBoolean fenceTriggered = new AtomicBoolean();
            session = new KafkaStreamingSession(TOPIC, options.recordParser(),
                    () -> fencingConsumer(consumerProperties, fenceTriggered));

            StreamingBatch failedBatch = pollNonEmpty(session, 30_000L);
            assertEquals(startOffset, checkpointOffset(failedBatch, "startOffsets"));
            assertEquals(startOffset, committedOffset(admin, groupId));

            Exception commitFailure = null;
            try {
                session.commit(failedBatch.getCheckpoint());
            } catch (Exception failure) {
                commitFailure = failure;
            }
            assertTrue("duplicate static member must fence the committing consumer", fenceTriggered.get());
            assertTrue("commit must fail with a Kafka group fencing error",
                    commitFailure instanceof FencedInstanceIdException
                            || commitFailure instanceof CommitFailedException);
            assertEquals(startOffset, committedOffset(admin, groupId));
            assertEquals(Long.valueOf(1L), session.metrics().getCounters().get("commitFailures"));

            String failedBatchId = failedBatch.getBatchId();
            session.close();
            session = open(groupId);
            StreamingBatch replayedBatch = pollNonEmpty(session, 30_000L);
            assertEquals(failedBatchId, replayedBatch.getBatchId());
            session.commit(replayedBatch.getCheckpoint());
            await(() -> committedOffsetUnchecked(admin, groupId) == startOffset + 1L, 10_000L);

            System.out.println("M7_KAFKA_COMMIT_FAILURE_IT topic=" + TOPIC
                    + " group=" + groupId
                    + " startOffset=" + startOffset
                    + " committedOffset=" + committedOffset(admin, groupId)
                    + " batchId=" + failedBatchId
                    + " failureType=" + commitFailure.getClass().getSimpleName());
        }
    }

    @SuppressWarnings("unchecked")
    private Consumer<String, String> fencingConsumer(Properties properties,
                                                      AtomicBoolean fenceTriggered) {
        Properties delegateProperties = new Properties();
        delegateProperties.putAll(properties);
        KafkaConsumer<String, String> delegate =
                new KafkaConsumer<String, String>(delegateProperties);
        AtomicBoolean interceptCommit = new AtomicBoolean();
        return (Consumer<String, String>) Proxy.newProxyInstance(
                Consumer.class.getClassLoader(),
                new Class<?>[]{Consumer.class},
                (proxy, method, arguments) -> {
                    if ("commitSync".equals(method.getName())
                            && interceptCommit.compareAndSet(false, true)) {
                        fenceStaticMember(properties);
                        fenceTriggered.set(true);
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private void fenceStaticMember(Properties properties) {
        Properties fencerProperties = new Properties();
        fencerProperties.putAll(properties);
        fencerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG,
                properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG) + "-fencer");
        try (KafkaConsumer<String, String> fencer =
                     new KafkaConsumer<String, String>(fencerProperties)) {
            fencer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(20L).toNanos();
            while (fencer.assignment().isEmpty() && System.nanoTime() < deadline) {
                fencer.poll(Duration.ofMillis(250L));
            }
            assertFalse("duplicate static member did not join the group",
                    fencer.assignment().isEmpty());
        }
    }

    private KafkaStreamingSession open(String groupId) {
        KafkaReaderOptions options = KafkaReaderOptions.from(readerConfiguration(groupId));
        return new KafkaStreamingSession(options.consumerProperties(), options.getTopic(), options.recordParser());
    }

    private StreamingBatch pollNonEmpty(KafkaStreamingSession activeSession, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime() + Duration.ofMillis(timeoutMillis).toNanos();
        while (System.nanoTime() < deadline) {
            StreamingBatch batch = activeSession.pollBatch(
                    new StreamingPollRequest(1_000L, 100, 1024L * 1024L));
            if (!batch.isEmpty()) {
                return batch;
            }
        }
        throw new AssertionError("Kafka streaming batch was not available within " + timeoutMillis + "ms");
    }

    private void ensureTopic(AdminClient admin) throws Exception {
        Set<String> topics = admin.listTopics().names().get(30, TimeUnit.SECONDS);
        if (!topics.contains(TOPIC)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
    }

    private long latestOffset(AdminClient admin) throws Exception {
        ListOffsetsResult result = admin.listOffsets(Map.of(PARTITION, OffsetSpec.latest()));
        return result.partitionResult(PARTITION).get(30, TimeUnit.SECONDS).offset();
    }

    private long committedOffset(AdminClient admin, String groupId) throws Exception {
        OffsetAndMetadata offset = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS).get(PARTITION);
        return offset == null ? -1L : offset.offset();
    }

    private long committedOffsetUnchecked(AdminClient admin, String groupId) {
        try {
            return committedOffset(admin, groupId);
        } catch (Exception failure) {
            return -1L;
        }
    }

    private List<Long> produce(String runTag, String scenario, int count) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        List<Long> offsets = new ArrayList<Long>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties)) {
            for (int index = 0; index < count; index++) {
                String id = runTag + "-" + scenario + "-" + index;
                String value = "{\"id\":\"" + id + "\",\"name\":\"" + scenario
                        + "\",\"runTag\":\"" + runTag + "\"}";
                offsets.add(producer.send(new ProducerRecord<String, String>(TOPIC, id, value))
                        .get(30, TimeUnit.SECONDS).offset());
            }
            producer.flush();
        }
        return offsets;
    }

    @SuppressWarnings("unchecked")
    private long checkpointOffset(StreamingBatch batch, String field) {
        Map<String, Long> offsets = (Map<String, Long>) batch.getCheckpoint().getState().get(field);
        return offsets.get(TOPIC + ":0");
    }

    private Configuration readerConfiguration(String groupId) {
        return Configuration.from("{"
                + "\"bootstrapServers\":\"" + BOOTSTRAP_SERVERS + "\","
                + "\"groupId\":\"" + groupId + "\","
                + "\"topic\":\"" + TOPIC + "\","
                + "\"offsetReset\":\"earliest\","
                + "\"batchSize\":100,"
                + "\"parsingRules\":\"json\","
                + "\"columns\":["
                + "{\"index\":0,\"name\":\"id\"},"
                + "{\"index\":1,\"name\":\"name\"},"
                + "{\"index\":2,\"name\":\"runTag\"}]} ");
    }

    private void await(BooleanSupplier condition, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + Duration.ofMillis(timeoutMillis).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(100L);
        }
        assertTrue("condition was not met within " + timeoutMillis + "ms", condition.getAsBoolean());
    }
}
