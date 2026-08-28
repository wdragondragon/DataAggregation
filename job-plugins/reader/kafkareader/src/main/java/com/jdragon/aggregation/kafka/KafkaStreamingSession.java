package com.jdragon.aggregation.kafka;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.streaming.job.StreamingBatch;
import com.jdragon.aggregation.core.streaming.job.StreamingCheckpoint;
import com.jdragon.aggregation.core.streaming.job.StreamingExecutionException;
import com.jdragon.aggregation.core.streaming.job.StreamingMetricsSnapshot;
import com.jdragon.aggregation.core.streaming.job.StreamingPollRequest;
import com.jdragon.aggregation.core.streaming.job.StreamingSession;
import com.jdragon.aggregation.datasource.scan.QueueCheckpointReadable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

final class KafkaStreamingSession implements StreamingSession, QueueCheckpointReadable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamingSession.class);
    private static final Duration HEARTBEAT_POLL = Duration.ofMillis(250L);
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(30L);
    private static final long START_TIMEOUT_MILLIS = 30_000L;

    private final String topic;
    private final KafkaRecordParser parser;
    private final ConsumerFactory consumerFactory;
    private final boolean resetOffsetOnFirstAssignment;
    private final BlockingQueue<StreamingBatch> readyBatches = new ArrayBlockingQueue<StreamingBatch>(1);
    private final BlockingQueue<ControlCommand> commands = new LinkedBlockingQueue<ControlCommand>();
    private final Object commandLifecycleMonitor = new Object();
    private final AtomicReference<StreamingPollRequest> pollRequest =
            new AtomicReference<StreamingPollRequest>(StreamingPollRequest.defaults());
    private final AtomicReference<Consumer<String, String>> activeConsumer =
            new AtomicReference<Consumer<String, String>>();
    private final AtomicReference<Throwable> terminalFailure = new AtomicReference<Throwable>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final CountDownLatch started = new CountDownLatch(1);
    private final Thread sourceThread;

    private final AtomicLong recordsRead = new AtomicLong();
    private final AtomicLong recordsDecoded = new AtomicLong();
    private final AtomicLong bytesRead = new AtomicLong();
    private final AtomicLong batchesRead = new AtomicLong();
    private final AtomicLong dirtyRecords = new AtomicLong();
    private final AtomicLong commits = new AtomicLong();
    private final AtomicLong commitFailures = new AtomicLong();
    private final AtomicLong aborts = new AtomicLong();
    private final AtomicLong rebalances = new AtomicLong();
    private final AtomicLong currentLag = new AtomicLong();
    private final AtomicLong maxLag = new AtomicLong();
    private final AtomicLong lastMessageTimestamp = new AtomicLong();

    private final Deque<ConsumerRecord<String, String>> bufferedRecords =
            new ArrayDeque<ConsumerRecord<String, String>>();
    private final Map<TopicPartition, Long> pendingRestoreOffsets =
            new LinkedHashMap<TopicPartition, Long>();
    private boolean sourceStopped;
    private boolean initialOffsetResetApplied;
    private volatile CheckpointWindow inFlight;

    KafkaStreamingSession(Properties properties, String topic, KafkaRecordParser parser) {
        this(properties, topic, parser, false);
    }

    KafkaStreamingSession(Properties properties,
                          String topic,
                          KafkaRecordParser parser,
                          boolean resetOffsetOnFirstAssignment) {
        this(topic, parser, () -> new KafkaConsumer<String, String>(properties),
                resetOffsetOnFirstAssignment);
    }

    KafkaStreamingSession(String topic, KafkaRecordParser parser, ConsumerFactory consumerFactory) {
        this(topic, parser, consumerFactory, false);
    }

    KafkaStreamingSession(String topic,
                          KafkaRecordParser parser,
                          ConsumerFactory consumerFactory,
                          boolean resetOffsetOnFirstAssignment) {
        this.topic = topic;
        this.parser = parser;
        this.consumerFactory = consumerFactory;
        this.resetOffsetOnFirstAssignment = resetOffsetOnFirstAssignment;
        this.sourceThread = new Thread(this::runSourceLoop,
                "DataAggregation-Kafka-Streaming-" + sanitize(topic));
        this.sourceThread.setDaemon(true);
        this.sourceThread.start();
    }

    @Override
    public StreamingBatch pollBatch(StreamingPollRequest request) throws Exception {
        if (request == null) {
            throw new IllegalArgumentException("Kafka streaming poll request must not be null");
        }
        pollRequest.set(request);
        awaitStarted();
        rethrowTerminalFailure();
        StreamingBatch batch = readyBatches.poll(request.getTimeoutMillis(), TimeUnit.MILLISECONDS);
        rethrowTerminalFailure();
        return batch == null ? StreamingBatch.empty() : batch;
    }

    @Override
    public void commit(StreamingCheckpoint checkpoint) throws Exception {
        executeCommand(ControlCommand.commit(checkpoint));
    }

    @Override
    public void abort(StreamingCheckpoint checkpoint, Throwable failure) {
        if (checkpoint == null || closed.get()) {
            return;
        }
        try {
            executeCommand(ControlCommand.abort(checkpoint));
        } catch (Exception abortFailure) {
            throw new StreamingExecutionException(
                    "Kafka streaming checkpoint abort failed for batch " + checkpoint.getBatchId(), abortFailure);
        }
    }

    @Override
    public void wakeup() {
        Consumer<String, String> consumer = activeConsumer.get();
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    @Override
    public StreamingMetricsSnapshot metrics() {
        Map<String, Long> counters = new LinkedHashMap<String, Long>();
        counters.put("recordsRead", recordsRead.get());
        counters.put("recordsDecoded", recordsDecoded.get());
        counters.put("bytesRead", bytesRead.get());
        counters.put("batchesRead", batchesRead.get());
        counters.put("dirtyRecords", dirtyRecords.get());
        counters.put("commits", commits.get());
        counters.put("commitFailures", commitFailures.get());
        counters.put("aborts", aborts.get());
        counters.put("rebalanceCount", rebalances.get());
        Map<String, Long> gauges = new LinkedHashMap<String, Long>();
        gauges.put("lag", currentLag.get());
        gauges.put("maxLag", maxLag.get());
        long lastMessage = lastMessageTimestamp.get();
        return new StreamingMetricsSnapshot(counters, gauges, lastMessage == 0L ? null : lastMessage);
    }

    @Override
    public void close() {
        closed.compareAndSet(false, true);
        if (Thread.currentThread() == sourceThread) {
            return;
        }
        wakeup();
        joinSourceThread(10_000L);
        if (sourceThread.isAlive()) {
            sourceThread.interrupt();
            wakeup();
            joinSourceThread(2_000L);
        }
        if (sourceThread.isAlive()) {
            LOG.warn("Kafka streaming source thread did not stop within 12 seconds for topic {}", topic);
        } else {
            LOG.info("Kafka streaming session closed for topic {}", topic);
        }
        failPendingCommands(new IllegalStateException("Kafka streaming session is closed"));
    }

    private void joinSourceThread(long timeoutMillis) {
        try {
            sourceThread.join(timeoutMillis);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        try {
            executeCommand(ControlCommand.restore(state));
        } catch (Exception failure) {
            throw new StreamingExecutionException("Kafka streaming checkpoint restore failed", failure);
        }
    }

    @Override
    public Map<String, Object> snapshotState() {
        CheckpointWindow active = inFlight;
        return active == null
                ? new LinkedHashMap<String, Object>()
                : new LinkedHashMap<String, Object>(active.checkpoint.getState());
    }

    @Override
    public void commitState(Map<String, Object> state) {
        String batchId = stringValue(state == null ? null : state.get("batchId"));
        if (batchId.isEmpty()) {
            throw new IllegalArgumentException("Kafka checkpoint state requires batchId");
        }
        try {
            commit(new StreamingCheckpoint(batchId, state));
        } catch (Exception failure) {
            throw new StreamingExecutionException("Kafka streaming checkpoint commit failed", failure);
        }
    }

    private void runSourceLoop() {
        Consumer<String, String> consumer = null;
        try {
            consumer = consumerFactory.create();
            activeConsumer.set(consumer);
            Consumer<String, String> subscribedConsumer = consumer;
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    rebalances.incrementAndGet();
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    rebalances.incrementAndGet();
                    resetOffsetsOnFirstAssignment(subscribedConsumer, partitions);
                    applyPendingRestoreOffsets(subscribedConsumer, partitions);
                }
            });
            started.countDown();
            while (!closed.get()) {
                processCommands(consumer);
                if (closed.get()) {
                    break;
                }
                if (inFlight != null) {
                    heartbeat(consumer);
                    continue;
                }
                if (bufferedRecords.isEmpty()) {
                    ConsumerRecords<String, String> records = poll(consumer, sourcePollDuration());
                    for (ConsumerRecord<String, String> record : records) {
                        bufferedRecords.addLast(record);
                    }
                    updateLag(consumer);
                }
                if (!bufferedRecords.isEmpty()) {
                    publishNextBatch(consumer, pollRequest.get());
                }
            }
        } catch (Throwable failure) {
            if (!closed.get()) {
                terminalFailure.compareAndSet(null, failure);
                LOG.error("Kafka streaming source failed for topic {}, errorType={}, detail={}",
                        topic, failure.getClass().getSimpleName(), safeMessage(failure));
            }
        } finally {
            closeConsumer(consumer);
            started.countDown();
            activeConsumer.set(null);
            Throwable failure = terminalFailure.get();
            markSourceStopped(failure == null
                    ? new IllegalStateException("Kafka streaming source stopped") : failure);
        }
    }

    private void closeConsumer(Consumer<String, String> consumer) {
        if (consumer == null) {
            return;
        }
        boolean interrupted = Thread.interrupted();
        try {
            for (int attempt = 1; attempt <= 2; attempt++) {
                try {
                    consumer.close(Duration.ofSeconds(10L));
                    return;
                } catch (WakeupException wakeup) {
                    if (attempt == 2) {
                        LOG.warn("Kafka consumer close was interrupted by wakeup for topic {}, errorType={}",
                                topic, wakeup.getClass().getSimpleName());
                    }
                } catch (RuntimeException failure) {
                    LOG.warn("Kafka consumer close failed for topic {}, errorType={}, detail={}",
                            topic, failure.getClass().getSimpleName(), safeMessage(failure));
                    return;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void publishNextBatch(Consumer<String, String> consumer, StreamingPollRequest request)
            throws InterruptedException {
        List<ConsumerRecord<String, String>> selected = new ArrayList<ConsumerRecord<String, String>>();
        long selectedBytes = 0L;
        while (!bufferedRecords.isEmpty() && selected.size() < request.getMaxRecords()) {
            ConsumerRecord<String, String> candidate = bufferedRecords.peekFirst();
            long candidateBytes = recordBytes(candidate);
            if (!selected.isEmpty() && selectedBytes + candidateBytes > request.getMaxBytes()) {
                break;
            }
            bufferedRecords.removeFirst();
            selected.add(candidate);
            selectedBytes += candidateBytes;
        }
        if (selected.isEmpty()) {
            return;
        }

        StreamingCheckpoint checkpoint = checkpointFor(selected);
        List<Record> decoded = new ArrayList<Record>(selected.size());
        long windowStart = System.currentTimeMillis();
        long newestMessage = 0L;
        for (ConsumerRecord<String, String> record : selected) {
            if (record.timestamp() > 0L) {
                windowStart = Math.min(windowStart, record.timestamp());
                newestMessage = Math.max(newestMessage, record.timestamp());
            }
            try {
                decoded.add(parser.parse(record.value()));
                recordsDecoded.incrementAndGet();
            } catch (Exception failure) {
                dirtyRecords.incrementAndGet();
                LOG.warn("Kafka record parse failed at {}-{} offset {}: {}",
                        record.topic(), record.partition(), record.offset(), safeMessage(failure));
            }
        }
        recordsRead.addAndGet(selected.size());
        bytesRead.addAndGet(selectedBytes);
        batchesRead.incrementAndGet();
        lastMessageTimestamp.set(newestMessage == 0L ? System.currentTimeMillis() : newestMessage);

        CheckpointWindow window = new CheckpointWindow(checkpoint);
        inFlight = window;
        Set<TopicPartition> assignment = consumer.assignment();
        if (!assignment.isEmpty()) {
            consumer.pause(assignment);
        }
        StreamingBatch batch = new StreamingBatch(decoded, checkpoint, windowStart, selectedBytes);
        while (!closed.get() && !readyBatches.offer(batch, 100L, TimeUnit.MILLISECONDS)) {
            processCommands(consumer);
        }
    }

    private void heartbeat(Consumer<String, String> consumer) {
        poll(consumer, HEARTBEAT_POLL);
        updateLag(consumer);
    }

    private ConsumerRecords<String, String> poll(Consumer<String, String> consumer, Duration duration) {
        try {
            return consumer.poll(duration);
        } catch (WakeupException wakeup) {
            if (closed.get()) {
                throw wakeup;
            }
            return ConsumerRecords.empty();
        }
    }

    private void processCommands(Consumer<String, String> consumer) {
        ControlCommand command;
        while ((command = commands.poll()) != null) {
            try {
                if (command.type == CommandType.COMMIT) {
                    commitCheckpoint(consumer, command.checkpoint);
                } else if (command.type == CommandType.ABORT) {
                    abortCheckpoint(consumer, command.checkpoint);
                } else {
                    restoreOffsets(consumer, command.state);
                }
                command.completion.complete(null);
            } catch (Throwable failure) {
                command.completion.completeExceptionally(failure);
            }
        }
    }

    private void commitCheckpoint(Consumer<String, String> consumer, StreamingCheckpoint checkpoint) {
        CheckpointWindow active = requireInFlight(checkpoint);
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetMetadata(
                active.checkpoint.getState().get("nextOffsets"));
        try {
            consumer.commitSync(offsets, COMMIT_TIMEOUT);
        } catch (RuntimeException failure) {
            commitFailures.incrementAndGet();
            throw failure;
        }
        commits.incrementAndGet();
        finishInFlight(consumer);
    }

    private void abortCheckpoint(Consumer<String, String> consumer, StreamingCheckpoint checkpoint) {
        CheckpointWindow active = requireInFlight(checkpoint);
        restoreOffsets(consumer, active.checkpoint.getState());
        aborts.incrementAndGet();
        finishInFlight(consumer);
    }

    private void restoreOffsets(Consumer<String, String> consumer, Map<String, Object> state) {
        Object value = state == null ? null : state.get("startOffsets");
        if (value == null && state != null) {
            value = state.get("nextOffsets");
        }
        Map<TopicPartition, Long> offsets = offsets(value);
        Set<TopicPartition> assignment = consumer.assignment();
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            if (assignment.contains(entry.getKey())) {
                consumer.seek(entry.getKey(), entry.getValue());
                pendingRestoreOffsets.remove(entry.getKey());
            } else {
                pendingRestoreOffsets.put(entry.getKey(), entry.getValue());
            }
        }
        bufferedRecords.clear();
    }

    private void applyPendingRestoreOffsets(Consumer<String, String> consumer,
                                            Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            Long offset = pendingRestoreOffsets.remove(partition);
            if (offset != null) {
                consumer.seek(partition, offset);
            }
        }
    }

    private void resetOffsetsOnFirstAssignment(Consumer<String, String> consumer,
                                               Collection<TopicPartition> partitions) {
        if (!resetOffsetOnFirstAssignment || initialOffsetResetApplied || partitions.isEmpty()) {
            return;
        }
        Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(partitions);
        Map<TopicPartition, OffsetAndMetadata> offsets =
                new LinkedHashMap<TopicPartition, OffsetAndMetadata>();
        for (TopicPartition partition : partitions) {
            Long beginning = beginnings.get(partition);
            long offset = beginning == null ? 0L : beginning.longValue();
            consumer.seek(partition, offset);
            offsets.put(partition, new OffsetAndMetadata(offset));
        }
        consumer.commitSync(offsets, COMMIT_TIMEOUT);
        initialOffsetResetApplied = true;
        LOG.info("Kafka streaming consumer offset reset applied for topic {} partitions={}",
                topic, partitions.size());
    }

    private void finishInFlight(Consumer<String, String> consumer) {
        Set<TopicPartition> paused = consumer.paused();
        if (!paused.isEmpty()) {
            consumer.resume(paused);
        }
        inFlight = null;
    }

    private CheckpointWindow requireInFlight(StreamingCheckpoint checkpoint) {
        CheckpointWindow active = inFlight;
        if (active == null) {
            throw new IllegalStateException("Kafka streaming session has no in-flight batch");
        }
        if (checkpoint == null || !active.checkpoint.getBatchId().equals(checkpoint.getBatchId())) {
            throw new IllegalArgumentException("Kafka checkpoint does not match the in-flight batch");
        }
        return active;
    }

    private void executeCommand(ControlCommand command) throws Exception {
        awaitStarted();
        synchronized (commandLifecycleMonitor) {
            rethrowTerminalFailure();
            if (closed.get() || sourceStopped) {
                throw new IllegalStateException("Kafka streaming session is closed");
            }
            commands.put(command);
        }
        wakeup();
        try {
            command.completion.get(COMMIT_TIMEOUT.toMillis() + 5_000L, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new StreamingExecutionException("Kafka streaming control command failed", cause);
        } catch (TimeoutException failure) {
            throw new StreamingExecutionException("Kafka streaming control command timed out", failure);
        }
    }

    private void awaitStarted() throws Exception {
        if (!started.await(START_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            throw new StreamingExecutionException("Kafka streaming source did not start within 30 seconds");
        }
    }

    private void rethrowTerminalFailure() throws Exception {
        Throwable failure = terminalFailure.get();
        if (failure == null) {
            return;
        }
        if (failure instanceof Exception) {
            throw (Exception) failure;
        }
        throw new StreamingExecutionException("Kafka streaming source failed", failure);
    }

    private void updateLag(Consumer<String, String> consumer) {
        long lag = 0L;
        try {
            for (TopicPartition partition : consumer.assignment()) {
                OptionalLong partitionLag = consumer.currentLag(partition);
                if (partitionLag.isPresent()) {
                    lag += Math.max(0L, partitionLag.getAsLong());
                }
            }
            currentLag.set(lag);
            maxLag.accumulateAndGet(lag, Math::max);
        } catch (RuntimeException failure) {
            LOG.debug("Kafka lag snapshot failed for topic {}", topic, failure);
        }
    }

    private Duration sourcePollDuration() {
        return Duration.ofMillis(Math.min(1_000L, Math.max(1L, pollRequest.get().getTimeoutMillis())));
    }

    private void failPendingCommands(Throwable failure) {
        synchronized (commandLifecycleMonitor) {
            failPendingCommandsLocked(failure);
        }
    }

    private void markSourceStopped(Throwable failure) {
        synchronized (commandLifecycleMonitor) {
            sourceStopped = true;
            failPendingCommandsLocked(failure);
        }
    }

    private void failPendingCommandsLocked(Throwable failure) {
        ControlCommand command;
        while ((command = commands.poll()) != null) {
            command.completion.completeExceptionally(failure);
        }
    }

    static StreamingCheckpoint checkpointFor(Collection<ConsumerRecord<String, String>> records) {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("Kafka checkpoint requires at least one record");
        }
        Map<TopicPartition, OffsetRange> ranges = new LinkedHashMap<TopicPartition, OffsetRange>();
        for (ConsumerRecord<String, String> record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            OffsetRange range = ranges.get(partition);
            if (range == null) {
                ranges.put(partition, new OffsetRange(record.offset(), record.offset() + 1L));
            } else {
                range.start = Math.min(range.start, record.offset());
                range.next = Math.max(range.next, record.offset() + 1L);
            }
        }
        List<Map.Entry<TopicPartition, OffsetRange>> sorted =
                new ArrayList<Map.Entry<TopicPartition, OffsetRange>>(ranges.entrySet());
        sorted.sort(Comparator
                .comparing((Map.Entry<TopicPartition, OffsetRange> entry) -> entry.getKey().topic())
                .thenComparingInt(entry -> entry.getKey().partition()));

        Map<String, Long> starts = new LinkedHashMap<String, Long>();
        Map<String, Long> next = new LinkedHashMap<String, Long>();
        StringBuilder canonical = new StringBuilder();
        for (Map.Entry<TopicPartition, OffsetRange> entry : sorted) {
            String key = partitionKey(entry.getKey());
            starts.put(key, entry.getValue().start);
            next.put(key, entry.getValue().next);
            canonical.append(entry.getKey().topic()).append('\u0000')
                    .append(entry.getKey().partition()).append('\u0000')
                    .append(entry.getValue().start).append('\u0000')
                    .append(entry.getValue().next).append('\n');
        }
        String batchId = "kafka-" + sha256(canonical.toString());
        Map<String, Object> state = new LinkedHashMap<String, Object>();
        state.put("source", "kafka");
        state.put("batchId", batchId);
        state.put("startOffsets", starts);
        state.put("nextOffsets", next);
        return new StreamingCheckpoint(batchId, state);
    }

    private static Map<TopicPartition, OffsetAndMetadata> offsetMetadata(Object value) {
        Map<TopicPartition, Long> offsets = offsets(value);
        Map<TopicPartition, OffsetAndMetadata> result =
                new LinkedHashMap<TopicPartition, OffsetAndMetadata>();
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            result.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
        }
        return result;
    }

    private static Map<TopicPartition, Long> offsets(Object value) {
        if (!(value instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("Kafka checkpoint offsets are missing");
        }
        Map<TopicPartition, Long> result = new LinkedHashMap<TopicPartition, Long>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            String key = String.valueOf(entry.getKey());
            int separator = key.lastIndexOf(':');
            if (separator <= 0 || separator == key.length() - 1) {
                throw new IllegalArgumentException("Invalid Kafka checkpoint partition: " + key);
            }
            String partitionTopic = key.substring(0, separator);
            int partition = Integer.parseInt(key.substring(separator + 1));
            Object offset = entry.getValue();
            long offsetValue = offset instanceof Number
                    ? ((Number) offset).longValue() : Long.parseLong(String.valueOf(offset));
            result.put(new TopicPartition(partitionTopic, partition), offsetValue);
        }
        return result;
    }

    private static String partitionKey(TopicPartition partition) {
        return partition.topic() + ":" + partition.partition();
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte item : digest) {
                result.append(String.format("%02x", item & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is not available", failure);
        }
    }

    private static long recordBytes(ConsumerRecord<String, String> record) {
        long keyBytes = record.serializedKeySize() >= 0
                ? record.serializedKeySize() : utf8Length(record.key());
        long valueBytes = record.serializedValueSize() >= 0
                ? record.serializedValueSize() : utf8Length(record.value());
        return Math.max(0L, keyBytes) + Math.max(0L, valueBytes);
    }

    private static long utf8Length(String value) {
        return value == null ? 0L : value.getBytes(StandardCharsets.UTF_8).length;
    }

    private static String sanitize(String value) {
        return value.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private static String stringValue(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }

    private static String safeMessage(Throwable failure) {
        if (failure == null) {
            return "unknown error";
        }
        String message = failure.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return failure.getClass().getSimpleName();
        }
        String normalized = message.replaceAll("[\\r\\n\\t]+", " ").trim();
        normalized = normalized.replaceAll(
                "(?i)(password|passwd|secret|token|sasl\\.jaas\\.config)\\s*[=:]\\s*([^,;\\s]+|\"[^\"]*\")",
                "$1=<redacted>");
        return normalized.length() <= 512 ? normalized : normalized.substring(0, 512) + "...";
    }

    @FunctionalInterface
    interface ConsumerFactory {
        Consumer<String, String> create();
    }

    private enum CommandType {
        COMMIT,
        ABORT,
        RESTORE
    }

    private static final class ControlCommand {
        private final CommandType type;
        private final StreamingCheckpoint checkpoint;
        private final Map<String, Object> state;
        private final CompletableFuture<Void> completion = new CompletableFuture<Void>();

        private ControlCommand(CommandType type,
                               StreamingCheckpoint checkpoint,
                               Map<String, Object> state) {
            this.type = type;
            this.checkpoint = checkpoint;
            this.state = state;
        }

        private static ControlCommand commit(StreamingCheckpoint checkpoint) {
            return new ControlCommand(CommandType.COMMIT, checkpoint, null);
        }

        private static ControlCommand abort(StreamingCheckpoint checkpoint) {
            return new ControlCommand(CommandType.ABORT, checkpoint, null);
        }

        private static ControlCommand restore(Map<String, Object> state) {
            return new ControlCommand(CommandType.RESTORE, null,
                    state == null ? Collections.<String, Object>emptyMap()
                            : new LinkedHashMap<String, Object>(state));
        }
    }

    private static final class CheckpointWindow {
        private final StreamingCheckpoint checkpoint;

        private CheckpointWindow(StreamingCheckpoint checkpoint) {
            this.checkpoint = checkpoint;
        }
    }

    private static final class OffsetRange {
        private long start;
        private long next;

        private OffsetRange(long start, long next) {
            this.start = start;
            this.next = next;
        }
    }
}
