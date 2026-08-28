package com.jdragon.aggregation.kafka;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.streaming.job.StreamingSession;
import com.jdragon.aggregation.core.streaming.job.StreamingSource;
import com.jdragon.aggregation.core.streaming.job.StreamingSourceContext;
import com.jdragon.aggregation.datasource.queue.kafka.KafkaQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaReader extends Reader.Job implements StreamingSource {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaReader.class);

    private final KafkaQueue kafkaQueue = new KafkaQueue();
    private KafkaReaderOptions options;
    private KafkaRecordParser parser;
    private int runCount;

    @Override
    public void init() {
        runCount = getJobPointReporter().get("count", 1);
        LOG.info("Kafka reader run count: {}", runCount);
        options = KafkaReaderOptions.from(super.getPluginJobConf());
        parser = options.recordParser();

        Configuration kafkaConfig = Configuration.newDefault();
        kafkaConfig.set("topic", options.getTopic());
        Properties properties = options.consumerProperties();
        properties.forEach((key, value) -> kafkaConfig.set(key.toString(), value));
        kafkaQueue.setPluginQueueConf(kafkaConfig);
        kafkaQueue.init();
        if (options.isResetOffset()) {
            kafkaQueue.resetOffset();
        }
    }

    @Override
    public void startRead(RecordSender recordSender) {
        if (options == null || parser == null) {
            throw new IllegalStateException("Kafka reader must be initialized before startRead");
        }
        AtomicLong dataCount = new AtomicLong();
        AtomicInteger emptyPollCount = new AtomicInteger();
        long startTime = System.currentTimeMillis();
        try {
            kafkaQueue.receiveRecords(records -> {
                if (Thread.currentThread().isInterrupted()) {
                    return false;
                }
                if (options.getRetryPoll() > 0 && emptyPollCount.get() > options.getRetryPoll()) {
                    return false;
                }
                for (ConsumerRecord<String, String> consumerRecord : records) {
                    long currentCount = dataCount.incrementAndGet();
                    if (currentCount <= 5L) {
                        LOG.debug("Kafka reader received record {} at {}-{} offset {}",
                                currentCount, consumerRecord.topic(), consumerRecord.partition(),
                                consumerRecord.offset());
                    }
                    try {
                        Record record = parser.parse(consumerRecord.value());
                        recordSender.sendToWriter(record);
                    } catch (Exception failure) {
                        LOG.warn("Kafka record parse failed at {}-{} offset {}: {}",
                                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                                safeMessage(failure));
                        super.getTaskPluginCollector().collectDirtyRecord(recordSender.createRecord(), failure);
                    }
                }
                if (records.isEmpty()) {
                    int polls = emptyPollCount.incrementAndGet();
                    LOG.debug("Kafka reader empty poll count: {}", polls);
                    recordSender.flush();
                } else {
                    emptyPollCount.set(0);
                }
                if (options.getKeepReadTime() > 0
                        && System.currentTimeMillis() - startTime > options.getKeepReadTime()) {
                    LOG.info("Kafka reader reached keepReadTimeMs={}, stopping batch read",
                            options.getKeepReadTime());
                    return false;
                }
                return true;
            });
        } catch (InterruptException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public StreamingSession open(StreamingSourceContext context) {
        if (context == null) {
            throw new IllegalArgumentException("Kafka streaming source context must not be null");
        }
        KafkaReaderOptions streamingOptions = KafkaReaderOptions.from(context.getReaderConfiguration());
        return new KafkaStreamingSession(streamingOptions.consumerProperties(),
                streamingOptions.getTopic(), streamingOptions.recordParser(),
                streamingOptions.isResetOffset());
    }

    @Override
    public void post() {
        getJobPointReporter().put("count", runCount + 1);
    }

    @Override
    public void destroy() {
        LOG.info("Kafka reader closing");
        kafkaQueue.destroy();
        LOG.info("Kafka reader closed");
    }

    private static String safeMessage(Throwable failure) {
        if (failure == null) {
            return "unknown error";
        }
        String message = failure.getMessage();
        return message == null || message.trim().isEmpty()
                ? failure.getClass().getSimpleName() : message;
    }
}
