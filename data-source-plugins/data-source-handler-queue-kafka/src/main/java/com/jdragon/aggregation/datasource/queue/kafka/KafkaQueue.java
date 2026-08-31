package com.jdragon.aggregation.datasource.queue.kafka;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.queue.QueueAbstract;
import com.jdragon.aggregation.datasource.scan.QueueCheckpointReadable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class KafkaQueue extends QueueAbstract implements QueueCheckpointReadable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaQueue.class);

    private Properties props;

    private KafkaProducer<String, String> producer;

    private String topic;

    private static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";

    public KafkaQueue() {

    }

    public Set<String> listTopic() throws Exception {
        try (AdminClient adminClient = AdminClient.create(props)) {
            return adminClient.listTopics().names().get();
        }
    }

    @Override
    public boolean checkConnectivity() {
        try {
            listTopic();
        } catch (Exception e) {
            log.error("获取topic list失败", e);
            return false;
        }
        return true;
    }

    /**
     * Validates one model/topic locator without coupling it to the broker
     * connection test. Callers that need object-level validation can use this
     * method after the datasource connection has been established.
     */
    public boolean topicExists(String topicName) throws Exception {
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new IllegalArgumentException("Kafka操作topic不能为空");
        }
        return listTopic().contains(topicName.trim());
    }

    @Override
    public void init() {
        Configuration configuration = getPluginQueueConf();
        // 从 Map 中提取参数
        props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaAuthUtil.copyKafkaClientProperties(props, configuration);
        KafkaAuthUtil.login(props, configuration);
        Set<String> keys = configuration.getKeys();
        for (String key : keys) {
            Object value = readConfigurationValue(configuration, key);
            if (value == null) {
                throw new IllegalArgumentException("Kafka配置项不能为空: " + key);
            }
            if ("topic".equalsIgnoreCase(key)) {
                topic = String.valueOf(value).trim();
            } else if (!isStudioOption(key)) {
                props.put(key, value);
            }
        }

        Object bootstrapServers = props.get(BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrapServers == null || String.valueOf(bootstrapServers).trim().isEmpty()) {
            throw new IllegalArgumentException("Kafka配置项bootstrap.servers不能为空");
        }
    }

    /**
     * Kafka client properties use literal dotted keys (for example
     * bootstrap.servers). Configuration#get treats dots as nested paths, so
     * prefer the literal map lookup and retain support for nested input.
     */
    private Object readConfigurationValue(Configuration configuration, String key) {
        Object literalValue = configuration.getMapValue("", key);
        if (literalValue != null) {
            return literalValue;
        }
        try {
            return configuration.get(key);
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private boolean isStudioOption(String key) {
        if (key == null) {
            return true;
        }
        String normalized = key.trim().toLowerCase(Locale.ROOT);
        return "bootstrapservers".equals(normalized)
                || "brokers".equals(normalized)
                || "username".equals(normalized)
                || "password".equals(normalized)
                || "kerberos".equals(normalized)
                || "principal".equals(normalized)
                || "kerberoskeytabfilepath".equals(normalized)
                || "krb5conf".equals(normalized)
                || "kerberosdomain".equals(normalized)
                || "consumergroup".equals(normalized)
                || "groupid".equals(normalized)
                || "offsetreset".equals(normalized)
                || "resetoffset".equals(normalized)
                || "polltimeoutms".equals(normalized)
                || "batchsize".equals(normalized)
                || "keepreadtime".equals(normalized)
                || "retrypoll".equals(normalized)
                || "parsingrules".equals(normalized)
                || "fielddelimiter".equals(normalized)
                || "columns".equals(normalized)
                || "otherproperties".equals(normalized)
                || normalized.startsWith("otherproperties.");
    }

    @Override
    public void sendMessage(String message) {
        requireTopic();
        if (this.producer == null) {
            this.producer = new KafkaProducer<>(props);
        }
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record);
        producer.flush();
        LOG.debug("Kafka消息已发送: {}", message);
    }

    public void sendMessage(String topic, String key, String message) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Kafka操作topic不能为空");
        }
        if (this.producer == null) {
            this.producer = new KafkaProducer<>(props);
        }
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        producer.send(record);
        producer.flush();
    }

    @Override
    public void receiveMessage(Function<String, Boolean> messageProcessor) {
        receiveMessage(1000L, messageProcessor);
    }

    public void receiveMessage(long pollTimeoutMs, Function<String, Boolean> messageProcessor) {
        requireTopic();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        // 持续消费消息，直到 messageProcessor 返回 false
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(normalizePollTimeout(pollTimeoutMs)));
            AtomicBoolean sign = new AtomicBoolean(true);
            records.forEach(record -> {
                String message = record.value();
                boolean continueConsume = messageProcessor.apply(message);
                if (!continueConsume) {
                    LOG.info("停止消费消息: {}", message);
                    sign.set(false);
                    return;
                }
                LOG.debug("Kafka 消费消息: {}", message);
            });
            if (!records.isEmpty()) {
                kafkaConsumer.commitSync();
            }
            if (!sign.get()) {
                break;
            }
        }
        kafkaConsumer.close();
    }

    public void receiveRecords(Function<ConsumerRecords<String, String>, Boolean> messageProcessor) {
        receiveRecords(1000L, messageProcessor);
    }

    public void receiveRecords(long pollTimeoutMs,
                               Function<ConsumerRecords<String, String>, Boolean> messageProcessor) {
        requireTopic();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(normalizePollTimeout(pollTimeoutMs)));
            Boolean apply = messageProcessor.apply(records);
            // A false callback normally means the current batch was handled
            // and the consumer should stop (for example keepReadTime). Keep
            // that historical commit behavior, but never commit a batch that
            // was abandoned because the worker thread was interrupted before
            // it could process the records.
            if (!records.isEmpty()
                    && (Boolean.TRUE.equals(apply) || !Thread.currentThread().isInterrupted())) {
                kafkaConsumer.commitSync();
            }
            if (!apply) {
                LOG.info("停止消费消息");
                break;
            }
        }
        kafkaConsumer.close();
    }

    public void resetOffset() {
        requireTopic();
        LOG.info("{}重置offset", topic);
        String groupId = props.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Kafka配置项group.id不能为空");
        }

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props)) {
            // Subscribe and poll once so the consumer joins the group and a
            // coordinator is assigned before seeking or committing. Manual
            // assign() does not join the group, which makes commitSync wait
            // for the coordinator until the API timeout.
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            Set<TopicPartition> topicPartitions = Collections.emptySet();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (topicPartitions.isEmpty() && System.nanoTime() < deadline) {
                kafkaConsumer.poll(Duration.ofMillis(1000));
                topicPartitions = kafkaConsumer.assignment();
            }
            if (topicPartitions.isEmpty()) {
                LOG.warn("{}没有可重置的分区", topic);
                return;
            }

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for (TopicPartition topicPartition : topicPartitions) {
                kafkaConsumer.seek(topicPartition, 0L);
                offsets.put(topicPartition, new OffsetAndMetadata(0L));
                LOG.info("分区：{} 重置offset值：0", topicPartition.partition());
            }
            kafkaConsumer.commitSync(offsets, Duration.ofSeconds(30));
        } catch (Exception e) {
            throw new IllegalStateException("Kafka消费组offset重置失败: " + groupId, e);
        }
        LOG.info("{}所有分区重置offset成功", topic);
    }

    private void requireTopic() {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalStateException("Kafka操作topic不能为空，请从模型physicalLocator或任务配置提供topic");
        }
    }

    private long normalizePollTimeout(long pollTimeoutMs) {
        return Math.max(1L, Math.min(60_000L, pollTimeoutMs));
    }

    @Override
    public void destroy() {
        if (producer != null) {
            producer.close();
        }
    }
}
