package com.jdragon.aggregation.datasource.queue.kafka;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.queue.QueueAbstract;
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

public class KafkaQueue extends QueueAbstract {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaQueue.class);

    private Properties props;

    private KafkaProducer<String, String> producer;

    private String topic;

    public KafkaQueue() {

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
        Set<String> keys = configuration.getKeys();
        for (String key : keys) {
            Object value = configuration.get(key);
            if (Objects.equals(key, "topic")) {
                topic = (String) value;
            } else {
                props.put(key, value);
            }
        }

    }

    @Override
    public void sendMessage(String message) {
        if (this.producer == null) {
            this.producer = new KafkaProducer<>(props);
        }
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record);
        producer.flush();
        LOG.debug("Kafka消息已发送: {}", message);
    }

    public void sendMessage(String topic, String key, String message) {
        if (this.producer == null) {
            this.producer = new KafkaProducer<>(props);
        }
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        producer.send(record);
        producer.flush();
    }

    @Override
    public void receiveMessage(Function<String, Boolean> messageProcessor) {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        // 持续消费消息，直到 messageProcessor 返回 false
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
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
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            Boolean apply = messageProcessor.apply(records);
            if (!records.isEmpty()) {
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
        LOG.info("{}重置offset", topic);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
        for (PartitionInfo partitionInfo : partitionInfos) {
            int partition = partitionInfo.partition();
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            kafkaConsumer.assign(Collections.singletonList(topicPartition));
            long position = kafkaConsumer.position(topicPartition);
            LOG.info("分区：{} 原offset值：{}", partition, position);
        }

        for (PartitionInfo partitionInfo : partitionInfos) {
            TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0L);
            offsets.put(topicPartition, offsetAndMetadata);
        }
        kafkaConsumer.commitSync(offsets);
        kafkaConsumer.close();
        LOG.info("{}所有分区重置offset成功", topic);
    }

    @Override
    public void destroy() {
        if (producer != null) {
            producer.close();
        }
    }
}
