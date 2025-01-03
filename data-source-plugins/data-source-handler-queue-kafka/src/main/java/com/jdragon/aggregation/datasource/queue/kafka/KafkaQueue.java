package com.jdragon.aggregation.datasource.queue.kafka;

import com.jdragon.aggregation.datasource.queue.QueueAbstract;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaQueue extends QueueAbstract {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaQueue.class);


    private KafkaProducer<String, String> producer;

    private KafkaConsumer<String, String> consumer;

    private String topic;

    public KafkaQueue() {

    }

    @Override
    public void init(Map<String, Object> config) {
        super.configParams = config;
        // 从 Map 中提取参数
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (Objects.equals(key, "topic")) {
                topic = (String) value;
            } else {
                props.put(key, value);
            }
        }
        this.producer = new KafkaProducer<>(props);
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void sendMessage(String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record);
        LOG.info("Kafka消息已发送: {}", message);
    }

    @Override
    public void receiveMessage(Function<String, Boolean> messageProcessor) {
        // 持续消费消息，直到 messageProcessor 返回 false
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            AtomicBoolean sign = new AtomicBoolean(true);
            records.forEach(record -> {
                String message = record.value();
                boolean continueConsume = messageProcessor.apply(message);
                if (!continueConsume) {
                    LOG.info("停止消费消息: {}", message);
                    sign.set(false);
                    return;
                }
                LOG.info("Kafka 消费消息: {}", message);
            });
            if (!sign.get()) {
                break;
            }
        }
    }


    @Override
    public void close() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
        if (producer != null) {
            producer.close();
        }
    }
}
