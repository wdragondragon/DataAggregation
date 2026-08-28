package com.jdragon.aggregation.kafka;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.queue.kafka.KafkaAuthUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

final class KafkaReaderOptions {

    private final Configuration configuration;
    private final String bootstrapServers;
    private final String groupId;
    private final String topic;
    private final String offsetReset;
    private final int batchSize;
    private final boolean resetOffset;
    private final long keepReadTime;
    private final long retryPoll;
    private final List<ColumnEntry> columns;
    private final String parsingRules;
    private final String fieldDelimiter;
    private final Map<String, String> otherProperties;

    private KafkaReaderOptions(Configuration configuration) {
        this.configuration = configuration.clone();
        this.bootstrapServers = required(configuration.getString(Key.BOOTSTRAP_SERVERS), Key.BOOTSTRAP_SERVERS);
        this.groupId = required(configuration.getString(Key.GROUP_ID), Key.GROUP_ID);
        this.topic = required(configuration.getString(Key.TOPIC), Key.TOPIC);
        this.offsetReset = defaultString(configuration.getString(Key.OFFSET_RESET), "latest");
        this.batchSize = Math.max(1, configuration.getInt(Key.BATCH_SIZE, 500));
        this.resetOffset = configuration.getBool(Key.RESET_OFFSET, false);
        this.keepReadTime = configuration.getLong(Key.KEEP_READ_TIME, 60 * 60 * 1000L);
        this.retryPoll = configuration.getLong(Key.RETRY_POLL, 0L);
        this.columns = Collections.unmodifiableList(ColumnEntry.getListColumnEntry(configuration, Key.COLUMN));
        this.parsingRules = defaultString(configuration.getString(Key.PARSING_RULES), "json");
        this.fieldDelimiter = configuration.getString(Key.SPLIT);
        this.otherProperties = Collections.unmodifiableMap(new LinkedHashMap<String, String>(
                configuration.getMap(Key.OTHER_PROPERTIES, new LinkedHashMap<String, String>(), String.class)));
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Kafka reader columns must not be empty");
        }
    }

    static KafkaReaderOptions from(Configuration configuration) {
        if (configuration == null) {
            throw new IllegalArgumentException("Kafka reader configuration must not be null");
        }
        return new KafkaReaderOptions(configuration);
    }

    Properties consumerProperties() {
        Properties properties = new Properties();
        KafkaAuthUtil.login(properties, configuration);
        properties.putAll(otherProperties);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(batchSize));
        properties.putIfAbsent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }

    String getGroupId() {
        return groupId;
    }

    String getTopic() {
        return topic;
    }

    int getBatchSize() {
        return batchSize;
    }

    boolean isResetOffset() {
        return resetOffset;
    }

    long getKeepReadTime() {
        return keepReadTime;
    }

    long getRetryPoll() {
        return retryPoll;
    }

    KafkaRecordParser recordParser() {
        return new KafkaRecordParser(parsingRules, fieldDelimiter, columns);
    }

    private String required(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Kafka reader " + name + " must not be blank");
        }
        return value.trim();
    }

    private String defaultString(String value, String defaultValue) {
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }
}
