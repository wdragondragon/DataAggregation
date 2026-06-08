package com.jdragon.aggregation.datamock.odps;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.odps.OdpsSourcePlugin;
import com.jdragon.aggregation.odps.OdpsReader;
import com.jdragon.aggregation.writer.kafka.KafkaWriter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OdpsToKafkaJobContainerIntegrationTest {

    private static final String ENABLE_FLAG = "runOdpsKafkaJobContainerIT";
    private static final String DEFAULT_ODPS_CONFIG = "C:\\Users\\jdrag\\Desktop\\odpscmd\\conf\\odps_config.ini";
    private static final String DEFAULT_KAFKA_BOOTSTRAP = "192.168.188.129:9092";
    private static final String CREATE_TOPIC_FLAG = "odps.kafka.createTopic";
    private static final List<String> COLUMNS = List.of("id", "customer_name", "amount", "biz_date");
    private static final List<ExpectedRow> EXPECTED_ROWS = List.of(
            new ExpectedRow("1", "order_alpha", "12.5", "2026-06-04"),
            new ExpectedRow("2", "order_beta", "24.75", "2026-06-05"),
            new ExpectedRow("3", "order_gamma", "99.01", "2026-06-06")
    );

    private final OdpsSourcePlugin odpsPlugin = new OdpsSourcePlugin();
    private BaseDataSourceDTO odpsDataSource;
    private String kafkaBootstrap;
    private String tableName;
    private String topicName;
    private boolean cleanupArtifacts;

    @BeforeClass
    public static void assumeEnabled() {
        boolean enabled = Boolean.getBoolean(ENABLE_FLAG)
                || isTargetedBySurefire(OdpsToKafkaJobContainerIntegrationTest.class);
        Assume.assumeTrue("set -D" + ENABLE_FLAG + "=true to run ODPS to Kafka JobContainer IT", enabled);
    }

    @After
    public void cleanup() {
        if (!cleanupArtifacts) {
            return;
        }
        if (odpsDataSource != null && hasText(tableName)) {
            try {
                odpsPlugin.executeUpdate(odpsDataSource, "DROP TABLE IF EXISTS " + tableName + ";");
            } catch (Exception ignored) {
            }
        }
        if (hasText(kafkaBootstrap) && hasText(topicName)) {
            try (AdminClient adminClient = AdminClient.create(adminProperties(kafkaBootstrap))) {
                adminClient.deleteTopics(Collections.singleton(topicName)).all().get(30, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void shouldCollectOdpsRowsIntoKafkaThroughJobContainer() throws Exception {
        odpsDataSource = loadOdpsDataSource();
        kafkaBootstrap = System.getProperty("kafka.bootstrapServers", DEFAULT_KAFKA_BOOTSTRAP);
        cleanupArtifacts = Boolean.getBoolean("odps.kafka.cleanupArtifacts");

        String suffix = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now());
        tableName = System.getProperty("odps.kafka.table", "codex_odps_kafka_" + suffix);
        topicName = System.getProperty("odps.kafka.topic", "codex_odps_to_kafka_" + suffix);
        System.out.println("ODPS to Kafka JobContainer IT table=" + tableName + ", topic=" + topicName);

        recreateOdpsTable();
        ensureKafkaTopic();

        JobContainer container = new JobContainer(Configuration.from(buildJobConfig()));
        container.addConsumerPlugin(PluginType.READER, new OdpsReader());
        container.addConsumerPlugin(PluginType.WRITER, new KafkaWriter());
        container.start();

        List<String> messages = consumeMessages(EXPECTED_ROWS.size(), Duration.ofSeconds(60));
        Map<String, JSONObject> rowsById = indexMessagesById(messages);

        assertEquals("Kafka should receive every ODPS row", EXPECTED_ROWS.size(), rowsById.size());
        for (ExpectedRow row : EXPECTED_ROWS) {
            JSONObject actual = rowsById.get(row.id);
            assertTrue("Kafka message for id " + row.id + " should exist. messages=" + messages, actual != null);
            assertEquals(row.customerName, actual.getString("customer_name"));
            assertEquals(row.amount, actual.getString("amount"));
            assertEquals(row.bizDate, actual.getString("biz_date"));
        }
    }

    private BaseDataSourceDTO loadOdpsDataSource() {
        Path configPath = Paths.get(System.getProperty("odps.config", DEFAULT_ODPS_CONFIG));
        Assume.assumeTrue("ODPS config file not found: " + configPath, Files.exists(configPath));

        Map<String, String> config = readIniConfig(configPath);
        BaseDataSourceDTO sourceDTO = new BaseDataSourceDTO();
        sourceDTO.setType("odps");
        sourceDTO.setHost(required(config, "end_point"));
        sourceDTO.setDatabase(required(config, "project_name"));
        sourceDTO.setUserName(required(config, "access_id"));
        sourceDTO.setPassword(required(config, "access_key"));

        Map<String, String> extraParams = new LinkedHashMap<>();
        putIfPresent(extraParams, "tunnelEndpoint", config.get("tunnel_endpoint"));
        putIfPresent(extraParams, "projectEnv", config.get("project_env"));
        putIfPresent(extraParams, "regionId", config.get("region_id"));
        putIfPresent(extraParams, "dataworksEndpoint", config.get("dataworks_endpoint"));
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("set.") && hasText(entry.getValue())) {
                extraParams.put(key.substring("set.".length()), entry.getValue());
            }
        }
        sourceDTO.setExtraParams(extraParams);
        return sourceDTO;
    }

    private void recreateOdpsTable() {
        odpsPlugin.executeUpdate(odpsDataSource,
                "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                        + "id BIGINT,"
                        + "customer_name STRING,"
                        + "amount DOUBLE,"
                        + "biz_date STRING"
                        + ")"
                        + ";"
        );
        for (ExpectedRow row : EXPECTED_ROWS) {
            odpsPlugin.executeUpdate(odpsDataSource,
                    "INSERT INTO TABLE " + tableName + " VALUES ("
                            + row.id + ","
                            + quoteSql(row.customerName) + ","
                            + row.amount + ","
                            + quoteSql(row.bizDate)
                            + ")"
                            + ";"
            );
        }
    }

    private void ensureKafkaTopic() throws Exception {
        Properties properties = adminProperties(kafkaBootstrap);
        try (AdminClient adminClient = AdminClient.create(properties)) {
            if (!adminClient.listTopics().names().get(30, TimeUnit.SECONDS).contains(topicName)) {
                if (!Boolean.parseBoolean(System.getProperty(CREATE_TOPIC_FLAG, "true"))) {
                    throw new IllegalStateException("Kafka topic does not exist: " + topicName
                            + ". Create it first, or remove -D" + CREATE_TOPIC_FLAG + "=false.");
                }
                adminClient.createTopics(Collections.singleton(new NewTopic(topicName, 1, (short) 1)))
                        .all()
                        .get(30, TimeUnit.SECONDS);
            }
        }
    }

    private Map<String, Object> buildJobConfig() {
        Map<String, Object> readerConfig = new LinkedHashMap<>();
        readerConfig.put("connect", buildConnectConfig());
        readerConfig.put("table", tableName);
        readerConfig.put("columns", COLUMNS);

        Map<String, Object> reader = new LinkedHashMap<>();
        reader.put("type", "odps");
        reader.put("config", readerConfig);

        Map<String, Object> writerConfig = new LinkedHashMap<>();
        writerConfig.put("bootstrapServers", kafkaBootstrap);
        writerConfig.put("topic", topicName);
        writerConfig.put("ack", "1");
        writerConfig.put("retries", "0");
        writerConfig.put("batchSize", "16384");
        writerConfig.put("fieldDelimiter", ",");
        writerConfig.put("autoCreateTopic", false);
        writerConfig.put("writeType", "json");
        writerConfig.put("otherProperties", new HashMap<String, String>());
        writerConfig.put("columns", buildWriterColumns());

        Map<String, Object> writer = new LinkedHashMap<>();
        writer.put("type", "kafka");
        writer.put("config", writerConfig);

        Map<String, Object> job = new LinkedHashMap<>();
        job.put("reader", reader);
        job.put("writer", writer);
        job.put("jobId", System.currentTimeMillis());
        return job;
    }

    private Map<String, Object> buildConnectConfig() {
        Map<String, Object> connect = new LinkedHashMap<>();
        connect.put("type", "odps");
        connect.put("host", odpsDataSource.getHost());
        connect.put("database", odpsDataSource.getDatabase());
        connect.put("userName", odpsDataSource.getUserName());
        connect.put("password", odpsDataSource.getPassword());
        connect.put("extraParams", new LinkedHashMap<>(odpsDataSource.getExtraParams()));
        return connect;
    }

    private List<Map<String, Object>> buildWriterColumns() {
        List<Map<String, Object>> columns = new ArrayList<>();
        for (int i = 0; i < COLUMNS.size(); i++) {
            Map<String, Object> column = new LinkedHashMap<>();
            column.put("index", i);
            column.put("name", COLUMNS.get(i));
            columns.add(column);
        }
        return columns;
    }

    private List<String> consumeMessages(int expectedCount, Duration timeout) {
        Properties properties = consumerProperties(kafkaBootstrap);
        List<String> messages = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton(topicName));
            while (messages.size() < expectedCount && System.nanoTime() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    messages.add(record.value());
                }
            }
        }
        return messages;
    }

    private Map<String, JSONObject> indexMessagesById(Collection<String> messages) {
        Map<String, JSONObject> rowsById = new LinkedHashMap<>();
        for (String message : messages) {
            JSONObject json = JSON.parseObject(message);
            rowsById.put(json.getString("id"), json);
        }
        return rowsById;
    }

    private static Properties adminProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("request.timeout.ms", "10000");
        properties.put("default.api.timeout.ms", "30000");
        return properties;
    }

    private static Properties consumerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", "codex_odps_kafka_it_" + System.currentTimeMillis());
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("max.poll.records", "100");
        return properties;
    }

    private static Map<String, String> readIniConfig(Path configPath) {
        Map<String, String> config = new LinkedHashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.length() == 0 || trimmed.startsWith("#") || trimmed.startsWith(";")) {
                    continue;
                }
                int splitIndex = trimmed.indexOf('=');
                if (splitIndex <= 0) {
                    continue;
                }
                String key = trimmed.substring(0, splitIndex).trim();
                String value = trimQuotes(trimmed.substring(splitIndex + 1).trim());
                if (key.length() > 0) {
                    config.put(key, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Read ODPS config file failed: " + configPath, e);
        }
        return config;
    }

    private static String required(Map<String, String> config, String key) {
        String value = config.get(key);
        if (!hasText(value)) {
            throw new IllegalArgumentException("Missing required ODPS config item: " + key);
        }
        return value;
    }

    private static void putIfPresent(Map<String, String> target, String key, String value) {
        if (hasText(value)) {
            target.put(key, value);
        }
    }

    private static String quoteSql(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static String trimQuotes(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return value.substring(1, value.length() - 1);
            }
        }
        return value;
    }

    private static boolean hasText(String value) {
        return value != null && value.trim().length() > 0;
    }

    private static boolean isTargetedBySurefire(Class<?> testClass) {
        String rawPattern = System.getProperty("test");
        if (!hasText(rawPattern)) {
            return false;
        }
        String[] selectors = rawPattern.split(",");
        for (String selector : selectors) {
            String candidate = selector == null ? "" : selector.trim();
            if (candidate.isEmpty()) {
                continue;
            }
            int methodSeparator = candidate.indexOf('#');
            if (methodSeparator >= 0) {
                candidate = candidate.substring(0, methodSeparator).trim();
            }
            if (candidate.equals(testClass.getSimpleName())
                    || candidate.equals(testClass.getName())
                    || wildcardMatches(candidate, testClass.getSimpleName())
                    || wildcardMatches(candidate, testClass.getName())) {
                return true;
            }
        }
        return false;
    }

    private static boolean wildcardMatches(String selector, String className) {
        if (selector.indexOf('*') < 0 && selector.indexOf('?') < 0) {
            return false;
        }
        String regex = selector
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return Pattern.matches(regex, className);
    }

    private static final class ExpectedRow {
        private final String id;
        private final String customerName;
        private final String amount;
        private final String bizDate;

        private ExpectedRow(String id, String customerName, String amount, String bizDate) {
            this.id = id;
            this.customerName = customerName;
            this.amount = amount;
            this.bizDate = bizDate;
        }
    }
}
