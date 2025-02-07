package com.jdragon.aggregation.writer.kafka;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.datasource.queue.kafka.KafkaAuthUtil;
import com.jdragon.aggregation.datasource.queue.kafka.KafkaQueue;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class KafkaWriter extends Writer.Job {

    private static final Logger logger = LoggerFactory.getLogger(KafkaWriter.class);

    private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");

    private String bootstrapServers;

    private String acks;

    private String retries;

    private String batchSize;


    private String fieldDelimiter;

    private Configuration conf;

    private String topic;

    private Map<String, String> otherProperties;

    private List<ColumnEntry> columnList;

    private List<String> columnArr;

    private final KafkaQueue kafkaQueue = new KafkaQueue();

    @Override
    public void init() {
        this.conf = super.getPluginJobConf();
        bootstrapServers = conf.getString(Key.BOOTSTRAP_SERVERS);
        acks = conf.getUnnecessaryValue(Key.ACK, "0", null);
        retries = conf.getUnnecessaryValue(Key.RETRIES, "0", null);
        batchSize = conf.getUnnecessaryValue(Key.BATCH_SIZE, "16384", null);

        fieldDelimiter = conf.getUnnecessaryValue(Key.FIELD_DELIMITER, "\t", null);
        topic = conf.getString(Key.TOPIC);
        otherProperties = conf.getMap(Key.OTHER_PROPERTIES, new HashMap<>(), String.class);

        //字段名，用于拼接json
        this.columnList = ColumnEntry.getListColumnEntry(conf, Key.COLUMNS);
        this.columnArr = columnList.stream().map(ColumnEntry::getName).collect(Collectors.toList());
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("acks", acks);//这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的保证。
        properties.put("retries", retries);
        properties.put("batch.size", batchSize);
        properties.put("linger.ms", 1);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

    @Override
    public void prepare() {
        Properties properties = getProperties();
        KafkaAuthUtil.login(properties, conf);
        properties.putAll(otherProperties);

        // 初始化kafka queue plugin
        Configuration kafkaConfig = Configuration.newDefault();
        kafkaConfig.set("topic", topic);
        properties.forEach((key, value1) -> kafkaConfig.set(key.toString(), value1));
        kafkaQueue.setPluginQueueConf(kafkaConfig);
        kafkaQueue.init();
        if (conf.getBool(Key.AUTO_CREATE_TOPIC, true)) {
            try (AdminClient adminClient = AdminClient.create(properties)) {
                ListTopicsResult topicsResult = adminClient.listTopics();
                String topic = conf.getNecessaryValue(Key.TOPIC, KafkaWriterErrorCode.REQUIRED_VALUE);
                if (!topicsResult.names().get().contains(topic)) {
                    new NewTopic(
                            topic,
                            Integer.parseInt(conf.getUnnecessaryValue(Key.CREATE_TOPIC_NUM_PARTITION, "1", null)),
                            Short.parseShort(conf.getUnnecessaryValue(Key.CREATE_TOPIC_REPLICATION_FACTOR, "1", null))
                    );
                    List<NewTopic> newTopics = new ArrayList<>();
                    adminClient.createTopics(newTopics);
                }
            } catch (Exception e) {
                throw AggregationException.asException(KafkaWriterErrorCode.CREATE_TOPIC, KafkaWriterErrorCode.REQUIRED_VALUE.getDescription());
            }
        }
    }

    @Override
    public void startWrite(RecordReceiver lineReceiver) {
        logger.info("start to writer kafka");
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {//说明还在读取数据,或者读取的数据没处理完
            //获取一行数据，按照指定分隔符 拼成字符串 发送出去
            if (conf.getUnnecessaryValue(Key.WRITE_TYPE, WriteType.SPLIT.name(), null)
                    .equalsIgnoreCase(WriteType.SPLIT.name())) {
                kafkaQueue.sendMessage(topic, getColumnName(record, columnArr), recordToString(record));
            } else if (conf.getUnnecessaryValue(Key.WRITE_TYPE, WriteType.RAWDATA.name(), null)
                    .equalsIgnoreCase(WriteType.RAWDATA.name())) {
                kafkaQueue.sendMessage(topic, getColumnName(record, columnArr), record.toString());
            } else if (conf.getUnnecessaryValue(Key.WRITE_TYPE, WriteType.JSON.name(), null)
                    .equalsIgnoreCase(WriteType.JSON.name())) {
                kafkaQueue.sendMessage(topic, getColumnName(record, columnArr), recordToJSONSTR(record));
            }
        }
    }


    @Override
    public void destroy() {
        logger.info("kafka ready close");
        kafkaQueue.destroy();
        logger.info("kafka close");
    }

    /**
     * 将数据解析成JSON
     *
     * @param record 记录
     */
    private String recordToJSONSTR(Record record) {
        int recordLength = record.getColumnNumber();
        if (0 == recordLength) {
            return NEWLINE_FLAG;
        }
        Column column;
        JSONObject columnJson = new JSONObject();
        for (int i = 0; i < recordLength; i++) {
            column = record.getColumn(i);
            if (column.getByteSize() == 0) {
                columnJson.put(columnArr.get(i), null);
            } else {
                columnJson.put(columnArr.get(i), column.asString());
            }
        }
        return columnJson.toJSONString();
    }

    /**
     * 数据格式化
     */
    private String recordToString(Record record) {
        int recordLength = record.getColumnNumber();
        if (0 == recordLength) {
            return NEWLINE_FLAG;
        }
        Column column;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < recordLength; i++) {
            column = record.getColumn(i);
            sb.append(column.asString()).append(fieldDelimiter);
        }

        sb.setLength(sb.length() - 1);
        sb.append(NEWLINE_FLAG);

        return sb.toString();
    }

    /**
     * 获取字段名
     *
     * @param record 记录
     */
    private String getColumnName(Record record, List<String> columnArr) {
        int recordLength = record.getColumnNumber();
        if (0 == recordLength) {
            return null;
        }
        StringBuilder columnName = new StringBuilder();
        for (int i = 0; i < recordLength; i++) {
            columnName.append(columnArr.get(i)).append(",");
        }
        columnName.setLength(columnName.length() - 1);
        return columnName.toString();
    }
}
