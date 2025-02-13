package com.jdragon.aggregation.kafka;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.util.FastJsonMemory;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.datasource.queue.kafka.KafkaAuthUtil;
import com.jdragon.aggregation.datasource.queue.kafka.KafkaQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class KafkaReader extends Reader.Job {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReader.class);

    private Configuration configuration;

    //kafka消息的分隔符
    private String split;
    //解析规则
    private String parsingRules;
    //kafka address
    private String bootstrapServers;
    //kafka groupid
    private String groupId;
    //kafkatopic
    private String kafkaTopic;
    //kafka中的数据一共有多少个字段
    private int count;

    private String offsetReset;

    private List<ColumnEntry> columnList;

    private Integer batchSize;

    private boolean resetOffset;

    private long keepReadTime;

    private Map<String, String> otherProperties;

    private final KafkaQueue kafkaQueue = new KafkaQueue();

    private int runCount;

    @Override
    public void init() {
        runCount = getJobPointReporter().get("count", 1);
        LOG.info("kafka任务运行次数：{}", runCount);
        configuration = super.getPluginJobConf();
        split = configuration.getString(Key.SPLIT);
        bootstrapServers = configuration.getString(Key.BOOTSTRAP_SERVERS);
        groupId = configuration.getString(Key.GROUP_ID);
        kafkaTopic = configuration.getString(Key.TOPIC);
        parsingRules = configuration.getString(Key.PARSING_RULES);
        offsetReset = configuration.getString(Key.OFFSET_RESET);
        columnList = ColumnEntry.getListColumnEntry(configuration, Key.COLUMN);
        count = columnList.size();
        batchSize = configuration.getInt(Key.BATCH_SIZE, 500);
        otherProperties = configuration.getMap(Key.OTHER_PROPERTIES, new HashMap<>(), String.class);
        resetOffset = configuration.getBool(Key.RESET_OFFSET, false);
        keepReadTime = configuration.getLong(Key.KEEP_READ_TIME, 60 * 60 * 1000L);

        Properties properties = getProperties();
        KafkaAuthUtil.login(properties, configuration);
        properties.putAll(otherProperties);

        // 初始化kafka queue plugin
        Configuration kafkaConfig = Configuration.newDefault();
        kafkaConfig.set("topic", kafkaTopic);
        properties.forEach((key, value1) -> kafkaConfig.set(key.toString(), value1));
        kafkaQueue.setPluginQueueConf(kafkaConfig);
        kafkaQueue.init();
        if (resetOffset) {
            kafkaQueue.resetOffset();
        }
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", groupId);// 消费者组id
        properties.put("enable.auto.commit", "false");//提交
        properties.put("auto.offset.reset", offsetReset);
        properties.put("max.poll.records", batchSize);
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//key 序列化类
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//value序列化类
        return properties;
    }

    @Override
    public void startRead(RecordSender recordSender) {
        AtomicLong dataCount = new AtomicLong(0);
        AtomicInteger retry = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();
        kafkaQueue.receiveRecords(records -> {
            if (retry.get() > 5) {
                return false;
            }
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                try {
                    if (dataCount.incrementAndGet() < 5) {
                        LOG.info("打印第{}条数据：{}", dataCount.get(), value);
                    }
                    Record oneRecord = buildOneRecord(recordSender, value);
                    //如果返回值不等于null表示不是异常消息。
                    if (oneRecord != null) {
                        recordSender.sendToWriter(oneRecord);
                    }
                } catch (Exception e) {
                    LOG.error("解析数据异常，数据详情：{}", value, e);
                    super.getTaskPluginCollector().collectDirtyRecord(recordSender.createRecord(), e);
                }
            }
            if (records.isEmpty()) {
                LOG.info("read records is empty, try read [{}] times", retry.incrementAndGet());
            } else {
                retry.set(0);
            }
            long endTime = System.currentTimeMillis();
            if (endTime - startTime > keepReadTime) {
                LOG.info("kafka read over {}s, break", keepReadTime);
                return false;
            }
            return true;
        });
    }

    @Override
    public void post() {
        getJobPointReporter().put("count", runCount + 1);
    }

    private Record buildOneRecord(RecordSender recordSender, String value) throws Exception {
        Record record = null;
        switch (parsingRules) {
            case "json":
                record = parseJson(value, recordSender);
                break;
            case "split":
                record = parseSplit(value, recordSender);
                break;
        }
        return record;
    }

    private Record parseSplit(String value, RecordSender recordSender) throws Exception {
        Record record = recordSender.createRecord();
        String[] splits = value.split(this.split);
        if (splits.length != count) {
            throw new Exception("解析数据与字段数量不相符:" + value);
        }
        parseOrders(Arrays.asList(splits), record);
        return record;
    }

    private void parseOrders(List<String> datas, Record record) {
        //writerOrder
        List<Integer> orders = this.columnList.stream().map(ColumnEntry::getIndex).collect(Collectors.toList());
        for (Integer order : orders) {
            String data = datas.get(order);
            record.addColumn(new StringColumn(data));
        }
    }

    private Record parseJson(String value, RecordSender recordSender) throws Exception {
        FastJsonMemory fastJsonMemory = new FastJsonMemory(value);
        Record record = recordSender.createRecord();
        List<String> columns = this.columnList.stream().map(ColumnEntry::getName).collect(Collectors.toList());
        List<String> datas = new ArrayList<>();
        for (String column : columns) {
            Object o = fastJsonMemory.get(column.trim());
            if (o == null) {
                datas.add(null);
            } else {
                datas.add(o.toString());
            }
        }
        if (datas.size() != count) {
            throw new Exception("解析数据与字段数量不相符:" + value);
        }
        for (String data : datas) {
            record.addColumn(new StringColumn(data));
        }
        return record;
    }

    @Override
    public void destroy() {
        LOG.info("kafka ready close");
        kafkaQueue.destroy();
        LOG.info("kafka close");
    }
}
