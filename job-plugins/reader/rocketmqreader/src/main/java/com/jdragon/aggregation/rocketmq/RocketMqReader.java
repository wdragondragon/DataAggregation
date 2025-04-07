package com.jdragon.aggregation.rocketmq;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.util.FastJsonMemory;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.datasource.queue.rocketmq.RocketQueue;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class RocketMqReader extends Reader.Job {
    private static final Logger LOG = LoggerFactory.getLogger(RocketMqReader.class);

    private Configuration configuration;

    //kafka消息的分隔符
    private String split;
    //解析规则
    private String parsingRules;

    //kafka中的数据一共有多少个字段
    private int count;

    private List<ColumnEntry> columnList;

    private long keepReadTime;

    private long retryPoll;

    private final RocketQueue rocketQueue = new RocketQueue();

    @Override
    public void init() {
        configuration = this.getPluginJobConf();
        split = configuration.getString(Key.SPLIT);
        columnList = ColumnEntry.getListColumnEntry(configuration, Key.COLUMN);
        count = columnList.size();
        parsingRules = configuration.getString(Key.PARSING_RULES);

        keepReadTime = configuration.getLong(Key.KEEP_READ_TIME, 60 * 60 * 1000L);
        retryPoll = configuration.getInt(Key.RETRY_POLL, 0);

        Configuration connectConfig = configuration.getConfiguration("connect");
        rocketQueue.setPluginQueueConf(connectConfig);
        rocketQueue.init();
    }

    @Override
    public void startRead(RecordSender recordSender) {
        AtomicLong dataCount = new AtomicLong(0);
        AtomicInteger retry = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();
        try {
            rocketQueue.receiveRecords(records -> {
                if (Thread.currentThread().isInterrupted()) {
                    return false;
                }
                if (retryPoll > 0 && retry.get() > retryPoll) {
                    return false;
                }
                for (MessageExt record : records) {
                    String value = record.toString();
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
                    recordSender.flush();
                } else {
                    retry.set(0);
                }
                long endTime = System.currentTimeMillis();
                if (keepReadTime > 0 && endTime - startTime > keepReadTime) {
                    LOG.info("kafka read over {}s, break", keepReadTime);
                    return false;
                }
                return true;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void post() {
        LOG.info("rocketQueue ready close");
        rocketQueue.destroy();
        LOG.info("rocketQueue close");
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
}
