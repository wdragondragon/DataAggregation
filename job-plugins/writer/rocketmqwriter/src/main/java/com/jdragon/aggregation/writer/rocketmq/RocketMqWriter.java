package com.jdragon.aggregation.writer.rocketmq;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.datasource.queue.rocketmq.RocketQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class RocketMqWriter extends Writer.Job {

    private static final Logger logger = LoggerFactory.getLogger(RocketMqWriter.class);

    private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");

    private String fieldDelimiter;

    private String writeType;

    private Configuration conf;

    private List<ColumnEntry> columnList;

    private List<String> columnArr;

    private final RocketQueue rocketQueue = new RocketQueue();

    @Override
    public void init() {
        this.conf = super.getPluginJobConf();
        fieldDelimiter = conf.getUnnecessaryValue(Key.FIELD_DELIMITER, "\t", null);
        writeType = conf.getString(Key.WRITE_TYPE);

        //字段名，用于拼接json
        this.columnList = ColumnEntry.getListColumnEntry(conf, Key.COLUMNS);
        this.columnArr = columnList.stream().map(ColumnEntry::getName).collect(Collectors.toList());

        // 初始化kafka queue plugin
        Configuration connect = this.getPluginJobConf().getConfiguration("connect");
        rocketQueue.setPluginQueueConf(connect);
        rocketQueue.init();
    }

    @Override
    public void startWrite(RecordReceiver lineReceiver) {
        logger.info("start to writer kafka");
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {//说明还在读取数据,或者读取的数据没处理完
            String sendMsg = "";
            //获取一行数据，按照指定分隔符 拼成字符串 发送出去
            if (WriteType.SPLIT.name().equalsIgnoreCase(writeType)) {
                sendMsg = recordToString(record);
            } else if (WriteType.RAWDATA.name().equalsIgnoreCase(writeType)) {
                sendMsg = record.toString();
            } else if (WriteType.JSON.name().equalsIgnoreCase(writeType)) {
                sendMsg = recordToJSONSTR(record);
            } else {
                throw AggregationException.asException("错误的发送格式:" + writeType);
            }
            try {
                rocketQueue.sendMessage(sendMsg);
            } catch (Exception e) {
                logger.error("rocket发送消息失败：{}", e.getMessage(), e);
            }
        }
    }


    @Override
    public void destroy() {
        logger.info("kafka ready close");
        rocketQueue.destroy();
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
}
