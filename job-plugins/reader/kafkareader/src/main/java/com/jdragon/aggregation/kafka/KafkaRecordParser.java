package com.jdragon.aggregation.kafka;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.FastJsonMemory;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class KafkaRecordParser {

    private final String parsingRules;
    private final String fieldDelimiter;
    private final List<ColumnEntry> columns;

    KafkaRecordParser(String parsingRules, String fieldDelimiter, List<ColumnEntry> columns) {
        this.parsingRules = parsingRules;
        this.fieldDelimiter = fieldDelimiter;
        this.columns = new ArrayList<ColumnEntry>(columns);
        if (!"json".equals(parsingRules) && !"split".equals(parsingRules)) {
            throw new IllegalArgumentException("Unsupported Kafka parsingRules: " + parsingRules);
        }
        if ("split".equals(parsingRules)
                && (fieldDelimiter == null || fieldDelimiter.isEmpty())) {
            throw new IllegalArgumentException("Kafka split parser requires fieldDelimiter");
        }
    }

    Record parse(String value) throws Exception {
        if (value == null) {
            throw new IllegalArgumentException("Kafka message value must not be null");
        }
        return "json".equals(parsingRules) ? parseJson(value) : parseSplit(value);
    }

    private Record parseSplit(String value) throws Exception {
        String[] values = value.split(fieldDelimiter, -1);
        if (values.length != columns.size()) {
            throw new Exception("Kafka split field count " + values.length
                    + " does not match configured columns " + columns.size());
        }
        DefaultRecord record = new DefaultRecord();
        List<String> data = Arrays.asList(values);
        for (ColumnEntry column : columns) {
            Integer index = column.getIndex();
            if (index == null || index < 0 || index >= data.size()) {
                throw new Exception("Kafka column index is out of range: " + index);
            }
            record.addColumn(new StringColumn(data.get(index)));
        }
        return record;
    }

    private Record parseJson(String value) {
        try {
            FastJsonMemory json = new FastJsonMemory(value);
            DefaultRecord record = new DefaultRecord();
            for (ColumnEntry column : columns) {
                String name = column.getName();
                if (name == null || name.trim().isEmpty()) {
                    throw new IllegalArgumentException("Kafka JSON column name must not be blank");
                }
                Object data = json.get(name.trim());
                record.addColumn(new StringColumn(data == null ? null : data.toString()));
            }
            return record;
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException("Kafka JSON message could not be parsed", failure);
        }
    }
}
