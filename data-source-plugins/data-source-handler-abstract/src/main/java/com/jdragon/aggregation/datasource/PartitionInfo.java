package com.jdragon.aggregation.datasource;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Setter
@Getter
public class PartitionInfo {

    private Map<String, String> partitionValues; // 分区字段及值

    private long size;

    private Date createdTime;

    private Date lastMetaModifiedTime;

    private Date lastDataModifiedTime;

    public PartitionInfo() {
    }

    public PartitionInfo(Map<String, String> partitionValues) {
        this.partitionValues = partitionValues;
    }

    public PartitionInfo(String spec) {
        if (spec == null) {
            throw new IllegalArgumentException("Argument 'spec' cnnot be null");
        }
        String[] groups = spec.split("[,/]");
        for (String group : groups) {
            String[] kv = group.split("=");
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid partition spec");
            }

            String k = kv[0].trim();
            String v = kv[1].trim().replace("'", "").replace("\"", "");
            if (k.isEmpty() || v.isEmpty()) {
                throw new IllegalArgumentException("Invalid partition spec.");
            }

            partitionValues.put(k, v);
        }
    }

    public String toString() {
        return toString(",");
    }

    public String toString(String delimiter) {
        List<String> entries = new LinkedList<>();
        String[] keys = partitionValues.keySet().toArray(new String[0]);

        for (String key : keys) {
            String entryBuilder = key + "=" +
                    partitionValues.get(key);
            entries.add(entryBuilder);
        }

        return String.join(delimiter, entries);
    }

    public String toWhere() {
        return toString(" and ");
    }
}
