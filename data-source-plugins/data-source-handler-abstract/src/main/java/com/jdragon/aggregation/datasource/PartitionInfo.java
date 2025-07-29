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

    public PartitionInfo() {}

    public PartitionInfo(Map<String, String> partitionValues) {
        this.partitionValues = partitionValues;
    }

    public String toString() {
        String delimiter = ",";

        List<String> entries = new LinkedList<>();
        String[] keys = partitionValues.keySet().toArray(new String[0]);

        for (String key : keys) {
            String entryBuilder = key + "=" +
                    partitionValues.get(key);
            entries.add(entryBuilder);
        }

        return String.join(delimiter, entries);
    }
}
