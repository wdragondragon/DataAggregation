package com.jdragon.aggregation.core.sortmerge;

import com.jdragon.aggregation.core.streaming.PartitionedSpillStore;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;

public class OverflowBucketStore implements AutoCloseable {

    @Getter
    private final PartitionedSpillStore store;
    @Getter
    private long spilledRows;

    public OverflowBucketStore(String jobId, String spillPath, int partitionCount, boolean keepTempFiles) {
        this.store = new PartitionedSpillStore(jobId, spillPath, partitionCount, keepTempFiles);
    }

    public void append(String sourceId, OrderedKey key, Map<String, Object> row) {
        store.append(sourceId, com.jdragon.aggregation.core.streaming.CompositeKey.fromRecord(row, key.getFields()),
                row != null ? new LinkedHashMap<String, Object>(row) : new LinkedHashMap<String, Object>());
        spilledRows++;
    }

    @Override
    public void close() {
        store.close();
    }

    public void cleanup() {
        store.cleanup();
    }
}
