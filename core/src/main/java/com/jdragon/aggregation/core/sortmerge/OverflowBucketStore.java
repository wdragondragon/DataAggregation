package com.jdragon.aggregation.core.sortmerge;

import com.jdragon.aggregation.core.streaming.PartitionedSpillStore;
import com.jdragon.aggregation.core.streaming.SpillGuard;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 供自适应 sort-merge 使用的溢写桶存储。
 *
 * <p>只有无法继续留在内存窗口中的未决 key，或已经切入 bucketMode 后的新数据，
 * 才会写入这里。最终回放仍复用既有 partition processor，因此功能语义与旧桶链路保持一致。
 */
public class OverflowBucketStore implements AutoCloseable {

    @Getter
    private final PartitionedSpillStore store;
    @Getter
    private long spilledRows;

    public OverflowBucketStore(String jobId, String spillPath, int partitionCount, boolean keepTempFiles) {
        this(jobId, spillPath, partitionCount, keepTempFiles, null);
    }

    public OverflowBucketStore(String jobId,
                               String spillPath,
                               int partitionCount,
                               boolean keepTempFiles,
                               SpillGuard spillGuard) {
        this.store = new PartitionedSpillStore(jobId, spillPath, partitionCount, keepTempFiles, spillGuard);
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
