package com.jdragon.aggregation.core.streaming;

import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;

public class StreamExecutionOptions {

    private int partitionCount = 16;
    private int rebalancePartitionMultiplier = 4;
    private int parallelSourceCount = 1;
    private int memoryLimitMB = 512;
    private boolean keepTempFiles = false;
    private String spillPath;
    private int fetchSize = 1000;

    public static StreamExecutionOptions fromFusionConfig(FusionConfig config) {
        StreamExecutionOptions options = new StreamExecutionOptions();
        if (config == null) {
            return options;
        }
        FusionConfig.CacheConfig cacheConfig = config.getCacheConfig();
        FusionConfig.PerformanceConfig performanceConfig = config.getPerformanceConfig();
        if (cacheConfig != null) {
            options.setPartitionCount(cacheConfig.getPartitionCount());
            options.setRebalancePartitionMultiplier(cacheConfig.getRebalancePartitionMultiplier());
        }
        if (performanceConfig != null) {
            options.setParallelSourceCount(performanceConfig.getParallelSourceCount());
            options.setMemoryLimitMB(performanceConfig.getMemoryLimitMB());
        }
        return options;
    }

    public static StreamExecutionOptions fromConsistencyRule(ConsistencyRule rule) {
        StreamExecutionOptions options = new StreamExecutionOptions();
        if (rule == null) {
            return options;
        }
        if (rule.getCacheConfig() != null) {
            options.setPartitionCount(rule.getCacheConfig().getPartitionCount());
            options.setRebalancePartitionMultiplier(rule.getCacheConfig().getRebalancePartitionMultiplier());
            options.setSpillPath(rule.getCacheConfig().getSpillPath());
            options.setKeepTempFiles(rule.getCacheConfig().getKeepTempFiles());
        }
        if (rule.getPerformanceConfig() != null) {
            options.setParallelSourceCount(rule.getPerformanceConfig().getParallelSourceCount());
            options.setMemoryLimitMB(rule.getPerformanceConfig().getMemoryLimitMB());
        }
        return options;
    }

    public int getMaxKeysPerPartition() {
        int estimatedPerRecordBytes = 1024;
        int maxByMemory = Math.max(1024, (memoryLimitMB * 1024 * 1024) / estimatedPerRecordBytes);
        return Math.max(1024, maxByMemory / Math.max(1, partitionCount));
    }

    public int getRebalancePartitionCount(int currentPartitionCount) {
        int normalizedCurrent = Math.max(1, currentPartitionCount);
        long multiplied = (long) normalizedCurrent * Math.max(1, rebalancePartitionMultiplier);
        long strictlyLarger = Math.max((long) normalizedCurrent + 1L, multiplied);
        return (int) Math.min(Integer.MAX_VALUE, Math.max(4L, strictlyLarger));
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = Math.max(1, partitionCount);
    }

    public int getRebalancePartitionMultiplier() {
        return rebalancePartitionMultiplier;
    }

    public void setRebalancePartitionMultiplier(int rebalancePartitionMultiplier) {
        this.rebalancePartitionMultiplier = Math.max(1, rebalancePartitionMultiplier);
    }

    public int getParallelSourceCount() {
        return parallelSourceCount;
    }

    public void setParallelSourceCount(int parallelSourceCount) {
        this.parallelSourceCount = Math.max(1, parallelSourceCount);
    }

    public int getMemoryLimitMB() {
        return memoryLimitMB;
    }

    public void setMemoryLimitMB(int memoryLimitMB) {
        this.memoryLimitMB = Math.max(64, memoryLimitMB);
    }

    public boolean isKeepTempFiles() {
        return keepTempFiles;
    }

    public void setKeepTempFiles(boolean keepTempFiles) {
        this.keepTempFiles = keepTempFiles;
    }

    public String getSpillPath() {
        return spillPath;
    }

    public void setSpillPath(String spillPath) {
        this.spillPath = spillPath;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = Math.max(1, fetchSize);
    }
}
