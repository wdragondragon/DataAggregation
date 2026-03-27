package com.jdragon.aggregation.core.sortmerge;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.streaming.SourceRowScanner;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 负责按 key 顺序流式读取单个 source，并把连续同 key 的多行折叠成一个
 * {@link OrderedKeyGroup}。
 *
 * <p>该 cursor 只保留当前聚合中的 group 和一个很小的事件队列，因此不会把整个
 * source 全量放进内存。数据库来源可尝试自动包 ORDER BY；文件来源则依赖输入天然有序。
 */
public class OrderedSourceCursor {

    /**
     * cursor 工作线程投递给调度线程的事件载体。
     */
    @Getter
    public static class CursorEvent {
        private final String sourceId;
        private final OrderedKeyGroup group;
        private final boolean endOfStream;
        private final Throwable error;

        private CursorEvent(String sourceId, OrderedKeyGroup group, boolean endOfStream, Throwable error) {
            this.sourceId = sourceId;
            this.group = group;
            this.endOfStream = endOfStream;
            this.error = error;
        }

        static CursorEvent group(String sourceId, OrderedKeyGroup group) {
            return new CursorEvent(sourceId, group, false, null);
        }

        static CursorEvent end(String sourceId) {
            return new CursorEvent(sourceId, null, true, null);
        }

        static CursorEvent error(String sourceId, Throwable error) {
            return new CursorEvent(sourceId, null, false, error);
        }
    }

    private final String sourceId;
    private final DataSourceConfig dataSourceConfig;
    private final SourceRowScanner rowScanner;
    private final List<String> keyFields;
    // 有界队列用于在生产线程和调度线程之间建立背压，避免 source 侧无限制预读。
    private final LinkedBlockingQueue<CursorEvent> queue = new LinkedBlockingQueue<>(4);
    private final boolean preferOrderedQuery;

    @Getter
    private volatile long scannedRecords;
    @Getter
    private volatile long producedGroups;
    @Getter
    @Setter
    // 由调度线程在消费到 end 事件后置为 true，之后该来源会被视为“不会再产出新 key”。
    private volatile boolean finished;
    @Getter
    // 由生产线程在 finally 中置为 true，用于表达“worker 已彻底退出”。
    private volatile boolean producerDone;
    private Thread worker;

    public OrderedSourceCursor(SourceRowScanner rowScanner,
                               DataSourceConfig dataSourceConfig,
                               List<String> keyFields,
                               boolean preferOrderedQuery) {
        this.rowScanner = rowScanner;
        this.dataSourceConfig = dataSourceConfig;
        this.keyFields = keyFields != null ? keyFields : new ArrayList<>();
        this.sourceId = dataSourceConfig.getSourceId();
        this.preferOrderedQuery = preferOrderedQuery;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void start() {
        if (worker != null) {
            return;
        }
        // source 的读取与 group 折叠在独立线程中完成，主线程只负责消费 queue 中的事件。
        worker = new Thread(this::produceGroups, "sortmerge-cursor-" + sourceId);
        worker.setDaemon(true);
        worker.start();
    }

    public CursorEvent pollEvent() {
        return queue.poll();
    }

    public CursorEvent takeEvent() throws InterruptedException {
        return queue.take();
    }

    private void produceGroups() {
        final GroupAccumulator accumulator = new GroupAccumulator();
        try {
            rowScanner.scan(buildScanConfig(), row -> {
                scannedRecords++;
                accumulator.accept(row);
            });
            accumulator.flush();
            // 结束事件一旦入队，调度线程就会把该 source 标记为 finished，
            // 某些还在等待它“补齐”的 pending key 也可能因此立刻变为可判定。
            queue.put(CursorEvent.end(sourceId));
        } catch (Throwable throwable) {
            try {
                // 错误通过事件队列传回调度线程，由调度线程统一终止整条归并链路。
                queue.put(CursorEvent.error(sourceId, throwable));
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            }
        } finally {
            producerDone = true;
        }
    }

    private DataSourceConfig buildScanConfig() {
        DataSourceConfig scanConfig = new DataSourceConfig();
        scanConfig.setSourceId(dataSourceConfig.getSourceId());
        scanConfig.setSourceName(dataSourceConfig.getSourceName());
        scanConfig.setPluginName(dataSourceConfig.getPluginName());
        scanConfig.setConnectionConfig(dataSourceConfig.getConnectionConfig() != null
                ? dataSourceConfig.getConnectionConfig().clone()
                : null);
        scanConfig.setQuerySql(dataSourceConfig.getQuerySql());
        scanConfig.setTableName(dataSourceConfig.getTableName());
        scanConfig.setConfidenceWeight(dataSourceConfig.getConfidenceWeight());
        scanConfig.setPriority(dataSourceConfig.getPriority());
        scanConfig.setMaxRecords(dataSourceConfig.getMaxRecords());
        scanConfig.setFieldMappings(dataSourceConfig.getFieldMappings());
        scanConfig.setExtConfig(dataSourceConfig.getExtConfig() != null
                ? dataSourceConfig.getExtConfig().clone()
                : Configuration.newDefault());
        scanConfig.setUpdateTarget(dataSourceConfig.getUpdateTarget());

        if (preferOrderedQuery && !isFileSource(scanConfig) && keyFields != null && !keyFields.isEmpty()) {
            String orderedQuery = buildOrderedQuery(scanConfig);
            if (orderedQuery != null) {
                scanConfig.setQuerySql(orderedQuery);
            }
        }
        return scanConfig;
    }

    private String buildOrderedQuery(DataSourceConfig scanConfig) {
        String query = scanConfig.getQuerySql();
        if (query == null || query.trim().isEmpty()) {
            if (scanConfig.getTableName() == null || scanConfig.getTableName().trim().isEmpty()) {
                return null;
            }
            query = "SELECT * FROM " + scanConfig.getTableName();
        }
        String trimmed = query.trim();
        if (trimmed.endsWith(";")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT * FROM (").append(trimmed).append(") ordered_merge_source ORDER BY ");
        for (int i = 0; i < keyFields.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            String key = keyFields.get(i);
            builder.append("CASE WHEN ").append(key).append(" IS NULL THEN 0 ELSE 1 END");
            builder.append(", ").append(key);
        }
        return builder.toString();
    }

    private boolean isFileSource(DataSourceConfig config) {
        String pluginName = config.getPluginName();
        if (pluginName == null) {
            return false;
        }
        String lowerName = pluginName.toLowerCase();
        return lowerName.startsWith("file-")
                || lowerName.contains("ftp")
                || lowerName.contains("sftp")
                || lowerName.contains("hdfs")
                || lowerName.contains("minio")
                || lowerName.contains("s3")
                || lowerName.contains("oss")
                || lowerName.equals("local")
                || lowerName.equals("localfile");
    }

    private class GroupAccumulator {
        private OrderedKey currentKey;
        private Map<String, Object> firstRow;
        private int duplicateCount;
        private long groupRecords;

        void accept(Map<String, Object> row) {
            OrderedKey rowKey = OrderedKey.fromRow(row, keyFields);
            if (currentKey == null) {
                startGroup(rowKey, row);
                return;
            }
            if (currentKey.getEncoded().equals(rowKey.getEncoded())) {
                duplicateCount++;
                groupRecords++;
                return;
            }
            emitCurrent();
            startGroup(rowKey, row);
        }

        void flush() {
            if (currentKey != null) {
                emitCurrent();
                currentKey = null;
                firstRow = null;
            }
        }

        private void startGroup(OrderedKey key, Map<String, Object> row) {
            this.currentKey = key;
            this.firstRow = row != null ? new LinkedHashMap<>(row) : new LinkedHashMap<>();
            this.duplicateCount = 0;
            this.groupRecords = 1L;
        }

        private void emitCurrent() {
            OrderedKeyGroup group = new OrderedKeyGroup();
            group.setSourceId(sourceId);
            group.setKey(currentKey);
            group.setFirstRow(firstRow != null ? new LinkedHashMap<>(firstRow) : new LinkedHashMap<>());
            group.setDuplicateCount(duplicateCount);
            group.setScannedRecords(groupRecords);
            producedGroups++;
            try {
                // group 事件入队后，调度线程就能据此推进 pendingWindow、判定可归并 key，
                // 队列满时这里会阻塞，从而把背压直接传回 source 读取线程。
                queue.put(CursorEvent.group(sourceId, group));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while publishing ordered group", e);
            }
        }
    }
}
