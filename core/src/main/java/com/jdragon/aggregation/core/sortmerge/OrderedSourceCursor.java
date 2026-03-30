package com.jdragon.aggregation.core.sortmerge;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.streaming.RowCodec;
import com.jdragon.aggregation.core.streaming.SourceRowScanner;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Streams one source in ordered-group form.
 *
 * <p>The cursor collapses consecutive rows with the same key into an
 * {@link OrderedKeyGroup}. When local disorder buffering is enabled, those
 * groups first pass through a bounded source-side reorder buffer so that small
 * local key regressions can be absorbed before they reach the coordinator.
 */
@Slf4j
public class OrderedSourceCursor {

    /**
     * Event exchanged between the producer thread and the merge coordinator.
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
    private final OrderedKeySchema keySchema;
    private final AdaptiveMergeConfig adaptiveMergeConfig;
    private final List<String> keyFields;
    private final LinkedBlockingQueue<CursorEvent> queue = new LinkedBlockingQueue<CursorEvent>(4);
    private final boolean preferOrderedQuery;

    @Getter
    private volatile long scannedRecords;
    @Getter
    private volatile long producedGroups;
    @Getter
    private volatile long localReorderedGroupCount;
    @Getter
    private volatile long localMergedDuplicateGroupCount;
    @Getter
    @Setter
    private volatile boolean finished;
    @Getter
    private volatile boolean producerDone;
    private Thread worker;

    public OrderedSourceCursor(SourceRowScanner rowScanner,
                               DataSourceConfig dataSourceConfig,
                               List<String> keyFields,
                               boolean preferOrderedQuery) {
        this(
                rowScanner,
                dataSourceConfig,
                new OrderedKeySchema(keyFields, Collections.<String, OrderedKeyType>emptyMap()),
                keyFields,
                preferOrderedQuery,
                disabledLocalDisorderConfig()
        );
    }

    public OrderedSourceCursor(SourceRowScanner rowScanner,
                               DataSourceConfig dataSourceConfig,
                               OrderedKeySchema keySchema,
                               List<String> keyFields,
                               boolean preferOrderedQuery,
                               AdaptiveMergeConfig adaptiveMergeConfig) {
        this.rowScanner = rowScanner;
        this.dataSourceConfig = dataSourceConfig;
        this.keySchema = keySchema != null
                ? keySchema
                : new OrderedKeySchema(keyFields, Collections.<String, OrderedKeyType>emptyMap());
        this.adaptiveMergeConfig = adaptiveMergeConfig != null ? adaptiveMergeConfig : disabledLocalDisorderConfig();
        this.keyFields = keyFields != null ? new ArrayList<String>(keyFields) : new ArrayList<String>();
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
        final LocalDisorderBuffer disorderBuffer = new LocalDisorderBuffer();
        final GroupAccumulator accumulator = new GroupAccumulator(disorderBuffer);
        try {
            rowScanner.scan(buildScanConfig(), row -> {
                scannedRecords++;
                accumulator.accept(row);
            });
            accumulator.flush();
            disorderBuffer.flush();
            queue.put(CursorEvent.end(sourceId));
        } catch (Throwable throwable) {
            try {
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

        if (preferOrderedQuery && !isFileSource(scanConfig) && !keyFields.isEmpty()) {
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

    private int compareKeys(OrderedKey left, OrderedKey right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        return keySchema.compare(left, right);
    }

    private boolean localDisorderEnabled() {
        return adaptiveMergeConfig != null && adaptiveMergeConfig.isLocalDisorderEnabled();
    }

    private long localDisorderMaxMemoryBytes() {
        return adaptiveMergeConfig != null
                ? Math.max(1L, adaptiveMergeConfig.getLocalDisorderMaxMemoryBytes())
                : 1L;
    }

    private int localDisorderMaxGroups() {
        return adaptiveMergeConfig != null
                ? Math.max(1, adaptiveMergeConfig.getLocalDisorderMaxGroups())
                : 1;
    }

    private long estimateGroupBytes(OrderedKeyGroup group) {
        int rowBytes = RowCodec.encode(group.getFirstRow()).getBytes(StandardCharsets.UTF_8).length;
        int keyBytes = group.getKey() != null
                ? group.getKey().getEncoded().getBytes(StandardCharsets.UTF_8).length
                : 0;
        return rowBytes + keyBytes + 128L;
    }

    private OrderedKeyGroup copyGroup(OrderedKeyGroup group) {
        OrderedKeyGroup copy = new OrderedKeyGroup();
        copy.setSourceId(group.getSourceId());
        copy.setKey(group.getKey());
        copy.setFirstRow(group.getFirstRow() != null
                ? new LinkedHashMap<String, Object>(group.getFirstRow())
                : new LinkedHashMap<String, Object>());
        copy.setDuplicateCount(group.getDuplicateCount());
        copy.setScannedRecords(group.getScannedRecords());
        return copy;
    }

    private int toDuplicateCount(long scannedRecords) {
        long duplicates = Math.max(0L, scannedRecords - 1L);
        return duplicates > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) duplicates;
    }

    private void publishGroup(OrderedKeyGroup group) {
        producedGroups++;
        try {
            queue.put(CursorEvent.group(sourceId, group));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while publishing ordered group", e);
        }
    }

    private static AdaptiveMergeConfig disabledLocalDisorderConfig() {
        AdaptiveMergeConfig config = new AdaptiveMergeConfig();
        config.setLocalDisorderEnabled(false);
        return config;
    }

    private class GroupAccumulator {
        private final LocalDisorderBuffer disorderBuffer;
        private OrderedKey currentKey;
        private Map<String, Object> firstRow;
        private int duplicateCount;
        private long groupRecords;

        private GroupAccumulator(LocalDisorderBuffer disorderBuffer) {
            this.disorderBuffer = disorderBuffer;
        }

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
                duplicateCount = 0;
                groupRecords = 0L;
            }
        }

        private void startGroup(OrderedKey key, Map<String, Object> row) {
            currentKey = key;
            firstRow = row != null ? new LinkedHashMap<String, Object>(row) : new LinkedHashMap<String, Object>();
            duplicateCount = 0;
            groupRecords = 1L;
        }

        private void emitCurrent() {
            OrderedKeyGroup group = new OrderedKeyGroup();
            group.setSourceId(sourceId);
            group.setKey(currentKey);
            group.setFirstRow(firstRow != null
                    ? new LinkedHashMap<String, Object>(firstRow)
                    : new LinkedHashMap<String, Object>());
            group.setDuplicateCount(duplicateCount);
            group.setScannedRecords(groupRecords);
            disorderBuffer.accept(group);
        }
    }

    private class LocalDisorderBuffer {
        private final TreeMap<OrderedKey, BufferedGroup> bufferedGroups =
                new TreeMap<OrderedKey, BufferedGroup>((left, right) -> compareKeys(left, right));
        private OrderedKey lastObservedKey;
        private long estimatedBytes;

        void accept(OrderedKeyGroup group) {
            if (!localDisorderEnabled()) {
                publishGroup(group);
                return;
            }
            if (lastObservedKey != null && compareKeys(group.getKey(), lastObservedKey) < 0) {
                localReorderedGroupCount++;
            }
            lastObservedKey = group.getKey();

            BufferedGroup existing = bufferedGroups.get(group.getKey());
            if (existing == null) {
                OrderedKeyGroup copied = copyGroup(group);
                long bytes = estimateGroupBytes(copied);
                bufferedGroups.put(copied.getKey(), new BufferedGroup(copied, bytes));
                estimatedBytes += bytes;
            } else {
                estimatedBytes -= existing.estimatedBytes;
                long totalScannedRecords = existing.group.getScannedRecords() + group.getScannedRecords();
                existing.group.setScannedRecords(totalScannedRecords);
                existing.group.setDuplicateCount(toDuplicateCount(totalScannedRecords));
                existing.estimatedBytes = estimateGroupBytes(existing.group);
                estimatedBytes += existing.estimatedBytes;
                localMergedDuplicateGroupCount++;
            }

            releaseOverflowIfNeeded();
        }

        void flush() {
//            log.info("{},队列剩余：{}", dataSourceConfig.getSourceId(), bufferedGroups.size());
            while (!bufferedGroups.isEmpty()) {
                releaseSmallest();
            }
        }

        private void releaseOverflowIfNeeded() {
            while (bufferedGroups.size() > localDisorderMaxGroups()
                    || estimatedBytes > localDisorderMaxMemoryBytes()) {
                releaseSmallest();
            }
        }

        private void releaseSmallest() {
            Map.Entry<OrderedKey, BufferedGroup> firstEntry = bufferedGroups.firstEntry();
            if (firstEntry == null) {
                return;
            }
            bufferedGroups.remove(firstEntry.getKey());
            estimatedBytes -= firstEntry.getValue().estimatedBytes;
            publishGroup(firstEntry.getValue().group);
        }
    }

    private static class BufferedGroup {
        private final OrderedKeyGroup group;
        private long estimatedBytes;

        private BufferedGroup(OrderedKeyGroup group, long estimatedBytes) {
            this.group = group;
            this.estimatedBytes = estimatedBytes;
        }
    }
}
