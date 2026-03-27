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

public class OrderedSourceCursor {

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
    private final LinkedBlockingQueue<CursorEvent> queue = new LinkedBlockingQueue<CursorEvent>(4);
    private final boolean preferOrderedQuery;

    @Getter
    private volatile long scannedRecords;
    @Getter
    private volatile long producedGroups;
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
        this.rowScanner = rowScanner;
        this.dataSourceConfig = dataSourceConfig;
        this.keyFields = keyFields != null ? keyFields : new ArrayList<String>();
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
        worker = new Thread(new Runnable() {
            @Override
            public void run() {
                produceGroups();
            }
        }, "sortmerge-cursor-" + sourceId);
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
            rowScanner.scan(buildScanConfig(), new java.util.function.Consumer<Map<String, Object>>() {
                @Override
                public void accept(Map<String, Object> row) {
                    scannedRecords++;
                    accumulator.accept(row);
                }
            });
            accumulator.flush();
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
            this.firstRow = row != null ? new LinkedHashMap<String, Object>(row) : new LinkedHashMap<String, Object>();
            this.duplicateCount = 0;
            this.groupRecords = 1L;
        }

        private void emitCurrent() {
            OrderedKeyGroup group = new OrderedKeyGroup();
            group.setSourceId(sourceId);
            group.setKey(currentKey);
            group.setFirstRow(firstRow != null ? new LinkedHashMap<String, Object>(firstRow) : new LinkedHashMap<String, Object>());
            group.setDuplicateCount(duplicateCount);
            group.setScannedRecords(groupRecords);
            producedGroups++;
            try {
                queue.put(CursorEvent.group(sourceId, group));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while publishing ordered group", e);
            }
        }
    }
}
