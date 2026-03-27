package com.jdragon.aggregation.core.streaming;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.exception.CommonErrorCode;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.pluginloader.LoadUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
public class SourceRowScanner {

    private final DataSourcePluginManager pluginManager;

    public SourceRowScanner(DataSourcePluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    public void scanSources(List<DataSourceConfig> configs, int parallelism, BiConsumer<String, Map<String, Object>> consumer) {
        if (configs == null || configs.isEmpty()) {
            return;
        }
        int workerCount = Math.max(1, parallelism);
        ExecutorService executor = workerCount == 1 ? null : Executors.newFixedThreadPool(workerCount);
        try {
            if (executor == null) {
                for (DataSourceConfig config : configs) {
                    scan(config, row -> consumer.accept(config.getSourceId(), row));
                }
            } else {
                CompletableFuture<?>[] futures = new CompletableFuture<?>[configs.size()];
                for (int i = 0; i < configs.size(); i++) {
                    final DataSourceConfig config = configs.get(i);
                    futures[i] = CompletableFuture.runAsync(
                            () -> scan(config, row -> consumer.accept(config.getSourceId(), row)),
                            executor
                    );
                }
                CompletableFuture.allOf(futures).join();
            }
        } finally {
            if (executor != null) {
                executor.shutdown();
                try {
                    executor.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public void scan(DataSourceConfig config, Consumer<Map<String, Object>> consumer) {
        if (isFileDataSource(config)) {
            scanFileSource(config, consumer);
            return;
        }
        scanDatabaseSource(config, consumer);
    }

    private boolean isFileDataSource(DataSourceConfig config) {
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

    private void scanFileSource(DataSourceConfig config, Consumer<Map<String, Object>> consumer) {
        try (FileHelper fileHelper = LoadUtil.loadJobPlugin(SourcePluginType.SOURCE, config.getPluginName())) {
            if (!fileHelper.connect(config.getConnectionConfig())) {
                throw new RuntimeException("Failed to connect to file source: " + config.getSourceName());
            }
            String filePath = getFilePath(config);
            String fileType = getFileType(config, filePath);
            Integer maxRecords = config.getMaxRecords();
            final int[] scanned = {0};
            fileHelper.readFile(filePath, fileType, row -> {
                if (maxRecords != null && maxRecords > 0 && scanned[0] >= maxRecords) {
                    return;
                }
                scanned[0]++;
                consumer.accept(applyFieldMappings(row, config.getFieldMappings()));
            }, config.getExtConfig() != null ? config.getExtConfig() : Configuration.newDefault());
        } catch (Exception e) {
            throw AggregationException.asException(
                    CommonErrorCode.RUNTIME_ERROR,
                    "Failed to scan file source: " + config.getSourceName(),
                    e
            );
        }
    }

    private void scanDatabaseSource(DataSourceConfig config, Consumer<Map<String, Object>> consumer) {
        try {
            AbstractDataSourcePlugin plugin = pluginManager.getDataSourcePlugin(config.getPluginName());
            BaseDataSourceDTO dto = DataSourcePluginManager.createDataSourceDTO(config);
            String query = applyLimitClause(buildQuery(config), config.getMaxRecords());
            plugin.scanQuery(dto, query, true, row -> consumer.accept(applyFieldMappings(row, config.getFieldMappings())));
        } catch (Exception e) {
            throw AggregationException.asException(
                    CommonErrorCode.RUNTIME_ERROR,
                    "Failed to scan source: " + config.getSourceName(),
                    e
            );
        }
    }

    private String getFilePath(DataSourceConfig config) {
        if (config.getQuerySql() != null && !config.getQuerySql().trim().isEmpty()) {
            return config.getQuerySql().trim();
        }
        if (config.getTableName() != null && !config.getTableName().trim().isEmpty()) {
            return config.getTableName().trim();
        }
        throw new IllegalArgumentException("Either querySql or tableName must be provided as file path for source: " + config.getSourceId());
    }

    private String getFileType(DataSourceConfig config, String filePath) {
        if (config.getExtConfig() != null) {
            String format = config.getExtConfig().getString("file.format");
            if (format != null && !format.trim().isEmpty()) {
                return format.trim().toLowerCase();
            }
        }
        String lowerPath = filePath.toLowerCase();
        if (lowerPath.endsWith(".csv")) {
            return "csv";
        } else if (lowerPath.endsWith(".json") || lowerPath.endsWith(".jsonl") || lowerPath.endsWith(".ndjson")) {
            return "json";
        } else if (lowerPath.endsWith(".parquet")) {
            return "parquet";
        } else if (lowerPath.endsWith(".avro")) {
            return "avro";
        } else if (lowerPath.endsWith(".xml")) {
            return "xml";
        }
        return "csv";
    }

    private String buildQuery(DataSourceConfig config) {
        if (config.getQuerySql() != null && !config.getQuerySql().trim().isEmpty()) {
            return config.getQuerySql();
        }
        if (config.getTableName() != null && !config.getTableName().trim().isEmpty()) {
            return String.format("SELECT * FROM %s", config.getTableName());
        }
        throw new IllegalArgumentException("Either querySql or tableName must be provided for source: " + config.getSourceId());
    }

    private String applyLimitClause(String query, Integer maxRecords) {
        if (maxRecords == null || maxRecords <= 0) {
            return query;
        }
        String upperQuery = query.toUpperCase();
        if (upperQuery.contains(" LIMIT ") || upperQuery.contains(" FETCH ") || upperQuery.contains(" ROWNUM ")) {
            return query;
        }
        String trimmed = query.trim();
        if (trimmed.endsWith(";")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed + " LIMIT " + maxRecords;
    }

    private Map<String, Object> applyFieldMappings(Map<String, Object> row, Map<String, String> fieldMappings) {
        if (row == null) {
            return new HashMap<>();
        }
        if (fieldMappings == null || fieldMappings.isEmpty()) {
            return new HashMap<>(row);
        }
        Map<String, Object> mappedRow = new HashMap<>();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String originalField = entry.getKey();
            String mappedField = fieldMappings.getOrDefault(originalField, originalField);
            mappedRow.put(mappedField, entry.getValue());
        }
        return mappedRow;
    }
}
