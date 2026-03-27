package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.exception.CommonErrorCode;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.streaming.SourceRowScanner;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.pluginloader.LoadUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Slf4j
public class DataFetcher {

    private final DataSourcePluginManager pluginManager;
    private final boolean parallelFetch;
    private final SourceRowScanner rowScanner;

    public DataFetcher(DataSourcePluginManager pluginManager) {
        this(pluginManager, true);
    }

    public DataFetcher(DataSourcePluginManager pluginManager, boolean parallelFetch) {
        this.pluginManager = pluginManager;
        this.parallelFetch = parallelFetch;
        this.rowScanner = new SourceRowScanner(pluginManager);
    }

    public void scanSources(List<DataSourceConfig> dataSourceConfigs,
                            int parallelism,
                            BiConsumer<String, Map<String, Object>> consumer) {
        rowScanner.scanSources(dataSourceConfigs, parallelism, consumer);
    }

    public void scanSources(List<DataSourceConfig> dataSourceConfigs,
                            BiConsumer<String, Map<String, Object>> consumer) {
        int parallelism = parallelFetch ? Math.max(1, dataSourceConfigs != null ? dataSourceConfigs.size() : 1) : 1;
        scanSources(dataSourceConfigs, parallelism, consumer);
    }

    public Map<String, List<Map<String, Object>>> fetchDataFromSources(List<DataSourceConfig> dataSourceConfigs) {
        if (parallelFetch) {
            return fetchDataFromSourcesParallel(dataSourceConfigs);
        } else {
            return fetchDataFromSourcesSequential(dataSourceConfigs);
        }
    }

    private Map<String, List<Map<String, Object>>> fetchDataFromSourcesSequential(List<DataSourceConfig> dataSourceConfigs) {
        Map<String, List<Map<String, Object>>> sourceData = new HashMap<>();

        for (DataSourceConfig config : dataSourceConfigs) {
            try {
                List<Map<String, Object>> data = fetchFromSource(config);
                sourceData.put(config.getSourceId(), data);
                log.info("Fetched {} records from source: {}", data.size(), config.getSourceName());
            } catch (Exception e) {
                log.error("Failed to fetch data from source: {}", config.getSourceName(), e);
                throw AggregationException.asException(CommonErrorCode.RUNTIME_ERROR,
                        "Failed to fetch data from source: " + config.getSourceName(), e);
            }
        }

        return sourceData;
    }

    private Map<String, List<Map<String, Object>>> fetchDataFromSourcesParallel(List<DataSourceConfig> dataSourceConfigs) {
        Map<String, CompletableFuture<List<Map<String, Object>>>> futures = new LinkedHashMap<>();

        for (DataSourceConfig config : dataSourceConfigs) {
            CompletableFuture<List<Map<String, Object>>> future = CompletableFuture.supplyAsync(() -> {
                try {
                    List<Map<String, Object>> data = fetchFromSource(config);
                    log.info("Fetched {} records from source: {}", data.size(), config.getSourceName());
                    return data;
                } catch (Exception e) {
                    log.error("Failed to fetch data from source: {}", config.getSourceName(), e);
                    throw AggregationException.asException(CommonErrorCode.RUNTIME_ERROR,
                            "Failed to fetch data from source: " + config.getSourceName(), e);
                }
            });
            futures.put(config.getSourceId(), future);
        }

        Map<String, List<Map<String, Object>>> sourceData = new LinkedHashMap<>();
        List<Throwable> errors = new ArrayList<>();

        for (Map.Entry<String, CompletableFuture<List<Map<String, Object>>>> entry : futures.entrySet()) {
            String sourceId = entry.getKey();
            CompletableFuture<List<Map<String, Object>>> future = entry.getValue();
            try {
                sourceData.put(sourceId, future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                errors.add(AggregationException.asException(CommonErrorCode.RUNTIME_ERROR,
                        "Interrupted while waiting for data from source: " + sourceId, e));
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                log.error("Failed to fetch data from source: {}", sourceId, cause);
                errors.add(cause);
            } catch (Exception e) {
                log.error("Unexpected error waiting for data from source: {}", sourceId, e);
                errors.add(e);
            }
        }

        if (!errors.isEmpty()) {
            if (errors.size() == 1) {
                throw errors.get(0) instanceof RuntimeException ?
                        (RuntimeException) errors.get(0) :
                        AggregationException.asException(CommonErrorCode.RUNTIME_ERROR,
                                "Failed to fetch data from source: " + errors.get(0).getMessage(), errors.get(0));
            } else {
                AggregationException aggregated = new AggregationException(CommonErrorCode.RUNTIME_ERROR,
                        "Multiple data sources failed to fetch: " + errors.stream()
                                .map(Throwable::getMessage)
                                .collect(Collectors.joining("; ")));
                for (Throwable error : errors) {
                    aggregated.addSuppressed(error);
                }
                throw aggregated;
            }
        }

        return sourceData;
    }

    private List<Map<String, Object>> fetchFromSource(DataSourceConfig config) {
        // 判断是否为文件数据源
        if (isFileDataSource(config)) {
            return fetchFromFileSource(config);
        } else {
            return fetchFromDatabaseSource(config);
        }
    }

    private boolean isFileDataSource(DataSourceConfig config) {
        String pluginName = config.getPluginName();
        if (pluginName == null) {
            return false;
        }

        String lowerName = pluginName.toLowerCase();
        // 判断插件名称是否表示文件数据源
        return lowerName.startsWith("file-") ||
                lowerName.contains("ftp") ||
                lowerName.contains("sftp") ||
                lowerName.contains("hdfs") ||
                lowerName.contains("minio") ||
                lowerName.contains("s3") ||
                lowerName.contains("oss") ||
                lowerName.equals("local") ||
                lowerName.equals("localfile");
    }

    private List<Map<String, Object>> fetchFromFileSource(DataSourceConfig config) {
        List<Map<String, Object>> results = new ArrayList<>();

        // 加载FileHelper插件
        try (FileHelper fileHelper = LoadUtil.loadJobPlugin(SourcePluginType.SOURCE, config.getPluginName());) {
            // 初始化连接
            if (!fileHelper.connect(config.getConnectionConfig())) {
                throw new RuntimeException("Failed to connect to file source: " + config.getSourceName());
            }

            // 获取文件路径和格式
            String filePath = getFilePath(config);
            String fileType = getFileType(config, filePath);
            Integer maxRecords = config.getMaxRecords();

            log.debug("Reading file for source {}: {}, type: {}", config.getSourceId(), filePath, fileType);

            // 收集数据
            AtomicInteger recordCount = new AtomicInteger(0);

            fileHelper.readFile(filePath, fileType, (Map<String, Object> row) -> {
                // 应用字段映射
                Map<String, Object> mappedRow = applyFieldMappings(row, config.getFieldMappings());
                results.add(mappedRow);

                // 检查记录数限制
                if (maxRecords != null && maxRecords > 0 && recordCount.incrementAndGet() >= maxRecords) {
                    throw new RecordLimitReachedException();
                }
            }, config.getExtConfig());

            log.info("Fetched {} records from file source: {}", results.size(), config.getSourceName());
            return results;

        } catch (RecordLimitReachedException e) {
            // 正常达到记录限制，返回已收集的数据
            log.info("Reached record limit {} for file source: {}", config.getMaxRecords(), config.getSourceName());
            return results;
        } catch (Exception e) {
            log.error("Failed to fetch data from file source: {}", config.getSourceName(), e);
            throw AggregationException.asException(
                    CommonErrorCode.RUNTIME_ERROR,
                    "Failed to fetch data from file source: " + config.getSourceName(), e);
        }
    }

    private static class RecordLimitReachedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private List<Map<String, Object>> fetchFromDatabaseSource(DataSourceConfig config) {
        AbstractDataSourcePlugin plugin = pluginManager.getDataSourcePlugin(config.getPluginName());
        BaseDataSourceDTO dto = DataSourcePluginManager.createDataSourceDTO(config);

        String query = buildQuery(config);
        query = applyLimitClause(query, config.getMaxRecords());
        log.debug("Executing query for source {}: {}", config.getSourceId(), query);

        Table<Map<String, Object>> table = plugin.executeQuerySql(dto, query, true);

        if (table == null || table.getBodies() == null) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> results = new ArrayList<>();
        for (Map<String, Object> row : table.getBodies()) {
            results.add(applyFieldMappings(row, config.getFieldMappings()));
        }

        return results;
    }

    private String getFilePath(DataSourceConfig config) {
        // 优先使用querySql作为文件路径，其次使用tableName
        if (config.getQuerySql() != null && !config.getQuerySql().trim().isEmpty()) {
            return config.getQuerySql().trim();
        }

        if (config.getTableName() != null && !config.getTableName().trim().isEmpty()) {
            return config.getTableName().trim();
        }

        throw new IllegalArgumentException("Either querySql or tableName must be provided as file path for source: " + config.getSourceId());
    }

    private String getFileType(DataSourceConfig config, String filePath) {
        // 从配置中获取文件格式
        if (config.getConnectionConfig() != null) {
            String format = config.getExtConfig().getString("file.format");
            if (format != null && !format.trim().isEmpty()) {
                return format.trim().toLowerCase();
            }
        }

        // 根据文件扩展名推断格式
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
        } else {
            // 默认尝试CSV格式
            return "csv";
        }
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
        // Check if query already contains LIMIT or FETCH FIRST clause
        if (upperQuery.contains(" LIMIT ") || upperQuery.contains(" FETCH ") || upperQuery.contains(" ROWNUM ")) {
            return query;
        }

        // Remove trailing semicolon if present
        String trimmed = query.trim();
        if (trimmed.endsWith(";")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }

        return trimmed + " LIMIT " + maxRecords;
    }

    private Map<String, Object> applyFieldMappings(Map<String, Object> row, Map<String, String> fieldMappings) {
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

    public Map<String, Map<String, List<Map<String, Object>>>> groupByMatchKeys(
            Map<String, List<Map<String, Object>>> sourceData,
            List<String> matchKeys) {

        Map<String, Map<String, List<Map<String, Object>>>> groupedData = new LinkedHashMap<>();

        for (Map.Entry<String, List<Map<String, Object>>> entry : sourceData.entrySet()) {
            String sourceId = entry.getKey();
            List<Map<String, Object>> records = entry.getValue();

            Map<String, List<Map<String, Object>>> sourceGrouped = new LinkedHashMap<>();
            for (Map<String, Object> record : records) {
                String matchKey = generateMatchKey(record, matchKeys);
                sourceGrouped.computeIfAbsent(matchKey, k -> new ArrayList<>()).add(record);
            }

            groupedData.put(sourceId, sourceGrouped);
        }

        return groupedData;
    }

    private String generateMatchKey(Map<String, Object> record, List<String> matchKeys) {
        if (matchKeys == null || matchKeys.isEmpty()) {
            return "default";
        }

        return matchKeys.stream()
                .map(key -> {
                    Object value = record.get(key);
                    return value != null ? value.toString() : "NULL";
                })
                .collect(Collectors.joining("-|-"));
    }
}
