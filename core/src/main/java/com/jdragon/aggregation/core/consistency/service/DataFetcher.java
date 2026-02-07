package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.exception.CommonErrorCode;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class DataFetcher {
    
    private final DataSourcePluginManager pluginManager;
    private final boolean parallelFetch;
    
    public DataFetcher(DataSourcePluginManager pluginManager) {
        this(pluginManager, true);
    }
    
    public DataFetcher(DataSourcePluginManager pluginManager, boolean parallelFetch) {
        this.pluginManager = pluginManager;
        this.parallelFetch = parallelFetch;
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
        AbstractDataSourcePlugin plugin = pluginManager.getDataSourcePlugin(config.getPluginName());
        BaseDataSourceDTO dto = DataSourcePluginManager.createDataSourceDTO(config);
        
        String query = buildQuery(config);
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
    
    private String buildQuery(DataSourceConfig config) {
        if (config.getQuerySql() != null && !config.getQuerySql().trim().isEmpty()) {
            return config.getQuerySql();
        }
        
        if (config.getTableName() != null && !config.getTableName().trim().isEmpty()) {
            return String.format("SELECT * FROM %s", config.getTableName());
        }
        
        throw new IllegalArgumentException("Either querySql or tableName must be provided for source: " + config.getSourceId());
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