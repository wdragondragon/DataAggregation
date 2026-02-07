package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.*;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class UpdateExecutor {

    private final DataSourcePluginManager pluginManager;

    public UpdateExecutor(DataSourcePluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    public UpdateResult executeUpdates(DataSourceConfig targetDataSource, List<DifferenceRecord> resolvedDifferences, List<String> matchKeys, Map<String, String> fieldMappings, int bufferSize) {
        UpdateResult result = new UpdateResult();
        result.setTargetSourceId(targetDataSource.getSourceId());
        result.setRuleId("update-" + System.currentTimeMillis());

        if (resolvedDifferences == null || resolvedDifferences.isEmpty()) {
            log.info("No resolved differences to update");
            return result;
        }

        if (targetDataSource.getTableName() == null || targetDataSource.getTableName().trim().isEmpty()) {
            log.warn("Target data source {} has no table name specified, cannot execute updates", targetDataSource.getSourceId());
            result.incrementFailed("N/A", "Target data source has no table name specified");
            return result;
        }

        try {
            BaseDataSourceDTO dto = DataSourcePluginManager.createDataSourceDTO(targetDataSource);
            AbstractDataSourcePlugin plugin = pluginManager.getDataSourcePlugin(targetDataSource.getPluginName());

            if (plugin == null) {
                log.error("Plugin not found for data source: {}", targetDataSource.getPluginName());
                result.incrementFailed("N/A", "Plugin not found: " + targetDataSource.getPluginName());
                return result;
            }

            List<String> updateStatements = new ArrayList<>();
            Map<String, DifferenceRecord> statementToDiffMap = new HashMap<>();

            // Build update statements for each resolved difference
            for (DifferenceRecord diff : resolvedDifferences) {
                if (diff.getResolutionResult() == null || diff.getResolutionResult().getResolvedValues() == null) {
                    continue;
                }

                try {
                    String updateSql = buildUpdateStatement(targetDataSource, diff, matchKeys, fieldMappings);
                    updateStatements.add(updateSql);
                    statementToDiffMap.put(updateSql, diff);
                } catch (Exception e) {
                    log.warn("Failed to build update statement for record {}: {}", diff.getRecordId(), e.getMessage());
                    result.incrementFailed(diff.getRecordId(), "Failed to build update statement: " + e.getMessage(), diff.getMatchKeyValues());
                }
            }

            if (updateStatements.isEmpty()) {
                log.info("No valid update statements generated");
                return result;
            }

            log.info("Generated {} update statements for target data source {}", updateStatements.size(), targetDataSource.getSourceId());

            // Try batch update first
            boolean batchSuccess = false;
            if (bufferSize > 0 && updateStatements.size() > 1) {
                try {
                    log.info("Attempting batch update with buffer size {}", bufferSize);
                    batchSuccess = executeBatchUpdate(plugin, dto, updateStatements, bufferSize, statementToDiffMap, result);
                } catch (Exception e) {
                    log.warn("Batch update failed, falling back to individual updates: {}", e.getMessage());
                    batchSuccess = false;
                }
            }

            // If batch failed or not attempted, execute individual updates
            if (!batchSuccess) {
                log.info("Executing individual updates");
                executeIndividualUpdates(plugin, dto, updateStatements, statementToDiffMap, result);
            }

            log.info("Update execution completed: {} successful, {} failed", result.getSuccessfulUpdates(), result.getFailedUpdates());

        } catch (Exception e) {
            log.error("Failed to execute updates: {}", e.getMessage(), e);
            result.incrementFailed("N/A", "Execution failed: " + e.getMessage());
        }

        return result;
    }

    private boolean executeBatchUpdate(AbstractDataSourcePlugin plugin, BaseDataSourceDTO dto, List<String> updateStatements, int bufferSize, Map<String, DifferenceRecord> statementToDiffMap, UpdateResult result) {
        try {
            // Split statements into batches
            for (int i = 0; i < updateStatements.size(); i += bufferSize) {
                int end = Math.min(i + bufferSize, updateStatements.size());
                List<String> batch = updateStatements.subList(i, end);

                try {
                    plugin.executeBatch(dto, batch);
                    // All statements in this batch succeeded
                    for (String sql : batch) {
                        DifferenceRecord diff = statementToDiffMap.get(sql);
                        result.incrementSuccessful();
                        log.debug("Batch update succeeded for record {}", diff.getRecordId());
                    }
                } catch (Exception e) {
                    log.warn("Batch update failed, falling back to individual updates for batch: {}", e.getMessage());
                    // Execute this batch individually
                    for (String sql : batch) {
                        DifferenceRecord diff = statementToDiffMap.get(sql);
                        try {
                            plugin.executeUpdate(dto, sql);
                            result.incrementSuccessful();
                            log.debug("Individual update succeeded for record {} after batch failure", diff.getRecordId());
                        } catch (Exception ex) {
                            log.error("Failed to update record {}: {}", diff.getRecordId(), ex.getMessage());
                            result.incrementFailed(diff.getRecordId(), "Update failed: " + ex.getMessage(), diff.getMatchKeyValues());
                        }
                    }
                }
            }
            return true;
        } catch (Exception e) {
            log.error("Batch update execution failed: {}", e.getMessage(), e);
            return false;
        }
    }

    private void executeIndividualUpdates(AbstractDataSourcePlugin plugin, BaseDataSourceDTO dto, List<String> updateStatements, Map<String, DifferenceRecord> statementToDiffMap, UpdateResult result) {
        for (String sql : updateStatements) {
            DifferenceRecord diff = statementToDiffMap.get(sql);
            try {
                plugin.executeUpdate(dto, sql);
                result.incrementSuccessful();
                log.debug("Update succeeded for record {}", diff.getRecordId());
            } catch (Exception e) {
                log.error("Failed to update record {}: {}", diff.getRecordId(), e.getMessage());
                result.incrementFailed(diff.getRecordId(), "Update failed: " + e.getMessage(), diff.getMatchKeyValues());
            }
        }
    }

    private String buildUpdateStatement(DataSourceConfig targetDataSource, DifferenceRecord diff, List<String> matchKeys, Map<String, String> fieldMappings) {
        String tableName = targetDataSource.getTableName();
        Map<String, Object> resolvedValues = diff.getResolutionResult().getResolvedValues();
        Map<String, Object> matchKeyValues = diff.getMatchKeyValues();

        // Determine which fields actually have differences
        List<String> fieldsToUpdate = new ArrayList<>();
        if (diff.getDifferences() != null && !diff.getDifferences().isEmpty()) {
            fieldsToUpdate.addAll(diff.getDifferences().keySet());
        } else {
            // If no differences map, update all resolved values
            fieldsToUpdate.addAll(resolvedValues.keySet());
        }

        // Apply field mappings
        Map<String, String> reverseMappings = new HashMap<>();
        if (fieldMappings != null) {
            for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
                reverseMappings.put(entry.getValue(), entry.getKey());
            }
        }

        // Build SET clause
        StringBuilder setClause = new StringBuilder();
        for (String field : fieldsToUpdate) {
            String columnName = reverseMappings.getOrDefault(field, field);
            if ("missing_data".equals(columnName)) {
                continue;
            }
            Object value = resolvedValues.get(field);

            if (setClause.length() > 0) {
                setClause.append(", ");
            }
            setClause.append(columnName).append(" = ").append(formatValue(value));
        }

        if (setClause.length() == 0) {
            throw new IllegalArgumentException("No fields to update for record " + diff.getRecordId());
        }

        // Build WHERE clause from match keys
        StringBuilder whereClause = new StringBuilder();
        for (String matchKey : matchKeys) {
            Object value = matchKeyValues.get(matchKey);
            if (value == null) {
                throw new IllegalArgumentException("Match key value is null for key: " + matchKey);
            }

            String columnName = reverseMappings.getOrDefault(matchKey, matchKey);
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }
            whereClause.append(columnName).append(" = ").append(formatValue(value));
        }

        if (whereClause.length() == 0) {
            throw new IllegalArgumentException("No valid match keys for record " + diff.getRecordId());
        }

        return String.format("UPDATE %s SET %s WHERE %s", tableName, setClause, whereClause);
    }

    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }

        if (value instanceof String) {
            // Escape single quotes
            String str = (String) value;
            str = str.replace("'", "''");
            return "'" + str + "'";
        }

        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }

        // For other types, convert to string and escape
        String str = value.toString();
        str = str.replace("'", "''");
        return "'" + str + "'";
    }
}