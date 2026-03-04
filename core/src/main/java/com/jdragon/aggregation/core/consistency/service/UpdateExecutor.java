package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.*;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.commons.pagination.Table;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class UpdateExecutor {
    
    enum OperationType {
        INSERT,
        UPDATE,
        DELETE
    }

    private final DataSourcePluginManager pluginManager;

    public UpdateExecutor(DataSourcePluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    public UpdateResult executeUpdates(DataSourceConfig targetDataSource, List<DifferenceRecord> resolvedDifferences, List<String> matchKeys, Map<String, String> fieldMappings, int bufferSize, int retryAttempts, long retryDelayMs, double retryBackoffMultiplier) {
        return executeUpdates(targetDataSource, resolvedDifferences, matchKeys, fieldMappings, bufferSize, retryAttempts, retryDelayMs, retryBackoffMultiplier, false, true, true, true);
    }
    
    public UpdateResult executeUpdates(DataSourceConfig targetDataSource, List<DifferenceRecord> resolvedDifferences, List<String> matchKeys, Map<String, String> fieldMappings, int bufferSize, int retryAttempts, long retryDelayMs, double retryBackoffMultiplier, boolean validateBeforeUpdate, boolean allowInsert, boolean allowDelete, boolean skipUnchangedUpdates) {
        UpdateResult result = new UpdateResult();
        result.setTargetSourceId(targetDataSource.getSourceId());
        result.setId("update-" + System.currentTimeMillis());

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

            List<String> sqlStatements = new ArrayList<>();
            Map<String, DifferenceRecord> sqlToDiffMap = new HashMap<>();
            Map<String, OperationType> sqlToOperationType = new HashMap<>();

            // Build SQL statements for each resolved difference
            for (DifferenceRecord diff : resolvedDifferences) {
                if (diff.getResolutionResult() == null) {
                    continue;
                }

                 String targetSourceId = targetDataSource.getSourceId();
                 OperationType operationType = determineOperationType(diff, targetSourceId);
                 
                  // Skip UPDATE operations that have no non-match-key values to update
                  if (operationType == OperationType.UPDATE && !hasNonMatchKeyValues(diff)) {
                      log.debug("No non-match-key values to update for record {}, skipping UPDATE operation", diff.getRecordId());
                      continue;
                  }
                
                // For UPDATE operations, check if target already has same values to avoid unnecessary updates
                if (operationType == OperationType.UPDATE && skipUnchangedUpdates) {
                    try {
//                        Map<String, Object> targetCurrentValues = fetchTargetCurrentValues(plugin, dto, targetDataSource, diff, matchKeys, fieldMappings);
                        Map<String, Object> targetCurrentValues = diff.getSourceValues().get(targetDataSource.getSourceId());
                        Map<String, Object> resolvedValues = diff.getResolutionResult().getResolvedValues();
                        Map<String, Object> matchKeyValues = diff.getMatchKeyValues();
                        
                        if (valuesAreEqual(resolvedValues, targetCurrentValues, matchKeyValues)) {
                            log.debug("Target already has same values for record {}, skipping UPDATE operation", diff.getRecordId());
                            result.incrementSkip(diff.getRecordId(), "Target already has same values");
                            continue;
                        }
                    } catch (Exception e) {
                        log.warn("Failed to compare values for record {}, proceeding with UPDATE: {}", diff.getRecordId(), e.getMessage());
                        // Continue with UPDATE if comparison fails
                    }
                }
                
                // Check if operation is allowed
                if (operationType == OperationType.INSERT && !allowInsert) {
                    log.warn("INSERT operation not allowed for record {}, skipping", diff.getRecordId());
                    result.incrementFailed(diff.getRecordId(), "INSERT operation not allowed", diff.getMatchKeyValues());
                    continue;
                }
                if (operationType == OperationType.DELETE && !allowDelete) {
                    log.warn("DELETE operation not allowed for record {}, skipping", diff.getRecordId());
                    result.incrementFailed(diff.getRecordId(), "DELETE operation not allowed", diff.getMatchKeyValues());
                    continue;
                }
                
                // For UPDATE and DELETE, optionally validate record exists
                if (validateBeforeUpdate && (operationType == OperationType.UPDATE || operationType == OperationType.DELETE)) {
                    if (!validateRecordExists(plugin, dto, targetDataSource, diff, matchKeys, fieldMappings)) {
                        result.incrementFailed(diff.getRecordId(), "Target record not found for " + operationType, diff.getMatchKeyValues());
                        continue;
                    }
                }
                
                // For INSERT, if validateBeforeUpdate is true and record exists, skip or handle conflict
                if (validateBeforeUpdate && operationType == OperationType.INSERT) {
                    if (validateRecordExists(plugin, dto, targetDataSource, diff, matchKeys, fieldMappings)) {
                        log.warn("Record already exists for INSERT operation, skipping: {}", diff.getRecordId());
                        result.incrementFailed(diff.getRecordId(), "Record already exists, cannot INSERT", diff.getMatchKeyValues());
                        continue;
                    }
                }

                try {
                    String sql;
                    switch (operationType) {
                        case INSERT:
                            sql = buildInsertStatement(targetDataSource, diff, matchKeys, fieldMappings);
                            log.debug("Generated INSERT statement for record {}: {}", diff.getRecordId(), sql);
                            break;
                        case UPDATE:
                            sql = buildUpdateStatement(targetDataSource, diff, matchKeys, fieldMappings);
                            log.debug("Generated UPDATE statement for record {}: {}", diff.getRecordId(), sql);
                            break;
                        case DELETE:
                            sql = buildDeleteStatement(targetDataSource, diff, matchKeys, fieldMappings);
                            log.debug("Generated DELETE statement for record {}: {}", diff.getRecordId(), sql);
                            break;
                        default:
                            throw new IllegalStateException("Unknown operation type: " + operationType);
                    }
                    
                    sqlStatements.add(sql);
                    sqlToDiffMap.put(sql, diff);
                    sqlToOperationType.put(sql, operationType);
                } catch (Exception e) {
                    log.warn("Failed to build {} statement for record {}: {}", operationType, diff.getRecordId(), e.getMessage());
                    result.incrementFailed(diff.getRecordId(), "Failed to build " + operationType + " statement: " + e.getMessage(), diff.getMatchKeyValues());
                }
            }

            if (sqlStatements.isEmpty()) {
                log.info("No valid SQL statements generated");
                return result;
            }

            log.info("Generated {} SQL statements for target data source {}", sqlStatements.size(), targetDataSource.getSourceId());

            // Try batch execution first
            boolean batchSuccess = false;
            if (bufferSize > 0 && sqlStatements.size() > 1) {
                try {
                    log.info("Attempting batch execution with buffer size {}", bufferSize);
                     batchSuccess = executeBatchUpdate(plugin, dto, sqlStatements, bufferSize, sqlToDiffMap, sqlToOperationType, result, retryAttempts, retryDelayMs, retryBackoffMultiplier);
                } catch (Exception e) {
                    log.warn("Batch update failed, falling back to individual updates: {}", e.getMessage());
                    batchSuccess = false;
                }
            }

            // If batch failed or not attempted, execute individual updates
            if (!batchSuccess) {
                log.info("Executing individual updates");
                 executeIndividualUpdates(plugin, dto, sqlStatements, sqlToDiffMap, sqlToOperationType, result, retryAttempts, retryDelayMs, retryBackoffMultiplier);
            }
            if (result.getSkipCount() > 0) {
                log.info("Skipped {} UPDATE operations because target already had same values", result.getSkipCount());
            }
            log.info("Update execution completed: {} successful, {} failed, {} skipped", result.getSuccessfulUpdates(), result.getFailedUpdates(), result.getSkipCount());

        } catch (Exception e) {
            log.error("Failed to execute updates: {}", e.getMessage(), e);
            result.incrementFailed("N/A", "Execution failed: " + e.getMessage());
        }

        return result;
    }

    private boolean executeBatchUpdate(AbstractDataSourcePlugin plugin, BaseDataSourceDTO dto, List<String> sqlStatements, int bufferSize, Map<String, DifferenceRecord> sqlToDiffMap, Map<String, OperationType> sqlToOperationType, UpdateResult result, int retryAttempts, long retryDelayMs, double retryBackoffMultiplier) {
        try {
            // Split statements into batches
            for (int i = 0; i < sqlStatements.size(); i += bufferSize) {
                int end = Math.min(i + bufferSize, sqlStatements.size());
                List<String> batch = sqlStatements.subList(i, end);

                try {
                    plugin.executeBatch(dto, batch);
                    // All statements in this batch succeeded
                     for (String sql : batch) {
                         DifferenceRecord diff = sqlToDiffMap.get(sql);
                         result.incrementSuccessful();
                         OperationType opType = sqlToOperationType.get(sql);
                         if (opType != null) {
                             switch (opType) {
                                 case INSERT: result.incrementInsert(diff.getRecordId()); break;
                                 case UPDATE: result.incrementUpdate(diff.getRecordId()); break;
                                 case DELETE: result.incrementDelete(diff.getRecordId()); break;
                             }
                         }
                         log.debug("Batch update succeeded for record {}", diff.getRecordId());
                     }
                } catch (Exception e) {
                    log.warn("Batch update failed, falling back to individual updates for batch: {}", e.getMessage());
                     // Execute this batch individually with retry
                    for (String sql : batch) {
                        DifferenceRecord diff = sqlToDiffMap.get(sql);
                         OperationType operationType = sqlToOperationType.get(sql);
                         boolean success = executeUpdateWithRetry(plugin, dto, sql, diff, result, operationType, retryAttempts, retryDelayMs, retryBackoffMultiplier);
                        if (!success) {
                            log.error("Failed to update record {} after batch failure and {} retries", diff.getRecordId(), retryAttempts);
                            result.incrementFailed(diff.getRecordId(), "Update failed after batch failure and " + retryAttempts + " retries", diff.getMatchKeyValues());
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
    
     private void executeIndividualUpdates(AbstractDataSourcePlugin plugin, BaseDataSourceDTO dto, List<String> sqlStatements, Map<String, DifferenceRecord> sqlToDiffMap, Map<String, OperationType> sqlToOperationType, UpdateResult result, int retryAttempts, long retryDelayMs, double retryBackoffMultiplier) {
        for (String sql : sqlStatements) {
            DifferenceRecord diff = sqlToDiffMap.get(sql);
             OperationType operationType = sqlToOperationType.get(sql);
             boolean success = executeUpdateWithRetry(plugin, dto, sql, diff, result, operationType, retryAttempts, retryDelayMs, retryBackoffMultiplier);
            if (!success) {
                log.error("Failed to execute SQL for record {} after {} retries", diff.getRecordId(), retryAttempts);
                result.incrementFailed(diff.getRecordId(), "SQL execution failed after " + retryAttempts + " retries", diff.getMatchKeyValues());
            }
        }
    }
    
     private boolean executeUpdateWithRetry(AbstractDataSourcePlugin plugin, BaseDataSourceDTO dto, String sql, DifferenceRecord diff, UpdateResult result, OperationType operationType, int maxRetries, long baseDelayMs, double backoffMultiplier) {
        int attempt = 0;
        long delayMs = baseDelayMs;
        while (attempt <= maxRetries) {
            try {
                 plugin.executeUpdate(dto, sql);
                 result.incrementSuccessful();
                 if (operationType != null) {
                     switch (operationType) {
                         case INSERT: result.incrementInsert(diff.getRecordId()); break;
                         case UPDATE: result.incrementUpdate(diff.getRecordId()); break;
                         case DELETE: result.incrementDelete(diff.getRecordId()); break;
                     }
                 }
                 log.debug("SQL execution succeeded for record {} on attempt {}", diff.getRecordId(), attempt + 1);
                 return true;
            } catch (Exception e) {
                attempt++;
                if (attempt > maxRetries) {
                    log.warn("SQL execution failed for record {} on attempt {}: {}", diff.getRecordId(), attempt, e.getMessage());
                    return false;
                }
                log.info("Retrying SQL execution for record {} in {} ms (attempt {}/{})", diff.getRecordId(), delayMs, attempt, maxRetries);
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("SQL retry interrupted for record {}", diff.getRecordId());
                    return false;
                }
                delayMs = (long) (delayMs * backoffMultiplier);
            }
        }
        return false;
    }
    
    public OperationType determineOperationType(DifferenceRecord diff, String targetSourceId) {
        // Check if target source exists
        List<String> missingSources = diff.getMissingSources();
        if (missingSources == null) {
            missingSources = new ArrayList<>();
        }

        // Determine if record should exist
        // Priority 1: Use winning source if available
        // Priority 2: Fall back to checking if there are non-match-key values
        boolean recordShouldExist = false;
        if (diff.getResolutionResult() != null) {
            String winningSource = diff.getResolutionResult().getWinningSource();
            if (winningSource != null) {
                // Use winning source to determine if record should exist
                recordShouldExist = !missingSources.contains(winningSource);
            } else {
                // No winning source, check if there are non-match-key values
                recordShouldExist = hasNonMatchKeyValues(diff);
            }
        }
        // If resolution result is null, recordShouldExist remains false

        boolean targetExists = !missingSources.contains(targetSourceId);
        
        // Determine operation based on resolution result and target existence
        // Rule 1: Resolution says record should exist, but target doesn't exist -> INSERT
        if (recordShouldExist && !targetExists) {
            return OperationType.INSERT;
        }
        
        // Rule 2: Resolution says record should NOT exist, but target exists -> DELETE
        if (!recordShouldExist && targetExists) {
            return OperationType.DELETE;
        }
        
        // Rule 3: All other cases -> UPDATE
        // This includes:
        // - Record should exist and target exists (normal update case)
        // - Record should not exist and target doesn't exist (no operation needed)
        return OperationType.UPDATE;
    }
    
    private boolean validateRecordExists(AbstractDataSourcePlugin plugin, BaseDataSourceDTO dto, DataSourceConfig targetDataSource, DifferenceRecord diff, List<String> matchKeys, Map<String, String> fieldMappings) {
        try {
            Map<String, Object> matchKeyValues = diff.getMatchKeyValues();
            Map<String, String> reverseMappings = new HashMap<>();
            if (fieldMappings != null) {
                for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
                    reverseMappings.put(entry.getValue(), entry.getKey());
                }
            }
            
            StringBuilder whereClause = new StringBuilder();
            for (String matchKey : matchKeys) {
                Object value = matchKeyValues.get(matchKey);
                if (value == null) {
                    log.warn("Match key value is null for key: {}, record: {}", matchKey, diff.getRecordId());
                    return false;
                }
                String columnName = reverseMappings.getOrDefault(matchKey, matchKey);
                if (whereClause.length() > 0) {
                    whereClause.append(" AND ");
                }
                whereClause.append(columnName).append(" = ").append(formatValue(value));
            }
            
            if (whereClause.length() == 0) {
                log.warn("No valid match keys for record {}", diff.getRecordId());
                return false;
            }
            
            String query = String.format("SELECT 1 FROM %s WHERE %s", targetDataSource.getTableName(), whereClause);
            Table<Map<String, Object>> result = plugin.executeQuerySql(dto, query, true);
            return result != null && result.getBodies() != null && !result.getBodies().isEmpty();
        } catch (Exception e) {
            log.warn("Failed to validate record existence for {}: {}", diff.getRecordId(), e.getMessage());
            return false;
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
    
    private String buildInsertStatement(DataSourceConfig targetDataSource, DifferenceRecord diff, List<String> matchKeys, Map<String, String> fieldMappings) {
        String tableName = targetDataSource.getTableName();
        Map<String, Object> resolvedValues = diff.getResolutionResult().getResolvedValues();
        Map<String, Object> matchKeyValues = diff.getMatchKeyValues();
        
        // Combine match keys and resolved values for INSERT
        Map<String, Object> allValues = new HashMap<>();
        if (matchKeyValues != null) {
            allValues.putAll(matchKeyValues);
        }
        if (resolvedValues != null) {
            allValues.putAll(resolvedValues);
        }
        
        // Apply field mappings
        Map<String, String> reverseMappings = new HashMap<>();
        if (fieldMappings != null) {
            for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
                reverseMappings.put(entry.getValue(), entry.getKey());
            }
        }
        
        // Build column list and value list
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        
        for (Map.Entry<String, Object> entry : allValues.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();
            String columnName = reverseMappings.getOrDefault(field, field);
            
            if (columns.length() > 0) {
                columns.append(", ");
                values.append(", ");
            }
            columns.append(columnName);
            values.append(formatValue(value));
        }
        
        if (columns.length() == 0) {
            throw new IllegalArgumentException("No values to insert for record " + diff.getRecordId());
        }
        
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values);
    }
    
    private String buildDeleteStatement(DataSourceConfig targetDataSource, DifferenceRecord diff, List<String> matchKeys, Map<String, String> fieldMappings) {
        String tableName = targetDataSource.getTableName();
        Map<String, Object> matchKeyValues = diff.getMatchKeyValues();
        
        // Apply field mappings
        Map<String, String> reverseMappings = new HashMap<>();
        if (fieldMappings != null) {
            for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
                reverseMappings.put(entry.getValue(), entry.getKey());
            }
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

        return String.format("DELETE FROM %s WHERE %s", tableName, whereClause);
    }

    private boolean hasNonMatchKeyValues(DifferenceRecord diff) {
        ResolutionResult resolutionResult = diff.getResolutionResult();
        Map<String, Object> resolvedValues = resolutionResult != null ? resolutionResult.getResolvedValues() : null;
        if (resolvedValues == null || resolvedValues.isEmpty()) {
            return false;
        }
        Set<String> matchKeysSet = new HashSet<>(diff.getMatchKeyValues() != null ? 
                diff.getMatchKeyValues().keySet() : Collections.emptySet());
        for (Map.Entry<String, Object> entry : resolvedValues.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();
            if (!matchKeysSet.contains(field) && value != null) {
                return true;
            }
        }
        return false;
    }
    
    private Map<String, Object> fetchTargetCurrentValues(AbstractDataSourcePlugin plugin, BaseDataSourceDTO dto, DataSourceConfig targetDataSource, DifferenceRecord diff, List<String> matchKeys, Map<String, String> fieldMappings) {
        try {
            Map<String, Object> matchKeyValues = diff.getMatchKeyValues();
            Map<String, String> reverseMappings = new HashMap<>();
            if (fieldMappings != null) {
                for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
                    reverseMappings.put(entry.getValue(), entry.getKey());
                }
            }
            
            StringBuilder whereClause = new StringBuilder();
            for (String matchKey : matchKeys) {
                Object value = matchKeyValues.get(matchKey);
                if (value == null) {
                    log.warn("Match key value is null for key: {}, record: {}", matchKey, diff.getRecordId());
                    return Collections.emptyMap();
                }
                String columnName = reverseMappings.getOrDefault(matchKey, matchKey);
                if (whereClause.length() > 0) {
                    whereClause.append(" AND ");
                }
                whereClause.append(columnName).append(" = ").append(formatValue(value));
            }
            
            if (whereClause.length() == 0) {
                log.warn("No valid match keys for record {}", diff.getRecordId());
                return Collections.emptyMap();
            }
            
            String query = String.format("SELECT * FROM %s WHERE %s", targetDataSource.getTableName(), whereClause);
            Table<Map<String, Object>> result = plugin.executeQuerySql(dto, query, true);
            if (result != null && result.getBodies() != null && !result.getBodies().isEmpty()) {
                Map<String, Object> targetValues = result.getBodies().get(0);
                
                // Apply reverse mapping to convert column names back to field names
                Map<String, Object> mappedValues = new HashMap<>();
                for (Map.Entry<String, Object> entry : targetValues.entrySet()) {
                    String columnName = entry.getKey();
                    Object value = entry.getValue();
                    // Find the field name that maps to this column
                    String fieldName = columnName;
                    for (Map.Entry<String, String> mapping : fieldMappings.entrySet()) {
                        if (mapping.getValue().equals(columnName)) {
                            fieldName = mapping.getKey();
                            break;
                        }
                    }
                    mappedValues.put(fieldName, value);
                }
                return mappedValues;
            }
            return Collections.emptyMap();
        } catch (Exception e) {
            log.warn("Failed to fetch target current values for {}: {}", diff.getRecordId(), e.getMessage());
            return Collections.emptyMap();
        }
    }
    
    private boolean valuesAreEqual(Map<String, Object> resolvedValues, Map<String, Object> targetValues, Map<String, Object> matchKeyValues) {
        if (resolvedValues == null && targetValues == null) {
            return true;
        }
        if (resolvedValues == null || targetValues == null) {
            return false;
        }
        
        // Only compare fields present in resolved values (excluding match keys)
        for (Map.Entry<String, Object> entry : resolvedValues.entrySet()) {
            String field = entry.getKey();
            Object resolvedValue = entry.getValue();
            
            // Skip match keys in comparison
            if (matchKeyValues != null && matchKeyValues.containsKey(field)) {
                continue;
            }
            
            Object targetValue = targetValues.get(field);
            
            // Compare values, handling nulls
            if (resolvedValue == null && targetValue == null) {
                continue;
            }
            if (resolvedValue == null || targetValue == null) {
                return false;
            }
            if (!resolvedValue.equals(targetValue)) {
                return false;
            }
        }
        return true;
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