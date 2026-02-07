package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.i18n.MessageResource;
import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;
import com.jdragon.aggregation.core.consistency.model.UpdateResult;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DataConsistencyService {
    
    private final DataSourcePluginManager pluginManager;
    private DataFetcher dataFetcher;
    
    private final ConsistencyRuleManager ruleManager;
    
    private final ResultRecorder resultRecorder;
    
    public DataConsistencyService() {
        this.pluginManager = new DataSourcePluginManager();
        this.ruleManager = new ConsistencyRuleManager();
        this.resultRecorder = new FileResultRecorder("./consistency-results");
    }
    
    public DataConsistencyService(String outputDirectory) {
        this.pluginManager = new DataSourcePluginManager();
        this.ruleManager = new ConsistencyRuleManager();
        this.resultRecorder = new FileResultRecorder(outputDirectory);
    }
    
    public DataConsistencyService(DataSourcePluginManager pluginManager,
                                 ConsistencyRuleManager ruleManager,
                                 DataFetcher dataFetcher,
                                 ResultRecorder resultRecorder) {
        this.pluginManager = pluginManager;
        this.ruleManager = ruleManager;
        this.dataFetcher = dataFetcher;
        this.resultRecorder = resultRecorder;
    }
    
    public ComparisonResult executeRule(String ruleId) {
        ConsistencyRule rule = ruleManager.getRule(ruleId);
        if (rule == null) {
            throw new IllegalArgumentException("Rule not found: " + ruleId);
        }
        
        return executeRule(rule);
    }
    
    public ComparisonResult executeRule(ConsistencyRule rule) {
        if (!rule.isEnabled()) {
            log.warn("Rule {} is disabled, skipping execution", rule.getRuleId());
            ComparisonResult result = new ComparisonResult();
            result.setRuleId(rule.getRuleId());
            result.setStatus(ComparisonResult.Status.FAILED);
            result.getSummary().put("error", "Rule is disabled");
            return result;
        }
        
        log.info("Executing consistency rule: {} - {}", rule.getRuleId(), rule.getRuleName());
        
        ComparisonResult result = new ComparisonResult();
        result.setRuleId(rule.getRuleId());
        result.setStatus(ComparisonResult.Status.RUNNING);
        
        try {
            List<DifferenceRecord> resolvedDifferences = new ArrayList<>();
            DataFetcher fetcher = this.dataFetcher != null ? this.dataFetcher : new DataFetcher(pluginManager, rule.isParallelFetch());
            
            Map<String, List<Map<String, Object>>> sourceData = 
                    fetcher.fetchDataFromSources(rule.getDataSources());
            
            Map<String, Map<String, List<Map<String, Object>>>> groupedData = 
                    fetcher.groupByMatchKeys(sourceData, rule.getMatchKeys());
            
            OutputConfig.ReportLanguage language = rule.getOutputConfig() != null ? 
                    rule.getOutputConfig().getReportLanguage() : OutputConfig.ReportLanguage.ENGLISH;
            MessageResource messages = MessageResource.forLanguage(language);
            
            DataComparator comparator = new DataComparator(
                    rule.getToleranceThreshold(), 
                    rule.getCompareFields(),
                    messages);
            
            List<DifferenceRecord> differences = comparator.compareData(groupedData, rule.getMatchKeys());
            List<DifferenceRecord> allDifferences = new ArrayList<>(differences);
            
            result.setTotalRecords(calculateTotalRecords(groupedData));
            result.setConsistentRecords(result.getTotalRecords() - differences.size());
            result.setInconsistentRecords(differences.size());
            
            if (!differences.isEmpty()) {
                ConflictResolver resolver = ConflictResolverFactory.createResolver(
                        rule.getConflictResolutionStrategy(),
                        rule.getDataSources(),
                        rule.getResolutionParams());
                
                for (DifferenceRecord diff : differences) {
                    if (resolver.canResolve(diff)) {
                        diff.setResolutionResult(resolver.resolve(diff));
                        resolvedDifferences.add(diff);
                        result.incrementResolved();
                        
                        if (diff.getResolutionResult() != null && diff.getResolutionResult().getResolvedValues() != null) {
                            Map<String, Object> resolvedRow = new HashMap<>(diff.getResolutionResult().getResolvedValues());
                            // Include match keys for identification
                            if (diff.getMatchKeyValues() != null) {
                                resolvedRow.putAll(diff.getMatchKeyValues());
                            }
                            result.getResolvedRows().add(resolvedRow);
                        }
                    }
                }
            }
            
            result.setStatus(differences.isEmpty() ? 
                    ComparisonResult.Status.SUCCESS : 
                    ComparisonResult.Status.PARTIAL_SUCCESS);
            
            // Execute updates if configured
            if (rule.isAutoApplyResolutions() && rule.getUpdateTargetSourceId() != null 
                    && !resolvedDifferences.isEmpty()) {
                try {
                    DataSourceConfig targetDataSource = findTargetDataSource(rule);
                    if (targetDataSource != null) {
                        UpdateExecutor updateExecutor = new UpdateExecutor(pluginManager);
                        UpdateResult updateResult = updateExecutor.executeUpdates(
                                targetDataSource,
                                resolvedDifferences,
                                rule.getMatchKeys(),
                                targetDataSource.getFieldMappings(),
                                rule.getUpdateBufferSize() != null ? rule.getUpdateBufferSize() : 100
                        );
                        result.setUpdateResult(updateResult);
                        
                        log.info("Updates executed: {} successful, {} failed",
                                updateResult.getSuccessfulUpdates(), updateResult.getFailedUpdates());
                    } else {
                        log.warn("Target data source not found: {}", rule.getUpdateTargetSourceId());
                    }
                } catch (Exception e) {
                    log.error("Failed to execute updates: {}", e.getMessage(), e);
                    // Continue with recording results even if updates fail
                }
            }
            
            recordResults(result, allDifferences, resolvedDifferences, rule);
            
            log.info("Rule execution completed: {} - {} inconsistent records found, {} resolved",
                    rule.getRuleId(), differences.size(), resolvedDifferences.size());
            
        } catch (Exception e) {
            log.error("Failed to execute consistency rule: {}", rule.getRuleId(), e);
            result.setStatus(ComparisonResult.Status.FAILED);
            result.getSummary().put("error", e.getMessage());
        }
        
        return result;
    }
    
    private int calculateTotalRecords(Map<String, Map<String, List<Map<String, Object>>>> groupedData) {
        int total = 0;
        for (Map<String, List<Map<String, Object>>> sourceGroup : groupedData.values()) {
            for (List<Map<String, Object>> records : sourceGroup.values()) {
                total += records.size();
            }
        }
        return total;
    }
    
    private void recordResults(ComparisonResult result, 
                              List<DifferenceRecord> allDifferences,
                              List<DifferenceRecord> resolvedDifferences,
                              ConsistencyRule rule) {
        
        resultRecorder.recordComparisonResult(result);
        
        if (!allDifferences.isEmpty()) {
            resultRecorder.recordDifferences(allDifferences);
        }
        
        if (!resolvedDifferences.isEmpty()) {
            resultRecorder.recordResolutionResults(resolvedDifferences);
        }
        
        OutputConfig outputConfig = rule != null ? rule.getOutputConfig() : null;
        String reportPath = resultRecorder.generateReport(result, allDifferences, outputConfig);
        result.setReportPath(reportPath);
    }
    
    public void addRule(ConsistencyRule rule) {
        ruleManager.addRule(rule);
    }
    
    public void updateRule(ConsistencyRule rule) {
        ruleManager.updateRule(rule);
    }
    
    public void deleteRule(String ruleId) {
        ruleManager.deleteRule(ruleId);
    }
    
    public ConsistencyRule getRule(String ruleId) {
        return ruleManager.getRule(ruleId);
    }
    
    private DataSourceConfig findTargetDataSource(ConsistencyRule rule) {
        String targetSourceId = rule.getUpdateTargetSourceId();
        if (targetSourceId == null) {
            return null;
        }
        
        for (DataSourceConfig dsConfig : rule.getDataSources()) {
            if (targetSourceId.equals(dsConfig.getSourceId())) {
                return dsConfig;
            }
        }
        return null;
    }
    
    public void shutdown() {
        pluginManager.clearCache();
        log.info("Data consistency service shutdown");
    }
}