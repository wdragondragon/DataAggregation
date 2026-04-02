package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.*;
import lombok.extern.slf4j.Slf4j;

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
        if (!rule.getEnabled()) {
            log.warn("Rule {} is disabled, skipping execution", rule.getRuleId());
            ComparisonResult result = new ComparisonResult();
            result.setRuleId(rule.getRuleId());
            result.setStatus(ComparisonResult.Status.FAILED);
            result.getSummary().put("error", "Rule is disabled");
            return result;
        }

        log.info("Executing consistency rule: {} - {}", rule.getRuleId(), rule.getRuleName());
        try {
            DataFetcher fetcher = this.dataFetcher != null ? this.dataFetcher : new DataFetcher(pluginManager, rule.getParallelFetch());
            this.dataFetcher = fetcher;
            AdaptiveSortMergeConsistencyExecutor executor = new AdaptiveSortMergeConsistencyExecutor(pluginManager, resultRecorder);
            ComparisonResult result = executor.execute(rule);
            log.info("Rule execution completed: {} - {} inconsistent records found, {} resolved",
                    rule.getRuleId(), result.getInconsistentRecords(), result.getResolvedRecords());
            return result;
        } catch (Exception e) {
            log.error("Failed to execute consistency rule: {}", rule.getRuleId(), e);
            ComparisonResult result = new ComparisonResult();
            result.setRuleId(rule.getRuleId());
            result.setStatus(ComparisonResult.Status.FAILED);
            result.getSummary().put("error", e.getMessage());
            return result;
        }
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
    public void shutdown() {
        pluginManager.clearCache();
        log.info("Data consistency service shutdown");
    }
}
