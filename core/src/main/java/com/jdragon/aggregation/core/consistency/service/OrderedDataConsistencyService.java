package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderedDataConsistencyService extends DataConsistencyService {

    private final DataSourcePluginManager pluginManager;
    private final ResultRecorder resultRecorder;

    public OrderedDataConsistencyService() {
        this("./consistency-results");
    }

    public OrderedDataConsistencyService(String outputDirectory) {
        super(outputDirectory);
        this.pluginManager = new DataSourcePluginManager();
        this.resultRecorder = new FileResultRecorder(outputDirectory);
    }

    public ComparisonResult executeSortMergeRule(ConsistencyRule rule) {
        if (!rule.getEnabled()) {
            ComparisonResult result = new ComparisonResult();
            result.setRuleId(rule.getRuleId());
            result.setStatus(ComparisonResult.Status.FAILED);
            result.getSummary().put("error", "Rule is disabled");
            return result;
        }
        try {
            return new AdaptiveSortMergeConsistencyExecutor(pluginManager, resultRecorder).execute(rule);
        } catch (Exception e) {
            log.error("Failed to execute sort-merge consistency rule: {}", rule.getRuleId(), e);
            ComparisonResult result = new ComparisonResult();
            result.setRuleId(rule.getRuleId());
            result.setStatus(ComparisonResult.Status.FAILED);
            result.getSummary().put("error", e.getMessage());
            return result;
        }
    }
}
