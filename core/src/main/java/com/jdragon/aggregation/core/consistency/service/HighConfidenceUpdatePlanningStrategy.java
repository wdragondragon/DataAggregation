package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.UpdatePlan;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

public class HighConfidenceUpdatePlanningStrategy implements UpdatePlanningStrategy {

    @Override
    public boolean supports(ConflictResolutionStrategy strategy) {
        return strategy == ConflictResolutionStrategy.HIGH_CONFIDENCE;
    }

    @Override
    public UpdatePlan plan(DifferenceRecord differenceRecord, ConsistencyRule rule, DataSourceConfig targetDataSource) {
        UpdatePlan updatePlan = new UpdatePlan();
        updatePlan.setTargetSourceId(targetDataSource != null ? targetDataSource.getSourceId() : null);

        DataSourceConfig authoritativeSource = resolveAuthoritativeSource(rule, targetDataSource);
        if (authoritativeSource == null) {
            updatePlan.setOperationType(UpdateExecutor.OperationType.SKIP.name());
            updatePlan.setReason("no authoritative source available for high confidence update");
            copyMatchKeys(differenceRecord, updatePlan);
            return updatePlan;
        }

        String authoritativeSourceId = authoritativeSource.getSourceId();
        updatePlan.setReferenceSourceId(authoritativeSourceId);
        copyMatchKeys(differenceRecord, updatePlan);

        Set<String> missingSources = new HashSet<>(differenceRecord != null && differenceRecord.getMissingSources() != null
                ? differenceRecord.getMissingSources()
                : Collections.emptyList());
        boolean targetExists = updatePlan.getTargetSourceId() != null && !missingSources.contains(updatePlan.getTargetSourceId());
        boolean authoritativeExists = !missingSources.contains(authoritativeSourceId);

        if (authoritativeExists && differenceRecord != null && differenceRecord.getSourceValues() != null) {
            LinkedHashMap<String, Object> authoritativeValues = new LinkedHashMap<>();
            if (differenceRecord.getSourceValues().get(authoritativeSourceId) != null) {
                authoritativeValues.putAll(differenceRecord.getSourceValues().get(authoritativeSourceId));
            }
            updatePlan.setResolvedValues(authoritativeValues);
        }

        if (authoritativeExists && !targetExists) {
            updatePlan.setOperationType(UpdateExecutor.OperationType.INSERT.name());
            updatePlan.setReason("authoritative source exists while target source is missing");
            return updatePlan;
        }
        if (!authoritativeExists && targetExists) {
            updatePlan.setOperationType(UpdateExecutor.OperationType.DELETE.name());
            updatePlan.setReason("authoritative source is missing while target source exists");
            return updatePlan;
        }
        if (authoritativeExists) {
            updatePlan.setOperationType(UpdateExecutor.OperationType.UPDATE.name());
            updatePlan.setReason("authoritative source exists and target source should align with it");
            return updatePlan;
        }
        updatePlan.setOperationType(UpdateExecutor.OperationType.SKIP.name());
        updatePlan.setReason("authoritative source and target source are both missing");
        return updatePlan;
    }

    private DataSourceConfig resolveAuthoritativeSource(ConsistencyRule rule, DataSourceConfig targetDataSource) {
        if (rule == null || rule.getDataSources() == null || rule.getDataSources().isEmpty()) {
            return null;
        }
        String targetSourceId = targetDataSource != null ? targetDataSource.getSourceId() : rule.getUpdateTargetSourceId();
        if (targetSourceId == null) {
            return null;
        }
        return rule.getDataSources().stream()
                .filter(config -> config != null && config.getSourceId() != null)
                .filter(config -> !targetSourceId.equals(config.getSourceId()))
                .max(Comparator
                        .comparing((DataSourceConfig config) -> config.getConfidenceWeight() != null ? config.getConfidenceWeight() : 1.0d)
                        .thenComparingInt(DataSourceConfig::getPriority))
                .orElse(null);
    }

    private void copyMatchKeys(DifferenceRecord differenceRecord, UpdatePlan updatePlan) {
        if (differenceRecord != null && differenceRecord.getMatchKeyValues() != null) {
            updatePlan.setMatchKeyValues(new LinkedHashMap<>(differenceRecord.getMatchKeyValues()));
        }
    }
}
