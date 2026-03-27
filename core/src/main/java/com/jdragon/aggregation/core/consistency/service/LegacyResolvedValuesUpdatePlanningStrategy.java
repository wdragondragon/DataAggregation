package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.UpdatePlan;

import java.util.LinkedHashMap;

public class LegacyResolvedValuesUpdatePlanningStrategy implements UpdatePlanningStrategy {

    @Override
    public boolean supports(ConflictResolutionStrategy strategy) {
        return strategy == null
                || strategy == ConflictResolutionStrategy.WEIGHTED_AVERAGE
                || strategy == ConflictResolutionStrategy.MAJORITY_VOTE
                || strategy == ConflictResolutionStrategy.CUSTOM_RULE
                || strategy == ConflictResolutionStrategy.NO_RESOLVE;
    }

    @Override
    public UpdatePlan plan(DifferenceRecord differenceRecord, ConsistencyRule rule, DataSourceConfig targetDataSource) {
        UpdatePlan updatePlan = new UpdatePlan();
        updatePlan.setTargetSourceId(targetDataSource != null ? targetDataSource.getSourceId() : null);
        updatePlan.setReferenceSourceId(differenceRecord != null && differenceRecord.getResolutionResult() != null
                ? differenceRecord.getResolutionResult().getWinningSource()
                : null);
        updatePlan.setOperationType(LegacyUpdatePlanningSupport
                .determineOperationType(differenceRecord, updatePlan.getTargetSourceId()).name());
        if (differenceRecord != null && differenceRecord.getResolutionResult() != null
                && differenceRecord.getResolutionResult().getResolvedValues() != null) {
            updatePlan.setResolvedValues(new LinkedHashMap<>(differenceRecord.getResolutionResult().getResolvedValues()));
        }
        if (differenceRecord != null && differenceRecord.getMatchKeyValues() != null) {
            updatePlan.setMatchKeyValues(new LinkedHashMap<>(differenceRecord.getMatchKeyValues()));
        }
        updatePlan.setReason("planned from legacy resolved-values behavior");
        return updatePlan;
    }
}
