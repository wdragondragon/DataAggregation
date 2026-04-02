package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.UpdatePlan;

import java.util.LinkedHashMap;

public class ManualReviewUpdatePlanningStrategy implements UpdatePlanningStrategy {

    @Override
    public boolean supports(ConflictResolutionStrategy strategy) {
        return strategy == ConflictResolutionStrategy.MANUAL_REVIEW;
    }

    @Override
    public UpdatePlan plan(DifferenceRecord differenceRecord, ConsistencyRule rule, DataSourceConfig targetDataSource) {
        UpdatePlan updatePlan = new UpdatePlan();
        updatePlan.setOperationType(UpdateExecutor.OperationType.SKIP.name());
        updatePlan.setTargetSourceId(targetDataSource != null ? targetDataSource.getSourceId() : null);
        if (differenceRecord != null && differenceRecord.getResolutionResult() != null
                && differenceRecord.getResolutionResult().getResolvedValues() != null) {
            updatePlan.setResolvedValues(new LinkedHashMap<>(differenceRecord.getResolutionResult().getResolvedValues()));
        }
        if (differenceRecord != null && differenceRecord.getMatchKeyValues() != null) {
            updatePlan.setMatchKeyValues(new LinkedHashMap<>(differenceRecord.getMatchKeyValues()));
        }
        updatePlan.setReason("manual review strategy does not auto apply updates");
        return updatePlan;
    }
}
