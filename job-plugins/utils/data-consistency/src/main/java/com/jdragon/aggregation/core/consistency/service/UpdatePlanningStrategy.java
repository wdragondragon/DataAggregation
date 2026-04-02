package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.UpdatePlan;

public interface UpdatePlanningStrategy {

    boolean supports(ConflictResolutionStrategy strategy);

    UpdatePlan plan(DifferenceRecord differenceRecord, ConsistencyRule rule, DataSourceConfig targetDataSource);
}
