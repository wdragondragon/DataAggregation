package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;

public final class UpdatePlanningStrategyFactory {

    private UpdatePlanningStrategyFactory() {
    }

    public static UpdatePlanningStrategy createStrategy(ConsistencyRule rule) {
        return UpdatePlanningStrategyRegistry.getInstance().findStrategy(
                rule != null ? rule.getConflictResolutionStrategy() : null
        );
    }
}
