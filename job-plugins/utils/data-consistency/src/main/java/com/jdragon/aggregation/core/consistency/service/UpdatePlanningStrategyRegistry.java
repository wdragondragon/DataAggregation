package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class UpdatePlanningStrategyRegistry {

    private static final UpdatePlanningStrategyRegistry INSTANCE = new UpdatePlanningStrategyRegistry();

    private final List<UpdatePlanningStrategy> strategies = new CopyOnWriteArrayList<>();
    private final UpdatePlanningStrategy fallbackStrategy = new LegacyResolvedValuesUpdatePlanningStrategy();

    private UpdatePlanningStrategyRegistry() {
        strategies.add(new HighConfidenceUpdatePlanningStrategy());
        strategies.add(new ManualReviewUpdatePlanningStrategy());
    }

    public static UpdatePlanningStrategyRegistry getInstance() {
        return INSTANCE;
    }

    public UpdatePlanningStrategy findStrategy(ConflictResolutionStrategy strategy) {
        if (strategy != null) {
            for (UpdatePlanningStrategy candidate : strategies) {
                if (candidate.supports(strategy)) {
                    return candidate;
                }
            }
        }
        return fallbackStrategy;
    }

    public void register(UpdatePlanningStrategy strategy) {
        if (strategy != null) {
            strategies.add(0, strategy);
        }
    }
}
