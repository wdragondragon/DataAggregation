package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class UpdatePlanningStrategyFactoryTest {

    @Test
    public void testHighConfidenceMapsToDedicatedPlanner() {
        ConsistencyRule rule = new ConsistencyRule();
        rule.setConflictResolutionStrategy(ConflictResolutionStrategy.HIGH_CONFIDENCE);

        assertTrue(UpdatePlanningStrategyFactory.createStrategy(rule) instanceof HighConfidenceUpdatePlanningStrategy);
    }

    @Test
    public void testManualReviewMapsToSkipPlanner() {
        ConsistencyRule rule = new ConsistencyRule();
        rule.setConflictResolutionStrategy(ConflictResolutionStrategy.MANUAL_REVIEW);

        assertTrue(UpdatePlanningStrategyFactory.createStrategy(rule) instanceof ManualReviewUpdatePlanningStrategy);
    }

    @Test
    public void testWeightedAverageFallsBackToLegacyPlanner() {
        ConsistencyRule rule = new ConsistencyRule();
        rule.setConflictResolutionStrategy(ConflictResolutionStrategy.WEIGHTED_AVERAGE);

        assertTrue(UpdatePlanningStrategyFactory.createStrategy(rule) instanceof LegacyResolvedValuesUpdatePlanningStrategy);
    }
}
