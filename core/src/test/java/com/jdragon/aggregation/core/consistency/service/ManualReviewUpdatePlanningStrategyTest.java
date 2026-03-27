package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import com.jdragon.aggregation.core.consistency.model.UpdatePlan;
import org.junit.Test;

import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;

public class ManualReviewUpdatePlanningStrategyTest {

    @Test
    public void testManualReviewDefaultsToSkip() {
        ManualReviewUpdatePlanningStrategy strategy = new ManualReviewUpdatePlanningStrategy();
        ConsistencyRule rule = new ConsistencyRule();
        rule.setConflictResolutionStrategy(ConflictResolutionStrategy.MANUAL_REVIEW);

        DifferenceRecord differenceRecord = new DifferenceRecord();
        ResolutionResult resolutionResult = new ResolutionResult();
        resolutionResult.setResolvedValues(new LinkedHashMap<String, Object>() {{
            put("id", 1);
            put("name", "review-me");
        }});
        differenceRecord.setResolutionResult(resolutionResult);

        DataSourceConfig target = new DataSourceConfig();
        target.setSourceId("target");

        UpdatePlan plan = strategy.plan(differenceRecord, rule, target);

        assertEquals("SKIP", plan.getOperationType());
        assertEquals("target", plan.getTargetSourceId());
    }
}
