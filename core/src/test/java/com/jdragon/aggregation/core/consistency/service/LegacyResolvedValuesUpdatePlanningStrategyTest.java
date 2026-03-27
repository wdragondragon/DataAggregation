package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import com.jdragon.aggregation.core.consistency.model.UpdatePlan;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LegacyResolvedValuesUpdatePlanningStrategyTest {

    private final LegacyResolvedValuesUpdatePlanningStrategy strategy = new LegacyResolvedValuesUpdatePlanningStrategy();
    private final UpdateExecutor updateExecutor = new UpdateExecutor(null);

    @Test
    public void testPreservesLegacyInsertBehavior() {
        DifferenceRecord diff = createDifferenceRecord(
                createMatchKeys("id", 1, "name", "test"),
                createResolvedValues("id", 1, "name", "test", "age", 25, "salary", 50000),
                Collections.singletonList("target"),
                "source-2"
        );

        UpdatePlan plan = strategy.plan(diff, new ConsistencyRule(), targetSource("target"));

        assertEquals(updateExecutor.determineOperationType(diff, "target").name(), plan.getOperationType());
        assertEquals("source-2", plan.getReferenceSourceId());
    }

    @Test
    public void testPreservesLegacyDeleteBehavior() {
        DifferenceRecord diff = createDifferenceRecord(
                createMatchKeys("id", 1, "name", "test"),
                createResolvedValues("id", 1, "name", "test"),
                Collections.singletonList("source-1"),
                null
        );

        UpdatePlan plan = strategy.plan(diff, new ConsistencyRule(), targetSource("target"));

        assertEquals(updateExecutor.determineOperationType(diff, "target").name(), plan.getOperationType());
    }

    @Test
    public void testPreservesLegacyWinningSourceTolerance() {
        DifferenceRecord diff = createDifferenceRecord(
                createMatchKeys("user_id", "7", "username", "wujiu"),
                createResolvedValues("user_id", "7", "username", "wujiu", "age", null, "salary", null),
                Arrays.asList("target", "source-2"),
                "source-2"
        );
        diff.setSourceValues(new LinkedHashMap<String, Map<String, Object>>() {{
            put("target", createMatchKeys("user_id", "7", "username", "wujiu"));
            put("source-2", createMatchKeys("user_id", "7", "username", "wujiu"));
            put("source-3", createResolvedValues("user_id", "7", "username", "wujiu", "age", null, "salary", null));
        }});

        UpdatePlan plan = strategy.plan(diff, new ConsistencyRule(), targetSource("target"));

        assertEquals(updateExecutor.determineOperationType(diff, "target").name(), plan.getOperationType());
    }

    private DataSourceConfig targetSource(String sourceId) {
        DataSourceConfig config = new DataSourceConfig();
        config.setSourceId(sourceId);
        return config;
    }

    private DifferenceRecord createDifferenceRecord(Map<String, Object> matchKeys,
                                                    Map<String, Object> resolvedValues,
                                                    java.util.List<String> missingSources,
                                                    String winningSource) {
        DifferenceRecord diff = new DifferenceRecord();
        diff.setRecordId("test-record");
        diff.setMatchKeyValues(matchKeys);
        diff.setMissingSources(missingSources);
        ResolutionResult resolution = new ResolutionResult();
        resolution.setResolvedValues(resolvedValues);
        resolution.setWinningSource(winningSource);
        diff.setResolutionResult(resolution);
        return diff;
    }

    private Map<String, Object> createMatchKeys(Object... keyValuePairs) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            map.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return map;
    }

    private Map<String, Object> createResolvedValues(Object... keyValuePairs) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            map.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return map;
    }
}
