package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import com.jdragon.aggregation.core.consistency.model.UpdatePlan;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class HighConfidenceUpdatePlanningStrategyTest {

    private final HighConfidenceUpdatePlanningStrategy strategy = new HighConfidenceUpdatePlanningStrategy();

    @Test
    public void testUsesHighestConfidenceNonTargetSourceForInsert() {
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("source-1"),
                createSourceValues(
                        "source-1", createRow("user_id", 7, "username", "wujiu"),
                        "source-2", createRow("user_id", 7, "username", "wujiu", "age", 28, "salary", 13000),
                        "source-3", createRow("user_id", 7, "username", "wujiu", "age", 27, "salary", 11500)
                )
        );

        UpdatePlan plan = strategy.plan(diff, createRule(), createTargetSource("source-1"));

        assertEquals("source-2", plan.getReferenceSourceId());
        assertEquals("INSERT", plan.getOperationType());
        assertEquals(28, plan.getResolvedValues().get("age"));
        assertEquals(13000, plan.getResolvedValues().get("salary"));
    }

    @Test
    public void testUsesHighestConfidenceNonTargetSourceForDelete() {
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("source-2"),
                createSourceValues(
                        "source-1", createRow("user_id", 9, "username", "orphan", "age", 35),
                        "source-2", createRow("user_id", 9, "username", "orphan"),
                        "source-3", createRow("user_id", 9, "username", "orphan", "age", 35)
                )
        );

        UpdatePlan plan = strategy.plan(diff, createRule(), createTargetSource("source-1"));

        assertEquals("source-2", plan.getReferenceSourceId());
        assertEquals("DELETE", plan.getOperationType());
    }

    @Test
    public void testUsesHighestConfidenceNonTargetSourceForUpdate() {
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList(),
                createSourceValues(
                        "source-1", createRow("user_id", 1, "username", "zhangsan", "age", 20, "salary", 10000),
                        "source-2", createRow("user_id", 1, "username", "zhangsan", "age", 21, "salary", 12000),
                        "source-3", createRow("user_id", 1, "username", "zhangsan", "age", 19, "salary", 9000)
                )
        );

        UpdatePlan plan = strategy.plan(diff, createRule(), createTargetSource("source-1"));

        assertEquals("source-2", plan.getReferenceSourceId());
        assertEquals("UPDATE", plan.getOperationType());
        assertEquals(21, plan.getResolvedValues().get("age"));
    }

    @Test
    public void testSkipsWhenTargetAndAuthoritativeSourceAreBothMissing() {
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("source-1", "source-2"),
                createSourceValues(
                        "source-1", createRow("user_id", 10, "username", "only-third"),
                        "source-2", createRow("user_id", 10, "username", "only-third"),
                        "source-3", createRow("user_id", 10, "username", "only-third", "age", 22)
                )
        );

        UpdatePlan plan = strategy.plan(diff, createRule(), createTargetSource("source-1"));

        assertEquals("source-2", plan.getReferenceSourceId());
        assertEquals("SKIP", plan.getOperationType());
    }

    private ConsistencyRule createRule() {
        ConsistencyRule rule = new ConsistencyRule();
        rule.setAutoApplyResolutions(true);
        rule.setUpdateTargetSourceId("source-1");
        rule.setConflictResolutionStrategy(ConflictResolutionStrategy.HIGH_CONFIDENCE);
        rule.setDataSources(Arrays.asList(
                createSourceConfig("source-1", 1.0, 1),
                createSourceConfig("source-2", 1.8, 2),
                createSourceConfig("source-3", 0.9, 3)
        ));
        return rule;
    }

    private DataSourceConfig createTargetSource(String sourceId) {
        DataSourceConfig config = new DataSourceConfig();
        config.setSourceId(sourceId);
        return config;
    }

    private DataSourceConfig createSourceConfig(String sourceId, double confidence, int priority) {
        DataSourceConfig config = new DataSourceConfig();
        config.setSourceId(sourceId);
        config.setConfidenceWeight(confidence);
        config.setPriority(priority);
        return config;
    }

    private DifferenceRecord createDifferenceRecord(Iterable<String> missingSources,
                                                    Map<String, Map<String, Object>> sourceValues) {
        DifferenceRecord differenceRecord = new DifferenceRecord();
        differenceRecord.setSourceValues(sourceValues);
        List<String> missing = new ArrayList<>();
        for (String missingSource : missingSources) {
            missing.add(missingSource);
        }
        differenceRecord.setMissingSources(missing);
        differenceRecord.setMatchKeyValues(createRow("user_id", sourceValues.get("source-3").get("user_id"),
                "username", sourceValues.get("source-3").get("username")));
        ResolutionResult resolutionResult = new ResolutionResult();
        resolutionResult.setResolvedValues(new LinkedHashMap<>(sourceValues.get("source-3")));
        differenceRecord.setResolutionResult(resolutionResult);
        return differenceRecord;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Object>> createSourceValues(Object... keyValuePairs) {
        Map<String, Map<String, Object>> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            values.put((String) keyValuePairs[i], (Map<String, Object>) keyValuePairs[i + 1]);
        }
        return values;
    }

    private Map<String, Object> createRow(Object... keyValuePairs) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            row.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return row;
    }
}
