package com.jdragon.aggregation.datamock.fusion;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FusionJobContainerMysqlIntegrationTest {

    @Test
    public void shouldFuseTwoMysqlTablesIntoTargetTableThroughJobContainer() throws Exception {
        FusionJobContainerMysqlTestSupport.ScenarioResult result =
                FusionJobContainerMysqlTestSupport.runScenario(
                        "fusion_job_small",
                        100,
                        FusionJobContainerMysqlTestSupport.benchmarkRoot("fusion_job_small_it"),
                        false
                );

        assertAggregate(result);
        assertSamples(result);
        assertEquals(result.getExpectedMetrics().getOrderedRows(), result.getActualOutput().getOrderedRows());
    }

    private void assertAggregate(FusionJobContainerMysqlTestSupport.ScenarioResult result) {
        FusionJobContainerMysqlTestSupport.AggregateResult expected = result.getExpectedMetrics().getAggregate();
        FusionJobContainerMysqlTestSupport.AggregateResult actual = result.getActualOutput().getAggregate();
        assertEquals(expected.getRowCount(), actual.getRowCount());
        assertEquals(expected.getTotalAge(), actual.getTotalAge());
        assertEquals(expected.getTotalActiveFlag(), actual.getTotalActiveFlag());
        assertEquals(expected.getTotalSalary(), actual.getTotalSalary());
        assertEquals(expected.getTotalSalaryDouble(), actual.getTotalSalaryDouble());
        assertEquals(expected.getSeniorCount(), actual.getSeniorCount());
        assertEquals(expected.getScenarioTagCount(), actual.getScenarioTagCount());
        assertEquals(expected.getScenarioTag(), actual.getScenarioTag());
    }

    private void assertSamples(FusionJobContainerMysqlTestSupport.ScenarioResult result) {
        for (Map.Entry<Long, List<String>> entry : result.getExpectedMetrics().getSampleRows().entrySet()) {
            assertEquals("sample row mismatch for biz_id=" + entry.getKey(),
                    entry.getValue(),
                    result.getActualOutput().getSampleRows().get(entry.getKey()));
        }
    }
}
