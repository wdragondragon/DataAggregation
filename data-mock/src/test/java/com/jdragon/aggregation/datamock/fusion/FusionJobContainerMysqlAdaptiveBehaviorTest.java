package com.jdragon.aggregation.datamock.fusion;

import com.jdragon.aggregation.core.sortmerge.SortMergeStats;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.datamock.support.MysqlIntegrationAssumptions;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FusionJobContainerMysqlAdaptiveBehaviorTest {

    @BeforeClass
    public static void assumeMysqlIntegrationEnabled() {
        MysqlIntegrationAssumptions.assumeEnabled(FusionJobContainerMysqlAdaptiveBehaviorTest.class);
    }

    @Test
    public void shouldMergeSparseOutOfOrderKeysThroughJobContainer() throws Exception {
        FusionJobContainerMysqlTestSupport.ScenarioOptions options =
                FusionJobContainerMysqlTestSupport.ScenarioOptions.defaults()
                        .withPreferOrderedQuery(false)
                        .withPendingKeyThreshold(4096)
                        .withPendingMemoryMB(64)
                        .withOverflowPartitionCount(8)
                        .enableSparseOutOfOrder("sourceB", 128);

        FusionJobContainerMysqlTestSupport.ScenarioResult result =
                FusionJobContainerMysqlTestSupport.runScenario(
                        "fusion_job_sparse_out_of_order",
                        2_000,
                        FusionJobContainerMysqlTestSupport.benchmarkRoot("fusion_job_sparse_out_of_order_it"),
                        false,
                        options
                );

        assertSuccessfulOutput(result, false);
        SortMergeStats stats = result.getSortMergeStats();
        assertNotNull(stats);
        assertEquals("sortmerge", stats.getExecutionEngine());
        assertTrue("pending window should hold at least one key under sparse out-of-order input",
                stats.getPendingPeakKeyCount() > 0L);
        assertTrue("complete keys should still resolve during scan",
                stats.getWindowImmediateResolvedKeyCount() > 0L);
        assertEquals(0L, stats.getMergeSpilledKeyCount());
        assertEquals(0L, stats.getSpillBytes());
    }

    @Test
    public void shouldSpillAndStillFuseCorrectlyWhenDisorderExceedsLocalBuffer() throws Exception {
        FusionJobContainerMysqlTestSupport.ScenarioOptions options =
                FusionJobContainerMysqlTestSupport.ScenarioOptions.defaults()
                        .withJoinType(FusionConfig.JoinType.INNER)
                        .withPreferOrderedQuery(false)
                        .withPendingKeyThreshold(1)
                        .withPendingMemoryMB(64)
                        .withOverflowPartitionCount(4)
                        .withMaxSpillBytesMB(128)
                        .withMinFreeDiskMB(1)
                        .enableWindowReverseDisorder("sourceB", 5_000);

        FusionJobContainerMysqlTestSupport.ScenarioResult result =
                FusionJobContainerMysqlTestSupport.runScenario(
                        "fusion_job_spill_recovery",
                        5_000,
                        FusionJobContainerMysqlTestSupport.benchmarkRoot("fusion_job_spill_recovery_it"),
                        false,
                        options
                );

        assertSuccessfulOutput(result, false);
        SortMergeStats stats = result.getSortMergeStats();
        assertNotNull(stats);
        assertEquals("hybrid", stats.getExecutionEngine());
        assertTrue("spill should reserve bytes", stats.getSpillBytes() > 0L);
        assertTrue("spill should move some keys out of memory", stats.getMergeSpilledKeyCount() > 0L);
        assertTrue("window eviction count should reflect spilled pending keys", stats.getWindowEvictedKeyCount() > 0L);
    }

    @Test
    public void shouldReleaseActiveSpillQuotaAfterHybridSpillCompletes() throws Exception {
        FusionJobContainerMysqlTestSupport.ScenarioOptions options =
                FusionJobContainerMysqlTestSupport.ScenarioOptions.defaults()
                        .withJoinType(FusionConfig.JoinType.INNER)
                        .withPreferOrderedQuery(false)
                        .withPendingKeyThreshold(1)
                        .withPendingMemoryMB(64)
                        .withOverflowPartitionCount(4)
                        .withMaxSpillBytesMB(128)
                        .withMinFreeDiskMB(1)
                        .enableWindowReverseDisorder("sourceB", 8_000);

        FusionJobContainerMysqlTestSupport.ScenarioResult result =
                FusionJobContainerMysqlTestSupport.runScenario(
                        "fusion_job_spill_cleanup",
                        8_000,
                        FusionJobContainerMysqlTestSupport.benchmarkRoot("fusion_job_spill_cleanup_it"),
                        false,
                        options
                );

        assertSuccessfulOutput(result, false);
        SortMergeStats stats = result.getSortMergeStats();
        assertNotNull(stats);
        assertTrue("hybrid spill should reserve cumulative spill bytes", stats.getSpillBytes() > 0L);
        assertTrue("late rows for spilled keys should bypass memory window", stats.getSpillLateArrivalKeyCount() > 0L);
        assertEquals("active spill quota should be fully released after job cleanup", 0L, stats.getActiveSpillBytes());
    }

    private void assertSuccessfulOutput(FusionJobContainerMysqlTestSupport.ScenarioResult result,
                                        boolean assertOrderedRows) {
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

        for (Map.Entry<Long, List<String>> entry : result.getExpectedMetrics().getSampleRows().entrySet()) {
            assertEquals("sample row mismatch for biz_id=" + entry.getKey(),
                    entry.getValue(),
                    result.getActualOutput().getSampleRows().get(entry.getKey()));
        }

        if (assertOrderedRows && !result.getExpectedMetrics().getOrderedRows().isEmpty()) {
            assertEquals(result.getExpectedMetrics().getOrderedRows(), result.getActualOutput().getOrderedRows());
        }
    }
}
