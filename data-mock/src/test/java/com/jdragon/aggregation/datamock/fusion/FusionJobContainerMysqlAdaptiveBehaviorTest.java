package com.jdragon.aggregation.datamock.fusion;

import com.jdragon.aggregation.core.sortmerge.AdaptiveMergeConfig;
import com.jdragon.aggregation.core.sortmerge.SortMergeStats;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FusionJobContainerMysqlAdaptiveBehaviorTest {

    @Test
    public void shouldAbsorbSparseLocalDisorderThroughJobContainer() throws Exception {
        FusionJobContainerMysqlTestSupport.ScenarioOptions options =
                FusionJobContainerMysqlTestSupport.ScenarioOptions.defaults()
                        .withPreferOrderedQuery(false)
                        .withValidateSourceOrder(true)
                        .withLocalDisorderEnabled(true)
                        .withLocalDisorderMaxGroups(2)
                        .withLocalDisorderMaxMemoryMB(16)
                        .withPendingKeyThreshold(4096)
                        .withPendingMemoryMB(64)
                        .withOverflowPartitionCount(8)
                        .withOnOrderViolation(AdaptiveMergeConfig.OrderViolationAction.RECOVER_LOCAL)
                        .enableSparseLocalDisorder("sourceB", 128);

        FusionJobContainerMysqlTestSupport.ScenarioResult result =
                FusionJobContainerMysqlTestSupport.runScenario(
                        "fusion_job_sparse_local_disorder",
                        2_000,
                        FusionJobContainerMysqlTestSupport.benchmarkRoot("fusion_job_sparse_local_disorder_it"),
                        false,
                        options
                );

        assertSuccessfulOutput(result, false);
        SortMergeStats stats = result.getSortMergeStats();
        assertNotNull(stats);
        assertEquals("sortmerge", stats.getExecutionEngine());
        assertTrue("should observe local reordered groups before source-side buffering absorbs them",
                stats.getLocalReorderedGroupCount() > 0L);
        assertEquals(0L, stats.getOrderRecoveryCount());
        assertEquals(0L, stats.getMergeSpilledKeyCount());
        assertEquals(0L, stats.getSpillBytes());
    }

    @Test
    public void shouldSpillAndStillFuseCorrectlyWhenDisorderExceedsLocalBuffer() throws Exception {
        FusionJobContainerMysqlTestSupport.ScenarioOptions options =
                FusionJobContainerMysqlTestSupport.ScenarioOptions.defaults()
                        .withJoinType(FusionConfig.JoinType.INNER)
                        .withPreferOrderedQuery(false)
                        .withValidateSourceOrder(true)
                        .withLocalDisorderEnabled(false)
                        .withPendingKeyThreshold(1)
                        .withPendingMemoryMB(64)
                        .withOverflowPartitionCount(4)
                        .withMaxSpillBytesMB(128)
                        .withMinFreeDiskMB(1)
                        .withOnOrderViolation(AdaptiveMergeConfig.OrderViolationAction.RECOVER_LOCAL)
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
        assertTrue("local recovery should be triggered under severe disorder", stats.getOrderRecoveryCount() > 0L);
    }

    @Test
    public void shouldReleaseActiveSpillQuotaAfterHybridSpillCompletes() throws Exception {
        FusionJobContainerMysqlTestSupport.ScenarioOptions options =
                FusionJobContainerMysqlTestSupport.ScenarioOptions.defaults()
                        .withJoinType(FusionConfig.JoinType.INNER)
                        .withPreferOrderedQuery(false)
                        .withValidateSourceOrder(true)
                        .withLocalDisorderEnabled(false)
                        .withPendingKeyThreshold(1)
                        .withPendingMemoryMB(64)
                        .withOverflowPartitionCount(4)
                        .withMaxSpillBytesMB(128)
                        .withMinFreeDiskMB(1)
                        .withOnOrderViolation(AdaptiveMergeConfig.OrderViolationAction.RECOVER_LOCAL)
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
