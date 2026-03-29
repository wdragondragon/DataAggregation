package com.jdragon.aggregation.datamock.sortmerge;

import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.sortmerge.AdaptiveMergeConfig;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AdaptiveSortMergeMysqlIntegrationTest {

    @Test
    public void shouldRunConsistencyAndFusionAgainstMysqlDemoSource() throws Exception {
        AdaptiveSortMergeTestSupport.ScenarioResult result = AdaptiveSortMergeTestSupport.runScenario(
                "mysql_small",
                100,
                AdaptiveSortMergeTestSupport.benchmarkRoot("mysql_small_it"),
                true
        );

        assertScenario(result);
    }

    @Test
    public void shouldAbsorbSparseLocalDisorderWithoutEarlyBucket() throws Exception {
        AdaptiveSortMergeTestSupport.ScenarioOptions options = AdaptiveSortMergeTestSupport.ScenarioOptions.defaults()
                .withPreferOrderedQuery(false)
                .withValidateSourceOrder(true)
                .withLocalDisorderEnabled(true)
                .withLocalDisorderMaxGroups(2)
                .withLocalDisorderMaxMemoryMB(16)
                .withPendingKeyThreshold(4096)
                .withPendingMemoryMB(64)
                .withOnOrderViolation(AdaptiveMergeConfig.OrderViolationAction.RECOVER_LOCAL)
                .enableSparseLocalDisorder(128L, "sourceB");

        AdaptiveSortMergeTestSupport.ScenarioResult result = AdaptiveSortMergeTestSupport.runScenario(
                "mysql_sparse_local_disorder",
                2_000,
                AdaptiveSortMergeTestSupport.benchmarkRoot("mysql_sparse_local_disorder_it"),
                true,
                options
        );

        assertScenario(result);
        assertNull(result.getConsistencyExecution().getErrorMessage());
        assertNull(result.getFusionExecution().getErrorMessage());
        assertEquals("sortmerge", result.getConsistencyExecution().getExecutionEngine());
        assertEquals("sortmerge", result.getFusionExecution().getStats().getExecutionEngine());
        assertTrue("should observe local reorder before source-side buffering absorbs it",
                result.getConsistencyExecution().getLocalReorderedGroupCount() > 0L
                        || result.getFusionExecution().getStats().getLocalReorderedGroupCount() > 0L);
        assertEquals(0L, result.getConsistencyExecution().getOrderRecoveryCount());
        assertEquals(0L, result.getFusionExecution().getStats().getOrderRecoveryCount());
    }

    @Test
    public void shouldFailFastWhenSpillQuotaTooSmall() throws Exception {
        AdaptiveSortMergeTestSupport.ScenarioOptions options = AdaptiveSortMergeTestSupport.ScenarioOptions.defaults()
                .withPreferOrderedQuery(false)
                .withValidateSourceOrder(true)
                .withLocalDisorderEnabled(false)
                .withPendingKeyThreshold(64)
                .withPendingMemoryMB(8)
                .withOverflowPartitionCount(4)
                .withMaxSpillBytesMB(1)
                .withMinFreeDiskMB(1)
                .withOnOrderViolation(AdaptiveMergeConfig.OrderViolationAction.RECOVER_LOCAL)
                .enableSparseLocalDisorder(2L, "sourceB");

        AdaptiveSortMergeTestSupport.ScenarioResult result = AdaptiveSortMergeTestSupport.runScenario(
                "mysql_small_spill_guard",
                5_000,
                AdaptiveSortMergeTestSupport.benchmarkRoot("mysql_small_spill_guard_it"),
                true,
                options
        );

        AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
        AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();

        assertNotNull(consistency.getComparisonResult());
        assertEquals(ComparisonResult.Status.FAILED, consistency.getComparisonResult().getStatus());
        assertTrue("consistency spill guard should trigger", consistency.isSpillGuardTriggered());
        assertTrue("consistency spill guard reason should mention spill limit: " + consistency.getSpillGuardReason(),
                consistency.getSpillGuardReason() != null
                        && consistency.getSpillGuardReason().contains("Spill limit exceeded"));
        assertTrue("consistency should reserve some spill bytes", consistency.getSpillBytes() > 0L);
        assertTrue("fusion should fail with spill guard message: " + fusion.getErrorMessage(),
                fusion.getErrorMessage() != null && fusion.getErrorMessage().contains("Spill limit exceeded"));
    }

    private void assertScenario(AdaptiveSortMergeTestSupport.ScenarioResult result) {
        AdaptiveSortMergeTestSupport.DatasetExpectations expectations = result.getExpectations();
        AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
        AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();
        ComparisonResult comparisonResult = consistency.getComparisonResult();

        assertNotNull(comparisonResult);
        assertNull(consistency.getErrorMessage());
        assertNull(fusion.getErrorMessage());
        assertNotNull(fusion.getStats());
        assertEquals(expectations.getTotalKeys(), comparisonResult.getTotalRecords());
        assertEquals(expectations.getExpectedInconsistentKeys(), comparisonResult.getInconsistentRecords());
        assertEquals(expectations.getExpectedDuplicateIgnoredCount(), consistency.getDuplicateIgnoredCount());
        assertEquals(expectations.getFusionOutputCount(), fusion.getOutputCount());
        assertTrue("consistency should use ordered sort-merge path",
                "sortmerge".equals(consistency.getExecutionEngine()) || "hybrid".equals(consistency.getExecutionEngine()));
        assertEquals("sortmerge", fusion.getStats().getExecutionEngine());
        assertEquals(expectations.getTotalKeys(), fusion.getStats().getMergeResolvedKeyCount());
        assertEquals(expectations.getExpectedDuplicateIgnoredCount(), fusion.getStats().getDuplicateIgnoredCount());
        assertEquals(0L, fusion.getStats().getMergeSpilledKeyCount());
        assertNull(fusion.getStats().getFallbackReason());

        for (Map.Entry<Long, List<String>> entry : expectations.getExpectedFusionSamples().entrySet()) {
            assertEquals("sample row mismatch for biz_id=" + entry.getKey(), entry.getValue(), fusion.getSampleRows().get(entry.getKey()));
        }

        if (!expectations.getExpectedOrderedRows().isEmpty()) {
            assertEquals("ordered fusion row count mismatch", expectations.getExpectedOrderedRows().size(), fusion.getOrderedRows().size());
            for (int index = 0; index < expectations.getExpectedOrderedRows().size(); index++) {
                assertEquals("ordered fusion row mismatch at index=" + index,
                        expectations.getExpectedOrderedRows().get(index),
                        fusion.getOrderedRows().get(index));
            }
        }

        assertEquals("fusion digest mismatch, expected sample rows=" + expectations.getExpectedFusionSamples()
                        + ", actual sample rows=" + fusion.getSampleRows()
                        + ", first output keys=" + fusion.getFirstOutputKeys(),
                expectations.getExpectedFusionDigest(),
                fusion.getDigest());
    }
}
