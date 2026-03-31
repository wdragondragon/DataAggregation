package com.jdragon.aggregation.datamock.sortmerge;

import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
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

        assertScenario(result, false);
    }

    @Test
    public void shouldMergeSparseOutOfOrderKeysWithoutSpill() throws Exception {
        AdaptiveSortMergeTestSupport.ScenarioOptions options = AdaptiveSortMergeTestSupport.ScenarioOptions.defaults()
                .withPreferOrderedQuery(false)
                .withPendingKeyThreshold(4096)
                .withPendingMemoryMB(64)
                .enableSparseOutOfOrder(128L, "sourceB");

        AdaptiveSortMergeTestSupport.ScenarioResult result = AdaptiveSortMergeTestSupport.runScenario(
                "mysql_sparse_out_of_order",
                2_000,
                AdaptiveSortMergeTestSupport.benchmarkRoot("mysql_sparse_out_of_order_it"),
                true,
                options
        );

        assertScenario(result, false);
        assertNull(result.getConsistencyExecution().getErrorMessage());
        assertNull(result.getFusionExecution().getErrorMessage());
        assertEquals("sortmerge", result.getConsistencyExecution().getExecutionEngine());
        assertEquals("sortmerge", result.getFusionExecution().getStats().getExecutionEngine());
        assertTrue("pending window should capture some out-of-order keys",
                result.getConsistencyExecution().getPendingPeakKeyCount() > 0L
                        || result.getFusionExecution().getStats().getPendingPeakKeyCount() > 0L);
        assertEquals(0L, result.getConsistencyExecution().getWindowEvictedKeyCount());
        assertEquals(0L, result.getFusionExecution().getStats().getWindowEvictedKeyCount());
    }

    @Test
    public void shouldSpillWhenPendingWindowIsTight() throws Exception {
        AdaptiveSortMergeTestSupport.ScenarioOptions options = AdaptiveSortMergeTestSupport.ScenarioOptions.defaults()
                .withPreferOrderedQuery(false)
                .withPendingKeyThreshold(8)
                .withPendingMemoryMB(8)
                .withOverflowPartitionCount(4)
                .withMaxSpillBytesMB(128)
                .withMinFreeDiskMB(1)
                .enableSparseOutOfOrder(2L, "sourceB");

        AdaptiveSortMergeTestSupport.ScenarioResult result = AdaptiveSortMergeTestSupport.runScenario(
                "mysql_tight_pending_spill",
                2_000,
                AdaptiveSortMergeTestSupport.benchmarkRoot("mysql_tight_pending_spill_it"),
                true,
                options
        );

        assertSpillScenario(result);
        AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
        AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();

        assertNotNull(consistency.getComparisonResult());
        assertTrue("consistency should either stay sortmerge or degrade to hybrid under tight window",
                "sortmerge".equals(consistency.getExecutionEngine())
                        || "hybrid".equals(consistency.getExecutionEngine()));
        assertTrue("fusion should either stay sortmerge or degrade to hybrid under tight window",
                "sortmerge".equals(fusion.getStats().getExecutionEngine())
                        || "hybrid".equals(fusion.getStats().getExecutionEngine()));
        assertTrue("at least one execution path should spill when pending window is tiny",
                consistency.getMergeSpilledKeyCount() > 0L
                        || fusion.getStats().getMergeSpilledKeyCount() > 0L);
        assertTrue("tight-window spill should reserve some bytes",
                consistency.getSpillBytes() > 0L || fusion.getStats().getSpillBytes() > 0L);
    }

    private void assertScenario(AdaptiveSortMergeTestSupport.ScenarioResult result, boolean spillAllowed) {
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
        assertTrue("fusion should use sortmerge or hybrid path",
                "sortmerge".equals(fusion.getStats().getExecutionEngine())
                        || "hybrid".equals(fusion.getStats().getExecutionEngine()));
        assertEquals(expectations.getTotalKeys(), fusion.getStats().getMergeResolvedKeyCount());
        assertEquals(expectations.getExpectedDuplicateIgnoredCount(), fusion.getStats().getDuplicateIgnoredCount());
        if (!spillAllowed) {
            assertEquals(0L, fusion.getStats().getMergeSpilledKeyCount());
            assertNull(fusion.getStats().getFallbackReason());
        }

        for (Map.Entry<Long, List<String>> entry : expectations.getExpectedFusionSamples().entrySet()) {
            assertEquals("sample row mismatch for biz_id=" + entry.getKey(), entry.getValue(), fusion.getSampleRows().get(entry.getKey()));
        }

        assertEquals("fusion content digest mismatch, expected sample rows=" + expectations.getExpectedFusionSamples()
                        + ", actual sample rows=" + fusion.getSampleRows()
                        + ", first output keys=" + fusion.getFirstOutputKeys(),
                expectations.getExpectedFusionContentDigest(),
                fusion.getContentDigest());
    }

    private void assertSpillScenario(AdaptiveSortMergeTestSupport.ScenarioResult result) {
        AdaptiveSortMergeTestSupport.DatasetExpectations expectations = result.getExpectations();
        AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
        AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();

        assertNotNull(consistency.getComparisonResult());
        assertNull(consistency.getErrorMessage());
        assertNull(fusion.getErrorMessage());
        assertNotNull(fusion.getStats());
        assertEquals(expectations.getTotalKeys(), consistency.getComparisonResult().getTotalRecords());
        assertEquals(expectations.getExpectedInconsistentKeys(), consistency.getComparisonResult().getInconsistentRecords());
        assertEquals(expectations.getExpectedDuplicateIgnoredCount(), consistency.getDuplicateIgnoredCount());

        for (Map.Entry<Long, List<String>> entry : expectations.getExpectedFusionSamples().entrySet()) {
            List<String> actual = fusion.getSampleRows().get(entry.getKey());
            if (actual != null) {
                assertEquals("sample row mismatch for biz_id=" + entry.getKey(), entry.getValue(), actual);
            }
        }
    }
}
