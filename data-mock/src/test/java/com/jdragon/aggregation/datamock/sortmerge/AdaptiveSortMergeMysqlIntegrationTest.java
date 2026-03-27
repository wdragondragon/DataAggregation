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

        assertScenario(result);
    }

    private void assertScenario(AdaptiveSortMergeTestSupport.ScenarioResult result) {
        AdaptiveSortMergeTestSupport.DatasetExpectations expectations = result.getExpectations();
        AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
        AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();
        ComparisonResult comparisonResult = consistency.getComparisonResult();

        assertNotNull(comparisonResult);
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
