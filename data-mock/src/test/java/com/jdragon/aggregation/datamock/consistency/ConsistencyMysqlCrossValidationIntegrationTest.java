package com.jdragon.aggregation.datamock.consistency;

import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.UpdateResult;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConsistencyMysqlCrossValidationIntegrationTest {

    @Test
    public void shouldCrossValidateConsistencyExampleAndReaderForInsertUpdateDelete() throws Exception {
        ConsistencyMysqlCrossValidationTestSupport.CrossValidationResult result =
                ConsistencyMysqlCrossValidationTestSupport.runCrossValidationScenario(
                        "consistency_cross_validation_small",
                        100,
                        ConsistencyMysqlCrossValidationTestSupport.benchmarkRoot("consistency_cross_validation_small_it"),
                        false,
                        ConsistencyMysqlCrossValidationTestSupport.ScenarioProfile.FULL_VALIDATION
                );

        assertComparisonResult("example", result.getExpectedMetrics(), result.getExampleResult());
        assertTargetAligned("example",
                result.getExampleReferenceSnapshot(),
                result.getExampleTargetSnapshot());

        assertComparisonResult("reader", result.getExpectedMetrics(), result.getReaderResult());
        assertWriterOutput(result);
        assertTargetAligned("reader",
                result.getReaderReferenceSnapshot(),
                result.getReaderTargetSnapshot());

        assertEquals(result.getExampleResult().getInconsistentRecords(), result.getReaderResult().getInconsistentRecords());
        assertEquals(result.getExampleResult().getResolvedRecords(), result.getReaderResult().getResolvedRecords());
        assertEquals(result.getExampleReferenceSnapshot().getOrderedRows(), result.getReaderReferenceSnapshot().getOrderedRows());
        assertEquals(result.getExampleTargetSnapshot().getOrderedRows(), result.getReaderTargetSnapshot().getOrderedRows());
    }

    private void assertComparisonResult(String label,
                                        ConsistencyMysqlCrossValidationTestSupport.ExpectedMetrics expected,
                                        ComparisonResult actual) {
        assertNotNull(label + " comparison result should not be null", actual);
        assertEquals(label + " status", ComparisonResult.Status.PARTIAL_SUCCESS, actual.getStatus());
        assertEquals(label + " totalRecords", expected.getTotalRecords(), actual.getTotalRecords());
        assertEquals(label + " consistentRecords", expected.getConsistentRecords(), actual.getConsistentRecords());
        assertEquals(label + " inconsistentRecords", expected.getInconsistentRecords(), actual.getInconsistentRecords());
        assertEquals(label + " resolvedRecords", expected.getResolvedRecords(), actual.getResolvedRecords());

        UpdateResult updateResult = actual.getUpdateResult();
        assertNotNull(label + " updateResult should not be null", updateResult);
        assertEquals(label + " insertCount", expected.getInsertCount(), updateResult.getInsertCount());
        assertEquals(label + " updateCount", expected.getUpdateCount(), updateResult.getUpdateCount());
        assertEquals(label + " deleteCount", expected.getDeleteCount(), updateResult.getDeleteCount());
        assertEquals(label + " skipCount", 0, updateResult.getSkipCount());
    }

    private void assertTargetAligned(String label,
                                     ConsistencyMysqlCrossValidationTestSupport.TableSnapshot expected,
                                     ConsistencyMysqlCrossValidationTestSupport.TableSnapshot actual) {
        assertNotNull(label + " expected snapshot should not be null", expected);
        assertNotNull(label + " actual snapshot should not be null", actual);
        assertAggregate(label, expected.getAggregate(), actual.getAggregate());
        assertEquals(label + " orderedRows", expected.getOrderedRows(), actual.getOrderedRows());
        for (Map.Entry<Long, List<String>> entry : expected.getSampleRows().entrySet()) {
            assertEquals(label + " sample row for biz_id=" + entry.getKey(),
                    entry.getValue(),
                    actual.getSampleRows().get(entry.getKey()));
        }
    }

    private void assertWriterOutput(ConsistencyMysqlCrossValidationTestSupport.CrossValidationResult result) {
        ConsistencyMysqlCrossValidationTestSupport.ExpectedMetrics expected = result.getExpectedMetrics();
        ConsistencyMysqlCrossValidationTestSupport.WriterOutputSnapshot actual = result.getWriterOutputSnapshot();
        assertNotNull("reader writer output should not be null", actual);
        assertEquals("writer rowCount", expected.getInconsistentRecords(), actual.getRowCount());
        assertEquals("writer conflictTypeCounts", expected.getConflictTypeCounts(), actual.getConflictTypeCounts());
        assertEquals("writer diffKeys", expected.getDiffKeys(), actual.getDiffKeys());
    }

    private void assertAggregate(String label,
                                 ConsistencyMysqlCrossValidationTestSupport.AggregateResult expected,
                                 ConsistencyMysqlCrossValidationTestSupport.AggregateResult actual) {
        assertNotNull(label + " expected aggregate should not be null", expected);
        assertNotNull(label + " actual aggregate should not be null", actual);
        assertEquals(label + " aggregate rowCount", expected.getRowCount(), actual.getRowCount());
        assertEquals(label + " aggregate totalAge", expected.getTotalAge(), actual.getTotalAge());
        assertEquals(label + " aggregate totalActiveFlag", expected.getTotalActiveFlag(), actual.getTotalActiveFlag());
        assertEquals(label + " aggregate totalSalary", expected.getTotalSalary(), actual.getTotalSalary());
        assertEquals(label + " aggregate totalBonusRate", expected.getTotalBonusRate(), actual.getTotalBonusRate());
    }
}
