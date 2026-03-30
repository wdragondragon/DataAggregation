package com.jdragon.aggregation.datamock.consistency;

import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConsistencyMysqlCrossValidationBenchmarkTest {

    @Test
    public void benchmarkConsistencyExampleAndReaderWithMillionRows() throws Exception {
        Assume.assumeTrue("set -DrunConsistencyMysqlBenchmarks=true to run the Consistency MySQL benchmarks",
                Boolean.getBoolean("runConsistencyMysqlBenchmarks"));

        List<ScenarioSpec> specs = new ArrayList<ScenarioSpec>();
        specs.add(new ScenarioSpec("consistency_cross_validation_large_bench", 1_000_000,
                ConsistencyMysqlCrossValidationTestSupport.ScenarioProfile.LARGE_PRESSURE));

        String scenarioFilter = System.getProperty("consistencyMysqlScenario");
        List<ConsistencyMysqlCrossValidationTestSupport.CrossValidationResult> results =
                new ArrayList<ConsistencyMysqlCrossValidationTestSupport.CrossValidationResult>();
        for (ScenarioSpec spec : specs) {
            if (scenarioFilter != null && !scenarioFilter.trim().isEmpty()
                    && !scenarioFilter.trim().equals(spec.getLabel())) {
                continue;
            }
            ConsistencyMysqlCrossValidationTestSupport.CrossValidationResult result =
                    ConsistencyMysqlCrossValidationTestSupport.runCrossValidationScenario(
                            spec.getLabel(),
                            spec.getRowCount(),
                            ConsistencyMysqlCrossValidationTestSupport.benchmarkRoot(spec.getLabel()),
                            true,
                            spec.getProfile()
                    );
            assertSummary(result);
            assertTargetAggregate(result);
            assertWriterOutput(result);
            results.add(result);
        }

        if (results.isEmpty()) {
            throw new AssertionError("No Consistency benchmark scenario matched filter: " + scenarioFilter);
        }

        ConsistencyMysqlCrossValidationTestSupport.writeText(resolveReportPath(), buildReport(results));
    }

    private void assertSummary(ConsistencyMysqlCrossValidationTestSupport.CrossValidationResult result) {
        ConsistencyMysqlCrossValidationTestSupport.ExpectedMetrics expected = result.getExpectedMetrics();
        assertEquals(ConsistencyMysqlCrossValidationTestSupport.ScenarioProfile.LARGE_PRESSURE, result.getProfile());
        assertEquals(expected.getTotalRecords(), result.getExampleResult().getTotalRecords());
        assertEquals(expected.getInconsistentRecords(), result.getExampleResult().getInconsistentRecords());
        assertEquals(expected.getResolvedRecords(), result.getExampleResult().getResolvedRecords());
        assertEquals(expected.getInsertCount(), result.getExampleResult().getUpdateResult().getInsertCount());
        assertEquals(expected.getUpdateCount(), result.getExampleResult().getUpdateResult().getUpdateCount());
        assertEquals(expected.getDeleteCount(), result.getExampleResult().getUpdateResult().getDeleteCount());

        assertEquals(expected.getTotalRecords(), result.getReaderResult().getTotalRecords());
        assertEquals(expected.getInconsistentRecords(), result.getReaderResult().getInconsistentRecords());
        assertEquals(expected.getResolvedRecords(), result.getReaderResult().getResolvedRecords());
        assertEquals(expected.getInsertCount(), result.getReaderResult().getUpdateResult().getInsertCount());
        assertEquals(expected.getUpdateCount(), result.getReaderResult().getUpdateResult().getUpdateCount());
        assertEquals(expected.getDeleteCount(), result.getReaderResult().getUpdateResult().getDeleteCount());
    }

    private void assertTargetAggregate(ConsistencyMysqlCrossValidationTestSupport.CrossValidationResult result) {
        assertAggregate(result.getExampleReferenceSnapshot().getAggregate(), result.getExampleTargetSnapshot().getAggregate());
        assertAggregate(result.getReaderReferenceSnapshot().getAggregate(), result.getReaderTargetSnapshot().getAggregate());
    }

    private void assertWriterOutput(ConsistencyMysqlCrossValidationTestSupport.CrossValidationResult result) {
        ConsistencyMysqlCrossValidationTestSupport.ExpectedMetrics expected = result.getExpectedMetrics();
        ConsistencyMysqlCrossValidationTestSupport.WriterOutputSnapshot actual = result.getWriterOutputSnapshot();
        assertEquals(expected.getInconsistentRecords(), actual.getRowCount());
        assertEquals(expected.getConflictTypeCounts(), actual.getConflictTypeCounts());
        assertEquals(expected.getDiffKeys(), actual.getDiffKeys());
    }

    private void assertAggregate(ConsistencyMysqlCrossValidationTestSupport.AggregateResult expected,
                                 ConsistencyMysqlCrossValidationTestSupport.AggregateResult actual) {
        assertEquals(expected.getRowCount(), actual.getRowCount());
        assertEquals(expected.getTotalAge(), actual.getTotalAge());
        assertEquals(expected.getTotalActiveFlag(), actual.getTotalActiveFlag());
        assertEquals(expected.getTotalSalary(), actual.getTotalSalary());
        assertEquals(expected.getTotalBonusRate(), actual.getTotalBonusRate());
    }

    private String buildReport(List<ConsistencyMysqlCrossValidationTestSupport.CrossValidationResult> results) {
        StringBuilder report = new StringBuilder();
        report.append("# Consistency MySQL Cross Validation Benchmark\n\n");
        report.append("generatedAt: ")
                .append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .append("\n\n");
        report.append("| scenario | rows | setupMs | exampleMs | readerMs | verifyMs | inconsistent | inserts | updates | deletes | finalRows | jobConfig |\n");
        report.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n");
        for (ConsistencyMysqlCrossValidationTestSupport.CrossValidationResult result : results) {
            ConsistencyMysqlCrossValidationTestSupport.ExpectedMetrics expected = result.getExpectedMetrics();
            report.append("| ")
                    .append(result.getLabel()).append(" | ")
                    .append(result.getRowCount()).append(" | ")
                    .append(result.getSetupElapsedMs()).append(" | ")
                    .append(result.getExampleElapsedMs()).append(" | ")
                    .append(result.getReaderElapsedMs()).append(" | ")
                    .append(result.getVerifyElapsedMs()).append(" | ")
                    .append(expected.getInconsistentRecords()).append(" | ")
                    .append(expected.getInsertCount()).append(" | ")
                    .append(expected.getUpdateCount()).append(" | ")
                    .append(expected.getDeleteCount()).append(" | ")
                    .append(expected.getFinalRowCount()).append(" | ")
                    .append(escape(result.getJobConfigFile().toAbsolutePath().toString())).append(" |\n");
        }
        return report.toString();
    }

    private Path resolveReportPath() {
        return Paths.get("target", "consistency-cross-validation-benchmark", "consistency-cross-validation-benchmark.md");
    }

    private String escape(String value) {
        return value == null ? "" : value.replace("|", "\\|");
    }

    private static final class ScenarioSpec {
        private final String label;
        private final int rowCount;
        private final ConsistencyMysqlCrossValidationTestSupport.ScenarioProfile profile;

        private ScenarioSpec(String label,
                             int rowCount,
                             ConsistencyMysqlCrossValidationTestSupport.ScenarioProfile profile) {
            this.label = label;
            this.rowCount = rowCount;
            this.profile = profile;
        }

        public String getLabel() {
            return label;
        }

        public int getRowCount() {
            return rowCount;
        }

        public ConsistencyMysqlCrossValidationTestSupport.ScenarioProfile getProfile() {
            return profile;
        }
    }
}
