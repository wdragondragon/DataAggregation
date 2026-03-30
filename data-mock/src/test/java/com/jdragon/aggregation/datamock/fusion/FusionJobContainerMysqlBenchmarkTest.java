package com.jdragon.aggregation.datamock.fusion;

import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FusionJobContainerMysqlBenchmarkTest {

    @Test
    public void benchmarkFusionJobContainerWithMysqlDemoConnection() throws Exception {
        Assume.assumeTrue("set -DrunFusionJobContainerMysqlBenchmarks=true to run the Fusion JobContainer MySQL benchmarks",
                Boolean.getBoolean("runFusionJobContainerMysqlBenchmarks"));

        List<ScenarioSpec> specs = Arrays.asList(
                new ScenarioSpec("fusion_job_small_bench", 100),
                new ScenarioSpec("fusion_job_medium_bench", 10_000),
                new ScenarioSpec("fusion_job_large_bench", 1_000_000)
        );

        String scenarioFilter = System.getProperty("fusionJobContainerScenario");
        List<FusionJobContainerMysqlTestSupport.ScenarioResult> results =
                new ArrayList<FusionJobContainerMysqlTestSupport.ScenarioResult>();
        for (ScenarioSpec spec : specs) {
            if (scenarioFilter != null && !scenarioFilter.trim().isEmpty()
                    && !scenarioFilter.trim().equals(spec.getLabel())) {
                continue;
            }
            FusionJobContainerMysqlTestSupport.ScenarioResult result =
                    FusionJobContainerMysqlTestSupport.runScenario(
                            spec.getLabel(),
                            spec.getRowCount(),
                            FusionJobContainerMysqlTestSupport.benchmarkRoot(spec.getLabel()),
                            true
                    );
            assertAggregate(result);
            assertSamples(result);
            results.add(result);
        }

        if (results.isEmpty()) {
            throw new AssertionError("No Fusion JobContainer benchmark scenario matched filter: " + scenarioFilter);
        }

        FusionJobContainerMysqlTestSupport.writeText(resolveReportPath(), buildReport(results));
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
            assertEquals("sample row mismatch for benchmark biz_id=" + entry.getKey(),
                    entry.getValue(),
                    result.getActualOutput().getSampleRows().get(entry.getKey()));
        }
    }

    private String buildReport(List<FusionJobContainerMysqlTestSupport.ScenarioResult> results) {
        StringBuilder report = new StringBuilder();
        report.append("# Fusion JobContainer MySQL Benchmark\n\n");
        report.append("generatedAt: ")
                .append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .append("\n\n");
        report.append("| scenario | rows | setupMs | jobMs | verifyMs | rowCount | totalSalary | totalSalaryDouble | seniorCount | config |\n");
        report.append("| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | --- |\n");
        for (FusionJobContainerMysqlTestSupport.ScenarioResult result : results) {
            FusionJobContainerMysqlTestSupport.AggregateResult aggregate = result.getActualOutput().getAggregate();
            report.append("| ")
                    .append(result.getLabel()).append(" | ")
                    .append(result.getRowCount()).append(" | ")
                    .append(result.getSetupElapsedMs()).append(" | ")
                    .append(result.getJobElapsedMs()).append(" | ")
                    .append(result.getVerifyElapsedMs()).append(" | ")
                    .append(aggregate.getRowCount()).append(" | ")
                    .append(escape(aggregate.getTotalSalary())).append(" | ")
                    .append(escape(aggregate.getTotalSalaryDouble())).append(" | ")
                    .append(aggregate.getSeniorCount()).append(" | ")
                    .append(escape(result.getJobConfigFile().toAbsolutePath().toString())).append(" |\n");
        }
        return report.toString();
    }

    private Path resolveReportPath() {
        return Paths.get("target", "fusion-jobcontainer-benchmark", "fusion-jobcontainer-benchmark.md");
    }

    private String escape(String value) {
        return value == null ? "" : value.replace("|", "\\|");
    }

    private static final class ScenarioSpec {
        private final String label;
        private final int rowCount;

        private ScenarioSpec(String label, int rowCount) {
            this.label = label;
            this.rowCount = rowCount;
        }

        public String getLabel() {
            return label;
        }

        public int getRowCount() {
            return rowCount;
        }
    }
}
