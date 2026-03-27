package com.jdragon.aggregation.datamock.sortmerge;

import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AdaptiveSortMergeMysqlBenchmarkTest {

    @Test
    public void benchmarkAdaptiveSortMergeAgainstMysqlDemoSource() throws Exception {
        Assume.assumeTrue("set -DrunSortMergeMysqlBenchmarks=true to run the MySQL benchmark suite",
                Boolean.getBoolean("runSortMergeMysqlBenchmarks"));

        List<ScenarioSpec> specs = Arrays.asList(
                new ScenarioSpec("mysql_small", 100),
                new ScenarioSpec("mysql_medium", 10_000),
                new ScenarioSpec("mysql_large", 1_000_000)
        );
        String scenarioFilter = System.getProperty("sortMergeScenario");

        List<AdaptiveSortMergeTestSupport.ScenarioResult> results = new ArrayList<AdaptiveSortMergeTestSupport.ScenarioResult>();
        for (ScenarioSpec spec : specs) {
            if (scenarioFilter != null && !scenarioFilter.trim().isEmpty() && !scenarioFilter.equals(spec.label)) {
                continue;
            }
            AdaptiveSortMergeTestSupport.ScenarioResult result = AdaptiveSortMergeTestSupport.runScenario(
                    spec.label,
                    spec.rowCount,
                    AdaptiveSortMergeTestSupport.benchmarkRoot(spec.label),
                    true
            );
            assertScenario(result);
            results.add(result);
        }

        String report = buildReport(results);
        String dateSuffix = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
        Path reportPath = AdaptiveSortMergeTestSupport.reportPath("sortmerge-mysql-benchmark-" + dateSuffix + ".md");
        AdaptiveSortMergeTestSupport.writeText(reportPath, report);
    }

    private void assertScenario(AdaptiveSortMergeTestSupport.ScenarioResult result) {
        AdaptiveSortMergeTestSupport.DatasetExpectations expectations = result.getExpectations();
        AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
        AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();
        ComparisonResult comparisonResult = consistency.getComparisonResult();

        assertEquals(expectations.getTotalKeys(), comparisonResult.getTotalRecords());
        assertEquals(expectations.getExpectedInconsistentKeys(), comparisonResult.getInconsistentRecords());
        assertEquals(expectations.getExpectedDuplicateIgnoredCount(), consistency.getDuplicateIgnoredCount());
        assertEquals(expectations.getFusionOutputCount(), fusion.getOutputCount());
        assertTrue("sort-merge execution engine expected",
                "sortmerge".equals(consistency.getExecutionEngine()) || "hybrid".equals(consistency.getExecutionEngine()));
        assertTrue("fusion execution engine should be sortmerge or hybrid for " + result.getLabel(),
                "sortmerge".equals(fusion.getStats().getExecutionEngine())
                        || "hybrid".equals(fusion.getStats().getExecutionEngine()));
        assertEquals(expectations.getExpectedDuplicateIgnoredCount(), fusion.getStats().getDuplicateIgnoredCount());
        assertNull(fusion.getStats().getFallbackReason());

        if ("sortmerge".equals(fusion.getStats().getExecutionEngine())) {
            assertEquals(0L, fusion.getStats().getMergeSpilledKeyCount());
            assertTrue("fusion output should stay ordered for " + result.getLabel()
                            + ", violation=" + fusion.getOrderViolationSample(),
                    fusion.isOutputOrdered());
            assertEquals("ordered fusion digest mismatch for scenario=" + result.getLabel(),
                    expectations.getExpectedFusionDigest(),
                    fusion.getDigest());
        } else {
            assertTrue("hybrid execution should spill at least one key for " + result.getLabel(),
                    fusion.getStats().getMergeSpilledKeyCount() > 0L);
        }

        for (Map.Entry<Long, List<String>> entry : expectations.getExpectedFusionSamples().entrySet()) {
            assertEquals("sample row mismatch for scenario=" + result.getLabel() + ", biz_id=" + entry.getKey(),
                    entry.getValue(),
                    fusion.getSampleRows().get(entry.getKey()));
        }

        assertEquals("fusion content digest mismatch for scenario=" + result.getLabel()
                        + ", sample rows=" + fusion.getSampleRows()
                        + ", first output keys=" + fusion.getFirstOutputKeys()
                        + ", ordered=" + fusion.isOutputOrdered()
                        + ", violation=" + fusion.getOrderViolationSample(),
                expectations.getExpectedFusionContentDigest(),
                fusion.getContentDigest());
    }

    private String buildReport(List<AdaptiveSortMergeTestSupport.ScenarioResult> results) {
        StringBuilder report = new StringBuilder();
        report.append("# SortMerge MySQL 测试报告\n\n");
        report.append("生成时间: ").append(LocalDate.now()).append("\n\n");
        report.append("数据源配置: 复用 `fusion-mysql-demo.json` 中的 `mysql8` 连接，测试库为 `agg_test`。\n\n");

        report.append("## 测试用例报告\n\n");
        report.append("| 场景 | 数据量 | consistency total | consistency inconsistent | duplicateIgnored | fusion output | fusion content match | consistency engine | fusion engine | spilled keys | fallback reason |\n");
        report.append("| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | ---: | --- |\n");
        for (AdaptiveSortMergeTestSupport.ScenarioResult result : results) {
            AdaptiveSortMergeTestSupport.DatasetExpectations expectations = result.getExpectations();
            AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
            AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();
            boolean digestMatched = expectations.getExpectedFusionContentDigest().equals(fusion.getContentDigest());

            report.append("| ")
                    .append(result.getLabel()).append(" | ")
                    .append(result.getRowCount()).append(" | ")
                    .append(consistency.getComparisonResult().getTotalRecords()).append(" | ")
                    .append(consistency.getComparisonResult().getInconsistentRecords()).append(" | ")
                    .append(consistency.getDuplicateIgnoredCount()).append(" | ")
                    .append(fusion.getOutputCount()).append(" | ")
                    .append(digestMatched ? "PASS" : "FAIL").append(" | ")
                    .append(consistency.getExecutionEngine()).append(" | ")
                    .append(fusion.getStats().getExecutionEngine()).append(" | ")
                    .append(fusion.getStats().getMergeSpilledKeyCount()).append(" | ")
                    .append(fusion.getStats().getFallbackReason() == null ? "" : fusion.getStats().getFallbackReason()).append(" |\n");
        }

        report.append("\n## 性能报告\n\n");
        report.append("| 场景 | 数据量 | 建表造数耗时(ms) | consistency耗时(ms) | consistency吞吐(keys/s) | consistency峰值堆MB | fusion耗时(ms) | fusion吞吐(rows/s) | fusion峰值堆MB | spilled keys | ordered output |\n");
        report.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n");
        for (AdaptiveSortMergeTestSupport.ScenarioResult result : results) {
            AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
            AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();

            double consistencyThroughput = throughput(result.getExpectations().getTotalKeys(), consistency.getElapsedMs());
            double fusionThroughput = throughput(result.getExpectations().getFusionOutputCount(), fusion.getElapsedMs());

            report.append("| ")
                    .append(result.getLabel()).append(" | ")
                    .append(result.getRowCount()).append(" | ")
                    .append(result.getSetupElapsedMs()).append(" | ")
                    .append(consistency.getElapsedMs()).append(" | ")
                    .append(formatDouble(consistencyThroughput)).append(" | ")
                    .append(formatDouble(consistency.getPeakHeapBytes() / 1024.0 / 1024.0)).append(" | ")
                    .append(fusion.getElapsedMs()).append(" | ")
                    .append(formatDouble(fusionThroughput)).append(" | ")
                    .append(formatDouble(fusion.getPeakHeapBytes() / 1024.0 / 1024.0)).append(" | ")
                    .append(fusion.getStats().getMergeSpilledKeyCount()).append(" | ")
                    .append(fusion.isOutputOrdered() ? "YES" : "NO").append(" |\n");
        }

        report.append("\n## 样例校验\n\n");
        report.append("以下样例来自融合结果，用于确认优先级回退和字段值选择正确：\n\n");
        for (AdaptiveSortMergeTestSupport.ScenarioResult result : results) {
            report.append("### ").append(result.getLabel()).append("\n\n");
            report.append("| biz_id | chosen_name | age | salary | department | status |\n");
            report.append("| ---: | --- | ---: | ---: | --- | --- |\n");
            for (Long sampleKey : result.getFusionExecution().getSampleRows().keySet()) {
                List<String> row = result.getFusionExecution().getSampleRows().get(sampleKey);
                report.append("| ")
                        .append(row.get(0)).append(" | ")
                        .append(row.get(1)).append(" | ")
                        .append(row.get(2)).append(" | ")
                        .append(row.get(3)).append(" | ")
                        .append(row.get(4)).append(" | ")
                        .append(row.get(5)).append(" |\n");
            }
            report.append("\n");
        }

        return report.toString();
    }

    private double throughput(long records, long elapsedMs) {
        if (elapsedMs <= 0L) {
            return 0D;
        }
        return records * 1000D / elapsedMs;
    }

    private String formatDouble(double value) {
        return String.format("%.2f", value);
    }

    private static final class ScenarioSpec {
        private final String label;
        private final int rowCount;

        private ScenarioSpec(String label, int rowCount) {
            this.label = label;
            this.rowCount = rowCount;
        }
    }
}
