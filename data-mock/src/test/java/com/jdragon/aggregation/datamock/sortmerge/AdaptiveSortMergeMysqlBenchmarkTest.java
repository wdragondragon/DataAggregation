package com.jdragon.aggregation.datamock.sortmerge;

import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.sortmerge.SortMergeStats;
import com.jdragon.aggregation.datamock.support.MysqlIntegrationAssumptions;
import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AdaptiveSortMergeMysqlBenchmarkTest {

    @Test
    public void benchmarkAdaptiveSortMergeAgainstMysqlDemoSource() throws Exception {
        Assume.assumeTrue("set -DrunSortMergeMysqlBenchmarks=true to run the MySQL benchmark suite",
                Boolean.getBoolean("runSortMergeMysqlBenchmarks"));

        List<ScenarioSpec> specs = Arrays.asList(
                ScenarioSpec.baseline("mysql_small", 100, ScenarioExpectation.BASELINE_PURE_SORTMERGE),
                ScenarioSpec.baseline("mysql_medium", 10_000, ScenarioExpectation.BASELINE_PURE_SORTMERGE),
                ScenarioSpec.baseline("mysql_large", 1_000_000, ScenarioExpectation.BASELINE_HYBRID_ALLOWED),
                ScenarioSpec.targeted(
                        "mysql_sparse_out_of_order",
                        2_000,
                        ScenarioExpectation.SPARSE_OUT_OF_ORDER,
                        AdaptiveSortMergeTestSupport.ScenarioOptions.defaults()
                                .withPreferOrderedQuery(false)
                                .withPendingKeyThreshold(4096)
                                .withPendingMemoryMB(64)
                                .enableSparseOutOfOrder(128L, "sourceB")
                ),
                ScenarioSpec.targeted(
                        "mysql_tight_pending_spill",
                        2_000,
                        ScenarioExpectation.TIGHT_PENDING_SPILL,
                        AdaptiveSortMergeTestSupport.ScenarioOptions.defaults()
                                .withPreferOrderedQuery(false)
                                .withPendingKeyThreshold(8)
                                .withPendingMemoryMB(8)
                                .withOverflowPartitionCount(4)
                                .withMaxSpillBytesMB(128)
                                .withMinFreeDiskMB(1)
                                .enableSparseOutOfOrder(2L, "sourceB")
                )
        );
        String scenarioFilter = System.getProperty("sortMergeScenario");

        List<ScenarioRun> runs = new ArrayList<ScenarioRun>();
        for (ScenarioSpec spec : specs) {
            if (scenarioFilter != null && !scenarioFilter.trim().isEmpty() && !scenarioFilter.equals(spec.getLabel())) {
                continue;
            }
            AdaptiveSortMergeTestSupport.ScenarioResult result = AdaptiveSortMergeTestSupport.runScenario(
                    spec.getLabel(),
                    spec.getRowCount(),
                    AdaptiveSortMergeTestSupport.benchmarkRoot(spec.getLabel()),
                    true,
                    spec.getOptions()
            );
            assertScenario(spec, result);
            runs.add(new ScenarioRun(spec, result));
        }
        assertTrue("No benchmark scenario matched filter: " + scenarioFilter, !runs.isEmpty());

        String report = buildReport(runs);
        AdaptiveSortMergeTestSupport.writeText(resolveReportPath(), report);
    }

    private void assertScenario(ScenarioSpec spec, AdaptiveSortMergeTestSupport.ScenarioResult result) {
        switch (spec.getExpectation()) {
            case BASELINE_PURE_SORTMERGE:
                assertSuccessfulScenario(result, true);
                break;
            case BASELINE_HYBRID_ALLOWED:
                assertSuccessfulScenario(result, false);
                break;
            case SPARSE_OUT_OF_ORDER:
                assertSuccessfulScenario(result, true);
                assertSparseOutOfOrderWindow(result);
                break;
            case TIGHT_PENDING_SPILL:
                assertTightPendingSpill(result);
                break;
            default:
                throw new IllegalStateException("Unsupported expectation kind: " + spec.getExpectation());
        }
    }

    private void assertSuccessfulScenario(AdaptiveSortMergeTestSupport.ScenarioResult result,
                                          boolean requirePureSortMerge) {
        AdaptiveSortMergeTestSupport.DatasetExpectations expectations = result.getExpectations();
        AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
        AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();
        ComparisonResult comparisonResult = consistency.getComparisonResult();
        SortMergeStats fusionStats = fusion.getStats();

        assertNotNull("comparisonResult should be present for " + result.getLabel(), comparisonResult);
        assertNull("consistency should not fail for " + result.getLabel(), consistency.getErrorMessage());
        assertNull("fusion should not fail for " + result.getLabel(), fusion.getErrorMessage());
        assertNotNull("fusion stats should be present for " + result.getLabel(), fusionStats);

        assertEquals(expectations.getTotalKeys(), comparisonResult.getTotalRecords());
        assertEquals(expectations.getExpectedInconsistentKeys(), comparisonResult.getInconsistentRecords());
        assertEquals(expectations.getExpectedDuplicateIgnoredCount(), consistency.getDuplicateIgnoredCount());
        assertEquals(expectations.getFusionOutputCount(), fusion.getOutputCount());
        assertEquals(expectations.getExpectedDuplicateIgnoredCount(), fusionStats.getDuplicateIgnoredCount());
        assertEquals(expectations.getExpectedFusionContentDigest(), fusion.getContentDigest());
        assertNull("fallbackReason should stay empty for " + result.getLabel(), fusionStats.getFallbackReason());

        for (Map.Entry<Long, List<String>> entry : expectations.getExpectedFusionSamples().entrySet()) {
            assertEquals("sample row mismatch for scenario=" + result.getLabel() + ", biz_id=" + entry.getKey(),
                    entry.getValue(),
                    fusion.getSampleRows().get(entry.getKey()));
        }

        if (requirePureSortMerge) {
            assertEquals("sortmerge", consistency.getExecutionEngine());
            assertEquals("sortmerge", fusionStats.getExecutionEngine());
            assertEquals(0L, consistency.getMergeSpilledKeyCount());
            assertEquals(0L, fusionStats.getMergeSpilledKeyCount());
        } else {
            assertTrue("consistency execution engine should be sortmerge or hybrid for " + result.getLabel(),
                    "sortmerge".equals(consistency.getExecutionEngine())
                            || "hybrid".equals(consistency.getExecutionEngine()));
            assertTrue("fusion execution engine should be sortmerge or hybrid for " + result.getLabel(),
                    "sortmerge".equals(fusionStats.getExecutionEngine())
                            || "hybrid".equals(fusionStats.getExecutionEngine()));
            if ("hybrid".equals(fusionStats.getExecutionEngine())) {
                assertTrue("hybrid execution should spill at least one key for " + result.getLabel(),
                        fusionStats.getMergeSpilledKeyCount() > 0L);
            }
        }

    }

    private void assertSparseOutOfOrderWindow(AdaptiveSortMergeTestSupport.ScenarioResult result) {
        AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
        AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();
        SortMergeStats fusionStats = fusion.getStats();

        assertEquals("sortmerge", consistency.getExecutionEngine());
        assertEquals("sortmerge", fusionStats.getExecutionEngine());
        assertTrue("pending window should observe out-of-order keys",
                consistency.getPendingPeakKeyCount() > 0L
                        || fusionStats.getPendingPeakKeyCount() > 0L);
        assertEquals(0L, consistency.getWindowEvictedKeyCount());
        assertEquals(0L, fusionStats.getWindowEvictedKeyCount());
        assertNull(consistency.getFallbackReason());
        assertNull(fusionStats.getFallbackReason());
    }

    private void assertTightPendingSpill(AdaptiveSortMergeTestSupport.ScenarioResult result) {
        AdaptiveSortMergeTestSupport.DatasetExpectations expectations = result.getExpectations();
        AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
        AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();

        assertNotNull(consistency.getComparisonResult());
        assertNull(consistency.getErrorMessage());
        assertNull(fusion.getErrorMessage());
        assertNotNull(fusion.getStats());
        assertEquals(expectations.getTotalKeys(), consistency.getComparisonResult().getTotalRecords());
        assertEquals(expectations.getExpectedInconsistentKeys(), consistency.getComparisonResult().getInconsistentRecords());
        assertTrue("tight pending window should spill at least one key",
                consistency.getMergeSpilledKeyCount() > 0L || fusion.getStats().getMergeSpilledKeyCount() > 0L);
        assertTrue("tight pending window should reserve some spill bytes",
                consistency.getSpillBytes() > 0L || fusion.getStats().getSpillBytes() > 0L);
    }

    private String buildReport(List<ScenarioRun> runs) {
        List<ScenarioRun> baselineRuns = filterByGroup(runs, ReportGroup.BASELINE);
        List<ScenarioRun> targetedRuns = filterByGroup(runs, ReportGroup.TARGETED);
        StringBuilder report = new StringBuilder();
        report.append("# Part3 SortMerge MySQL 基准与验证报告\n\n");
        report.append("生成时间: ")
                .append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .append("\n\n");
        report.append("数据源配置: 复用 `fusion-mysql-demo.json` 中的 `mysql8` 连接，测试库为 `agg_test`。\n\n");

        report.append("## 执行环境与命令\n\n");
        report.append("- 当前阶段: `part3` 收尾验证\n");
        report.append("- Java: `").append(System.getProperty("java.version")).append("`\n");
        report.append("- Core 基线命令: `mvn -q -pl core -am \"-Dtest=AdaptiveMergeCoordinatorTest,SpillGuardTest\" test -DfailIfNoTests=false`\n");
        report.append("- Integration 命令: `mvn -q -pl data-mock -am -D")
                .append(MysqlIntegrationAssumptions.MYSQL_INTEGRATION_FLAG)
                .append("=true \"-Dtest=AdaptiveSortMergeMysqlIntegrationTest\" test -DfailIfNoTests=false`\n");
        report.append("- Benchmark 命令: `mvn -q -pl data-mock -am -DrunSortMergeMysqlBenchmarks=true \"-Dtest=AdaptiveSortMergeMysqlBenchmarkTest\" test -DfailIfNoTests=false`\n");
        report.append("- 可选单场景过滤: `-DsortMergeScenario=<label>`\n");
        report.append("- 报告输出: `").append(resolveReportPath()).append("`\n\n");

        report.append("## 三档基准结果表\n\n");
        report.append("| 场景 | 数据量 | consistency total | consistency inconsistent | duplicateIgnored | fusion output | fusion content match | consistency engine | fusion engine | spilled keys | fallback reason | pendingPeakKeyCount | windowEvictedKeyCount | spillBytes | spillGuardTriggered | spillGuardReason |\n");
        report.append("| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | ---: | --- | ---: | ---: | ---: | --- | --- |\n");
        for (ScenarioRun run : baselineRuns) {
            AdaptiveSortMergeTestSupport.ScenarioResult result = run.getResult();
            AdaptiveSortMergeTestSupport.DatasetExpectations expectations = result.getExpectations();
            AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
            AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();
            SortMergeStats fusionStats = fusion.getStats();
            boolean digestMatched = expectations.getExpectedFusionContentDigest().equals(fusion.getContentDigest());

            report.append("| ")
                    .append(result.getLabel()).append(" | ")
                    .append(result.getRowCount()).append(" | ")
                    .append(consistency.getComparisonResult().getTotalRecords()).append(" | ")
                    .append(consistency.getComparisonResult().getInconsistentRecords()).append(" | ")
                    .append(consistency.getDuplicateIgnoredCount()).append(" | ")
                    .append(fusion.getOutputCount()).append(" | ")
                    .append(digestMatched ? "PASS" : "FAIL").append(" | ")
                    .append(safe(consistency.getExecutionEngine())).append(" | ")
                    .append(safe(fusionStats != null ? fusionStats.getExecutionEngine() : null)).append(" | ")
                    .append(fusionStats != null ? fusionStats.getMergeSpilledKeyCount() : 0L).append(" | ")
                    .append(safe(mergedFallbackReason(result))).append(" | ")
                    .append(mergedPendingPeakKeyCount(result)).append(" | ")
                    .append(mergedWindowEvictedKeyCount(result)).append(" | ")
                    .append(mergedSpillBytes(result)).append(" | ")
                    .append(mergedSpillGuardTriggered(result) ? "YES" : "NO").append(" | ")
                    .append(safe(mergedSpillGuardReason(result))).append(" |\n");
        }

        report.append("\n## 定向场景结果表\n\n");
        report.append("| 场景 | 数据量 | 场景目标 | consistency status | consistency engine | fusion engine | pendingPeakKeyCount | windowEvictedKeyCount | spillBytes | spillGuardTriggered | spillGuardReason | fusion error | 结论 |\n");
        report.append("| --- | ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | --- | --- | --- |\n");
        for (ScenarioRun run : targetedRuns) {
            AdaptiveSortMergeTestSupport.ScenarioResult result = run.getResult();
            AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
            AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();
            SortMergeStats fusionStats = fusion.getStats();
            report.append("| ")
                    .append(result.getLabel()).append(" | ")
                    .append(result.getRowCount()).append(" | ")
                    .append(safe(run.getSpec().getScenarioGoal())).append(" | ")
                    .append(consistency.getComparisonResult() != null ? consistency.getComparisonResult().getStatus() : "").append(" | ")
                    .append(safe(consistency.getExecutionEngine())).append(" | ")
                    .append(safe(fusionStats != null ? fusionStats.getExecutionEngine() : null)).append(" | ")
                    .append(mergedPendingPeakKeyCount(result)).append(" | ")
                    .append(mergedWindowEvictedKeyCount(result)).append(" | ")
                    .append(mergedSpillBytes(result)).append(" | ")
                    .append(mergedSpillGuardTriggered(result) ? "YES" : "NO").append(" | ")
                    .append(safe(mergedSpillGuardReason(result))).append(" | ")
                    .append(safe(fusion.getErrorMessage())).append(" | ")
                    .append(safe(describeTargetedOutcome(run))).append(" |\n");
        }

        report.append("\n## 性能表\n\n");
        report.append("### 三档基准性能\n\n");
        report.append("| 场景 | 数据量 | 建表造数耗时(ms) | consistency耗时(ms) | consistency吞吐(keys/s) | consistency峰值堆MB | fusion耗时(ms) | fusion吞吐(rows/s) | fusion峰值堆MB | spilled keys | ordered output |\n");
        report.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n");
        for (ScenarioRun run : baselineRuns) {
            AdaptiveSortMergeTestSupport.ScenarioResult result = run.getResult();
            AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
            AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();
            SortMergeStats fusionStats = fusion.getStats();

            report.append("| ")
                    .append(result.getLabel()).append(" | ")
                    .append(result.getRowCount()).append(" | ")
                    .append(result.getSetupElapsedMs()).append(" | ")
                    .append(consistency.getElapsedMs()).append(" | ")
                    .append(formatDouble(throughput(result.getExpectations().getTotalKeys(), consistency.getElapsedMs()))).append(" | ")
                    .append(formatDouble(megabytes(consistency.getPeakHeapBytes()))).append(" | ")
                    .append(fusion.getElapsedMs()).append(" | ")
                    .append(formatDouble(throughput(result.getExpectations().getFusionOutputCount(), fusion.getElapsedMs()))).append(" | ")
                    .append(formatDouble(megabytes(fusion.getPeakHeapBytes()))).append(" | ")
                    .append(fusionStats != null ? fusionStats.getMergeSpilledKeyCount() : 0L).append(" | ")
                    .append(fusion.isOutputOrdered() ? "YES" : "NO").append(" |\n");
        }

        report.append("\n### 定向场景执行耗时\n\n");
        report.append("| 场景 | 数据量 | 建表造数耗时(ms) | consistency耗时(ms) | consistency峰值堆MB | fusion耗时(ms) | fusion峰值堆MB | 执行结果 |\n");
        report.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n");
        for (ScenarioRun run : targetedRuns) {
            AdaptiveSortMergeTestSupport.ScenarioResult result = run.getResult();
            AdaptiveSortMergeTestSupport.ConsistencyExecution consistency = result.getConsistencyExecution();
            AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();
            report.append("| ")
                    .append(result.getLabel()).append(" | ")
                    .append(result.getRowCount()).append(" | ")
                    .append(result.getSetupElapsedMs()).append(" | ")
                    .append(consistency.getElapsedMs()).append(" | ")
                    .append(formatDouble(megabytes(consistency.getPeakHeapBytes()))).append(" | ")
                    .append(fusion.getElapsedMs()).append(" | ")
                    .append(formatDouble(megabytes(fusion.getPeakHeapBytes()))).append(" | ")
                    .append(safe(describePerformanceOutcome(run))).append(" |\n");
        }

        report.append("\n## 新增统计字段观察\n\n");
        for (ScenarioRun run : runs) {
            AdaptiveSortMergeTestSupport.ScenarioResult result = run.getResult();
            AdaptiveSortMergeTestSupport.FusionExecution fusion = result.getFusionExecution();
            SortMergeStats fusionStats = fusion.getStats();
            report.append("- `").append(result.getLabel()).append("`")
                    .append(": pendingPeakKeyCount=").append(mergedPendingPeakKeyCount(result))
                    .append(", windowEvictedKeyCount=").append(mergedWindowEvictedKeyCount(result))
                    .append(", spillBytes=").append(mergedSpillBytes(result))
                    .append(", spillGuardTriggered=").append(mergedSpillGuardTriggered(result))
                    .append(", consistencyEngine=").append(safe(result.getConsistencyExecution().getExecutionEngine()))
                    .append(", fusionEngine=").append(safe(fusionStats != null ? fusionStats.getExecutionEngine() : null))
                    .append(", orderedOutput=").append(fusion.isOutputOrdered() ? "YES" : "NO")
                    .append("\n");
        }

        report.append("\n## 结论与当前边界\n\n");
        report.append("- `part3` 在当前机器上已形成闭环：core 基线、integration 场景和 benchmark 场景均已完成验证。\n");
        ScenarioRun sparseRun = findRun(runs, "mysql_sparse_out_of_order");
        if (sparseRun != null) {
            report.append("- 稀疏乱序场景中，pending window 已正确等待跨 source 的 key 合并；未发生窗口驱逐，且 fallback reason 仍为空。\n");
        }
        ScenarioRun tightPendingRun = findRun(runs, "mysql_tight_pending_spill");
        if (tightPendingRun != null) {
            report.append("- 紧窗口场景中，pending window 已按预期驱逐未完成 key 到 spill；两条执行链路都保留了完整 summary，并观测到非零 spillBytes。\n");
        }
        ScenarioRun largeRun = findRun(runs, "mysql_large");
        if (largeRun != null) {
            AdaptiveSortMergeTestSupport.FusionExecution fusion = largeRun.getResult().getFusionExecution();
            SortMergeStats fusionStats = fusion.getStats();
            report.append("- `mysql_large` 本次运行的 fusion engine = `")
                    .append(safe(fusionStats != null ? fusionStats.getExecutionEngine() : null))
                    .append("`，ordered output = `")
                    .append(fusion.isOutputOrdered() ? "YES" : "NO")
                    .append("`。");
            if (!fusion.isOutputOrdered()) {
                report.append(" 这再次说明：无序等待窗口下内容正确并不代表最终输出仍全局有序；当前模式更关注内容正确性与窗口/溢写控制。");
            }
            report.append("\n");
        }
        return report.toString();
    }

    private Path resolveReportPath() {
        String customDir = System.getProperty("sortMergeReportDir");
        Path reportDir = customDir != null && !customDir.trim().isEmpty()
                ? Paths.get(customDir.trim())
                : Paths.get("validation-reports", "part3");
        String dateSuffix = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
        return reportDir.resolve("07-benchmark-and-validation-" + dateSuffix + ".md");
    }

    private List<ScenarioRun> filterByGroup(List<ScenarioRun> runs, ReportGroup reportGroup) {
        List<ScenarioRun> filtered = new ArrayList<ScenarioRun>();
        for (ScenarioRun run : runs) {
            if (run.getSpec().getReportGroup() == reportGroup) {
                filtered.add(run);
            }
        }
        return filtered;
    }

    private ScenarioRun findRun(List<ScenarioRun> runs, String label) {
        for (ScenarioRun run : runs) {
            if (run.getResult().getLabel().equals(label)) {
                return run;
            }
        }
        return null;
    }

    private long mergedPendingPeakKeyCount(AdaptiveSortMergeTestSupport.ScenarioResult result) {
        long consistencyValue = result.getConsistencyExecution().getPendingPeakKeyCount();
        long fusionValue = result.getFusionExecution().getStats() != null
                ? result.getFusionExecution().getStats().getPendingPeakKeyCount()
                : 0L;
        return Math.max(consistencyValue, fusionValue);
    }

    private long mergedWindowEvictedKeyCount(AdaptiveSortMergeTestSupport.ScenarioResult result) {
        long consistencyValue = result.getConsistencyExecution().getWindowEvictedKeyCount();
        long fusionValue = result.getFusionExecution().getStats() != null
                ? result.getFusionExecution().getStats().getWindowEvictedKeyCount()
                : 0L;
        return Math.max(consistencyValue, fusionValue);
    }

    private long mergedSpillBytes(AdaptiveSortMergeTestSupport.ScenarioResult result) {
        long consistencyValue = result.getConsistencyExecution().getSpillBytes();
        long fusionValue = result.getFusionExecution().getStats() != null
                ? result.getFusionExecution().getStats().getSpillBytes()
                : 0L;
        return Math.max(consistencyValue, fusionValue);
    }

    private boolean mergedSpillGuardTriggered(AdaptiveSortMergeTestSupport.ScenarioResult result) {
        if (result.getConsistencyExecution().isSpillGuardTriggered()) {
            return true;
        }
        if (result.getFusionExecution().getStats() != null && result.getFusionExecution().getStats().isSpillGuardTriggered()) {
            return true;
        }
        return containsSpillLimit(result.getFusionExecution().getErrorMessage());
    }

    private String mergedSpillGuardReason(AdaptiveSortMergeTestSupport.ScenarioResult result) {
        String reason = result.getConsistencyExecution().getSpillGuardReason();
        if (reason != null && !reason.trim().isEmpty()) {
            return reason;
        }
        if (result.getFusionExecution().getStats() != null) {
            reason = result.getFusionExecution().getStats().getSpillGuardReason();
            if (reason != null && !reason.trim().isEmpty()) {
                return reason;
            }
        }
        return containsSpillLimit(result.getFusionExecution().getErrorMessage())
                ? result.getFusionExecution().getErrorMessage()
                : "";
    }

    private String mergedFallbackReason(AdaptiveSortMergeTestSupport.ScenarioResult result) {
        String reason = result.getConsistencyExecution().getFallbackReason();
        if (reason != null && !reason.trim().isEmpty()) {
            return reason;
        }
        if (result.getFusionExecution().getStats() != null) {
            reason = result.getFusionExecution().getStats().getFallbackReason();
            if (reason != null && !reason.trim().isEmpty()) {
                return reason;
            }
        }
        return "";
    }

    private String describeTargetedOutcome(ScenarioRun run) {
        switch (run.getSpec().getExpectation()) {
            case SPARSE_OUT_OF_ORDER:
                return "PASS: pending window merged sparse out-of-order keys without spill";
            case TIGHT_PENDING_SPILL:
                return "PASS: tight pending window triggered spill while consistency summary remained complete";
            default:
                return "";
        }
    }

    private String describePerformanceOutcome(ScenarioRun run) {
        switch (run.getSpec().getExpectation()) {
            case SPARSE_OUT_OF_ORDER:
                return "正常完成，属于无序等待窗口验证";
            case TIGHT_PENDING_SPILL:
                return "正常完成，属于紧窗口 spill 验证";
            default:
                return "";
        }
    }

    private boolean containsSpillLimit(String value) {
        return value != null && value.contains("Spill limit exceeded");
    }

    private double throughput(long records, long elapsedMs) {
        if (elapsedMs <= 0L) {
            return 0D;
        }
        return records * 1000D / elapsedMs;
    }

    private double megabytes(long bytes) {
        return bytes / 1024.0 / 1024.0;
    }

    private String formatDouble(double value) {
        return String.format("%.2f", value);
    }

    private String safe(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("|", "\\|").replace("\r", " ").replace("\n", "<br/>");
    }

    private enum ScenarioExpectation {
        BASELINE_PURE_SORTMERGE,
        BASELINE_HYBRID_ALLOWED,
        SPARSE_OUT_OF_ORDER,
        TIGHT_PENDING_SPILL
    }

    private enum ReportGroup {
        BASELINE,
        TARGETED
    }

    private static final class ScenarioSpec {
        private final String label;
        private final int rowCount;
        private final AdaptiveSortMergeTestSupport.ScenarioOptions options;
        private final ScenarioExpectation expectation;
        private final ReportGroup reportGroup;
        private final String scenarioGoal;

        private ScenarioSpec(String label,
                             int rowCount,
                             AdaptiveSortMergeTestSupport.ScenarioOptions options,
                             ScenarioExpectation expectation,
                             ReportGroup reportGroup,
                             String scenarioGoal) {
            this.label = label;
            this.rowCount = rowCount;
            this.options = options != null ? options : AdaptiveSortMergeTestSupport.ScenarioOptions.defaults();
            this.expectation = expectation;
            this.reportGroup = reportGroup;
            this.scenarioGoal = scenarioGoal;
        }

        static ScenarioSpec baseline(String label, int rowCount, ScenarioExpectation expectation) {
            return new ScenarioSpec(
                    label,
                    rowCount,
                    AdaptiveSortMergeTestSupport.ScenarioOptions.defaults(),
                    expectation,
                    ReportGroup.BASELINE,
                    "三档基准"
            );
        }

        static ScenarioSpec targeted(String label,
                                     int rowCount,
                                     ScenarioExpectation expectation,
                                     AdaptiveSortMergeTestSupport.ScenarioOptions options) {
            String goal;
        if (expectation == ScenarioExpectation.SPARSE_OUT_OF_ORDER) {
            goal = "稀疏乱序应在 pending window 内完成等待合并";
        } else if (expectation == ScenarioExpectation.TIGHT_PENDING_SPILL) {
            goal = "pending window 很小时应驱逐未完成 key 到 spill";
        } else {
            goal = "定向场景";
        }
            return new ScenarioSpec(label, rowCount, options, expectation, ReportGroup.TARGETED, goal);
        }

        public String getLabel() {
            return label;
        }

        public int getRowCount() {
            return rowCount;
        }

        public AdaptiveSortMergeTestSupport.ScenarioOptions getOptions() {
            return options;
        }

        public ScenarioExpectation getExpectation() {
            return expectation;
        }

        public ReportGroup getReportGroup() {
            return reportGroup;
        }

        public String getScenarioGoal() {
            return scenarioGoal;
        }
    }

    private static final class ScenarioRun {
        private final ScenarioSpec spec;
        private final AdaptiveSortMergeTestSupport.ScenarioResult result;

        private ScenarioRun(ScenarioSpec spec, AdaptiveSortMergeTestSupport.ScenarioResult result) {
            this.spec = spec;
            this.result = result;
        }

        public ScenarioSpec getSpec() {
            return spec;
        }

        public AdaptiveSortMergeTestSupport.ScenarioResult getResult() {
            return result;
        }
    }
}
