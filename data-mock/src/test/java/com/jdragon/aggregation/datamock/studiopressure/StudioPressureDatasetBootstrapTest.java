package com.jdragon.aggregation.datamock.studiopressure;

import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StudioPressureDatasetBootstrapTest {

    @Test
    public void bootstrapMockDatasetForStudioPressure() throws Exception {
        Assume.assumeTrue("set -DrunStudioPressureBootstrap=true to build the mock_data/mock_data_target dataset",
                Boolean.getBoolean("runStudioPressureBootstrap"));

        StudioPressureMysqlTestSupport.PressureConfig config = StudioPressureMysqlTestSupport.loadConfig();
        String runId = StudioPressureMysqlTestSupport.newRunId();
        Path runRoot = StudioPressureMysqlTestSupport.runRoot(runId);
        try (StudioPressureMysqlTestSupport.ProgressLogger logger =
                     StudioPressureMysqlTestSupport.openProgressLogger(runRoot.resolve("live-progress.log"))) {
            logger.info("bootstrap 开始，runId=" + runId
                    + "，sourceDatabase=" + config.getGeneration().getSourceDatabase()
                    + "，targetDatabase=" + config.getGeneration().getTargetDatabase());
            StudioPressureMysqlTestSupport.writeText(runRoot.resolve("bootstrap-plan.md"), buildBootstrapPlan(config, runId));
            StudioPressureMysqlTestSupport.DatasetManifest manifest = StudioPressureMysqlTestSupport.prepareDataset(config, runId);

            assertNotNull("dataset manifest should be generated", manifest);
            assertEquals("table count should match configured generation size",
                    config.getGeneration().getTableCount().intValue(),
                    manifest.getTableCount());
            logger.info("bootstrap 数据集生成完成，开始做一致性校验");
            StudioPressureMysqlTestSupport.assertDatasetIntegrity(config, manifest);

            Map<String, Object> summary = new LinkedHashMap<String, Object>();
            summary.put("runId", runId);
            summary.put("sourceDatabase", manifest.getSourceDatabase());
            summary.put("targetDatabase", manifest.getTargetDatabase());
            summary.put("tableCount", manifest.getTableCount());
            summary.put("familyCount", manifest.getFamilyCount());
            StudioPressureMysqlTestSupport.writeJson(runRoot.resolve("dataset-summary.json"), summary);
            StudioPressureMysqlTestSupport.writeText(runRoot.resolve("bootstrap-result.md"), buildBootstrapResult(manifest));
            logger.info("bootstrap 完成，manifest=" + runRoot.resolve("dataset-manifest.json"));
        }
    }

    private String buildBootstrapPlan(StudioPressureMysqlTestSupport.PressureConfig config, String runId) {
        StringBuilder builder = new StringBuilder();
        builder.append("# 数据集构建测试条件与方案\n\n");
        builder.append("## 测试条件\n\n");
        builder.append("- runId: `").append(runId).append("`\n");
        builder.append("- 源库: `").append(config.getGeneration().getSourceDatabase()).append("`\n");
        builder.append("- 目标库: `").append(config.getGeneration().getTargetDatabase()).append("`\n");
        builder.append("- 目标规模: ").append(config.getGeneration().getTableCount()).append(" 张表，")
                .append(config.getGeneration().getFamilyCount()).append(" 个 family\n\n");
        builder.append("## 测试方案\n\n");
        builder.append("- 在同一 MySQL 实例上创建源/目标两个 schema\n");
        builder.append("- 生成 10000 组 1:1 对应表，列数按 family 变化\n");
        builder.append("- 只向源表写入分层数据，目标表保持空表\n");
        builder.append("- 输出 manifest，并校验源/目标表结构一致\n");
        return builder.toString();
    }

    private String buildBootstrapResult(StudioPressureMysqlTestSupport.DatasetManifest manifest) {
        StringBuilder builder = new StringBuilder();
        builder.append("# 数据集构建结果\n\n");
        builder.append("## 用例\n\n");
        builder.append("- 建库建表: PASS (source=`").append(manifest.getSourceDatabase())
                .append("`, target=`").append(manifest.getTargetDatabase()).append("`)\n");
        builder.append("- manifest 输出: PASS (tableCount=").append(manifest.getTableCount())
                .append(", familyCount=").append(manifest.getFamilyCount()).append(")\n");
        builder.append("- 结构一致性校验: PASS\n\n");
        builder.append("## 结论\n\n");
        builder.append("- 数据集构建完成，可继续执行 Studio 压测。\n");
        return builder.toString();
    }
}
