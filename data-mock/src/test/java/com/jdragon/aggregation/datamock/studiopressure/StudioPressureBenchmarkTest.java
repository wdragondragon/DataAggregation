package com.jdragon.aggregation.datamock.studiopressure;

import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StudioPressureBenchmarkTest {

    private static final int MAX_ERROR_SAMPLES = 10;
    private static final int WORKFLOW_PAGE_SIZE = 500;
    private static final int TREND_DAYS = 30;
    private static final int TOP_N = 10;
    private static final int WORKER_HEARTBEAT_GRACE_SECONDS = 120;
    private static final DateTimeFormatter WORKER_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private StudioPressureMysqlTestSupport.ProgressLogger progressLogger;
    private StudioPressureMysqlTestSupport.PressureConfig activeConfig;

    @Test
    public void benchmarkStudioPressureScenarios() throws Exception {
        Assume.assumeTrue("set -DrunStudioPressureBenchmarks=true to run the Studio pressure benchmark suite",
                Boolean.getBoolean("runStudioPressureBenchmarks"));

        StudioPressureMysqlTestSupport.PressureConfig config = StudioPressureMysqlTestSupport.loadConfig();
        StudioPressureMysqlTestSupport.DatasetManifest manifest = StudioPressureMysqlTestSupport.loadLatestManifest();
        String runId = StudioPressureMysqlTestSupport.newRunId();
        List<ScenarioSpec> scenarios = resolveScenarios(System.getProperty("studioPressureScenario", "all"));
        assertFalse("No pressure scenario selected", scenarios.isEmpty());

        Path runRoot = StudioPressureMysqlTestSupport.runRoot(runId);
        StudioPressureMysqlTestSupport.BenchmarkRunReport report = new StudioPressureMysqlTestSupport.BenchmarkRunReport();
        report.setRunId(runId);
        List<String> scenarioFailures = new ArrayList<String>();
        this.activeConfig = config;

        try (StudioPressureMysqlTestSupport.ProgressLogger logger =
                     StudioPressureMysqlTestSupport.openProgressLogger(runRoot.resolve("live-progress.log"))) {
            this.progressLogger = logger;
            log("Studio 压测开始，runId=" + runId
                    + "，datasetRunId=" + manifest.getRunId()
                    + "，scenarios=" + labelsOf(scenarios)
                    + "，progressLog=" + logger.getPath());

            Map<String, Object> context = new LinkedHashMap<String, Object>();
            context.put("runId", runId);
            context.put("datasetRunId", manifest.getRunId());
            context.put("sourceDatabase", manifest.getSourceDatabase());
            context.put("targetDatabase", manifest.getTargetDatabase());
            context.put("scenarioLabels", labelsOf(scenarios));
            StudioPressureMysqlTestSupport.writeJson(runRoot.resolve("benchmark-context.json"), context);
            writeBenchmarkPlan(config, manifest, scenarios, runRoot);

            for (ScenarioSpec scenario : scenarios) {
                log("准备执行场景 `" + scenario.getLabel() + "`");
                StudioPressureMysqlTestSupport.ScenarioBenchmarkResult result;
                try {
                    result = executeScenario(config, manifest, runId, scenario);
                } catch (Throwable e) {
                    result = failedScenarioResult(scenario, runId, e);
                    log("场景 `" + scenario.getLabel() + "` 执行异常：" + result.getFailureReason());
                }
                report.getScenarios().add(result);
                StudioPressureMysqlTestSupport.writeJson(runRoot.resolve("scenario-" + scenario.getLabel() + ".json"), result);
                log("场景 `" + scenario.getLabel() + "` 完成：sync=" + summarizeSync(result.getSyncResult())
                        + "；query=" + summarizeOperation(result.getQueryMetrics())
                        + "；statistics=" + summarizeOperation(result.getStatisticsMetrics())
                        + "；tasks=" + summarizeDrain(result.getTaskDrainMetrics())
                        + "；workflows=" + summarizeDrain(result.getWorkflowDrainMetrics()));
                if (!scenarioPassed(result)) {
                    scenarioFailures.add(scenario.getLabel() + ": " + scenarioFailureSummary(result));
                }
            }

            writeBenchmarkOutputs(runRoot, report);
            log("Studio 压测结束，报告已输出到 " + runRoot.resolve("benchmark-report.json"));
            assertTrue("Pressure benchmark failures: " + scenarioFailures, scenarioFailures.isEmpty());
        } finally {
            this.progressLogger = null;
            this.activeConfig = null;
        }
    }

    private void writeBenchmarkPlan(StudioPressureMysqlTestSupport.PressureConfig config,
                                    StudioPressureMysqlTestSupport.DatasetManifest manifest,
                                    List<ScenarioSpec> scenarios,
                                    Path runRoot) throws Exception {
        Map<String, Object> plan = new LinkedHashMap<String, Object>();
        plan.put("studioBaseUrl", config.getStudio().getBaseUrl());
        plan.put("sourceDatabase", manifest.getSourceDatabase());
        plan.put("targetDatabase", manifest.getTargetDatabase());
        plan.put("datasetRunId", manifest.getRunId());
        plan.put("tableCount", manifest.getTableCount());
        plan.put("familyCount", manifest.getFamilyCount());
        plan.put("selectedScenarios", labelsOf(scenarios));
        plan.put("scenarioPlans", buildScenarioPlanItems(scenarios));
        StudioPressureMysqlTestSupport.writeJson(runRoot.resolve("benchmark-plan.json"), plan);
        StudioPressureMysqlTestSupport.writeText(runRoot.resolve("benchmark-plan.md"),
                buildBenchmarkPlanMarkdown(config, manifest, scenarios));
    }

    private List<Map<String, Object>> buildScenarioPlanItems(List<ScenarioSpec> scenarios) {
        List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();
        for (ScenarioSpec scenario : scenarios) {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("label", scenario.label);
            item.put("fullSync", scenario.fullSync);
            item.put("sourceSyncCount", scenario.sourceSyncCount);
            item.put("targetSyncCount", scenario.targetSyncCount);
            item.put("syncBatchSize", scenario.syncBatchSize);
            item.put("syncConcurrency", scenario.syncConcurrency);
            item.put("queryRequests", scenario.queryRequests);
            item.put("queryConcurrency", scenario.queryConcurrency);
            item.put("statisticsRequests", scenario.statisticsRequests);
            item.put("statisticsConcurrency", scenario.statisticsConcurrency);
            item.put("taskFormula", "max(" + scenario.taskFloor + ", " + scenario.taskWorkerMultiplier + " * W)");
            item.put("workflowFormula", "max(" + scenario.workflowFloor + ", " + scenario.workflowWorkerMultiplier + " * W)");
            items.add(item);
        }
        return items;
    }

    private String buildBenchmarkPlanMarkdown(StudioPressureMysqlTestSupport.PressureConfig config,
                                              StudioPressureMysqlTestSupport.DatasetManifest manifest,
                                              List<ScenarioSpec> scenarios) {
        StringBuilder builder = new StringBuilder();
        builder.append("# Studio 万表压测测试条件与方案\n\n");
        builder.append("## 测试条件\n\n");
        builder.append("- Studio 地址: `").append(config.getStudio().getBaseUrl()).append("`\n");
        builder.append("- 源库: `").append(manifest.getSourceDatabase()).append("`\n");
        builder.append("- 目标库: `").append(manifest.getTargetDatabase()).append("`\n");
        builder.append("- 数据集 runId: `").append(manifest.getRunId()).append("`\n");
        builder.append("- 数据规模: ").append(manifest.getTableCount()).append(" 张表，")
                .append(manifest.getFamilyCount()).append(" 个 schema family\n");
        builder.append("- 前置要求: Studio 管理员账号可登录、当前租户下至少存在 1 个在线 worker、`/api/v1/statistics/options` 可返回技术元模型统计选项\n");
        builder.append("- 压测范围: 模型同步、模型筛选、模型统计分析、采集任务并发、工作流并发\n\n");
        builder.append("## 测试方案\n\n");
        builder.append("- 技术元模型查询与统计字段通过 `/api/v1/statistics/options` 动态解析，不写死 schemaCode 和 fieldKey\n");
        builder.append("- 查询压测命中 `SINGLE` 技术元模型上的 LIKE 字段；统计压测覆盖趋势图、柱状图、扇图、TopN\n");
        builder.append("- 模型同步阶段改为创建后台模型同步任务，并在日志中持续输出任务与表粒度进度\n");
        builder.append("- 采集任务使用单表 1:1 复制；工作流使用单节点 `COLLECTION_TASK`，并与直接任务使用不同任务集合\n");
        builder.append("- 任务和工作流都采用“先创建并上线，再 burst trigger，最后轮询排空”的方式执行\n\n");
        builder.append("## 分档用例\n\n");
        for (ScenarioSpec scenario : scenarios) {
            builder.append("### ").append(scenario.label).append("\n\n");
            if (scenario.fullSync) {
                builder.append("- 模型同步: 源/目标全量 10000 表按后台模型同步任务分批执行，taskBatch=")
                        .append(scenario.syncBatchSize)
                        .append("，创建并发=").append(scenario.syncConcurrency).append("\n");
            } else {
                builder.append("- 模型同步: 源 ").append(scenario.sourceSyncCount)
                        .append(" + 目标 ").append(scenario.targetSyncCount)
                        .append("，按后台模型同步任务分批执行，taskBatch=").append(scenario.syncBatchSize)
                        .append("，创建并发=").append(scenario.syncConcurrency).append("\n");
            }
            builder.append("- 模型筛选: ").append(scenario.queryRequests)
                    .append(" 次请求，并发 ").append(scenario.queryConcurrency).append("\n");
            builder.append("- 模型统计分析: ").append(scenario.statisticsRequests)
                    .append(" 次请求，并发 ").append(scenario.statisticsConcurrency).append("\n");
            builder.append("- 采集任务并发: `max(").append(scenario.taskFloor)
                    .append(", ").append(scenario.taskWorkerMultiplier).append(" * W)` 个任务\n");
            builder.append("- 工作流并发: `max(").append(scenario.workflowFloor)
                    .append(", ").append(scenario.workflowWorkerMultiplier).append(" * W)` 个工作流\n\n");
        }
        return builder.toString();
    }

    private StudioPressureMysqlTestSupport.ScenarioBenchmarkResult executeScenario(
            StudioPressureMysqlTestSupport.PressureConfig config,
            StudioPressureMysqlTestSupport.DatasetManifest manifest,
            String runId,
            ScenarioSpec scenario) throws Exception {
        StudioPressureMysqlTestSupport.ScenarioBenchmarkResult result = new StudioPressureMysqlTestSupport.ScenarioBenchmarkResult();
        result.setScenarioLabel(scenario.getLabel());
        log("[" + scenario.getLabel() + "] 开始执行，runId=" + runId);
        StudioPressureApiClient client = new StudioPressureApiClient(config);
        client.login();
        log("[" + scenario.getLabel() + "] Studio 登录完成，tenant=" + client.currentTenantId()
                + "，projectId=" + client.currentProjectId());

        String projectCode = "pressure_mock_" + runId + "_" + scenario.getLabel();
        result.setProjectCode(projectCode);
        StudioPressureApiClient.ProjectRef project = client.createProject(projectCode, projectCode);
        client.useProject(project.getId());
        log("[" + scenario.getLabel() + "] 已创建压测项目 `" + projectCode + "`，projectId=" + project.getId());

        List<StudioPressureApiClient.ProjectWorkerRef> workers = bindOnlineWorkers(client, project.getId());
        assertFalse("No online workers available for scenario " + scenario.getLabel(), workers.isEmpty());
        result.setWorkerCount(workers.size());
        log("[" + scenario.getLabel() + "] 在线 worker 绑定完成，workerCount=" + workers.size());

        StudioPressureApiClient.DataSourceRef sourceDatasource = client.saveMysqlDatasource(
                projectCode + "_source",
                config.getSourceMysql(),
                manifest.getSourceDatabase(),
                true
        );
        StudioPressureApiClient.DataSourceRef targetDatasource = client.saveMysqlDatasource(
                projectCode + "_target",
                config.getTargetMysql(),
                manifest.getTargetDatabase(),
                true
        );
        log("[" + scenario.getLabel() + "] 源/目标数据源已创建，sourceId=" + sourceDatasource.getId()
                + "，targetId=" + targetDatasource.getId());

        StudioPressureMysqlTestSupport.SyncResult syncResult = syncScenario(client, manifest, scenario, sourceDatasource, targetDatasource);
        log("[" + scenario.getLabel() + "] 模型同步完成，" + summarizeSync(syncResult));
        List<StudioPressureApiClient.ModelRef> sourceModels = client.listModelsByDatasource(sourceDatasource.getId());
        List<StudioPressureApiClient.ModelRef> targetModels = client.listModelsByDatasource(targetDatasource.getId());
        log("[" + scenario.getLabel() + "] 已回查同步模型，sourceModels=" + sourceModels.size()
                + "，targetModels=" + targetModels.size());

        List<StudioPressureMysqlTestSupport.TableSpec> syncedTables = scenario.isFullSync()
                ? StudioPressureMysqlTestSupport.selectTablesForSync(manifest, manifest.getTables().size())
                : StudioPressureMysqlTestSupport.selectTablesForSync(manifest, scenario.getSourceSyncCount());
        assertFalse("No synced tables resolved for scenario " + scenario.getLabel(), syncedTables.isEmpty());

        Map<String, StudioPressureApiClient.ModelRef> sourceModelMap = indexModelsByLocator(sourceModels);
        Map<String, StudioPressureApiClient.ModelRef> targetModelMap = indexModelsByLocator(targetModels);

        StudioPressureApiClient.StatisticsOptionsRef options = client.loadStatisticsOptions(sourceDatasource.getId(),
                sourceDatasource.getTypeCode(),
                "TECHNICAL");
        log("[" + scenario.getLabel() + "] 已加载统计 options，querySchemas=" + options.getQuerySchemas().size()
                + "，targetSchemas=" + options.getTargetSchemas().size());
        QuerySelection querySelection = resolveQuerySelection(client, sourceDatasource.getId(), options, sourceModels);
        List<StudioPressureApiClient.ModelRef> queryEligibleModels = filterModelsForQuery(sourceModels, querySelection);
        assertFalse("No eligible models available for query selection " + querySelection.describe(),
                queryEligibleModels.isEmpty());
        StatisticsSelection statisticsSelection = resolveStatisticsSelection(client, sourceDatasource.getId(), options);
        log("[" + scenario.getLabel() + "] 已解析查询/统计字段，queryField="
                + querySelection.describe()
                + "，queryEligibleModels=" + queryEligibleModels.size());

        StudioPressureMysqlTestSupport.OperationMetrics queryMetrics = runQueryPressure(
                client,
                scenario,
                sourceDatasource.getId(),
                queryEligibleModels,
                querySelection
        );
        log("[" + scenario.getLabel() + "] 模型筛选压测完成，" + summarizeOperation(queryMetrics));
        StudioPressureMysqlTestSupport.OperationMetrics statisticsMetrics = runStatisticsPressure(
                client,
                scenario,
                sourceDatasource.getId(),
                queryEligibleModels,
                querySelection,
                statisticsSelection
        );
        log("[" + scenario.getLabel() + "] 模型统计压测完成，" + summarizeOperation(statisticsMetrics));
        result.setSyncResult(syncResult);
        result.setQueryMetrics(queryMetrics);
        result.setStatisticsMetrics(statisticsMetrics);

        int directTaskCount = scenario.resolveTaskCount(workers.size());
        int workflowTaskCount = scenario.resolveWorkflowTaskCount(workers.size());
        List<StudioPressureMysqlTestSupport.TableSpec> directTables = StudioPressureMysqlTestSupport.selectTablesByOffset(
                syncedTables, 0, directTaskCount);
        List<StudioPressureMysqlTestSupport.TableSpec> workflowTables = StudioPressureMysqlTestSupport.selectTablesByOffset(
                syncedTables, directTables.size(), workflowTaskCount);
        assertTrue("Not enough synced tables for direct collection tasks in scenario " + scenario.getLabel(),
                directTables.size() == directTaskCount);
        assertTrue("Not enough synced tables for workflow collection tasks in scenario " + scenario.getLabel(),
                workflowTables.size() == workflowTaskCount);

        List<String> targetTableNames = new ArrayList<String>();
        targetTableNames.addAll(StudioPressureMysqlTestSupport.toPhysicalLocators(directTables));
        targetTableNames.addAll(StudioPressureMysqlTestSupport.toPhysicalLocators(workflowTables));
        StudioPressureMysqlTestSupport.truncateTargetTables(config, targetTableNames);
        log("[" + scenario.getLabel() + "] 已清空目标表，truncateCount=" + targetTableNames.size());

        List<StudioPressureApiClient.CollectionTaskRef> directTasks = createPublishedCollectionTasks(
                client,
                sourceDatasource,
                targetDatasource,
                sourceModelMap,
                targetModelMap,
                directTables,
                projectCode + "_task"
        );
        log("[" + scenario.getLabel() + "] 已创建并上线直接采集任务，count=" + directTasks.size());
        burstTriggerCollectionTasks(client, directTasks);
        StudioPressureMysqlTestSupport.DrainMetrics taskDrainMetrics = awaitCollectionTaskDrain(
                client,
                idsOfTasks(directTasks),
                scenario.getLabel(),
                scenario.resolveDrainTimeoutMs(config.getTimeouts())
        );
        result.setTaskDrainMetrics(taskDrainMetrics);
        log("[" + scenario.getLabel() + "] 采集任务并发排空完成，" + summarizeDrain(taskDrainMetrics));

        List<StudioPressureApiClient.CollectionTaskRef> workflowTasks = createPublishedCollectionTasks(
                client,
                sourceDatasource,
                targetDatasource,
                sourceModelMap,
                targetModelMap,
                workflowTables,
                projectCode + "_wf_task"
        );
        log("[" + scenario.getLabel() + "] 已创建并上线工作流任务底座，count=" + workflowTasks.size());
        List<StudioPressureApiClient.WorkflowRef> workflows = createPublishedWorkflows(
                client,
                workflowTasks,
                projectCode + "_workflow"
        );
        log("[" + scenario.getLabel() + "] 已创建并发布工作流，count=" + workflows.size());
        burstTriggerWorkflows(client, workflows);
        StudioPressureMysqlTestSupport.DrainMetrics workflowDrainMetrics = awaitWorkflowDrain(
                client,
                idsOfWorkflows(workflows),
                scenario.getLabel(),
                scenario.resolveDrainTimeoutMs(config.getTimeouts())
        );
        result.setWorkflowDrainMetrics(workflowDrainMetrics);
        log("[" + scenario.getLabel() + "] 工作流并发排空完成，" + summarizeDrain(workflowDrainMetrics));
        if (!scenarioPassed(result)) {
            log("[" + scenario.getLabel() + "] 场景存在失败项：" + scenarioFailureSummary(result));
        } else {
            log("[" + scenario.getLabel() + "] 场景执行成功结束");
        }
        return result;
    }

    private List<StudioPressureApiClient.ProjectWorkerRef> bindOnlineWorkers(StudioPressureApiClient client, Long projectId) {
        List<StudioPressureApiClient.ProjectWorkerRef> workers = client.listProjectWorkers(projectId);
        List<StudioPressureApiClient.ProjectWorkerRef> onlineWorkers = new ArrayList<StudioPressureApiClient.ProjectWorkerRef>();
        for (StudioPressureApiClient.ProjectWorkerRef worker : workers) {
            if (!isWorkerReady(worker)) {
                continue;
            }
            if (!Boolean.TRUE.equals(worker.getBoundToProject())) {
                client.bindProjectWorker(projectId, worker.getWorkerCode());
            }
            onlineWorkers.add(worker);
        }
        if (onlineWorkers.isEmpty()) {
            throw new IllegalStateException("No fresh online worker is available for project " + projectId
                    + "; check studio-worker heartbeat before starting the benchmark");
        }
        return onlineWorkers;
    }

    private StudioPressureMysqlTestSupport.SyncResult syncScenario(StudioPressureApiClient client,
                                                                   StudioPressureMysqlTestSupport.DatasetManifest manifest,
                                                                   ScenarioSpec scenario,
                                                                   StudioPressureApiClient.DataSourceRef sourceDatasource,
                                                                   StudioPressureApiClient.DataSourceRef targetDatasource) throws Exception {
        long startedAt = System.currentTimeMillis();
        int discoverPageSize = Math.max(1, Math.min(resolveSyncBatchSize(scenario, manifest), 500));
        StudioPressureApiClient.DiscoveryResult sourceDiscover = client.discoverModels(
                sourceDatasource.getId(), null, Integer.valueOf(1), Integer.valueOf(discoverPageSize));
        StudioPressureApiClient.DiscoveryResult targetDiscover = client.discoverModels(
                targetDatasource.getId(), null, Integer.valueOf(1), Integer.valueOf(discoverPageSize));
        long sourceDiscoverCount = resolveDiscoverCount(sourceDiscover);
        long targetDiscoverCount = resolveDiscoverCount(targetDiscover);
        log("[" + scenario.getLabel() + "] discover 完成，sourceTotal=" + sourceDiscoverCount
                + "，targetTotal=" + targetDiscoverCount
                + "，pageSize=" + discoverPageSize);

        int sourceSyncCount = scenario.isFullSync() ? manifest.getTableCount() : scenario.getSourceSyncCount();
        int targetSyncCount = scenario.isFullSync() ? manifest.getTableCount() : scenario.getTargetSyncCount();
        List<StudioPressureMysqlTestSupport.TableSpec> sourceTables = StudioPressureMysqlTestSupport.selectTablesForSync(
                manifest, sourceSyncCount);
        List<StudioPressureMysqlTestSupport.TableSpec> targetTables = StudioPressureMysqlTestSupport.selectTablesForSync(
                manifest, targetSyncCount);
        List<String> sourceLocators = StudioPressureMysqlTestSupport.toPhysicalLocators(sourceTables);
        List<String> targetLocators = StudioPressureMysqlTestSupport.toPhysicalLocators(targetTables);
        syncByModelSyncTasks(client,
                sourceDatasource.getId(),
                sourceLocators,
                resolveSyncBatchSize(scenario, manifest),
                scenario.getSyncConcurrency(),
                scenario.resolveDrainTimeoutMs(activeConfig.getTimeouts()),
                scenario.getLabel() + "-source");
        recoverMissingModels(client,
                sourceDatasource.getId(),
                sourceLocators,
                scenario.resolveDrainTimeoutMs(activeConfig.getTimeouts()),
                scenario.getLabel() + "-source");
        syncByModelSyncTasks(client,
                targetDatasource.getId(),
                targetLocators,
                resolveSyncBatchSize(scenario, manifest),
                scenario.getSyncConcurrency(),
                scenario.resolveDrainTimeoutMs(activeConfig.getTimeouts()),
                scenario.getLabel() + "-target");
        recoverMissingModels(client,
                targetDatasource.getId(),
                targetLocators,
                scenario.resolveDrainTimeoutMs(activeConfig.getTimeouts()),
                scenario.getLabel() + "-target");

        List<StudioPressureApiClient.ModelRef> sourceModels = client.listModelsByDatasource(sourceDatasource.getId());
        List<StudioPressureApiClient.ModelRef> targetModels = client.listModelsByDatasource(targetDatasource.getId());

        StudioPressureMysqlTestSupport.SyncResult result = new StudioPressureMysqlTestSupport.SyncResult();
        result.setElapsedMs(System.currentTimeMillis() - startedAt);
        result.setSourceDiscoverCount((int) Math.min(Integer.MAX_VALUE, sourceDiscoverCount));
        result.setTargetDiscoverCount((int) Math.min(Integer.MAX_VALUE, targetDiscoverCount));
        result.setSourceModelCount(sourceModels.size());
        result.setTargetModelCount(targetModels.size());

        if (scenario.isFullSync()) {
            assertTrue("Large scenario source sync should reach manifest size",
                    sourceModels.size() >= manifest.getTableCount());
            assertTrue("Large scenario target sync should reach manifest size",
                    targetModels.size() >= manifest.getTableCount());
        } else {
            assertTrue("Source sync should produce models for scenario " + scenario.getLabel(),
                    sourceModels.size() >= scenario.getSourceSyncCount());
            assertTrue("Target sync should produce models for scenario " + scenario.getLabel(),
                    targetModels.size() >= scenario.getTargetSyncCount());
        }
        return result;
    }

    private void syncByModelSyncTasks(StudioPressureApiClient client,
                                      Long datasourceId,
                                      List<String> physicalLocators,
                                      int batchSize,
                                      int concurrency,
                                      long timeoutMs,
                                      String syncLabel) throws Exception {
        List<List<String>> batches = partition(physicalLocators, batchSize);
        int effectiveCreateConcurrency = 1;
        log("[" + syncLabel + "] 模型同步任务开始，datasourceId=" + datasourceId
                + "，tables=" + physicalLocators.size()
                + "，tasks=" + batches.size()
                + "，taskBatch=" + batchSize
                + "，createConcurrency=" + effectiveCreateConcurrency);
        if (effectiveCreateConcurrency != concurrency) {
            log("[" + syncLabel + "] 当前压测聚焦后台模型同步任务执行，任务创建阶段串行化处理，configuredCreateConcurrency="
                    + concurrency + "，effectiveCreateConcurrency=" + effectiveCreateConcurrency);
        }
        List<StudioPressureApiClient.ModelSyncTaskRef> tasks = Collections.synchronizedList(new ArrayList<StudioPressureApiClient.ModelSyncTaskRef>());
        runConcurrent("create-model-sync-task-" + syncLabel, batches.size(), effectiveCreateConcurrency, new IndexedOperation() {
            @Override
            public void execute(int index) {
                List<String> page = batches.get(index);
                log("[" + syncLabel + "] task " + (index + 1) + "/" + batches.size()
                        + " 创建开始，batchCount=" + page.size()
                        + "，first=" + page.get(0)
                        + "，last=" + page.get(page.size() - 1));
                StudioPressureApiClient.ModelSyncTaskRef task = client.createModelSyncTask(datasourceId, page, "AUTO_PAGE");
                tasks.add(task);
                log("[" + syncLabel + "] task " + (index + 1) + "/" + batches.size()
                        + " 已创建，taskId=" + task.getId()
                        + "，name=" + task.getName());
            }
        });
        awaitModelSyncTasks(client, tasks, syncLabel, timeoutMs);
        log("[" + syncLabel + "] 模型同步任务结束");
    }

    private void recoverMissingModels(StudioPressureApiClient client,
                                      Long datasourceId,
                                      List<String> requestedLocators,
                                      long timeoutMs,
                                      String syncLabel) throws Exception {
        List<String> remaining = new ArrayList<String>(requestedLocators);
        for (int round = 1; round <= 3; round++) {
            remaining = findMissingLocators(client.listModelsByDatasource(datasourceId), remaining);
            if (remaining.isEmpty()) {
                log("[" + syncLabel + "] 同步补偿检查完成，无缺失模型");
                return;
            }
            log("[" + syncLabel + "] 检测到缺失模型 " + remaining.size() + "/" + requestedLocators.size()
                    + "，开始顺序补偿 round=" + round);
            syncByModelSyncTasks(client,
                    datasourceId,
                    remaining,
                    Math.min(20, Math.max(1, remaining.size())),
                    1,
                    timeoutMs,
                    syncLabel + "-recovery-" + round);
        }
        List<String> unresolved = findMissingLocators(client.listModelsByDatasource(datasourceId), remaining);
        if (!unresolved.isEmpty()) {
            throw new IllegalStateException("Missing synced models after recovery: " + unresolved.size()
                    + ", sample=" + unresolved.subList(0, Math.min(5, unresolved.size())));
        }
    }

    private List<String> findMissingLocators(List<StudioPressureApiClient.ModelRef> models,
                                             List<String> requestedLocators) {
        Set<String> syncedLocators = new LinkedHashSet<String>();
        if (models != null) {
            for (StudioPressureApiClient.ModelRef model : models) {
                if (!isBlank(model.getPhysicalLocator())) {
                    syncedLocators.add(normalizeLocator(model.getPhysicalLocator()));
                } else if (!isBlank(model.getName())) {
                    syncedLocators.add(normalizeLocator(model.getName()));
                }
            }
        }
        List<String> missing = new ArrayList<String>();
        for (String locator : requestedLocators) {
            if (!syncedLocators.contains(normalizeLocator(locator))) {
                missing.add(locator);
            }
        }
        return missing;
    }

    private void awaitModelSyncTasks(StudioPressureApiClient client,
                                     List<StudioPressureApiClient.ModelSyncTaskRef> tasks,
                                     String syncLabel,
                                     long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        String lastProgress = null;
        while (System.currentTimeMillis() < deadline) {
            int totalTasks = tasks.size();
            int completedTasks = 0;
            int runningTasks = 0;
            int pendingTasks = 0;
            int failedTasks = 0;
            int stoppedTasks = 0;
            int completedItems = 0;
            int totalItems = 0;
            List<String> failures = new ArrayList<String>();
            for (int index = 0; index < tasks.size(); index++) {
                StudioPressureApiClient.ModelSyncTaskRef task = client.getModelSyncTask(tasks.get(index).getId());
                tasks.set(index, task);
                totalItems += task.getTotalCount() == null ? 0 : task.getTotalCount().intValue();
                  completedItems += (task.getSuccessCount() == null ? 0 : task.getSuccessCount().intValue())
                          + (task.getFailedCount() == null ? 0 : task.getFailedCount().intValue())
                          + (task.getStoppedCount() == null ? 0 : task.getStoppedCount().intValue());
                  if (isTerminalModelSyncTaskStatus(task.getStatus())) {
                      completedTasks++;
                  } else if ("RUNNING".equalsIgnoreCase(task.getStatus())) {
                      runningTasks++;
                  } else {
                      pendingTasks++;
                  }
                  if ("FAILED".equalsIgnoreCase(task.getStatus())) {
                      failedTasks++;
                      if (failures.size() < MAX_ERROR_SAMPLES) {
                          failures.add(task.getId() + ":" + safe(task.getLastError()));
                    }
                } else if ("STOPPED".equalsIgnoreCase(task.getStatus())) {
                    stoppedTasks++;
                    if (failures.size() < MAX_ERROR_SAMPLES) {
                        failures.add(task.getId() + ":STOPPED");
                    }
                }
            }
              String progress = "tasks=" + completedTasks + "/" + totalTasks
                      + "，items=" + completedItems + "/" + totalItems
                      + "，running=" + runningTasks
                      + "，pending=" + pendingTasks
                      + "，failedTasks=" + failedTasks
                      + "，stoppedTasks=" + stoppedTasks;
            if (!progress.equals(lastProgress)) {
                log("[" + syncLabel + "] 后台同步进度 " + progress);
                lastProgress = progress;
            }
            if (completedTasks >= totalTasks) {
                if (!failures.isEmpty()) {
                    log("[" + syncLabel + "] 后台同步任务存在异常样本：" + failures);
                }
                return;
            }
            TimeUnit.SECONDS.sleep(3L);
        }
        throw new IllegalStateException("Model sync tasks timeout for " + syncLabel + " after " + timeoutMs + "ms");
    }

    private List<String> resolveSelectedLocators(List<StudioPressureApiClient.ModelRef> discoveredModels,
                                                 List<StudioPressureMysqlTestSupport.TableSpec> tables) {
        Map<String, String> locatorByNormalizedName = new LinkedHashMap<String, String>();
        if (discoveredModels != null) {
            for (StudioPressureApiClient.ModelRef model : discoveredModels) {
                if (model == null) {
                    continue;
                }
                String physicalLocator = model.getPhysicalLocator();
                String name = model.getName();
                if (!isBlank(physicalLocator)) {
                    locatorByNormalizedName.put(normalizeLocator(physicalLocator), physicalLocator);
                }
                if (!isBlank(name) && !locatorByNormalizedName.containsKey(normalizeLocator(name))) {
                    locatorByNormalizedName.put(normalizeLocator(name), name);
                }
            }
        }
        List<String> resolved = new ArrayList<String>();
        for (StudioPressureMysqlTestSupport.TableSpec table : tables) {
            String normalizedTableName = normalizeLocator(table.getTableName());
            String locator = locatorByNormalizedName.get(normalizedTableName);
            if (isBlank(locator)) {
                throw new IllegalStateException("Discovered models do not contain table: " + table.getTableName());
            }
            resolved.add(locator);
        }
        return resolved;
    }

    private StudioPressureMysqlTestSupport.OperationMetrics runQueryPressure(StudioPressureApiClient client,
                                                                             ScenarioSpec scenario,
                                                                             Long datasourceId,
                                                                             List<StudioPressureApiClient.ModelRef> sourceModels,
                                                                             QuerySelection selection) throws Exception {
        return runConcurrent("query-" + scenario.getLabel(),
                scenario.getQueryRequests(),
                scenario.getQueryConcurrency(),
                new IndexedOperation() {
                    @Override
                    public void execute(int index) {
                        StudioPressureApiClient.ModelRef sample = sourceModels.get(index % sourceModels.size());
                        Map<String, Object> payload = buildQueryPayload(datasourceId, selection, sample);
                        List<StudioPressureApiClient.ModelRef> models = client.queryModels(payload);
                        assertFalse("Dynamic query should match at least one model", models.isEmpty());
                    }
                });
    }

    private StudioPressureMysqlTestSupport.OperationMetrics runStatisticsPressure(StudioPressureApiClient client,
                                                                                  ScenarioSpec scenario,
                                                                                  Long datasourceId,
                                                                                  List<StudioPressureApiClient.ModelRef> sourceModels,
                                                                                  QuerySelection querySelection,
                                                                                  StatisticsSelection statisticsSelection) throws Exception {
        return runConcurrent("statistics-" + scenario.getLabel(),
                scenario.getStatisticsRequests(),
                scenario.getStatisticsConcurrency(),
                new IndexedOperation() {
                    @Override
                    public void execute(int index) throws Exception {
                        StudioPressureApiClient.ModelRef sample = sourceModels.get(index % sourceModels.size());
                        int selector = index % 6;
                        switch (selector) {
                            case 0:
                                verifyStringStatistics(client.statistics(buildRawStatisticsPayload(
                                        datasourceId,
                                        querySelection,
                                        sample,
                                        statisticsSelection.getPieSchema(),
                                        statisticsSelection.getPieField(),
                                        "COUNT_BY_VALUE"
                                )));
                                break;
                            case 1:
                                verifyNumericStatistics(client.statistics(buildRawStatisticsPayload(
                                        datasourceId,
                                        querySelection,
                                        sample,
                                        statisticsSelection.getBarSchema(),
                                        statisticsSelection.getBarField(),
                                        "SUMMARY"
                                )));
                                break;
                            case 2:
                                verifyChartWithRetry(client, buildChartPayload(
                                        datasourceId,
                                        querySelection,
                                        sample,
                                        statisticsSelection.getTrendSchema(),
                                        statisticsSelection.getTrendField(),
                                        "TREND"
                                ), true, "statistics-trend");
                                break;
                            case 3:
                                verifyChartWithRetry(client, buildChartPayload(
                                        datasourceId,
                                        querySelection,
                                        sample,
                                        statisticsSelection.getBarSchema(),
                                        statisticsSelection.getBarField(),
                                        "BAR"
                                ), false, "statistics-bar");
                                break;
                            case 4:
                                verifyChartWithRetry(client, buildChartPayload(
                                        datasourceId,
                                        querySelection,
                                        sample,
                                        statisticsSelection.getPieSchema(),
                                        statisticsSelection.getPieField(),
                                        "PIE"
                                ), false, "statistics-pie");
                                break;
                            default:
                                verifyChartWithRetry(client, buildChartPayload(
                                        datasourceId,
                                        querySelection,
                                        sample,
                                        statisticsSelection.getTopNSchema(),
                                        statisticsSelection.getTopNField(),
                                        "TOPN"
                                ), false, "statistics-topn");
                                break;
                        }
                    }
                });
    }

    private List<StudioPressureApiClient.CollectionTaskRef> createPublishedCollectionTasks(
            StudioPressureApiClient client,
            StudioPressureApiClient.DataSourceRef sourceDatasource,
            StudioPressureApiClient.DataSourceRef targetDatasource,
            Map<String, StudioPressureApiClient.ModelRef> sourceModelMap,
            Map<String, StudioPressureApiClient.ModelRef> targetModelMap,
            List<StudioPressureMysqlTestSupport.TableSpec> tables,
            String taskPrefix) {
        List<StudioPressureApiClient.CollectionTaskRef> result = new ArrayList<StudioPressureApiClient.CollectionTaskRef>();
        for (int i = 0; i < tables.size(); i++) {
            StudioPressureMysqlTestSupport.TableSpec table = tables.get(i);
            String locator = normalizeLocator(table.getTableName());
            StudioPressureApiClient.ModelRef sourceModel = sourceModelMap.get(locator);
            StudioPressureApiClient.ModelRef targetModel = targetModelMap.get(locator);
            assertNotNull("Source model missing for locator " + locator, sourceModel);
            assertNotNull("Target model missing for locator " + locator, targetModel);

            Map<String, Object> payload = new LinkedHashMap<String, Object>();
            payload.put("name", taskPrefix + "_" + String.format(Locale.ROOT, "%04d", Integer.valueOf(i + 1)));

            List<Map<String, Object>> sourceBindings = new ArrayList<Map<String, Object>>();
            Map<String, Object> sourceBinding = new LinkedHashMap<String, Object>();
            sourceBinding.put("sourceAlias", "src");
            sourceBinding.put("datasourceId", sourceDatasource.getId());
            sourceBinding.put("datasourceName", sourceDatasource.getName());
            sourceBinding.put("datasourceTypeCode", sourceDatasource.getTypeCode());
            sourceBinding.put("modelId", sourceModel.getId());
            sourceBinding.put("modelName", sourceModel.getName());
            sourceBinding.put("modelPhysicalLocator", sourceModel.getPhysicalLocator());
            sourceBindings.add(sourceBinding);
            payload.put("sourceBindings", sourceBindings);

            Map<String, Object> targetBinding = new LinkedHashMap<String, Object>();
            targetBinding.put("datasourceId", targetDatasource.getId());
            targetBinding.put("datasourceName", targetDatasource.getName());
            targetBinding.put("datasourceTypeCode", targetDatasource.getTypeCode());
            targetBinding.put("modelId", targetModel.getId());
            targetBinding.put("modelName", targetModel.getName());
            targetBinding.put("modelPhysicalLocator", targetModel.getPhysicalLocator());
            payload.put("targetBinding", targetBinding);

            List<Map<String, Object>> fieldMappings = new ArrayList<Map<String, Object>>();
            for (StudioPressureMysqlTestSupport.ColumnDefinition column : table.getColumns()) {
                Map<String, Object> mapping = new LinkedHashMap<String, Object>();
                mapping.put("sourceAlias", "src");
                mapping.put("sourceField", column.getName());
                mapping.put("targetField", column.getName());
                fieldMappings.add(mapping);
            }
            payload.put("fieldMappings", fieldMappings);

            Map<String, Object> executionOptions = new LinkedHashMap<String, Object>();
            executionOptions.put("writeMode", "insert");
            payload.put("executionOptions", executionOptions);

            StudioPressureApiClient.CollectionTaskRef saved = client.saveCollectionTask(payload);
            result.add(client.publishCollectionTask(saved.getId()));
            if ((i + 1) % 20 == 0 || i + 1 == tables.size()) {
                log("[" + taskPrefix + "] 采集任务创建进度 " + (i + 1) + "/" + tables.size());
            }
        }
        return result;
    }

    private void burstTriggerCollectionTasks(StudioPressureApiClient client,
                                             List<StudioPressureApiClient.CollectionTaskRef> tasks) throws Exception {
        log("[collection-trigger] 开始触发采集任务，count=" + tasks.size());
        runConcurrent("trigger-collection", tasks.size(), Math.min(tasks.size(), 16), new IndexedOperation() {
            @Override
            public void execute(int index) {
                client.triggerCollectionTask(tasks.get(index).getId());
            }
        });
        log("[collection-trigger] 采集任务触发完成");
    }

    private List<StudioPressureApiClient.WorkflowRef> createPublishedWorkflows(StudioPressureApiClient client,
                                                                               List<StudioPressureApiClient.CollectionTaskRef> tasks,
                                                                               String workflowPrefix) {
        List<StudioPressureApiClient.WorkflowRef> workflows = new ArrayList<StudioPressureApiClient.WorkflowRef>();
        for (int i = 0; i < tasks.size(); i++) {
            StudioPressureApiClient.CollectionTaskRef task = tasks.get(i);
            String suffix = String.format(Locale.ROOT, "%04d", Integer.valueOf(i + 1));

            Map<String, Object> payload = new LinkedHashMap<String, Object>();
            payload.put("code", workflowPrefix + "_" + suffix);
            payload.put("name", workflowPrefix + "_" + suffix);

            List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>();
            Map<String, Object> node = new LinkedHashMap<String, Object>();
            node.put("nodeCode", "node_1");
            node.put("nodeName", "Execute " + task.getName());
            node.put("nodeType", "COLLECTION_TASK");
            Map<String, Object> config = new LinkedHashMap<String, Object>();
            config.put("collectionTaskId", task.getId());
            node.put("config", config);
            node.put("fieldMappings", new ArrayList<Object>());
            nodes.add(node);
            payload.put("nodes", nodes);
            payload.put("edges", new ArrayList<Object>());

            StudioPressureApiClient.WorkflowRef saved = client.saveWorkflow(payload);
            workflows.add(client.publishWorkflow(saved.getId()));
            if ((i + 1) % 20 == 0 || i + 1 == tasks.size()) {
                log("[" + workflowPrefix + "] 工作流创建进度 " + (i + 1) + "/" + tasks.size());
            }
        }
        return workflows;
    }

    private void burstTriggerWorkflows(StudioPressureApiClient client,
                                       List<StudioPressureApiClient.WorkflowRef> workflows) throws Exception {
        log("[workflow-trigger] 开始触发工作流，count=" + workflows.size());
        runConcurrent("trigger-workflow", workflows.size(), Math.min(workflows.size(), 16), new IndexedOperation() {
            @Override
            public void execute(int index) {
                client.triggerWorkflow(workflows.get(index).getId());
            }
        });
        log("[workflow-trigger] 工作流触发完成");
    }

    private StudioPressureMysqlTestSupport.DrainMetrics awaitCollectionTaskDrain(StudioPressureApiClient client,
                                                                                 List<Long> taskIds,
                                                                                 String scenarioLabel,
                                                                                 long timeoutMs) throws Exception {
        long startedAt = System.currentTimeMillis();
        long deadline = startedAt + timeoutMs;
        Map<Long, StudioPressureApiClient.RunRecordRef> latestByTask = new LinkedHashMap<Long, StudioPressureApiClient.RunRecordRef>();
        Set<Long> pending = new LinkedHashSet<Long>(taskIds);
        int round = 0;
        int previousPending = pending.size();
        while (System.currentTimeMillis() <= deadline) {
            round++;
            Set<Long> unresolved = new LinkedHashSet<Long>();
            for (Long taskId : new ArrayList<Long>(pending)) {
                StudioPressureApiClient.RunListRef runList = client.listRuns(taskId, null);
                boolean queued = false;
                for (StudioPressureApiClient.QueuedTaskRef queuedTask : runList.getQueuedTasks()) {
                    if (taskId.equals(queuedTask.getCollectionTaskId())) {
                        queued = true;
                        break;
                    }
                }
                StudioPressureApiClient.RunRecordRef latest = latestRunRecord(runList.getRunRecords());
                if (latest != null) {
                    latestByTask.put(taskId, latest);
                }
                if (queued) {
                    unresolved.add(taskId);
                    continue;
                }
                if (latest == null || !isTerminalStatus(latest.getStatus())) {
                    unresolved.add(taskId);
                }
            }
            pending = unresolved;
            logDrainProgress("[" + scenarioLabel + "][collection-drain]", round, taskIds.size(), pending.size(), previousPending, startedAt);
            previousPending = pending.size();
            if (pending.isEmpty()) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(clientPollInterval(client));
        }
        return buildCollectionDrainMetrics(taskIds, latestByTask, pending, System.currentTimeMillis() - startedAt);
    }

    private StudioPressureMysqlTestSupport.DrainMetrics awaitWorkflowDrain(StudioPressureApiClient client,
                                                                           List<Long> workflowIds,
                                                                           String scenarioLabel,
                                                                           long timeoutMs) throws Exception {
        long startedAt = System.currentTimeMillis();
        long deadline = startedAt + timeoutMs;
        Map<Long, StudioPressureApiClient.WorkflowRunSummaryRef> latestByWorkflow = new LinkedHashMap<Long, StudioPressureApiClient.WorkflowRunSummaryRef>();
        Map<Long, StudioPressureApiClient.RunRecordRef> latestRunByWorkflow = new LinkedHashMap<Long, StudioPressureApiClient.RunRecordRef>();
        Set<Long> pending = new LinkedHashSet<Long>(workflowIds);
        int round = 0;
        int previousPending = pending.size();
        while (System.currentTimeMillis() <= deadline) {
            round++;
            Set<Long> unresolved = new LinkedHashSet<Long>();
            for (Long workflowId : new ArrayList<Long>(pending)) {
                StudioPressureApiClient.WorkflowRunPageRef page = client.listWorkflowRuns(workflowId, 1, WORKFLOW_PAGE_SIZE);
                StudioPressureApiClient.WorkflowRunSummaryRef latest = latestWorkflowRun(page.getItems());
                if (latest != null) {
                    latestByWorkflow.put(workflowId, latest);
                }
                StudioPressureApiClient.RunRecordRef latestRun = latestRunRecord(client.listRuns(null, workflowId).getRunRecords());
                if (latestRun != null) {
                    latestRunByWorkflow.put(workflowId, latestRun);
                }
                if (latest == null || !isTerminalStatus(latest.getStatus())) {
                    unresolved.add(workflowId);
                }
            }
            pending = unresolved;
            logDrainProgress("[" + scenarioLabel + "][workflow-drain]", round, workflowIds.size(), pending.size(), previousPending, startedAt);
            previousPending = pending.size();
            if (pending.isEmpty()) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(clientPollInterval(client));
        }
        return buildWorkflowDrainMetrics(workflowIds, latestByWorkflow, latestRunByWorkflow, pending,
                System.currentTimeMillis() - startedAt);
    }

    private StudioPressureMysqlTestSupport.DrainMetrics buildCollectionDrainMetrics(List<Long> taskIds,
                                                                                    Map<Long, StudioPressureApiClient.RunRecordRef> latestByTask,
                                                                                    Set<Long> pending,
                                                                                    long drainElapsedMs) {
        StudioPressureMysqlTestSupport.DrainMetrics metrics = new StudioPressureMysqlTestSupport.DrainMetrics();
        metrics.setTotalDefinitions(taskIds.size());
        metrics.setDrainElapsedMs(drainElapsedMs);
        for (Long taskId : taskIds) {
            if (pending.contains(taskId)) {
                metrics.setTimeoutCount(metrics.getTimeoutCount() + 1);
                addFailureSample(metrics.getFailureSamples(), "collectionTaskId=" + taskId + " timed out");
                continue;
            }
            StudioPressureApiClient.RunRecordRef record = latestByTask.get(taskId);
            if (record != null && isSuccessStatus(record.getStatus())) {
                metrics.setSuccessCount(metrics.getSuccessCount() + 1);
                recordWorker(metrics.getWorkerDistribution(), record.getWorkerCode());
            } else {
                metrics.setFailureCount(metrics.getFailureCount() + 1);
                addFailureSample(metrics.getFailureSamples(),
                        "collectionTaskId=" + taskId + " status=" + (record == null ? "UNKNOWN" : record.getStatus())
                                + " message=" + (record == null ? "" : safe(record.getMessage())));
                if (record != null) {
                    recordWorker(metrics.getWorkerDistribution(), record.getWorkerCode());
                }
            }
        }
        return metrics;
    }

    private StudioPressureMysqlTestSupport.DrainMetrics buildWorkflowDrainMetrics(List<Long> workflowIds,
                                                                                  Map<Long, StudioPressureApiClient.WorkflowRunSummaryRef> latestByWorkflow,
                                                                                  Map<Long, StudioPressureApiClient.RunRecordRef> latestRunByWorkflow,
                                                                                  Set<Long> pending,
                                                                                  long drainElapsedMs) {
        StudioPressureMysqlTestSupport.DrainMetrics metrics = new StudioPressureMysqlTestSupport.DrainMetrics();
        metrics.setTotalDefinitions(workflowIds.size());
        metrics.setDrainElapsedMs(drainElapsedMs);
        for (Long workflowId : workflowIds) {
            if (pending.contains(workflowId)) {
                metrics.setTimeoutCount(metrics.getTimeoutCount() + 1);
                addFailureSample(metrics.getFailureSamples(), "workflowDefinitionId=" + workflowId + " timed out");
                continue;
            }
            StudioPressureApiClient.WorkflowRunSummaryRef summary = latestByWorkflow.get(workflowId);
            StudioPressureApiClient.RunRecordRef runRecord = latestRunByWorkflow.get(workflowId);
            if (summary != null && isSuccessStatus(summary.getStatus())) {
                metrics.setSuccessCount(metrics.getSuccessCount() + 1);
                if (runRecord != null) {
                    recordWorker(metrics.getWorkerDistribution(), runRecord.getWorkerCode());
                }
            } else {
                metrics.setFailureCount(metrics.getFailureCount() + 1);
                addFailureSample(metrics.getFailureSamples(),
                        "workflowDefinitionId=" + workflowId + " status=" + (summary == null ? "UNKNOWN" : summary.getStatus()));
                if (runRecord != null) {
                    recordWorker(metrics.getWorkerDistribution(), runRecord.getWorkerCode());
                }
            }
        }
        return metrics;
    }

    private StudioPressureMysqlTestSupport.OperationMetrics runConcurrent(String label,
                                                                          int totalRequests,
                                                                          int concurrency,
                                                                          IndexedOperation operation) throws Exception {
        assertTrue("totalRequests must be positive for " + label, totalRequests > 0);
        int poolSize = Math.max(1, Math.min(concurrency, totalRequests));
        ExecutorService executorService = Executors.newFixedThreadPool(poolSize);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger completedCounter = new AtomicInteger();
        AtomicInteger successCounter = new AtomicInteger();
        AtomicInteger failureCounter = new AtomicInteger();
        int progressInterval = resolveProgressInterval(totalRequests);
        List<Future<RequestResult>> futures = new ArrayList<Future<RequestResult>>();
        log("[" + label + "] 并发执行开始，totalRequests=" + totalRequests + "，concurrency=" + poolSize);
        for (int i = 0; i < totalRequests; i++) {
            final int requestIndex = i;
            futures.add(executorService.submit(new Callable<RequestResult>() {
                @Override
                public RequestResult call() throws Exception {
                    startLatch.await();
                    long startedAt = System.currentTimeMillis();
                    try {
                        operation.execute(requestIndex);
                        successCounter.incrementAndGet();
                        return RequestResult.success(System.currentTimeMillis() - startedAt);
                    } catch (Throwable e) {
                        failureCounter.incrementAndGet();
                        return RequestResult.failure(System.currentTimeMillis() - startedAt, e);
                    } finally {
                        int completed = completedCounter.incrementAndGet();
                        if (completed == totalRequests || completed % progressInterval == 0) {
                            log("[" + label + "] 进度 " + completed + "/" + totalRequests
                                    + "，success=" + successCounter.get()
                                    + "，failure=" + failureCounter.get());
                        }
                    }
                }
            }));
        }

        long startedAt = System.currentTimeMillis();
        startLatch.countDown();
        List<Long> durations = new ArrayList<Long>();
        List<String> errors = new ArrayList<String>();
        int successCount = 0;
        int failureCount = 0;
        try {
            for (Future<RequestResult> future : futures) {
                RequestResult result;
                try {
                    result = future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                } catch (ExecutionException e) {
                    throw new IllegalStateException("Concurrent benchmark failed", e);
                }
                durations.add(Long.valueOf(result.getElapsedMs()));
                if (result.getErrorMessage() == null) {
                    successCount++;
                } else {
                    failureCount++;
                    addFailureSample(errors, result.getErrorMessage());
                }
            }
        } finally {
            executorService.shutdownNow();
        }

        long totalElapsedMs = Math.max(1L, System.currentTimeMillis() - startedAt);
        Collections.sort(durations);
        StudioPressureMysqlTestSupport.OperationMetrics metrics = new StudioPressureMysqlTestSupport.OperationMetrics();
        metrics.setTotalRequests(totalRequests);
        metrics.setSuccessCount(successCount);
        metrics.setFailureCount(failureCount);
        metrics.setTotalElapsedMs(totalElapsedMs);
        metrics.setP50Ms(StudioPressureMysqlTestSupport.percentile(durations, 50.0D));
        metrics.setP95Ms(StudioPressureMysqlTestSupport.percentile(durations, 95.0D));
        metrics.setP99Ms(StudioPressureMysqlTestSupport.percentile(durations, 99.0D));
        metrics.setThroughputPerSecond((double) totalRequests * 1000.0D / (double) totalElapsedMs);
        metrics.setErrorSamples(errors);
        log("[" + label + "] 并发执行结束，" + summarizeOperation(metrics));
        if (!errors.isEmpty()) {
            log("[" + label + "] failureSamples=" + errors);
        }
        return metrics;
    }

    private Map<String, Object> buildQueryPayload(Long datasourceId,
                                                  QuerySelection selection,
                                                  StudioPressureApiClient.ModelRef sample) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("datasourceId", datasourceId);
        payload.put("modelKind", "TABLE");
        if (selection != null && selection.isDynamic()) {
            payload.put("groups", Collections.singletonList(buildQueryGroup(selection, sample)));
        } else {
            payload.put("groups", new ArrayList<Object>());
        }
        return payload;
    }

    private Map<String, Object> buildRawStatisticsPayload(Long datasourceId,
                                                          QuerySelection querySelection,
                                                          StudioPressureApiClient.ModelRef sample,
                                                          SchemaFieldSelection targetSchema,
                                                          StudioPressureApiClient.FieldOptionRef targetField,
                                                          String statType) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("datasourceId", datasourceId);
        payload.put("modelKind", "TABLE");
        if (querySelection != null && querySelection.isDynamic()) {
            payload.put("groups", Collections.singletonList(buildQueryGroup(querySelection, sample)));
        } else {
            payload.put("groups", new ArrayList<Object>());
        }
        payload.put("targetMetaSchemaCode", targetSchema.getSchema().getSchemaCode());
        payload.put("targetFieldKey", targetField.getFieldKey());
        payload.put("targetScope", targetSchema.getSchema().getScope());
        payload.put("statType", statType);
        payload.put("topN", Integer.valueOf(TOP_N));
        return payload;
    }

    private Map<String, Object> buildChartPayload(Long datasourceId,
                                                  QuerySelection querySelection,
                                                  StudioPressureApiClient.ModelRef sample,
                                                  SchemaFieldSelection targetSchema,
                                                  StudioPressureApiClient.FieldOptionRef targetField,
                                                  String chartType) {
        Map<String, Object> payload = buildRawStatisticsPayload(datasourceId,
                querySelection,
                sample,
                targetSchema,
                targetField,
                "BAR".equals(chartType) ? "COUNT_BY_BUCKET" : "COUNT_BY_VALUE");
        payload.put("chartType", chartType);
        payload.put("days", Integer.valueOf(TREND_DAYS));
        payload.put("timeMode", "CREATED_AT");
        return payload;
    }

    private Map<String, Object> buildQueryGroup(QuerySelection selection,
                                                StudioPressureApiClient.ModelRef sample) {
        String sampleValue = resolveSampleValue(sample, selection.getField());
        assertTrue("Query sample value should not be blank for field " + selection.getField().getFieldKey(),
                !isBlank(sampleValue));

        String token = buildLikeToken(sampleValue);
        Map<String, Object> condition = new LinkedHashMap<String, Object>();
        condition.put("fieldKey", selection.getField().getFieldKey());
        condition.put("operator", "LIKE");
        condition.put("value", token);

        Map<String, Object> group = new LinkedHashMap<String, Object>();
        group.put("scope", selection.getSchema().getScope());
        group.put("metaSchemaCode", selection.getSchema().getSchemaCode());
        group.put("rowMatchMode", "SINGLE".equalsIgnoreCase(selection.getSchema().getDisplayMode()) ? null : "ANY_ITEM");
        group.put("conditions", Collections.singletonList(condition));
        return group;
    }

    private QuerySelection resolveQuerySelection(StudioPressureApiClient client,
                                                 Long datasourceId,
                                                 StudioPressureApiClient.StatisticsOptionsRef options,
                                                 List<StudioPressureApiClient.ModelRef> sourceModels) {
        List<String> preferredFieldKeys = Arrays.asList("physicalLocator", "name", "tableName", "remarks");
        for (String preferredFieldKey : preferredFieldKeys) {
            QuerySelection selection = findQuerySelection(options, sourceModels, preferredFieldKey);
            if (selection != null && validateDynamicQuerySelection(client, datasourceId, selection, sourceModels)) {
                return selection;
            }
        }
        for (StudioPressureApiClient.SchemaOptionRef schema : options.getQuerySchemas()) {
            if (!"TECHNICAL".equalsIgnoreCase(schema.getScope())) {
                continue;
            }
            if (!"SINGLE".equalsIgnoreCase(schema.getDisplayMode())) {
                continue;
            }
            for (StudioPressureApiClient.FieldOptionRef field : schema.getFields()) {
                if (!supportsLike(field)) {
                    continue;
                }
                if (hasSampleValue(sourceModels, field)) {
                    QuerySelection selection = QuerySelection.dynamic(schema, field);
                    if (validateDynamicQuerySelection(client, datasourceId, selection, sourceModels)) {
                        return selection;
                    }
                }
            }
        }
        log("[query-selection] 所有动态查询字段验证均未命中，降级为 datasourceId + modelKind(TABLE) 基础筛选");
        return QuerySelection.basicDatasourceFilter();
    }

    private QuerySelection findQuerySelection(StudioPressureApiClient.StatisticsOptionsRef options,
                                              List<StudioPressureApiClient.ModelRef> sourceModels,
                                              String fieldKey) {
        for (StudioPressureApiClient.SchemaOptionRef schema : options.getQuerySchemas()) {
            if (!"TECHNICAL".equalsIgnoreCase(schema.getScope())) {
                continue;
            }
            if (!"SINGLE".equalsIgnoreCase(schema.getDisplayMode())) {
                continue;
            }
            for (StudioPressureApiClient.FieldOptionRef field : schema.getFields()) {
                if (!fieldKey.equalsIgnoreCase(field.getFieldKey())) {
                    continue;
                }
                if (supportsLike(field) && hasSampleValue(sourceModels, field)) {
                    return QuerySelection.dynamic(schema, field);
                }
            }
        }
        return null;
    }

    private boolean validateDynamicQuerySelection(StudioPressureApiClient client,
                                                  Long datasourceId,
                                                  QuerySelection selection,
                                                  List<StudioPressureApiClient.ModelRef> sourceModels) {
        List<StudioPressureApiClient.ModelRef> candidates = filterModelsWithFieldValue(sourceModels, selection.getField());
        if (candidates.isEmpty()) {
            return false;
        }
        StudioPressureApiClient.ModelRef sample = candidates.get(0);
        try {
            List<StudioPressureApiClient.ModelRef> matched = client.queryModels(buildQueryPayload(datasourceId, selection, sample));
            boolean success = matched != null && !matched.isEmpty();
            if (!success) {
                log("[query-selection] 字段验证未命中，schema=" + selection.getSchema().getSchemaCode()
                        + "，field=" + selection.getField().getFieldKey());
            }
            return success;
        } catch (Exception e) {
            log("[query-selection] 字段验证失败，schema=" + selection.getSchema().getSchemaCode()
                    + "，field=" + selection.getField().getFieldKey()
                    + "，reason=" + safe(e.getMessage()));
            return false;
        }
    }

    private StatisticsSelection resolveStatisticsSelection(StudioPressureApiClient client,
                                                          Long datasourceId,
                                                          StudioPressureApiClient.StatisticsOptionsRef options) {
        SchemaFieldSelection trend = null;
        SchemaFieldSelection trendFallback = null;
        SchemaFieldSelection bar = null;
        SchemaFieldSelection pie = null;
        SchemaFieldSelection topN = null;
        for (StudioPressureApiClient.SchemaOptionRef schema : options.getTargetSchemas()) {
            if (!"TECHNICAL".equalsIgnoreCase(schema.getScope())) {
                continue;
            }
            for (StudioPressureApiClient.FieldOptionRef field : schema.getFields()) {
                if (trend == null
                        && "SINGLE".equalsIgnoreCase(schema.getDisplayMode())
                        && supportsChart(field, "TREND")) {
                    SchemaFieldSelection candidate = new SchemaFieldSelection(schema, field);
                    if (trendFallback == null) {
                        trendFallback = candidate;
                    }
                    if (validateTrendSelection(client, datasourceId, candidate)) {
                        trend = candidate;
                    }
                }
                if (bar == null
                        && supportsChart(field, "BAR")
                        && isNumericField(field.getValueType())) {
                    SchemaFieldSelection candidate = new SchemaFieldSelection(schema, field);
                    if (validateNumericSelection(client, datasourceId, candidate)) {
                        bar = candidate;
                    }
                }
                if (pie == null
                        && supportsChart(field, "PIE")
                        && isStringOrBooleanField(field.getValueType())) {
                    SchemaFieldSelection candidate = new SchemaFieldSelection(schema, field);
                    if (validateCategoricalSelection(client, datasourceId, candidate, "PIE")) {
                        pie = candidate;
                    }
                }
                if (topN == null
                        && supportsChart(field, "TOPN")
                        && isStringOrBooleanField(field.getValueType())) {
                    SchemaFieldSelection candidate = new SchemaFieldSelection(schema, field);
                    if (validateCategoricalSelection(client, datasourceId, candidate, "TOPN")) {
                        topN = candidate;
                    }
                }
            }
        }
        if (trend == null) {
            trend = trendFallback;
        }
        if (trend == null || bar == null || pie == null || topN == null) {
            throw new IllegalStateException("Statistics options do not provide all required chart capabilities for TECHNICAL scope");
        }
        return new StatisticsSelection(trend, bar, pie, topN);
    }

    private boolean validateTrendSelection(StudioPressureApiClient client,
                                           Long datasourceId,
                                           SchemaFieldSelection selection) {
        try {
            verifyChartWithRetry(client, buildChartPayload(
                    datasourceId,
                    QuerySelection.basicDatasourceFilter(),
                    null,
                    selection,
                    selection.getField(),
                    "TREND"
            ), true, "validate-trend");
            return true;
        } catch (Exception | AssertionError e) {
            log("[statistics-selection] 趋势字段校验未命中，schema="
                    + selection.getSchema().getSchemaCode()
                    + "，field=" + selection.getField().getFieldKey()
                    + "，reason=" + safe(e.getMessage()));
            return false;
        }
    }

    private boolean validateNumericSelection(StudioPressureApiClient client,
                                             Long datasourceId,
                                             SchemaFieldSelection selection) {
        try {
            verifyNumericStatistics(client.statistics(buildRawStatisticsPayload(
                    datasourceId,
                    QuerySelection.basicDatasourceFilter(),
                    null,
                    selection,
                    selection.getField(),
                    "SUMMARY"
            )));
            verifyChartWithRetry(client, buildChartPayload(
                    datasourceId,
                    QuerySelection.basicDatasourceFilter(),
                    null,
                    selection,
                    selection.getField(),
                    "BAR"
            ), false, "validate-bar");
            return true;
        } catch (Exception | AssertionError e) {
            log("[statistics-selection] 数值字段校验未命中，schema="
                    + selection.getSchema().getSchemaCode()
                    + "，field=" + selection.getField().getFieldKey()
                    + "，reason=" + safe(e.getMessage()));
            return false;
        }
    }

    private boolean validateCategoricalSelection(StudioPressureApiClient client,
                                                 Long datasourceId,
                                                 SchemaFieldSelection selection,
                                                 String chartType) {
        try {
            verifyStringStatistics(client.statistics(buildRawStatisticsPayload(
                    datasourceId,
                    QuerySelection.basicDatasourceFilter(),
                    null,
                    selection,
                    selection.getField(),
                    "COUNT_BY_VALUE"
            )));
            verifyChartWithRetry(client, buildChartPayload(
                    datasourceId,
                    QuerySelection.basicDatasourceFilter(),
                    null,
                    selection,
                    selection.getField(),
                    chartType
            ), false, "validate-" + chartType.toLowerCase(Locale.ROOT));
            return true;
        } catch (Exception | AssertionError e) {
            log("[statistics-selection] 分类字段校验未命中，schema="
                    + selection.getSchema().getSchemaCode()
                    + "，field=" + selection.getField().getFieldKey()
                    + "，chartType=" + chartType
                    + "，reason=" + safe(e.getMessage()));
            return false;
        }
    }

    private StudioPressureApiClient.ChartViewRef verifyChartWithRetry(StudioPressureApiClient client,
                                                                      Map<String, Object> payload,
                                                                      boolean trendChart,
                                                                      String label) throws Exception {
        Exception lastException = null;
        AssertionError lastAssertion = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                StudioPressureApiClient.ChartViewRef view = client.queryChart(payload);
                verifyChart(view, trendChart);
                return view;
            } catch (Exception e) {
                lastException = e;
            } catch (AssertionError e) {
                lastAssertion = e;
            }
            if (attempt < 3) {
                log("[" + label + "] 图表结果暂未稳定，准备重试，attempt=" + attempt);
                TimeUnit.MILLISECONDS.sleep(500L * attempt);
            }
        }
        if (lastException != null) {
            throw lastException;
        }
        if (lastAssertion != null) {
            throw lastAssertion;
        }
        throw new IllegalStateException("Chart verification failed");
    }

    private void verifyStringStatistics(StudioPressureApiClient.StatisticsViewRef view) {
        assertNotNull("Statistics response should not be null", view);
        assertTrue("matchedModelCount should be positive", asLong(view.getMatchedModelCount()) > 0L);
        assertTrue("matchedItemCount should be positive", asLong(view.getMatchedItemCount()) > 0L);
        assertFalse("COUNT_BY_VALUE should return buckets", view.getBuckets().isEmpty());
    }

    private void verifyNumericStatistics(StudioPressureApiClient.StatisticsViewRef view) {
        assertNotNull("Statistics response should not be null", view);
        assertTrue("matchedModelCount should be positive", asLong(view.getMatchedModelCount()) > 0L);
        assertTrue("Numeric summary should expose count",
                asLong(view.getSummaryMetrics().get("count")) > 0L);
    }

    private void verifyChart(StudioPressureApiClient.ChartViewRef view, boolean trendChart) {
        assertNotNull("Chart response should not be null", view);
        assertTrue("Chart should not be disabled: " + view.getDisabledReason(), isBlank(view.getDisabledReason()));
        if (trendChart) {
            assertTrue("Trend chart should contain rendered series, table rows or xAxis",
                    !view.getSeries().isEmpty() || !view.getTableRows().isEmpty() || !view.getXAxis().isEmpty());
            return;
        }
        assertTrue("Chart should contain rendered series or table rows",
                !view.getSeries().isEmpty() || !view.getTableRows().isEmpty());
    }

    private Map<String, StudioPressureApiClient.ModelRef> indexModelsByLocator(List<StudioPressureApiClient.ModelRef> models) {
        Map<String, StudioPressureApiClient.ModelRef> result = new LinkedHashMap<String, StudioPressureApiClient.ModelRef>();
        for (StudioPressureApiClient.ModelRef model : models) {
            result.put(normalizeLocator(model.getPhysicalLocator()), model);
        }
        return result;
    }

    private List<Long> idsOfTasks(List<StudioPressureApiClient.CollectionTaskRef> tasks) {
        List<Long> ids = new ArrayList<Long>();
        for (StudioPressureApiClient.CollectionTaskRef task : tasks) {
            ids.add(task.getId());
        }
        return ids;
    }

    private List<Long> idsOfWorkflows(List<StudioPressureApiClient.WorkflowRef> workflows) {
        List<Long> ids = new ArrayList<Long>();
        for (StudioPressureApiClient.WorkflowRef workflow : workflows) {
            ids.add(workflow.getId());
        }
        return ids;
    }

    private List<List<String>> partition(List<String> source, int batchSize) {
        List<List<String>> result = new ArrayList<List<String>>();
        int step = Math.max(1, batchSize);
        for (int i = 0; i < source.size(); i += step) {
            int end = Math.min(source.size(), i + step);
            result.add(new ArrayList<String>(source.subList(i, end)));
        }
        return result;
    }

    private List<ScenarioSpec> resolveScenarios(String filter) {
        List<ScenarioSpec> scenarios = Arrays.asList(
                new ScenarioSpec("small", false, 500, 500, 100, 4, 24, 4, 24, 4, 16, 2, 8, 1),
                new ScenarioSpec("medium", false, 3000, 3000, 200, 6, 48, 6, 48, 6, 64, 4, 32, 2),
                new ScenarioSpec("large", true, 0, 0, 500, 8, 96, 8, 96, 8, 256, 8, 128, 4)
        );
        if (isBlank(filter) || "all".equalsIgnoreCase(filter)) {
            return scenarios;
        }
        List<ScenarioSpec> selected = new ArrayList<ScenarioSpec>();
        for (ScenarioSpec scenario : scenarios) {
            if (scenario.getLabel().equalsIgnoreCase(filter)) {
                selected.add(scenario);
            }
        }
        return selected;
    }

    private List<String> labelsOf(List<ScenarioSpec> scenarios) {
        List<String> result = new ArrayList<String>();
        for (ScenarioSpec scenario : scenarios) {
            result.add(scenario.getLabel());
        }
        return result;
    }

    private boolean isOnlineWorker(StudioPressureApiClient.ProjectWorkerRef worker) {
        return worker != null && "ONLINE".equalsIgnoreCase(safe(worker.getStatus()));
    }

    private boolean isWorkerReady(StudioPressureApiClient.ProjectWorkerRef worker) {
        if (!isOnlineWorker(worker)) {
            return false;
        }
        LocalDateTime lastHeartbeatAt = parseWorkerTime(worker.getLastHeartbeatAt());
        if (lastHeartbeatAt == null) {
            return false;
        }
        return lastHeartbeatAt.isAfter(LocalDateTime.now().minusSeconds(WORKER_HEARTBEAT_GRACE_SECONDS));
    }

    private LocalDateTime parseWorkerTime(String value) {
        if (isBlank(value)) {
            return null;
        }
        try {
            return LocalDateTime.parse(value.trim(), WORKER_TIME_FORMATTER);
        } catch (Exception ignored) {
            return null;
        }
    }

    private boolean supportsLike(StudioPressureApiClient.FieldOptionRef field) {
        if (field == null || field.getQueryOperators() == null) {
            return false;
        }
        for (String operator : field.getQueryOperators()) {
            if ("LIKE".equalsIgnoreCase(operator)) {
                return true;
            }
        }
        return false;
    }

    private boolean supportsChart(StudioPressureApiClient.FieldOptionRef field, String chartType) {
        if (field == null || field.getSupportedChartTypes() == null) {
            return false;
        }
        for (String candidate : field.getSupportedChartTypes()) {
            if (chartType.equalsIgnoreCase(candidate)) {
                return true;
            }
        }
        return false;
    }

    private boolean isNumericField(String valueType) {
        return "INTEGER".equalsIgnoreCase(valueType)
                || "LONG".equalsIgnoreCase(valueType)
                || "DECIMAL".equalsIgnoreCase(valueType);
    }

    private boolean isStringOrBooleanField(String valueType) {
        return "STRING".equalsIgnoreCase(valueType) || "BOOLEAN".equalsIgnoreCase(valueType);
    }

    private boolean hasSampleValue(List<StudioPressureApiClient.ModelRef> models,
                                   StudioPressureApiClient.FieldOptionRef field) {
        for (StudioPressureApiClient.ModelRef model : models) {
            if (!isBlank(resolveSampleValue(model, field))) {
                return true;
            }
        }
        return false;
    }

    private List<StudioPressureApiClient.ModelRef> filterModelsWithFieldValue(List<StudioPressureApiClient.ModelRef> models,
                                                                              StudioPressureApiClient.FieldOptionRef field) {
        List<StudioPressureApiClient.ModelRef> result = new ArrayList<StudioPressureApiClient.ModelRef>();
        for (StudioPressureApiClient.ModelRef model : models) {
            if (!isBlank(resolveSampleValue(model, field))) {
                result.add(model);
            }
        }
        return result;
    }

    private List<StudioPressureApiClient.ModelRef> filterModelsForQuery(List<StudioPressureApiClient.ModelRef> models,
                                                                        QuerySelection selection) {
        if (selection == null || !selection.isDynamic()) {
            return new ArrayList<StudioPressureApiClient.ModelRef>(models);
        }
        return filterModelsWithFieldValue(models, selection.getField());
    }

    private String resolveSampleValue(StudioPressureApiClient.ModelRef model,
                                      StudioPressureApiClient.FieldOptionRef field) {
        if (model == null || field == null) {
            return null;
        }
        if ("physicalLocator".equalsIgnoreCase(field.getFieldKey())) {
            return model.getPhysicalLocator();
        }
        if ("name".equalsIgnoreCase(field.getFieldKey())) {
            return model.getName();
        }
        Object value = model.getTechnicalMetadata().get(field.getFieldKey());
        if (value == null && "tableName".equalsIgnoreCase(field.getFieldKey())) {
            value = normalizeLocator(model.getPhysicalLocator());
        }
        return value == null ? null : String.valueOf(value);
    }

    private String buildLikeToken(String sampleValue) {
        String normalized = sampleValue == null ? "" : sampleValue.trim();
        int end = Math.min(normalized.length(), 16);
        String prefix = normalized.substring(0, end);
        return "%" + prefix + "%";
    }

    private boolean isTerminalStatus(String status) {
        return isSuccessStatus(status)
                || "FAILED".equalsIgnoreCase(status)
                || "CANCELLED".equalsIgnoreCase(status)
                || "ABORTED".equalsIgnoreCase(status)
                || "ERROR".equalsIgnoreCase(status)
                || "SKIPPED".equalsIgnoreCase(status);
    }

    private StudioPressureApiClient.RunRecordRef latestRunRecord(List<StudioPressureApiClient.RunRecordRef> records) {
        StudioPressureApiClient.RunRecordRef latest = null;
        if (records == null) {
            return null;
        }
        for (StudioPressureApiClient.RunRecordRef record : records) {
            if (record == null) {
                continue;
            }
            if (latest == null || compareIds(record.getId(), latest.getId()) > 0) {
                latest = record;
            }
        }
        return latest;
    }

    private StudioPressureApiClient.WorkflowRunSummaryRef latestWorkflowRun(List<StudioPressureApiClient.WorkflowRunSummaryRef> items) {
        StudioPressureApiClient.WorkflowRunSummaryRef latest = null;
        if (items == null) {
            return null;
        }
        for (StudioPressureApiClient.WorkflowRunSummaryRef item : items) {
            if (item == null) {
                continue;
            }
            if (latest == null || compareIds(item.getWorkflowRunId(), latest.getWorkflowRunId()) > 0) {
                latest = item;
            }
        }
        return latest;
    }

    private boolean isSuccessStatus(String status) {
        return "SUCCESS".equalsIgnoreCase(status);
    }

    private boolean isTerminalModelSyncTaskStatus(String status) {
        return "SUCCESS".equalsIgnoreCase(status)
                || "FAILED".equalsIgnoreCase(status)
                || "STOPPED".equalsIgnoreCase(status);
    }

    private boolean isRetryableSyncException(Exception exception) {
        String message = exception == null ? null : exception.getMessage();
        if (isBlank(message)) {
            return false;
        }
        String normalized = message.toLowerCase(Locale.ROOT);
        return normalized.contains("deadlock found")
                || normalized.contains("lock wait timeout")
                || normalized.contains("sync selected batch returned no models")
                || normalized.contains("too many requests")
                || normalized.contains("connection reset")
                || normalized.contains("connection refused");
    }

    private int compareIds(Long left, Long right) {
        long leftValue = left == null ? Long.MIN_VALUE : left.longValue();
        long rightValue = right == null ? Long.MIN_VALUE : right.longValue();
        return Long.compare(leftValue, rightValue);
    }

    private void recordWorker(Map<String, Integer> workerDistribution, String workerCode) {
        if (isBlank(workerCode)) {
            return;
        }
        Integer current = workerDistribution.get(workerCode);
        workerDistribution.put(workerCode, Integer.valueOf(current == null ? 1 : current.intValue() + 1));
    }

    private void addFailureSample(List<String> samples, String value) {
        if (samples.size() >= MAX_ERROR_SAMPLES) {
            return;
        }
        samples.add(value);
    }

    private long asLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value == null) {
            return 0L;
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return 0L;
        }
        if (text.contains(".")) {
            return Math.round(Double.parseDouble(text));
        }
        return Long.parseLong(text);
    }

    private String normalizeLocator(String physicalLocator) {
        if (physicalLocator == null) {
            return null;
        }
        String value = physicalLocator.trim().replace("`", "");
        int dotIndex = value.lastIndexOf('.');
        return dotIndex >= 0 ? value.substring(dotIndex + 1) : value;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    private void log(String message) {
        if (progressLogger != null) {
            progressLogger.info(message);
            return;
        }
        System.out.println(message);
    }

    private int resolveSyncBatchSize(ScenarioSpec scenario,
                                     StudioPressureMysqlTestSupport.DatasetManifest manifest) {
        if (scenario.getSyncBatchSize() > 0) {
            return scenario.getSyncBatchSize();
        }
        return Math.min(500, Math.max(100, manifest.getTableCount()));
    }

    private int resolveEffectiveSyncConcurrency(ScenarioSpec scenario) {
        return Math.max(1, Math.min(1, scenario.getSyncConcurrency()));
    }

    private long resolveDiscoverCount(StudioPressureApiClient.DiscoveryResult discoveryResult) {
        if (discoveryResult == null) {
            return 0L;
        }
        if (discoveryResult.getTotal() > 0L) {
            return discoveryResult.getTotal();
        }
        return discoveryResult.getModels() == null ? 0L : discoveryResult.getModels().size();
    }

    private int resolveProgressInterval(int totalRequests) {
        if (totalRequests <= 5) {
            return 1;
        }
        return Math.max(1, totalRequests / 5);
    }

    private void logDrainProgress(String label,
                                  int round,
                                  int total,
                                  int pending,
                                  int previousPending,
                                  long startedAt) {
        if (round == 1 || pending != previousPending || round % 5 == 0) {
            log(label + " 轮询 round=" + round
                    + "，completed=" + (total - pending) + "/" + total
                    + "，pending=" + pending
                    + "，elapsedMs=" + (System.currentTimeMillis() - startedAt));
        }
    }

    private void writeBenchmarkOutputs(Path runRoot,
                                       StudioPressureMysqlTestSupport.BenchmarkRunReport report) throws Exception {
        StudioPressureMysqlTestSupport.writeJson(runRoot.resolve("benchmark-report.json"), report);
        StudioPressureMysqlTestSupport.writeText(StudioPressureMysqlTestSupport.resolveReportPath(),
                StudioPressureMysqlTestSupport.buildMarkdownReport(report));
    }

    private StudioPressureMysqlTestSupport.ScenarioBenchmarkResult failedScenarioResult(ScenarioSpec scenario,
                                                                                        String runId,
                                                                                        Throwable throwable) {
        StudioPressureMysqlTestSupport.ScenarioBenchmarkResult result = new StudioPressureMysqlTestSupport.ScenarioBenchmarkResult();
        result.setScenarioLabel(scenario.getLabel());
        result.setProjectCode("pressure_mock_" + runId + "_" + scenario.getLabel());
        result.setFailureReason(buildFailureReason(throwable));
        return result;
    }

    private boolean scenarioPassed(StudioPressureMysqlTestSupport.ScenarioBenchmarkResult result) {
        return result != null
                && isBlank(result.getFailureReason())
                && hasSyncedModels(result.getSyncResult())
                && metricsPassed(result.getQueryMetrics())
                && metricsPassed(result.getStatisticsMetrics())
                && drainPassed(result.getTaskDrainMetrics())
                && drainPassed(result.getWorkflowDrainMetrics());
    }

    private boolean hasSyncedModels(StudioPressureMysqlTestSupport.SyncResult metrics) {
        return metrics != null
                && metrics.getSourceModelCount() > 0
                && metrics.getTargetModelCount() > 0;
    }

    private boolean metricsPassed(StudioPressureMysqlTestSupport.OperationMetrics metrics) {
        return metrics != null
                && metrics.getTotalRequests() > 0
                && metrics.getSuccessCount() > 0
                && metrics.getFailureCount() == 0;
    }

    private boolean drainPassed(StudioPressureMysqlTestSupport.DrainMetrics metrics) {
        return metrics != null
                && metrics.getTotalDefinitions() > 0
                && metrics.getSuccessCount() == metrics.getTotalDefinitions()
                && metrics.getFailureCount() == 0
                && metrics.getTimeoutCount() == 0;
    }

    private String scenarioFailureSummary(StudioPressureMysqlTestSupport.ScenarioBenchmarkResult result) {
        if (result == null) {
            return "result=null";
        }
        List<String> reasons = new ArrayList<String>();
        if (!hasSyncedModels(result.getSyncResult())) {
            reasons.add("模型同步未达标");
        }
        if (!metricsPassed(result.getQueryMetrics())) {
            reasons.add("模型筛选失败=" + summarizeOperation(result.getQueryMetrics()));
        }
        if (!metricsPassed(result.getStatisticsMetrics())) {
            reasons.add("模型统计失败=" + summarizeOperation(result.getStatisticsMetrics()));
        }
        if (!drainPassed(result.getTaskDrainMetrics())) {
            reasons.add("采集任务并发失败=" + summarizeDrain(result.getTaskDrainMetrics()));
        }
        if (!drainPassed(result.getWorkflowDrainMetrics())) {
            reasons.add("工作流并发失败=" + summarizeDrain(result.getWorkflowDrainMetrics()));
        }
        if (!isBlank(result.getFailureReason())) {
            reasons.add("异常=" + result.getFailureReason());
        }
        return reasons.isEmpty() ? "unknown" : String.join("；", reasons);
    }

    private String buildFailureReason(Throwable throwable) {
        if (throwable == null) {
            return "unknown error";
        }
        String message = safe(throwable.getMessage());
        if (isBlank(message)) {
            message = throwable.getClass().getSimpleName();
        }
        return throwable.getClass().getSimpleName() + ": " + message;
    }

    private String summarizeSync(StudioPressureMysqlTestSupport.SyncResult metrics) {
        if (metrics == null) {
            return "n/a";
        }
        return "elapsedMs=" + metrics.getElapsedMs()
                + ", discover(source/target)=" + metrics.getSourceDiscoverCount() + "/" + metrics.getTargetDiscoverCount()
                + ", models(source/target)=" + metrics.getSourceModelCount() + "/" + metrics.getTargetModelCount();
    }

    private String summarizeOperation(StudioPressureMysqlTestSupport.OperationMetrics metrics) {
        if (metrics == null) {
            return "n/a";
        }
        return "success=" + metrics.getSuccessCount() + "/" + metrics.getTotalRequests()
                + ", failure=" + metrics.getFailureCount()
                + ", totalElapsedMs=" + metrics.getTotalElapsedMs()
                + ", p95=" + metrics.getP95Ms()
                + ", throughput=" + String.format(Locale.ROOT, "%.2f/s", Double.valueOf(metrics.getThroughputPerSecond()));
    }

    private String summarizeDrain(StudioPressureMysqlTestSupport.DrainMetrics metrics) {
        if (metrics == null) {
            return "n/a";
        }
        return "success=" + metrics.getSuccessCount() + "/" + metrics.getTotalDefinitions()
                + ", failure=" + metrics.getFailureCount()
                + ", timeout=" + metrics.getTimeoutCount()
                + ", drainElapsedMs=" + metrics.getDrainElapsedMs();
    }

    private long clientPollInterval(StudioPressureApiClient client) {
        StudioPressureMysqlTestSupport.PressureConfig config = activeConfig == null
                ? StudioPressureMysqlTestSupport.loadConfig()
                : activeConfig;
        return config.getTimeouts().getPollIntervalMs().longValue();
    }

    private interface IndexedOperation {
        void execute(int index) throws Exception;
    }

    private static final class RequestResult {
        private final long elapsedMs;
        private final String errorMessage;

        private RequestResult(long elapsedMs, String errorMessage) {
            this.elapsedMs = elapsedMs;
            this.errorMessage = errorMessage;
        }

        static RequestResult success(long elapsedMs) {
            return new RequestResult(elapsedMs, null);
        }

        static RequestResult failure(long elapsedMs, Throwable exception) {
            if (exception == null) {
                return new RequestResult(elapsedMs, "unknown error");
            }
            String message = exception.getMessage();
            if (message == null || message.trim().isEmpty()) {
                message = exception.getClass().getSimpleName();
            }
            return new RequestResult(elapsedMs, message);
        }

        long getElapsedMs() {
            return elapsedMs;
        }

        String getErrorMessage() {
            return errorMessage;
        }
    }

    private static final class QuerySelection {
        private final StudioPressureApiClient.SchemaOptionRef schema;
        private final StudioPressureApiClient.FieldOptionRef field;
        private final boolean dynamic;
        private final String description;

        private QuerySelection(StudioPressureApiClient.SchemaOptionRef schema,
                               StudioPressureApiClient.FieldOptionRef field,
                               boolean dynamic,
                               String description) {
            this.schema = schema;
            this.field = field;
            this.dynamic = dynamic;
            this.description = description;
        }

        static QuerySelection dynamic(StudioPressureApiClient.SchemaOptionRef schema,
                                      StudioPressureApiClient.FieldOptionRef field) {
            return new QuerySelection(schema, field, true, schema.getSchemaCode() + "." + field.getFieldKey());
        }

        static QuerySelection basicDatasourceFilter() {
            return new QuerySelection(null, null, false, "datasourceId+modelKind(TABLE)");
        }

        StudioPressureApiClient.SchemaOptionRef getSchema() {
            return schema;
        }

        StudioPressureApiClient.FieldOptionRef getField() {
            return field;
        }

        boolean isDynamic() {
            return dynamic;
        }

        String describe() {
            return description;
        }
    }

    private static final class SchemaFieldSelection {
        private final StudioPressureApiClient.SchemaOptionRef schema;
        private final StudioPressureApiClient.FieldOptionRef field;

        private SchemaFieldSelection(StudioPressureApiClient.SchemaOptionRef schema,
                                     StudioPressureApiClient.FieldOptionRef field) {
            this.schema = schema;
            this.field = field;
        }

        StudioPressureApiClient.SchemaOptionRef getSchema() {
            return schema;
        }

        StudioPressureApiClient.FieldOptionRef getField() {
            return field;
        }
    }

    private static final class StatisticsSelection {
        private final SchemaFieldSelection trendSchema;
        private final SchemaFieldSelection barSchema;
        private final SchemaFieldSelection pieSchema;
        private final SchemaFieldSelection topNSchema;

        private StatisticsSelection(SchemaFieldSelection trendSchema,
                                    SchemaFieldSelection barSchema,
                                    SchemaFieldSelection pieSchema,
                                    SchemaFieldSelection topNSchema) {
            this.trendSchema = trendSchema;
            this.barSchema = barSchema;
            this.pieSchema = pieSchema;
            this.topNSchema = topNSchema;
        }

        SchemaFieldSelection getTrendSchema() {
            return trendSchema;
        }

        SchemaFieldSelection getBarSchema() {
            return barSchema;
        }

        SchemaFieldSelection getPieSchema() {
            return pieSchema;
        }

        SchemaFieldSelection getTopNSchema() {
            return topNSchema;
        }

        StudioPressureApiClient.FieldOptionRef getTrendField() {
            return trendSchema.getField();
        }

        StudioPressureApiClient.FieldOptionRef getBarField() {
            return barSchema.getField();
        }

        StudioPressureApiClient.FieldOptionRef getPieField() {
            return pieSchema.getField();
        }

        StudioPressureApiClient.FieldOptionRef getTopNField() {
            return topNSchema.getField();
        }
    }

    private static final class ScenarioSpec {
        private final String label;
        private final boolean fullSync;
        private final int sourceSyncCount;
        private final int targetSyncCount;
        private final int syncBatchSize;
        private final int syncConcurrency;
        private final int queryRequests;
        private final int queryConcurrency;
        private final int statisticsRequests;
        private final int statisticsConcurrency;
        private final int taskFloor;
        private final int taskWorkerMultiplier;
        private final int workflowFloor;
        private final int workflowWorkerMultiplier;

        private ScenarioSpec(String label,
                             boolean fullSync,
                             int sourceSyncCount,
                             int targetSyncCount,
                             int syncBatchSize,
                             int syncConcurrency,
                             int queryRequests,
                             int queryConcurrency,
                             int statisticsRequests,
                             int statisticsConcurrency,
                             int taskFloor,
                             int taskWorkerMultiplier,
                             int workflowFloor,
                             int workflowWorkerMultiplier) {
            this.label = label;
            this.fullSync = fullSync;
            this.sourceSyncCount = sourceSyncCount;
            this.targetSyncCount = targetSyncCount;
            this.syncBatchSize = syncBatchSize;
            this.syncConcurrency = syncConcurrency;
            this.queryRequests = queryRequests;
            this.queryConcurrency = queryConcurrency;
            this.statisticsRequests = statisticsRequests;
            this.statisticsConcurrency = statisticsConcurrency;
            this.taskFloor = taskFloor;
            this.taskWorkerMultiplier = taskWorkerMultiplier;
            this.workflowFloor = workflowFloor;
            this.workflowWorkerMultiplier = workflowWorkerMultiplier;
        }

        String getLabel() {
            return label;
        }

        boolean isFullSync() {
            return fullSync;
        }

        int getSourceSyncCount() {
            return sourceSyncCount;
        }

        int getTargetSyncCount() {
            return targetSyncCount;
        }

        int getSyncBatchSize() {
            return syncBatchSize;
        }

        int getSyncConcurrency() {
            return syncConcurrency;
        }

        int getQueryRequests() {
            return queryRequests;
        }

        int getQueryConcurrency() {
            return queryConcurrency;
        }

        int getStatisticsRequests() {
            return statisticsRequests;
        }

        int getStatisticsConcurrency() {
            return statisticsConcurrency;
        }

        int resolveTaskCount(int workerCount) {
            return Math.max(taskFloor, taskWorkerMultiplier * workerCount);
        }

        int resolveWorkflowTaskCount(int workerCount) {
            return Math.max(workflowFloor, workflowWorkerMultiplier * workerCount);
        }

        long resolveDrainTimeoutMs(StudioPressureMysqlTestSupport.TimeoutConfig timeoutConfig) {
            if ("small".equalsIgnoreCase(label)) {
                return timeoutConfig.getSmallDrainTimeoutMs().longValue();
            }
            if ("medium".equalsIgnoreCase(label)) {
                return timeoutConfig.getMediumDrainTimeoutMs().longValue();
            }
            return timeoutConfig.getLargeDrainTimeoutMs().longValue();
        }
    }
}
