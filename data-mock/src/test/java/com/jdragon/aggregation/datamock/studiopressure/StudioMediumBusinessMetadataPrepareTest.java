package com.jdragon.aggregation.datamock.studiopressure;

import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
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

public class StudioMediumBusinessMetadataPrepareTest {

    private static final int SOURCE_SYNC_COUNT = 3000;
    private static final int TARGET_SYNC_COUNT = 3000;
    private static final int TASK_BATCH_SIZE = 200;
    private static final int RECOVERY_BATCH_SIZE = 20;
    private static final int BUSINESS_METADATA_SAVE_CONCURRENCY = 8;
    private static final int MAX_ERROR_SAMPLES = 10;
    private static final long POLL_INTERVAL_MS = 3000L;
    private static final DateTimeFormatter REPORT_DATE = DateTimeFormatter.BASIC_ISO_DATE;
    private static final List<String> BUSINESS_VALUES = Collections.unmodifiableList(Arrays.asList(
            "sales_domain",
            "finance_domain",
            "risk_domain",
            "customer_domain",
            "supply_domain",
            "product_domain",
            "operations_domain",
            "content_domain",
            "settlement_domain",
            "compliance_domain",
            "device_domain",
            "analytics_domain"
    ));

    private StudioPressureMysqlTestSupport.ProgressLogger progressLogger;

    @Test
    public void prepareMediumSyncedModelsWithBusinessMetadata() throws Exception {
        Assume.assumeTrue("set -DrunStudioMediumBusinessMetadataPrepare=true to run medium sync with business metadata",
                Boolean.getBoolean("runStudioMediumBusinessMetadataPrepare"));

        StudioPressureMysqlTestSupport.PressureConfig config = StudioPressureMysqlTestSupport.loadConfig();
        StudioPressureMysqlTestSupport.DatasetManifest manifest = StudioPressureMysqlTestSupport.loadLatestManifest();
        String runId = StudioPressureMysqlTestSupport.newRunId();
        Path runRoot = StudioPressureMysqlTestSupport.runRoot(runId);

        try (StudioPressureMysqlTestSupport.ProgressLogger logger =
                     StudioPressureMysqlTestSupport.openProgressLogger(runRoot.resolve("live-progress.log"))) {
            this.progressLogger = logger;
            log("medium 业务统计准备开始，runId=" + runId + "，datasetRunId=" + manifest.getRunId());
            StudioPressureMysqlTestSupport.writeText(runRoot.resolve("prepare-plan.md"),
                    buildPlanMarkdown(config, manifest, runId));

            StudioPressureApiClient client = new StudioPressureApiClient(config);
            client.login();
            String projectCode = "pressure_mock_" + runId + "_medium_business_stats";
            StudioPressureApiClient.ProjectRef project = client.createProject(projectCode, projectCode);
            client.useProject(project.getId());
            log("压测项目已创建，projectCode=" + projectCode + "，projectId=" + project.getId());

            List<StudioPressureApiClient.ProjectWorkerRef> workers = bindOnlineWorkers(client, project.getId());
            log("worker 绑定完成，workerCount=" + workers.size());

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
            log("源/目标数据源已创建，sourceId=" + sourceDatasource.getId() + "，targetId=" + targetDatasource.getId());

            List<StudioPressureMysqlTestSupport.TableSpec> sourceTables =
                    StudioPressureMysqlTestSupport.selectTablesForSync(manifest, SOURCE_SYNC_COUNT);
            List<StudioPressureMysqlTestSupport.TableSpec> targetTables =
                    StudioPressureMysqlTestSupport.selectTablesForSync(manifest, TARGET_SYNC_COUNT);
            List<String> sourceLocators = StudioPressureMysqlTestSupport.toPhysicalLocators(sourceTables);
            List<String> targetLocators = StudioPressureMysqlTestSupport.toPhysicalLocators(targetTables);

            long syncTimeoutMs = config.getTimeouts().getMediumDrainTimeoutMs();
            syncByModelSyncTasks(client, sourceDatasource.getId(), sourceLocators, TASK_BATCH_SIZE, syncTimeoutMs, "medium-source");
            recoverMissingModels(client, sourceDatasource.getId(), sourceLocators, syncTimeoutMs, "medium-source");
            syncByModelSyncTasks(client, targetDatasource.getId(), targetLocators, TASK_BATCH_SIZE, syncTimeoutMs, "medium-target");
            recoverMissingModels(client, targetDatasource.getId(), targetLocators, syncTimeoutMs, "medium-target");

            List<StudioPressureApiClient.ModelRef> sourceModels = sortModels(client.listModelsByDatasource(sourceDatasource.getId()));
            List<StudioPressureApiClient.ModelRef> targetModels = sortModels(client.listModelsByDatasource(targetDatasource.getId()));
            assertTrue("Source synced models should reach medium count", sourceModels.size() >= SOURCE_SYNC_COUNT);
            assertTrue("Target synced models should reach medium count", targetModels.size() >= TARGET_SYNC_COUNT);
            log("模型回查完成，sourceModels=" + sourceModels.size() + "，targetModels=" + targetModels.size());

            Long businessSchemaVersionId = resolveBusinessSchemaVersionId(client);
            log("业务元模型版本已解析，schemaVersionId=" + businessSchemaVersionId);

            BusinessAssignmentResult assignmentResult = applyBusinessMetadata(client, sourceDatasource.getId(), sourceModels, businessSchemaVersionId);
            log("业务元数据回填完成，updatedModels=" + assignmentResult.updatedCount
                    + "，distinctValues=" + assignmentResult.valueDistribution.size());

            waitForIndexQueueDrain(client, config.getTimeouts().getMediumDrainTimeoutMs());
            log("索引重建队列已排空");

            StudioPressureApiClient.StatisticsOptionsRef businessOptions = client.loadStatisticsOptions(
                    sourceDatasource.getId(),
                    sourceDatasource.getTypeCode(),
                    "BUSINESS");
            StudioPressureApiClient.SchemaOptionRef majorSchema = resolveBusinessTargetSchema(businessOptions);
            StudioPressureApiClient.FieldOptionRef majorField = resolveField(majorSchema, "major");
            StudioPressureApiClient.StatisticsViewRef statisticsView = client.statistics(
                    buildBusinessStatisticsPayload(sourceDatasource.getId(), majorSchema, majorField));

            Map<String, Object> summary = new LinkedHashMap<String, Object>();
            summary.put("runId", runId);
            summary.put("datasetRunId", manifest.getRunId());
            summary.put("projectCode", projectCode);
            summary.put("projectId", project.getId());
            summary.put("sourceDatasourceId", sourceDatasource.getId());
            summary.put("targetDatasourceId", targetDatasource.getId());
            summary.put("sourceSyncedModelCount", sourceModels.size());
            summary.put("targetSyncedModelCount", targetModels.size());
            summary.put("businessSchemaVersionId", businessSchemaVersionId);
            summary.put("businessValues", BUSINESS_VALUES);
            summary.put("updatedBusinessModelCount", assignmentResult.updatedCount);
            summary.put("businessValueDistribution", assignmentResult.valueDistribution);
            summary.put("statisticsMatchedModelCount", statisticsView.getMatchedModelCount());
            summary.put("statisticsMatchedItemCount", statisticsView.getMatchedItemCount());
            summary.put("statisticsBuckets", statisticsView.getBuckets());
            summary.put("statisticsSummaryMetrics", statisticsView.getSummaryMetrics());
            summary.put("sampleAssignments", assignmentResult.sampleAssignments);
            StudioPressureMysqlTestSupport.writeJson(runRoot.resolve("prepare-summary.json"), summary);
            String resultMarkdown = buildResultMarkdown(projectCode, sourceDatasource, targetDatasource,
                    sourceModels, targetModels, assignmentResult, statisticsView);
            StudioPressureMysqlTestSupport.writeText(runRoot.resolve("prepare-result.md"), resultMarkdown);
            StudioPressureMysqlTestSupport.writeText(resolvePrepareReportPath(), resultMarkdown);

            log("medium 业务统计准备完成，summary=" + runRoot.resolve("prepare-summary.json"));
        } finally {
            this.progressLogger = null;
        }
    }

    private BusinessAssignmentResult applyBusinessMetadata(StudioPressureApiClient client,
                                                           Long datasourceId,
                                                           List<StudioPressureApiClient.ModelRef> models,
                                                           Long schemaVersionId) throws Exception {
        assertFalse("Source models should not be empty", models.isEmpty());
        ExecutorService executorService = Executors.newFixedThreadPool(BUSINESS_METADATA_SAVE_CONCURRENCY);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger completed = new AtomicInteger();
        AtomicInteger success = new AtomicInteger();
        AtomicInteger failure = new AtomicInteger();
        List<String> failures = Collections.synchronizedList(new ArrayList<String>());
        Map<String, Integer> distribution = Collections.synchronizedMap(new LinkedHashMap<String, Integer>());
        List<Map<String, Object>> sampleAssignments = Collections.synchronizedList(new ArrayList<Map<String, Object>>());
        List<Future<Void>> futures = new ArrayList<Future<Void>>();
        int progressInterval = Math.max(1, models.size() / 10);
        log("开始回填业务元数据，models=" + models.size() + "，concurrency=" + BUSINESS_METADATA_SAVE_CONCURRENCY);

        for (int index = 0; index < models.size(); index++) {
            final int modelIndex = index;
            futures.add(executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    startLatch.await();
                    StudioPressureApiClient.ModelRef model = models.get(modelIndex);
                    String businessValue = resolveBusinessValue(model.getPhysicalLocator());
                    try {
                        client.saveModel(buildModelSavePayload(datasourceId, model, schemaVersionId, businessValue));
                        incrementDistribution(distribution, businessValue);
                        if (sampleAssignments.size() < 24) {
                            sampleAssignments.add(sampleAssignment(model, businessValue));
                        }
                        success.incrementAndGet();
                    } catch (Throwable e) {
                        if (failures.size() < MAX_ERROR_SAMPLES) {
                            failures.add(model.getPhysicalLocator() + ": " + e.getMessage());
                        }
                        failure.incrementAndGet();
                    } finally {
                        int done = completed.incrementAndGet();
                        if (done == models.size() || done % progressInterval == 0) {
                            log("业务元数据回填进度 " + done + "/" + models.size()
                                    + "，success=" + success.get()
                                    + "，failure=" + failure.get());
                        }
                    }
                    return null;
                }
            }));
        }

        startLatch.countDown();
        try {
            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    throw new IllegalStateException("Business metadata update failed", e);
                }
            }
        } finally {
            executorService.shutdownNow();
        }
        assertTrue("Business metadata update failures: " + failures, failures.isEmpty());
        BusinessAssignmentResult result = new BusinessAssignmentResult();
        result.updatedCount = success.get();
        result.valueDistribution = new LinkedHashMap<String, Integer>(distribution);
        result.sampleAssignments = new ArrayList<Map<String, Object>>(sampleAssignments);
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
            throw new IllegalStateException("No fresh online worker is available for project " + projectId);
        }
        return onlineWorkers;
    }

    private void syncByModelSyncTasks(StudioPressureApiClient client,
                                      Long datasourceId,
                                      List<String> physicalLocators,
                                      int batchSize,
                                      long timeoutMs,
                                      String label) throws Exception {
        List<List<String>> batches = partition(physicalLocators, batchSize);
        List<StudioPressureApiClient.ModelSyncTaskRef> tasks = new ArrayList<StudioPressureApiClient.ModelSyncTaskRef>();
        log("[" + label + "] 开始创建模型同步任务，tables=" + physicalLocators.size() + "，tasks=" + batches.size());
        for (int index = 0; index < batches.size(); index++) {
            List<String> batch = batches.get(index);
            StudioPressureApiClient.ModelSyncTaskRef task = client.createModelSyncTask(datasourceId, batch, "AUTO_PAGE");
            tasks.add(task);
            log("[" + label + "] task " + (index + 1) + "/" + batches.size() + " 已创建，taskId=" + task.getId()
                    + "，range=" + batch.get(0) + " -> " + batch.get(batch.size() - 1));
        }
        awaitModelSyncTasks(client, tasks, label, timeoutMs);
    }

    private void recoverMissingModels(StudioPressureApiClient client,
                                      Long datasourceId,
                                      List<String> requestedLocators,
                                      long timeoutMs,
                                      String label) throws Exception {
        List<String> remaining = new ArrayList<String>(requestedLocators);
        for (int round = 1; round <= 3; round++) {
            remaining = findMissingLocators(client.listModelsByDatasource(datasourceId), remaining);
            if (remaining.isEmpty()) {
                log("[" + label + "] 缺失模型补偿检查完成");
                return;
            }
            log("[" + label + "] 检测到缺失模型 " + remaining.size() + "，开始补偿 round=" + round);
            syncByModelSyncTasks(client,
                    datasourceId,
                    remaining,
                    Math.min(RECOVERY_BATCH_SIZE, Math.max(1, remaining.size())),
                    timeoutMs,
                    label + "-recovery-" + round);
        }
        List<String> unresolved = findMissingLocators(client.listModelsByDatasource(datasourceId), remaining);
        assertTrue("Missing synced models after recovery: " + unresolved, unresolved.isEmpty());
    }

    private void awaitModelSyncTasks(StudioPressureApiClient client,
                                     List<StudioPressureApiClient.ModelSyncTaskRef> tasks,
                                     String label,
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
                totalItems += valueOf(task.getTotalCount());
                completedItems += valueOf(task.getSuccessCount()) + valueOf(task.getFailedCount()) + valueOf(task.getStoppedCount());
                if (isTerminalTaskStatus(task.getStatus())) {
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
                    + "，failed=" + failedTasks
                    + "，stopped=" + stoppedTasks;
            if (!progress.equals(lastProgress)) {
                log("[" + label + "] 后台同步进度 " + progress);
                lastProgress = progress;
            }
            if (completedTasks >= totalTasks) {
                assertTrue("Model sync task failures: " + failures, failures.isEmpty());
                return;
            }
            TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
        }
        throw new IllegalStateException("Model sync tasks timeout for " + label + " after " + timeoutMs + "ms");
    }

    private void waitForIndexQueueDrain(StudioPressureApiClient client, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        String lastProgress = null;
        while (System.currentTimeMillis() < deadline) {
            StudioPressureApiClient.IndexQueueStatusRef status = client.getModelIndexQueueStatus();
            int pending = valueOf(status.getPendingRebuildCount());
            int queuedCommands = valueOf(status.getQueuedCommandCount());
            int activeCommands = valueOf(status.getActiveCommandCount());
            String progress = "pendingRebuild=" + pending
                    + "，queuedCommands=" + queuedCommands
                    + "，activeCommands=" + activeCommands
                    + "，busy=" + String.valueOf(status.getBusy());
            if (!progress.equals(lastProgress)) {
                log("索引队列状态 " + progress);
                lastProgress = progress;
            }
            if (!Boolean.TRUE.equals(status.getBusy()) && pending <= 0 && queuedCommands <= 0 && activeCommands <= 0) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
        }
        throw new IllegalStateException("Index rebuild queue did not drain within " + timeoutMs + "ms");
    }

    private Long resolveBusinessSchemaVersionId(StudioPressureApiClient client) {
        List<StudioPressureApiClient.MetadataSchemaRef> schemas = client.listMetaSchemas();
        for (StudioPressureApiClient.MetadataSchemaRef schema : schemas) {
            if ("business:org:table".equalsIgnoreCase(schema.getSchemaCode())) {
                return schema.getCurrentVersionId();
            }
        }
        throw new IllegalStateException("business:org:table schema not found");
    }

    private StudioPressureApiClient.SchemaOptionRef resolveBusinessTargetSchema(StudioPressureApiClient.StatisticsOptionsRef options) {
        assertNotNull("Statistics options should not be null", options);
        for (StudioPressureApiClient.SchemaOptionRef schema : options.getTargetSchemas()) {
            if ("business:org:table".equalsIgnoreCase(schema.getSchemaCode())) {
                return schema;
            }
        }
        throw new IllegalStateException("Business statistics target schema `business:org:table` not found");
    }

    private StudioPressureApiClient.FieldOptionRef resolveField(StudioPressureApiClient.SchemaOptionRef schema, String fieldKey) {
        if (schema == null || schema.getFields() == null) {
            throw new IllegalStateException("Schema fields are empty");
        }
        for (StudioPressureApiClient.FieldOptionRef field : schema.getFields()) {
            if (fieldKey.equalsIgnoreCase(field.getFieldKey())) {
                return field;
            }
        }
        throw new IllegalStateException("Field `" + fieldKey + "` not found under schema " + schema.getSchemaCode());
    }

    private Map<String, Object> buildBusinessStatisticsPayload(Long datasourceId,
                                                               StudioPressureApiClient.SchemaOptionRef schema,
                                                               StudioPressureApiClient.FieldOptionRef field) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("datasourceId", datasourceId);
        payload.put("modelKind", "TABLE");
        payload.put("groups", new ArrayList<Object>());
        payload.put("targetMetaSchemaCode", schema.getSchemaCode());
        payload.put("targetFieldKey", field.getFieldKey());
        payload.put("targetScope", schema.getScope());
        payload.put("statType", "COUNT_BY_VALUE");
        payload.put("topN", Integer.valueOf(20));
        return payload;
    }

    private Map<String, Object> buildModelSavePayload(Long datasourceId,
                                                      StudioPressureApiClient.ModelRef model,
                                                      Long businessSchemaVersionId,
                                                      String businessValue) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("id", model.getId());
        payload.put("datasourceId", datasourceId);
        payload.put("name", model.getName());
        payload.put("physicalLocator", model.getPhysicalLocator());
        payload.put("modelKind", model.getModelKind());
        payload.put("schemaVersionId", model.getSchemaVersionId());
        payload.put("technicalMetadata", model.getTechnicalMetadata() == null
                ? new LinkedHashMap<String, Object>()
                : new LinkedHashMap<String, Object>(model.getTechnicalMetadata()));
        payload.put("businessMetadata", singleBusinessMetadata(businessSchemaVersionId, "major", businessValue));
        return payload;
    }

    private Map<String, Object> singleBusinessMetadata(Long schemaVersionId, String fieldKey, Object value) {
        Map<String, Object> metadata = new LinkedHashMap<String, Object>();
        List<Map<String, Object>> entries = new ArrayList<Map<String, Object>>();
        Map<String, Object> entry = new LinkedHashMap<String, Object>();
        entry.put("schemaVersionId", schemaVersionId);
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put(fieldKey, value);
        entry.put("values", values);
        entries.add(entry);
        metadata.put("__metaModels", entries);
        return metadata;
    }

    private String resolveBusinessValue(String physicalLocator) {
        int family = extractFamilyNumber(physicalLocator);
        int index = Math.floorMod(family - 1, BUSINESS_VALUES.size());
        return BUSINESS_VALUES.get(index);
    }

    private int extractFamilyNumber(String locator) {
        if (locator == null) {
            return 1;
        }
        String normalized = normalizeLocator(locator);
        int marker = normalized.indexOf("_f");
        if (marker < 0 || marker + 4 > normalized.length()) {
            return 1;
        }
        String digits = normalized.substring(marker + 2, marker + 4);
        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    private List<String> findMissingLocators(List<StudioPressureApiClient.ModelRef> models, List<String> requestedLocators) {
        Set<String> existing = new LinkedHashSet<String>();
        if (models != null) {
            for (StudioPressureApiClient.ModelRef model : models) {
                existing.add(normalizeLocator(model.getPhysicalLocator()));
            }
        }
        List<String> missing = new ArrayList<String>();
        for (String locator : requestedLocators) {
            if (!existing.contains(normalizeLocator(locator))) {
                missing.add(locator);
            }
        }
        return missing;
    }

    private List<List<String>> partition(List<String> values, int size) {
        List<List<String>> parts = new ArrayList<List<String>>();
        if (values == null || values.isEmpty()) {
            return parts;
        }
        for (int index = 0; index < values.size(); index += size) {
            parts.add(new ArrayList<String>(values.subList(index, Math.min(index + size, values.size()))));
        }
        return parts;
    }

    private boolean isWorkerReady(StudioPressureApiClient.ProjectWorkerRef worker) {
        if (worker == null || !"ONLINE".equalsIgnoreCase(safe(worker.getStatus()))) {
            return false;
        }
        LocalDateTime lastHeartbeatAt = parseWorkerTime(worker.getLastHeartbeatAt());
        if (lastHeartbeatAt == null) {
            return false;
        }
        return lastHeartbeatAt.isAfter(LocalDateTime.now().minusSeconds(120L));
    }

    private LocalDateTime parseWorkerTime(String value) {
        if (isBlank(value)) {
            return null;
        }
        try {
            return LocalDateTime.parse(value.trim(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        } catch (Exception ignored) {
            return null;
        }
    }

    private boolean isTerminalTaskStatus(String status) {
        return "SUCCESS".equalsIgnoreCase(status)
                || "FAILED".equalsIgnoreCase(status)
                || "STOPPED".equalsIgnoreCase(status);
    }

    private List<StudioPressureApiClient.ModelRef> sortModels(List<StudioPressureApiClient.ModelRef> models) {
        List<StudioPressureApiClient.ModelRef> sorted = new ArrayList<StudioPressureApiClient.ModelRef>(models);
        sorted.sort(new Comparator<StudioPressureApiClient.ModelRef>() {
            @Override
            public int compare(StudioPressureApiClient.ModelRef left, StudioPressureApiClient.ModelRef right) {
                return safe(left.getPhysicalLocator()).compareToIgnoreCase(safe(right.getPhysicalLocator()));
            }
        });
        return sorted;
    }

    private Map<String, Object> sampleAssignment(StudioPressureApiClient.ModelRef model, String businessValue) {
        Map<String, Object> item = new LinkedHashMap<String, Object>();
        item.put("modelId", model.getId());
        item.put("modelName", model.getName());
        item.put("physicalLocator", model.getPhysicalLocator());
        item.put("major", businessValue);
        return item;
    }

    private void incrementDistribution(Map<String, Integer> distribution, String businessValue) {
        synchronized (distribution) {
            Integer current = distribution.get(businessValue);
            distribution.put(businessValue, Integer.valueOf(current == null ? 1 : current.intValue() + 1));
        }
    }

    private int valueOf(Number number) {
        return number == null ? 0 : number.intValue();
    }

    private String normalizeLocator(String locator) {
        if (locator == null) {
            return "";
        }
        String value = locator.trim().replace("`", "");
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

    private Path resolvePrepareReportPath() {
        Path moduleRoot = Paths.get("data-mock");
        if (!Files.exists(moduleRoot.resolve("pom.xml"))) {
            moduleRoot = Paths.get(".").toAbsolutePath().normalize();
        } else {
            moduleRoot = moduleRoot.toAbsolutePath().normalize();
        }
        return moduleRoot.resolve("validation-reports")
                .resolve("studio-medium-business-prepare-" + LocalDate.now().format(REPORT_DATE) + ".md");
    }

    private String buildPlanMarkdown(StudioPressureMysqlTestSupport.PressureConfig config,
                                     StudioPressureMysqlTestSupport.DatasetManifest manifest,
                                     String runId) {
        StringBuilder builder = new StringBuilder();
        builder.append("# Medium 模型同步与业务元数据准备方案\n\n");
        builder.append("## 测试条件\n\n");
        builder.append("- runId: `").append(runId).append("`\n");
        builder.append("- Studio 地址: `").append(config.getStudio().getBaseUrl()).append("`\n");
        builder.append("- 数据集 runId: `").append(manifest.getRunId()).append("`\n");
        builder.append("- 源库: `").append(manifest.getSourceDatabase()).append("`\n");
        builder.append("- 目标库: `").append(manifest.getTargetDatabase()).append("`\n");
        builder.append("- medium 同步规模: source ").append(SOURCE_SYNC_COUNT)
                .append(" + target ").append(TARGET_SYNC_COUNT).append("\n");
        builder.append("- 业务元数据字段: `business:org:table.major`\n");
        builder.append("- 业务值集合: ").append(BUSINESS_VALUES).append("\n\n");
        builder.append("## 测试方案\n\n");
        builder.append("- 创建独立项目与源/目标 mysql8 数据源\n");
        builder.append("- 同步阶段全部走后台模型同步任务，每批 ").append(TASK_BATCH_SIZE).append(" 张表\n");
        builder.append("- 仅给源侧同步模型回填业务元数据，目标侧模型保持空业务元数据\n");
        builder.append("- 业务值按 family 稳定映射到 12 个固定标签，不做随机散射\n");
        builder.append("- 等待索引重建队列排空后，执行一次业务字段统计验证可用性\n");
        return builder.toString();
    }

    private String buildResultMarkdown(String projectCode,
                                       StudioPressureApiClient.DataSourceRef sourceDatasource,
                                       StudioPressureApiClient.DataSourceRef targetDatasource,
                                       List<StudioPressureApiClient.ModelRef> sourceModels,
                                       List<StudioPressureApiClient.ModelRef> targetModels,
                                       BusinessAssignmentResult assignmentResult,
                                       StudioPressureApiClient.StatisticsViewRef statisticsView) {
        StringBuilder builder = new StringBuilder();
        builder.append("# Medium 模型同步与业务元数据准备结果\n\n");
        builder.append("## 用例\n\n");
        builder.append("- 独立项目创建: PASS (`").append(projectCode).append("`)\n");
        builder.append("- 模型同步: PASS (source=").append(sourceModels.size())
                .append(", target=").append(targetModels.size()).append(")\n");
        builder.append("- 业务元数据回填: PASS (updated=").append(assignmentResult.updatedCount)
                .append(", distinctValues=").append(assignmentResult.valueDistribution.size()).append(")\n");
        builder.append("- 索引队列排空: PASS\n");
        builder.append("- 业务统计验证: PASS (matchedModels=").append(String.valueOf(statisticsView.getMatchedModelCount()))
                .append(", matchedItems=").append(String.valueOf(statisticsView.getMatchedItemCount())).append(")\n\n");
        builder.append("## 结论\n\n");
        builder.append("- 数据已准备完成，可直接在项目 `").append(projectCode)
                .append("` 下按业务字段 `major` 做统计分析。\n");
        builder.append("- 业务元数据只写入源数据源 `").append(sourceDatasource.getName())
                .append("` 的同步模型，目标数据源 `").append(targetDatasource.getName())
                .append("` 未额外写入业务值，统计口径更清晰。\n");
        builder.append("- 当前已稳定写入的业务值分布为: ").append(assignmentResult.valueDistribution).append("\n");
        builder.append("- 统计接口桶结果样例: ").append(statisticsView.getBuckets()).append("\n");
        return builder.toString();
    }

    private static final class BusinessAssignmentResult {
        private int updatedCount;
        private Map<String, Integer> valueDistribution = new LinkedHashMap<String, Integer>();
        private List<Map<String, Object>> sampleAssignments = new ArrayList<Map<String, Object>>();
    }
}
