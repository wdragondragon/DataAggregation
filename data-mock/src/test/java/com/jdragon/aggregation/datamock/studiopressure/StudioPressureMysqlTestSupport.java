package com.jdragon.aggregation.datamock.studiopressure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.ColumnInfo;
import com.jdragon.aggregation.datasource.InsertDataDTO;
import com.jdragon.aggregation.datasource.mysql8.Mysql8SourcePlugin;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class StudioPressureMysqlTestSupport {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private static final Mysql8SourcePlugin MYSQL_PLUGIN = new Mysql8SourcePlugin();
    private static final DateTimeFormatter RUN_ID_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter LOG_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final LocalDate BASE_DATE = LocalDate.of(2024, 1, 1);
    private static final LocalDateTime BASE_DATE_TIME = LocalDateTime.of(2024, 1, 1, 8, 0, 0);
    private static final List<ColumnDefinition> BASE_COLUMNS = Collections.unmodifiableList(Arrays.asList(
            new ColumnDefinition("id", "BIGINT", false, false),
            new ColumnDefinition("biz_code", "VARCHAR(64)", false, false),
            new ColumnDefinition("tenant_code", "VARCHAR(32)", false, false),
            new ColumnDefinition("created_at", "DATETIME", false, false),
            new ColumnDefinition("updated_at", "DATETIME", false, false),
            new ColumnDefinition("enabled", "TINYINT", false, false),
            new ColumnDefinition("version_no", "INT", false, false)
    ));
    private static final List<ColumnDefinition> OPTIONAL_COLUMNS = Collections.unmodifiableList(Arrays.asList(
            new ColumnDefinition("owner_name", "VARCHAR(128)", true, true),
            new ColumnDefinition("category_code", "VARCHAR(64)", true, true),
            new ColumnDefinition("score", "INT", true, true),
            new ColumnDefinition("amount", "DECIMAL(18,2)", true, true),
            new ColumnDefinition("event_date", "DATE", true, true),
            new ColumnDefinition("event_time", "DATETIME", true, true),
            new ColumnDefinition("remarks", "TEXT", true, true),
            new ColumnDefinition("source_flag", "TINYINT", true, true),
            new ColumnDefinition("ref_id", "BIGINT", true, true),
            new ColumnDefinition("ratio", "DECIMAL(10,4)", true, true),
            new ColumnDefinition("region_code", "VARCHAR(32)", true, true)
    ));

    private StudioPressureMysqlTestSupport() {
    }

    static PressureConfig loadConfig() {
        File configFile = resolveConfigFile();
        try (InputStream inputStream = Files.newInputStream(configFile.toPath())) {
            PressureConfig config = OBJECT_MAPPER.readValue(inputStream, PressureConfig.class);
            if (config == null) {
                throw new IllegalStateException("studio pressure config is empty: " + configFile);
            }
            config.applyDefaults();
            return config;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load studio pressure config: " + configFile, e);
        }
    }

    static DatasetManifest prepareDataset(String runId) throws Exception {
        return prepareDataset(loadConfig(), runId);
    }

    static DatasetManifest prepareDataset(PressureConfig config, String runId) throws Exception {
        config.applyDefaults();
        Path runRoot = runRoot(runId);
        Files.createDirectories(runRoot);

        List<TableSpec> tableSpecs = buildTableSpecs(config);
        rebuildDatabase(config.getSourceMysql(), config.getGeneration().getSourceDatabase(), config.getGeneration().isDropExisting());
        rebuildDatabase(config.getTargetMysql(), config.getGeneration().getTargetDatabase(), config.getGeneration().isDropExisting());

        BaseDataSourceDTO sourceDto = toDataSource(config.getSourceMysql(), config.getGeneration().getSourceDatabase());
        BaseDataSourceDTO targetDto = toDataSource(config.getTargetMysql(), config.getGeneration().getTargetDatabase());

        createTables(sourceDto, tableSpecs);
        createTables(targetDto, tableSpecs);
        populateSourceTables(sourceDto, tableSpecs, config.getGeneration().getInsertBatchSize());

        DatasetManifest manifest = new DatasetManifest();
        manifest.setRunId(runId);
        manifest.setGeneratedAt(LocalDateTime.now().format(DATE_TIME_FORMAT));
        manifest.setSourceDatabase(config.getGeneration().getSourceDatabase());
        manifest.setTargetDatabase(config.getGeneration().getTargetDatabase());
        manifest.setTableCount(tableSpecs.size());
        manifest.setFamilyCount(config.getGeneration().getFamilyCount());
        manifest.setTables(tableSpecs);
        writeJson(runRoot.resolve("dataset-manifest.json"), manifest);
        return manifest;
    }

    static void assertDatasetIntegrity(PressureConfig config, DatasetManifest manifest) {
        config.applyDefaults();
        if (manifest == null) {
            throw new AssertionError("Dataset manifest is required");
        }
        if (manifest.getTables() == null || manifest.getTables().size() != config.getGeneration().getTableCount()) {
            throw new AssertionError("Unexpected manifest table count: " + (manifest.getTables() == null ? 0 : manifest.getTables().size()));
        }

        BaseDataSourceDTO sourceDto = toDataSource(config.getSourceMysql(), config.getGeneration().getSourceDatabase());
        BaseDataSourceDTO targetDto = toDataSource(config.getTargetMysql(), config.getGeneration().getTargetDatabase());
        List<String> sourceTables = MYSQL_PLUGIN.getTableNames(sourceDto, config.getGeneration().getTableNamePrefix());
        List<String> targetTables = MYSQL_PLUGIN.getTableNames(targetDto, config.getGeneration().getTableNamePrefix());
        if (sourceTables.size() != manifest.getTables().size()) {
            throw new AssertionError("Unexpected source table count: " + sourceTables.size());
        }
        if (targetTables.size() != manifest.getTables().size()) {
            throw new AssertionError("Unexpected target table count: " + targetTables.size());
        }
        if (!normalizeTableNames(sourceTables).equals(normalizeTableNames(targetTables))) {
            throw new AssertionError("Source and target table names do not match");
        }

        for (TableSpec sample : sampleSpecs(manifest)) {
            assertColumnsMatch(sourceDto, sample);
            assertColumnsMatch(targetDto, sample);
        }
    }

    static DatasetManifest loadLatestManifest() throws IOException {
        Path root = moduleRoot().resolve("target").resolve("studio-pressure");
        if (!Files.exists(root)) {
            throw new IllegalStateException("No studio pressure run directory found under " + root);
        }
        Path latestManifest = null;
        try (Stream<Path> stream = Files.walk(root)) {
            List<Path> manifests = stream
                    .filter(path -> path.getFileName().toString().equals("dataset-manifest.json"))
                    .sorted(Comparator.comparing(Path::toString))
                    .collect(Collectors.toList());
            if (!manifests.isEmpty()) {
                latestManifest = manifests.get(manifests.size() - 1);
            }
        }
        if (latestManifest == null) {
            throw new IllegalStateException("dataset-manifest.json not found under " + root);
        }
        try (InputStream inputStream = Files.newInputStream(latestManifest)) {
            return OBJECT_MAPPER.readValue(inputStream, DatasetManifest.class);
        }
    }

    static void truncateTargetTables(PressureConfig config, List<String> tableNames) {
        if (tableNames == null || tableNames.isEmpty()) {
            return;
        }
        BaseDataSourceDTO targetDto = toDataSource(config.getTargetMysql(), config.getGeneration().getTargetDatabase());
        List<String> statements = new ArrayList<String>();
        for (String tableName : tableNames) {
            statements.add("TRUNCATE TABLE `" + tableName + "`");
            if (statements.size() >= 100) {
                MYSQL_PLUGIN.executeBatch(targetDto, statements);
                statements = new ArrayList<String>();
            }
        }
        if (!statements.isEmpty()) {
            MYSQL_PLUGIN.executeBatch(targetDto, statements);
        }
    }

    static List<TableSpec> selectTablesForSync(DatasetManifest manifest, int count) {
        List<TableSpec> sorted = new ArrayList<TableSpec>(manifest.getTables());
        sorted.sort(new Comparator<TableSpec>() {
            @Override
            public int compare(TableSpec left, TableSpec right) {
                int rowCompare = Integer.compare(right.getRowCount(), left.getRowCount());
                if (rowCompare != 0) {
                    return rowCompare;
                }
                return left.getTableName().compareTo(right.getTableName());
            }
        });
        return new ArrayList<TableSpec>(sorted.subList(0, Math.min(count, sorted.size())));
    }

    static List<TableSpec> selectTablesByOffset(List<TableSpec> source, int offset, int count) {
        if (source == null || source.isEmpty() || count <= 0 || offset >= source.size()) {
            return Collections.emptyList();
        }
        int end = Math.min(offset + count, source.size());
        return new ArrayList<TableSpec>(source.subList(offset, end));
    }

    static List<String> toPhysicalLocators(List<TableSpec> tables) {
        List<String> locators = new ArrayList<String>();
        if (tables == null) {
            return locators;
        }
        for (TableSpec table : tables) {
            locators.add(table.getTableName());
        }
        return locators;
    }

    static String newRunId() {
        return LocalDateTime.now().format(RUN_ID_FORMAT);
    }

    static Path runRoot(String runId) {
        return moduleRoot().resolve("target").resolve("studio-pressure").resolve(runId);
    }

    static Path resolveReportPath() {
        String fileName = "studio-pressure-" + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE) + ".md";
        return moduleRoot().resolve("validation-reports").resolve(fileName);
    }

    static ProgressLogger openProgressLogger(Path path) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Files.write(path,
                Collections.singletonList("[" + LocalDateTime.now().format(LOG_TIME_FORMAT) + "] progress logger started"),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        return new ProgressLogger(path);
    }

    static void writeJson(Path path, Object payload) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        OBJECT_MAPPER.writeValue(path.toFile(), payload);
    }

    static void writeText(Path path, String content) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Files.write(path, content.getBytes(StandardCharsets.UTF_8));
    }

    static void cleanupPath(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(path)) {
            stream.sorted(Comparator.reverseOrder()).forEach(current -> {
                try {
                    Files.deleteIfExists(current);
                } catch (IOException ignored) {
                }
            });
        }
    }

    static String buildMarkdownReport(BenchmarkRunReport report) {
        StringBuilder builder = new StringBuilder();
        builder.append("# Studio 万表压测报告\n\n");
        builder.append("generatedAt: ").append(LocalDateTime.now().format(DATE_TIME_FORMAT)).append("\n\n");
        builder.append("runId: ").append(report.getRunId()).append("\n\n");
        builder.append("| scenario | projectCode | workerCount | syncMs | queryP95Ms | statsP95Ms | taskDrainMs | workflowDrainMs | taskSuccess | workflowSuccess |\n");
        builder.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
        for (ScenarioBenchmarkResult scenario : report.getScenarios()) {
            builder.append("| ")
                    .append(scenario.getScenarioLabel()).append(" | ")
                    .append(escape(scenario.getProjectCode())).append(" | ")
                    .append(scenario.getWorkerCount()).append(" | ")
                    .append(scenario.getSyncResult() == null ? 0L : scenario.getSyncResult().getElapsedMs()).append(" | ")
                    .append(formatLong(scenario.getQueryMetrics() == null ? null : scenario.getQueryMetrics().getP95Ms())).append(" | ")
                    .append(formatLong(scenario.getStatisticsMetrics() == null ? null : scenario.getStatisticsMetrics().getP95Ms())).append(" | ")
                    .append(formatLong(scenario.getTaskDrainMetrics() == null ? null : scenario.getTaskDrainMetrics().getDrainElapsedMs())).append(" | ")
                    .append(formatLong(scenario.getWorkflowDrainMetrics() == null ? null : scenario.getWorkflowDrainMetrics().getDrainElapsedMs())).append(" | ")
                      .append(scenario.getTaskDrainMetrics() == null ? 0 : scenario.getTaskDrainMetrics().getSuccessCount()).append(" | ")
                      .append(scenario.getWorkflowDrainMetrics() == null ? 0 : scenario.getWorkflowDrainMetrics().getSuccessCount()).append(" |\n");
        }
        builder.append("\n## 测试用例\n\n");
        for (ScenarioBenchmarkResult scenario : report.getScenarios()) {
            builder.append("### ").append(scenario.getScenarioLabel()).append("\n\n");
            appendCase(builder,
                    "模型同步",
                    hasSyncedModels(scenario.getSyncResult()),
                    "sourceModels=" + (scenario.getSyncResult() == null ? 0 : scenario.getSyncResult().getSourceModelCount())
                            + ", targetModels=" + (scenario.getSyncResult() == null ? 0 : scenario.getSyncResult().getTargetModelCount()));
            appendCase(builder,
                    "模型筛选",
                    metricsPassed(scenario.getQueryMetrics()),
                    "success=" + (scenario.getQueryMetrics() == null ? 0 : scenario.getQueryMetrics().getSuccessCount())
                            + ", failed=" + (scenario.getQueryMetrics() == null ? 0 : scenario.getQueryMetrics().getFailureCount())
                            + ", p95=" + (scenario.getQueryMetrics() == null ? 0 : scenario.getQueryMetrics().getP95Ms()) + "ms");
            appendCase(builder,
                    "模型统计分析",
                    metricsPassed(scenario.getStatisticsMetrics()),
                    "success=" + (scenario.getStatisticsMetrics() == null ? 0 : scenario.getStatisticsMetrics().getSuccessCount())
                            + ", failed=" + (scenario.getStatisticsMetrics() == null ? 0 : scenario.getStatisticsMetrics().getFailureCount())
                            + ", p95=" + (scenario.getStatisticsMetrics() == null ? 0 : scenario.getStatisticsMetrics().getP95Ms()) + "ms");
            appendCase(builder,
                    "采集任务并发",
                    drainPassed(scenario.getTaskDrainMetrics()),
                    "success=" + (scenario.getTaskDrainMetrics() == null ? 0 : scenario.getTaskDrainMetrics().getSuccessCount())
                            + ", failed=" + (scenario.getTaskDrainMetrics() == null ? 0 : scenario.getTaskDrainMetrics().getFailureCount())
                            + ", timeout=" + (scenario.getTaskDrainMetrics() == null ? 0 : scenario.getTaskDrainMetrics().getTimeoutCount()));
            appendCase(builder,
                    "工作流并发",
                    drainPassed(scenario.getWorkflowDrainMetrics()),
                    "success=" + (scenario.getWorkflowDrainMetrics() == null ? 0 : scenario.getWorkflowDrainMetrics().getSuccessCount())
                            + ", failed=" + (scenario.getWorkflowDrainMetrics() == null ? 0 : scenario.getWorkflowDrainMetrics().getFailureCount())
                            + ", timeout=" + (scenario.getWorkflowDrainMetrics() == null ? 0 : scenario.getWorkflowDrainMetrics().getTimeoutCount()));
            if (!isBlank(scenario.getFailureReason())) {
                builder.append("- 场景异常: ").append(escape(scenario.getFailureReason())).append("\n");
            }
            builder.append("\n");
        }
        builder.append("## 结论\n\n");
        builder.append("- 总体结论: ").append(allScenariosPassed(report) ? "通过" : "存在失败或超时场景").append("\n");
        for (ScenarioBenchmarkResult scenario : report.getScenarios()) {
            builder.append("- ").append(scenario.getScenarioLabel()).append(": ")
                    .append(scenarioPassed(scenario) ? "通过" : "未通过")
                    .append("；")
                    .append("模型同步=").append(hasSyncedModels(scenario.getSyncResult()) ? "通过" : "失败")
                    .append("，模型筛选=").append(metricsPassed(scenario.getQueryMetrics()) ? "通过" : "失败")
                    .append("，模型统计=").append(metricsPassed(scenario.getStatisticsMetrics()) ? "通过" : "失败")
                    .append("，采集任务并发=").append(drainPassed(scenario.getTaskDrainMetrics()) ? "通过" : "失败")
                    .append("，工作流并发=").append(drainPassed(scenario.getWorkflowDrainMetrics()) ? "通过" : "失败");
            if (!isBlank(scenario.getFailureReason())) {
                builder.append("，异常=").append(escape(scenario.getFailureReason()));
            }
            builder.append("\n");
        }
        builder.append("\n## Details\n\n");
        for (ScenarioBenchmarkResult scenario : report.getScenarios()) {
            builder.append("### ").append(scenario.getScenarioLabel()).append("\n\n");
            builder.append("- project: `").append(scenario.getProjectCode()).append("`\n");
            builder.append("- workers: ").append(scenario.getWorkerCount()).append("\n");
            if (!isBlank(scenario.getFailureReason())) {
                builder.append("- failureReason: ").append(escape(scenario.getFailureReason())).append("\n");
            }
            builder.append("- synced source models: ").append(scenario.getSyncResult() == null ? 0 : scenario.getSyncResult().getSourceModelCount()).append("\n");
            builder.append("- synced target models: ").append(scenario.getSyncResult() == null ? 0 : scenario.getSyncResult().getTargetModelCount()).append("\n");
            appendMetrics(builder, "query", scenario.getQueryMetrics());
            appendMetrics(builder, "statistics", scenario.getStatisticsMetrics());
            appendDrain(builder, "collection tasks", scenario.getTaskDrainMetrics());
            appendDrain(builder, "workflows", scenario.getWorkflowDrainMetrics());
            builder.append("\n");
        }
        return builder.toString();
    }

    static long percentile(List<Long> values, double percentile) {
        if (values == null || values.isEmpty()) {
            return 0L;
        }
        List<Long> sorted = new ArrayList<Long>(values);
        Collections.sort(sorted);
        int index = (int) Math.ceil(percentile / 100D * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        return sorted.get(index);
    }

    private static void appendMetrics(StringBuilder builder, String label, OperationMetrics metrics) {
        if (metrics == null) {
            builder.append("- ").append(label).append(": n/a\n");
            return;
        }
        builder.append("- ").append(label)
                .append(": total=").append(metrics.getTotalRequests())
                .append(", success=").append(metrics.getSuccessCount())
                .append(", failed=").append(metrics.getFailureCount())
                .append(", throughput=").append(formatDouble(metrics.getThroughputPerSecond()))
                .append("/s, p95=").append(metrics.getP95Ms()).append("ms, p99=").append(metrics.getP99Ms()).append("ms\n");
        if (metrics.getErrorSamples() != null && !metrics.getErrorSamples().isEmpty()) {
            builder.append("- ").append(label).append(" errors: ").append(escape(String.join("; ", metrics.getErrorSamples()))).append("\n");
        }
    }

    private static void appendDrain(StringBuilder builder, String label, DrainMetrics metrics) {
        if (metrics == null) {
            builder.append("- ").append(label).append(": n/a\n");
            return;
        }
        builder.append("- ").append(label)
                .append(": total=").append(metrics.getTotalDefinitions())
                .append(", success=").append(metrics.getSuccessCount())
                .append(", failed=").append(metrics.getFailureCount())
                .append(", timeout=").append(metrics.getTimeoutCount())
                .append(", drainMs=").append(metrics.getDrainElapsedMs()).append("\n");
        if (metrics.getWorkerDistribution() != null && !metrics.getWorkerDistribution().isEmpty()) {
            builder.append("- ").append(label).append(" workerDistribution: ").append(metrics.getWorkerDistribution()).append("\n");
        }
        if (metrics.getFailureSamples() != null && !metrics.getFailureSamples().isEmpty()) {
            builder.append("- ").append(label).append(" failures: ").append(escape(String.join("; ", metrics.getFailureSamples()))).append("\n");
        }
    }

    private static void appendCase(StringBuilder builder, String caseName, boolean passed, String detail) {
        builder.append("- ").append(caseName)
                .append(": ")
                .append(passed ? "PASS" : "FAIL");
        if (detail != null && !detail.trim().isEmpty()) {
            builder.append(" (").append(escape(detail)).append(")");
        }
        builder.append("\n");
    }

    private static boolean allScenariosPassed(BenchmarkRunReport report) {
        if (report == null || report.getScenarios() == null || report.getScenarios().isEmpty()) {
            return false;
        }
        for (ScenarioBenchmarkResult scenario : report.getScenarios()) {
            if (!scenarioPassed(scenario)) {
                return false;
            }
        }
        return true;
    }

    private static boolean scenarioPassed(ScenarioBenchmarkResult scenario) {
        return scenario != null
                && isBlank(scenario.getFailureReason())
                && hasSyncedModels(scenario.getSyncResult())
                && metricsPassed(scenario.getQueryMetrics())
                && metricsPassed(scenario.getStatisticsMetrics())
                && drainPassed(scenario.getTaskDrainMetrics())
                && drainPassed(scenario.getWorkflowDrainMetrics());
    }

    private static boolean hasSyncedModels(SyncResult syncResult) {
        return syncResult != null
                && syncResult.getSourceModelCount() > 0
                && syncResult.getTargetModelCount() > 0;
    }

    private static boolean metricsPassed(OperationMetrics metrics) {
        return metrics != null
                && metrics.getTotalRequests() > 0
                && metrics.getSuccessCount() > 0
                && metrics.getFailureCount() == 0;
    }

    private static boolean drainPassed(DrainMetrics metrics) {
        return metrics != null
                && metrics.getTotalDefinitions() > 0
                && metrics.getSuccessCount() == metrics.getTotalDefinitions()
                && metrics.getFailureCount() == 0
                && metrics.getTimeoutCount() == 0;
    }

    private static boolean isBlank(String text) {
        return text == null || text.trim().isEmpty();
    }

    private static String escape(String value) {
        return value == null ? "" : value.replace("|", "\\|").replace("\r", " ").replace("\n", " ");
    }

    private static String formatLong(Long value) {
        return value == null ? "0" : String.valueOf(value);
    }

    private static String formatDouble(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }

    private static BaseDataSourceDTO toDataSource(MysqlConnectConfig config, String databaseOverride) {
        MysqlConnectConfig resolved = config == null ? new MysqlConnectConfig() : config.copy();
        resolved.applyDefaults();
        if (databaseOverride != null && !databaseOverride.trim().isEmpty()) {
            resolved.setDatabase(databaseOverride);
        }
        BaseDataSourceDTO dto = new BaseDataSourceDTO();
        dto.setName("mysql8");
        dto.setType("mysql8");
        dto.setHost(resolved.getHost());
        dto.setPort(String.valueOf(resolved.getPort()));
        dto.setDatabase(resolved.getDatabase());
        dto.setUserName(resolved.getUserName());
        dto.setPassword(resolved.getPassword());
        dto.setOther(resolved.getOther());
        dto.setUsePool(Boolean.TRUE.equals(resolved.getUsePool()));
        Map<String, String> extraParams = new LinkedHashMap<String, String>();
        if (resolved.getExtraParams() != null) {
            extraParams.putAll(resolved.getExtraParams());
        }
        dto.setExtraParams(extraParams);
        return dto;
    }

    private static void rebuildDatabase(MysqlConnectConfig connectConfig, String databaseName, boolean dropExisting) {
        MysqlConnectConfig adminConfig = connectConfig.copy();
        adminConfig.applyDefaults();
        BaseDataSourceDTO adminDto = toDataSource(adminConfig, adminConfig.getDatabase());
        if (dropExisting) {
            MYSQL_PLUGIN.executeUpdate(adminDto, "DROP DATABASE IF EXISTS `" + databaseName + "`");
        }
        MYSQL_PLUGIN.executeUpdate(adminDto,
                "CREATE DATABASE IF NOT EXISTS `" + databaseName + "` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci");
    }

    private static void createTables(BaseDataSourceDTO dto, List<TableSpec> tableSpecs) {
        List<String> statements = new ArrayList<String>();
        for (TableSpec tableSpec : tableSpecs) {
            statements.add(buildCreateTableSql(tableSpec));
            if (statements.size() >= 100) {
                MYSQL_PLUGIN.executeBatch(dto, statements);
                statements = new ArrayList<String>();
            }
        }
        if (!statements.isEmpty()) {
            MYSQL_PLUGIN.executeBatch(dto, statements);
        }
    }

    private static void populateSourceTables(BaseDataSourceDTO dto, List<TableSpec> tableSpecs, int insertBatchSize) {
        int processed = 0;
        for (TableSpec tableSpec : tableSpecs) {
            InsertDataDTO insertData = new InsertDataDTO(extractFieldNames(tableSpec.getColumns()), buildRows(tableSpec));
            insertData.setBaseDataSourceDTO(dto);
            insertData.setTableName(tableSpec.getTableName());
            insertData.setBatchSize(insertBatchSize);
            insertData.setTruncate(false);
            MYSQL_PLUGIN.insertData(insertData);
            processed++;
            if (processed % 250 == 0) {
                System.out.println("studio-pressure bootstrap inserted source rows for tables: " + processed + "/" + tableSpecs.size());
            }
        }
    }

    private static List<List<String>> buildRows(TableSpec tableSpec) {
        List<List<String>> rows = new ArrayList<List<String>>(tableSpec.getRowCount());
        for (int rowIndex = 1; rowIndex <= tableSpec.getRowCount(); rowIndex++) {
            rows.add(buildRow(tableSpec, rowIndex));
        }
        return rows;
    }

    private static List<String> buildRow(TableSpec tableSpec, int rowIndex) {
        Map<String, String> valueMap = buildValueMap(tableSpec, rowIndex);
        List<String> values = new ArrayList<String>(tableSpec.getColumns().size());
        for (ColumnDefinition column : tableSpec.getColumns()) {
            values.add(valueMap.get(column.getName()));
        }
        return values;
    }

    private static Map<String, String> buildValueMap(TableSpec tableSpec, int rowIndex) {
        Map<String, String> values = new HashMap<String, String>();
        LocalDateTime createdAt = BASE_DATE_TIME.plusDays(rowIndex % 90).plusSeconds(tableSpec.getGlobalSequence());
        LocalDateTime updatedAt = createdAt.plusHours(rowIndex % 48);
        values.put("id", String.valueOf(rowIndex));
        values.put("biz_code", tableSpec.getTableName() + "_B" + String.format(Locale.ROOT, "%06d", rowIndex));
        values.put("tenant_code", "tenant_" + ((tableSpec.getGlobalSequence() % 7) + 1));
        values.put("created_at", createdAt.format(DATE_TIME_FORMAT));
        values.put("updated_at", updatedAt.format(DATE_TIME_FORMAT));
        values.put("enabled", rowIndex % 2 == 0 ? "1" : "0");
        values.put("version_no", String.valueOf(1 + (rowIndex % 9)));
        values.put("owner_name", "owner_" + ((tableSpec.getGlobalSequence() * 13L + rowIndex) % 2048L));
        values.put("category_code", tableSpec.getFamilyCode() + "_CAT_" + (rowIndex % 12));
        values.put("score", String.valueOf((tableSpec.getFamilyIndex() * 100 + rowIndex) % 1000));
        values.put("amount", decimalText(BigDecimal.valueOf(tableSpec.getGlobalSequence())
                .multiply(new BigDecimal("3.25"))
                .add(BigDecimal.valueOf(rowIndex).multiply(new BigDecimal("0.11"))), 2));
        values.put("event_date", BASE_DATE.plusDays((tableSpec.getGlobalSequence() + rowIndex) % 365).format(DATE_FORMAT));
        values.put("event_time", createdAt.plusMinutes(rowIndex % 180).format(DATE_TIME_FORMAT));
        values.put("remarks", "remarks_" + tableSpec.getTableName() + "_" + rowIndex);
        values.put("source_flag", rowIndex % 3 == 0 ? "1" : "0");
        values.put("ref_id", String.valueOf(tableSpec.getGlobalSequence() * 100000L + rowIndex));
        values.put("ratio", decimalText(BigDecimal.valueOf((tableSpec.getGlobalSequence() % 97) + rowIndex)
                .divide(new BigDecimal("1000"), 4, RoundingMode.HALF_UP), 4));
        values.put("region_code", "R" + String.format(Locale.ROOT, "%02d", (rowIndex % 12) + 1));
        return values;
    }

    private static String decimalText(BigDecimal value, int scale) {
        if (value == null) {
            return null;
        }
        BigDecimal normalized = value.setScale(scale, RoundingMode.HALF_UP).stripTrailingZeros();
        if (normalized.scale() < 0) {
            normalized = normalized.setScale(0);
        }
        return normalized.toPlainString();
    }

    private static List<String> extractFieldNames(List<ColumnDefinition> columns) {
        List<String> fields = new ArrayList<String>();
        for (ColumnDefinition column : columns) {
            fields.add(column.getName());
        }
        return fields;
    }

    private static void assertColumnsMatch(BaseDataSourceDTO dto, TableSpec tableSpec) {
        List<ColumnInfo> columns = MYSQL_PLUGIN.getColumns(dto, tableSpec.getTableName());
        List<String> actual = new ArrayList<String>();
        for (ColumnInfo column : columns) {
            actual.add(column.getColumnName());
        }
        List<String> expected = extractFieldNames(tableSpec.getColumns());
        if (!actual.equals(expected)) {
            throw new AssertionError("Column mismatch for " + tableSpec.getTableName() + ": expected=" + expected + ", actual=" + actual);
        }
    }

    private static Set<String> normalizeTableNames(List<String> names) {
        Set<String> normalized = new LinkedHashSet<String>();
        for (String name : names) {
            if (name == null) {
                continue;
            }
            if (name.contains(".")) {
                normalized.add(name.substring(name.indexOf('.') + 1));
            } else {
                normalized.add(name);
            }
        }
        return normalized;
    }

    private static List<TableSpec> sampleSpecs(DatasetManifest manifest) {
        if (manifest.getTables() == null || manifest.getTables().isEmpty()) {
            return Collections.emptyList();
        }
        List<TableSpec> samples = new ArrayList<TableSpec>();
        samples.add(manifest.getTables().get(0));
        samples.add(manifest.getTables().get(manifest.getTables().size() - 1));
        Set<Integer> families = new HashSet<Integer>();
        for (TableSpec tableSpec : manifest.getTables()) {
            if (families.add(tableSpec.getFamilyIndex())) {
                samples.add(tableSpec);
            }
            if (families.size() >= Math.min(manifest.getFamilyCount(), 8)) {
                break;
            }
        }
        return samples;
    }

    private static List<TableSpec> buildTableSpecs(PressureConfig config) {
        config.applyDefaults();
        int total = 0;
        for (TierConfig tier : config.getGeneration().getTiers()) {
            total += tier.getTableCount();
        }
        if (total != config.getGeneration().getTableCount()) {
            throw new IllegalStateException("Generation tiers tableCount sum does not equal configured tableCount");
        }
        List<FamilySpec> families = buildFamilies(config.getGeneration().getFamilyCount());
        List<TableSpec> tables = new ArrayList<TableSpec>(config.getGeneration().getTableCount());
        int sequence = 1;
        for (TierConfig tier : config.getGeneration().getTiers()) {
            for (int i = 0; i < tier.getTableCount(); i++) {
                FamilySpec family = families.get((sequence - 1) % families.size());
                TableSpec spec = new TableSpec();
                spec.setFamilyIndex(family.getFamilyIndex());
                spec.setFamilyCode(String.format(Locale.ROOT, "f%02d", family.getFamilyIndex()));
                spec.setTierCode(tier.getTierCode());
                spec.setGlobalSequence(sequence);
                spec.setTableName(String.format(Locale.ROOT, "%s_f%02d_%s_%05d",
                        config.getGeneration().getTableNamePrefix(),
                        family.getFamilyIndex(),
                        tier.getTierCode(),
                        sequence));
                spec.setRowCount(tier.getRowCount());
                List<ColumnDefinition> columns = new ArrayList<ColumnDefinition>(BASE_COLUMNS);
                columns.addAll(family.getOptionalColumns());
                spec.setColumns(columns);
                tables.add(spec);
                sequence++;
            }
        }
        return tables;
    }

    private static List<FamilySpec> buildFamilies(int familyCount) {
        List<FamilySpec> families = new ArrayList<FamilySpec>();
        Set<Integer> masks = new LinkedHashSet<Integer>();
        int seed = 1;
        while (families.size() < familyCount) {
            int mask = buildFamilyMask(seed++);
            if (mask == 0 || !masks.add(mask)) {
                continue;
            }
            FamilySpec family = new FamilySpec();
            family.setFamilyIndex(families.size() + 1);
            family.setMask(mask);
            family.setOptionalColumns(columnsFromMask(mask));
            families.add(family);
        }
        return families;
    }

    private static int buildFamilyMask(int seed) {
        int optionalSize = OPTIONAL_COLUMNS.size();
        int count = (seed % optionalSize) + 1;
        int step = (seed % (optionalSize - 1)) + 1;
        int start = (seed * 3) % optionalSize;
        boolean[] used = new boolean[optionalSize];
        int mask = 0;
        int index = start;
        int added = 0;
        int guard = 0;
        while (added < count && guard < optionalSize * 8) {
            if (!used[index]) {
                used[index] = true;
                mask = mask | (1 << index);
                added++;
            }
            index = (index + step) % optionalSize;
            guard++;
        }
        return mask;
    }

    private static List<ColumnDefinition> columnsFromMask(int mask) {
        List<ColumnDefinition> columns = new ArrayList<ColumnDefinition>();
        for (int i = 0; i < OPTIONAL_COLUMNS.size(); i++) {
            if ((mask & (1 << i)) != 0) {
                columns.add(OPTIONAL_COLUMNS.get(i));
            }
        }
        return columns;
    }

    private static String buildCreateTableSql(TableSpec tableSpec) {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE `").append(tableSpec.getTableName()).append("` (");
        for (int i = 0; i < tableSpec.getColumns().size(); i++) {
            ColumnDefinition column = tableSpec.getColumns().get(i);
            if (i > 0) {
                builder.append(",");
            }
            builder.append("`").append(column.getName()).append("` ").append(column.getSqlType());
            builder.append(column.isNullable() ? " NULL" : " NOT NULL");
        }
        builder.append(", PRIMARY KEY (`id`)");
        builder.append(", KEY `idx_biz_code` (`biz_code`)");
        builder.append(", KEY `idx_created_at` (`created_at`)");
        builder.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci");
        return builder.toString();
    }

    private static File resolveConfigFile() {
        String override = System.getProperty("studioPressureConfig");
        if (override != null && !override.trim().isEmpty()) {
            Path path = Paths.get(override.trim());
            if (Files.exists(path)) {
                return path.toFile();
            }
            throw new IllegalStateException("studioPressureConfig file not found: " + override);
        }
        List<Path> candidates = Arrays.asList(
                Paths.get("..", "core", "src", "main", "resources", "studio-pressure-mysql-demo.json").normalize(),
                Paths.get("core", "src", "main", "resources", "studio-pressure-mysql-demo.json").normalize(),
                moduleRoot().getParent().resolve("core").resolve("src").resolve("main").resolve("resources").resolve("studio-pressure-mysql-demo.json").normalize()
        );
        for (Path candidate : candidates) {
            if (Files.exists(candidate)) {
                return candidate.toFile();
            }
        }
        throw new IllegalStateException("studio-pressure-mysql-demo.json not found");
    }

    private static Path moduleRoot() {
        Path dataMockRoot = Paths.get("data-mock");
        if (Files.exists(dataMockRoot.resolve("pom.xml"))) {
            return dataMockRoot.toAbsolutePath().normalize();
        }
        Path current = Paths.get(".").toAbsolutePath().normalize();
        if (Files.exists(current.resolve("pom.xml"))
                && Files.exists(current.resolve("src"))
                && current.getFileName() != null
                && "data-mock".equalsIgnoreCase(current.getFileName().toString())) {
            return current;
        }
        return current;
    }

    static final class ProgressLogger implements AutoCloseable {
        private final Path path;

        private ProgressLogger(Path path) {
            this.path = path;
        }

        synchronized void info(String message) {
            String line = "[" + LocalDateTime.now().format(LOG_TIME_FORMAT) + "] " + String.valueOf(message);
            System.out.println(line);
            try {
                Files.write(path,
                        Collections.singletonList(line),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to write progress log: " + path, e);
            }
        }

        Path getPath() {
            return path;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    static final class PressureConfig {
        private MysqlConnectConfig sourceMysql;
        private MysqlConnectConfig targetMysql;
        private StudioConfig studio;
        private GenerationConfig generation;
        private TimeoutConfig timeouts;

        public MysqlConnectConfig getSourceMysql() {
            return sourceMysql;
        }

        public void setSourceMysql(MysqlConnectConfig sourceMysql) {
            this.sourceMysql = sourceMysql;
        }

        public MysqlConnectConfig getTargetMysql() {
            return targetMysql;
        }

        public void setTargetMysql(MysqlConnectConfig targetMysql) {
            this.targetMysql = targetMysql;
        }

        public StudioConfig getStudio() {
            return studio;
        }

        public void setStudio(StudioConfig studio) {
            this.studio = studio;
        }

        public GenerationConfig getGeneration() {
            return generation;
        }

        public void setGeneration(GenerationConfig generation) {
            this.generation = generation;
        }

        public TimeoutConfig getTimeouts() {
            return timeouts;
        }

        public void setTimeouts(TimeoutConfig timeouts) {
            this.timeouts = timeouts;
        }

        void applyDefaults() {
            if (sourceMysql == null) {
                sourceMysql = new MysqlConnectConfig();
            }
            if (targetMysql == null) {
                targetMysql = sourceMysql.copy();
            }
            sourceMysql.applyDefaults();
            targetMysql.applyDefaults();
            if (studio == null) {
                studio = new StudioConfig();
            }
            studio.applyDefaults();
            if (generation == null) {
                generation = new GenerationConfig();
            }
            generation.applyDefaults();
            if (timeouts == null) {
                timeouts = new TimeoutConfig();
            }
            timeouts.applyDefaults();
        }
    }

    static final class MysqlConnectConfig {
        private String host;
        private Integer port;
        private String database;
        private String userName;
        private String password;
        private String other;
        private Boolean usePool;
        private Map<String, String> extraParams = new LinkedHashMap<String, String>();

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public Integer getPort() {
            return port;
        }

        public void setPort(Integer port) {
            this.port = port;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getOther() {
            return other;
        }

        public void setOther(String other) {
            this.other = other;
        }

        public Boolean getUsePool() {
            return usePool;
        }

        public void setUsePool(Boolean usePool) {
            this.usePool = usePool;
        }

        public Map<String, String> getExtraParams() {
            return extraParams;
        }

        public void setExtraParams(Map<String, String> extraParams) {
            this.extraParams = extraParams;
        }

        void applyDefaults() {
            if (host == null || host.trim().isEmpty()) {
                host = "127.0.0.1";
            }
            if (port == null) {
                port = 3306;
            }
            if (database == null || database.trim().isEmpty()) {
                database = "agg_test";
            }
            if (userName == null || userName.trim().isEmpty()) {
                userName = "root";
            }
            if (password == null) {
                password = "";
            }
            if (other == null || other.trim().isEmpty()) {
                other = "{\"rewriteBatchedStatements\":true}";
            }
            if (usePool == null) {
                usePool = Boolean.TRUE;
            }
            if (extraParams == null) {
                extraParams = new LinkedHashMap<String, String>();
            }
        }

        MysqlConnectConfig copy() {
            MysqlConnectConfig copy = new MysqlConnectConfig();
            copy.host = host;
            copy.port = port;
            copy.database = database;
            copy.userName = userName;
            copy.password = password;
            copy.other = other;
            copy.usePool = usePool;
            copy.extraParams = extraParams == null
                    ? new LinkedHashMap<String, String>()
                    : new LinkedHashMap<String, String>(extraParams);
            return copy;
        }
    }

    static final class StudioConfig {
        private String baseUrl;
        private String username;
        private String password;
        private String tenantId;
        private String projectPrefix;

        public String getBaseUrl() {
            return baseUrl;
        }

        public void setBaseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getTenantId() {
            return tenantId;
        }

        public void setTenantId(String tenantId) {
            this.tenantId = tenantId;
        }

        public String getProjectPrefix() {
            return projectPrefix;
        }

        public void setProjectPrefix(String projectPrefix) {
            this.projectPrefix = projectPrefix;
        }

        void applyDefaults() {
            if (baseUrl == null || baseUrl.trim().isEmpty()) {
                baseUrl = "http://127.0.0.1:18080";
            }
            if (username == null || username.trim().isEmpty()) {
                username = "admin";
            }
            if (password == null || password.trim().isEmpty()) {
                password = "admin123";
            }
            if (tenantId == null || tenantId.trim().isEmpty()) {
                tenantId = "default";
            }
            if (projectPrefix == null || projectPrefix.trim().isEmpty()) {
                projectPrefix = "pressure_mock";
            }
        }
    }

    static final class GenerationConfig {
        private String sourceDatabase;
        private String targetDatabase;
        private String tableNamePrefix;
        private Integer familyCount;
        private Integer tableCount;
        private Boolean dropExisting;
        private Integer insertBatchSize;
        private List<TierConfig> tiers = new ArrayList<TierConfig>();

        public String getSourceDatabase() {
            return sourceDatabase;
        }

        public void setSourceDatabase(String sourceDatabase) {
            this.sourceDatabase = sourceDatabase;
        }

        public String getTargetDatabase() {
            return targetDatabase;
        }

        public void setTargetDatabase(String targetDatabase) {
            this.targetDatabase = targetDatabase;
        }

        public String getTableNamePrefix() {
            return tableNamePrefix;
        }

        public void setTableNamePrefix(String tableNamePrefix) {
            this.tableNamePrefix = tableNamePrefix;
        }

        public Integer getFamilyCount() {
            return familyCount;
        }

        public void setFamilyCount(Integer familyCount) {
            this.familyCount = familyCount;
        }

        public Integer getTableCount() {
            return tableCount;
        }

        public void setTableCount(Integer tableCount) {
            this.tableCount = tableCount;
        }

        public Boolean getDropExisting() {
            return dropExisting;
        }

        public void setDropExisting(Boolean dropExisting) {
            this.dropExisting = dropExisting;
        }

        public Integer getInsertBatchSize() {
            return insertBatchSize;
        }

        public void setInsertBatchSize(Integer insertBatchSize) {
            this.insertBatchSize = insertBatchSize;
        }

        public List<TierConfig> getTiers() {
            return tiers;
        }

        public void setTiers(List<TierConfig> tiers) {
            this.tiers = tiers;
        }

        boolean isDropExisting() {
            return Boolean.TRUE.equals(dropExisting);
        }

        void applyDefaults() {
            if (sourceDatabase == null || sourceDatabase.trim().isEmpty()) {
                sourceDatabase = "mock_data";
            }
            if (targetDatabase == null || targetDatabase.trim().isEmpty()) {
                targetDatabase = "mock_data_target";
            }
            if (tableNamePrefix == null || tableNamePrefix.trim().isEmpty()) {
                tableNamePrefix = "mock_tbl";
            }
            if (familyCount == null) {
                familyCount = 32;
            }
            if (tableCount == null) {
                tableCount = 10000;
            }
            if (dropExisting == null) {
                dropExisting = Boolean.TRUE;
            }
            if (insertBatchSize == null || insertBatchSize.intValue() <= 0) {
                insertBatchSize = 500;
            }
            if (tiers == null || tiers.isEmpty()) {
                tiers = Arrays.asList(
                        new TierConfig("cold", 8000, 10),
                        new TierConfig("warm", 1500, 100),
                        new TierConfig("hot", 400, 1000),
                        new TierConfig("heavy", 100, 10000)
                );
            }
        }
    }

    static final class TierConfig {
        private String tierCode;
        private Integer tableCount;
        private Integer rowCount;

        TierConfig() {
        }

        TierConfig(String tierCode, Integer tableCount, Integer rowCount) {
            this.tierCode = tierCode;
            this.tableCount = tableCount;
            this.rowCount = rowCount;
        }

        public String getTierCode() {
            return tierCode;
        }

        public void setTierCode(String tierCode) {
            this.tierCode = tierCode;
        }

        public Integer getTableCount() {
            return tableCount;
        }

        public void setTableCount(Integer tableCount) {
            this.tableCount = tableCount;
        }

        public Integer getRowCount() {
            return rowCount;
        }

        public void setRowCount(Integer rowCount) {
            this.rowCount = rowCount;
        }
    }

    static final class TimeoutConfig {
        private Integer requestTimeoutMs;
        private Integer pollIntervalMs;
        private Long smallDrainTimeoutMs;
        private Long mediumDrainTimeoutMs;
        private Long largeDrainTimeoutMs;

        public Integer getRequestTimeoutMs() {
            return requestTimeoutMs;
        }

        public void setRequestTimeoutMs(Integer requestTimeoutMs) {
            this.requestTimeoutMs = requestTimeoutMs;
        }

        public Integer getPollIntervalMs() {
            return pollIntervalMs;
        }

        public void setPollIntervalMs(Integer pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
        }

        public Long getSmallDrainTimeoutMs() {
            return smallDrainTimeoutMs;
        }

        public void setSmallDrainTimeoutMs(Long smallDrainTimeoutMs) {
            this.smallDrainTimeoutMs = smallDrainTimeoutMs;
        }

        public Long getMediumDrainTimeoutMs() {
            return mediumDrainTimeoutMs;
        }

        public void setMediumDrainTimeoutMs(Long mediumDrainTimeoutMs) {
            this.mediumDrainTimeoutMs = mediumDrainTimeoutMs;
        }

        public Long getLargeDrainTimeoutMs() {
            return largeDrainTimeoutMs;
        }

        public void setLargeDrainTimeoutMs(Long largeDrainTimeoutMs) {
            this.largeDrainTimeoutMs = largeDrainTimeoutMs;
        }

        void applyDefaults() {
            if (requestTimeoutMs == null || requestTimeoutMs.intValue() <= 0) {
                requestTimeoutMs = 300000;
            }
            if (pollIntervalMs == null || pollIntervalMs.intValue() <= 0) {
                pollIntervalMs = 3000;
            }
            if (smallDrainTimeoutMs == null || smallDrainTimeoutMs.longValue() <= 0L) {
                smallDrainTimeoutMs = 30L * 60L * 1000L;
            }
            if (mediumDrainTimeoutMs == null || mediumDrainTimeoutMs.longValue() <= 0L) {
                mediumDrainTimeoutMs = 45L * 60L * 1000L;
            }
            if (largeDrainTimeoutMs == null || largeDrainTimeoutMs.longValue() <= 0L) {
                largeDrainTimeoutMs = 90L * 60L * 1000L;
            }
        }
    }

    static final class DatasetManifest {
        private String runId;
        private String generatedAt;
        private String sourceDatabase;
        private String targetDatabase;
        private int tableCount;
        private int familyCount;
        private List<TableSpec> tables = new ArrayList<TableSpec>();

        public String getRunId() {
            return runId;
        }

        public void setRunId(String runId) {
            this.runId = runId;
        }

        public String getGeneratedAt() {
            return generatedAt;
        }

        public void setGeneratedAt(String generatedAt) {
            this.generatedAt = generatedAt;
        }

        public String getSourceDatabase() {
            return sourceDatabase;
        }

        public void setSourceDatabase(String sourceDatabase) {
            this.sourceDatabase = sourceDatabase;
        }

        public String getTargetDatabase() {
            return targetDatabase;
        }

        public void setTargetDatabase(String targetDatabase) {
            this.targetDatabase = targetDatabase;
        }

        public int getTableCount() {
            return tableCount;
        }

        public void setTableCount(int tableCount) {
            this.tableCount = tableCount;
        }

        public int getFamilyCount() {
            return familyCount;
        }

        public void setFamilyCount(int familyCount) {
            this.familyCount = familyCount;
        }

        public List<TableSpec> getTables() {
            return tables;
        }

        public void setTables(List<TableSpec> tables) {
            this.tables = tables;
        }
    }

    static final class TableSpec {
        private int familyIndex;
        private String familyCode;
        private String tierCode;
        private int globalSequence;
        private String tableName;
        private int rowCount;
        private List<ColumnDefinition> columns = new ArrayList<ColumnDefinition>();

        public int getFamilyIndex() {
            return familyIndex;
        }

        public void setFamilyIndex(int familyIndex) {
            this.familyIndex = familyIndex;
        }

        public String getFamilyCode() {
            return familyCode;
        }

        public void setFamilyCode(String familyCode) {
            this.familyCode = familyCode;
        }

        public String getTierCode() {
            return tierCode;
        }

        public void setTierCode(String tierCode) {
            this.tierCode = tierCode;
        }

        public int getGlobalSequence() {
            return globalSequence;
        }

        public void setGlobalSequence(int globalSequence) {
            this.globalSequence = globalSequence;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public int getRowCount() {
            return rowCount;
        }

        public void setRowCount(int rowCount) {
            this.rowCount = rowCount;
        }

        public List<ColumnDefinition> getColumns() {
            return columns;
        }

        public void setColumns(List<ColumnDefinition> columns) {
            this.columns = columns;
        }
    }

    static final class ColumnDefinition {
        private String name;
        private String sqlType;
        private boolean nullable;
        private boolean optional;

        ColumnDefinition() {
        }

        ColumnDefinition(String name, String sqlType, boolean nullable, boolean optional) {
            this.name = name;
            this.sqlType = sqlType;
            this.nullable = nullable;
            this.optional = optional;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSqlType() {
            return sqlType;
        }

        public void setSqlType(String sqlType) {
            this.sqlType = sqlType;
        }

        public boolean isNullable() {
            return nullable;
        }

        public void setNullable(boolean nullable) {
            this.nullable = nullable;
        }

        public boolean isOptional() {
            return optional;
        }

        public void setOptional(boolean optional) {
            this.optional = optional;
        }
    }

    static final class FamilySpec {
        private int familyIndex;
        private int mask;
        private List<ColumnDefinition> optionalColumns = new ArrayList<ColumnDefinition>();

        public int getFamilyIndex() {
            return familyIndex;
        }

        public void setFamilyIndex(int familyIndex) {
            this.familyIndex = familyIndex;
        }

        public int getMask() {
            return mask;
        }

        public void setMask(int mask) {
            this.mask = mask;
        }

        public List<ColumnDefinition> getOptionalColumns() {
            return optionalColumns;
        }

        public void setOptionalColumns(List<ColumnDefinition> optionalColumns) {
            this.optionalColumns = optionalColumns;
        }
    }

    static final class BenchmarkRunReport {
        private String runId;
        private List<ScenarioBenchmarkResult> scenarios = new ArrayList<ScenarioBenchmarkResult>();

        public String getRunId() {
            return runId;
        }

        public void setRunId(String runId) {
            this.runId = runId;
        }

        public List<ScenarioBenchmarkResult> getScenarios() {
            return scenarios;
        }

        public void setScenarios(List<ScenarioBenchmarkResult> scenarios) {
            this.scenarios = scenarios;
        }
    }

    static final class ScenarioBenchmarkResult {
        private String scenarioLabel;
        private String projectCode;
        private int workerCount;
        private SyncResult syncResult;
        private OperationMetrics queryMetrics;
        private OperationMetrics statisticsMetrics;
        private DrainMetrics taskDrainMetrics;
        private DrainMetrics workflowDrainMetrics;
        private String failureReason;

        public String getScenarioLabel() {
            return scenarioLabel;
        }

        public void setScenarioLabel(String scenarioLabel) {
            this.scenarioLabel = scenarioLabel;
        }

        public String getProjectCode() {
            return projectCode;
        }

        public void setProjectCode(String projectCode) {
            this.projectCode = projectCode;
        }

        public int getWorkerCount() {
            return workerCount;
        }

        public void setWorkerCount(int workerCount) {
            this.workerCount = workerCount;
        }

        public SyncResult getSyncResult() {
            return syncResult;
        }

        public void setSyncResult(SyncResult syncResult) {
            this.syncResult = syncResult;
        }

        public OperationMetrics getQueryMetrics() {
            return queryMetrics;
        }

        public void setQueryMetrics(OperationMetrics queryMetrics) {
            this.queryMetrics = queryMetrics;
        }

        public OperationMetrics getStatisticsMetrics() {
            return statisticsMetrics;
        }

        public void setStatisticsMetrics(OperationMetrics statisticsMetrics) {
            this.statisticsMetrics = statisticsMetrics;
        }

        public DrainMetrics getTaskDrainMetrics() {
            return taskDrainMetrics;
        }

        public void setTaskDrainMetrics(DrainMetrics taskDrainMetrics) {
            this.taskDrainMetrics = taskDrainMetrics;
        }

        public DrainMetrics getWorkflowDrainMetrics() {
            return workflowDrainMetrics;
        }

        public void setWorkflowDrainMetrics(DrainMetrics workflowDrainMetrics) {
            this.workflowDrainMetrics = workflowDrainMetrics;
        }

        public String getFailureReason() {
            return failureReason;
        }

        public void setFailureReason(String failureReason) {
            this.failureReason = failureReason;
        }
    }

    static final class SyncResult {
        private long elapsedMs;
        private int sourceDiscoverCount;
        private int targetDiscoverCount;
        private int sourceModelCount;
        private int targetModelCount;

        public long getElapsedMs() {
            return elapsedMs;
        }

        public void setElapsedMs(long elapsedMs) {
            this.elapsedMs = elapsedMs;
        }

        public int getSourceDiscoverCount() {
            return sourceDiscoverCount;
        }

        public void setSourceDiscoverCount(int sourceDiscoverCount) {
            this.sourceDiscoverCount = sourceDiscoverCount;
        }

        public int getTargetDiscoverCount() {
            return targetDiscoverCount;
        }

        public void setTargetDiscoverCount(int targetDiscoverCount) {
            this.targetDiscoverCount = targetDiscoverCount;
        }

        public int getSourceModelCount() {
            return sourceModelCount;
        }

        public void setSourceModelCount(int sourceModelCount) {
            this.sourceModelCount = sourceModelCount;
        }

        public int getTargetModelCount() {
            return targetModelCount;
        }

        public void setTargetModelCount(int targetModelCount) {
            this.targetModelCount = targetModelCount;
        }
    }

    static final class OperationMetrics {
        private int totalRequests;
        private int successCount;
        private int failureCount;
        private long totalElapsedMs;
        private long p50Ms;
        private long p95Ms;
        private long p99Ms;
        private double throughputPerSecond;
        private List<String> errorSamples = new ArrayList<String>();

        public int getTotalRequests() {
            return totalRequests;
        }

        public void setTotalRequests(int totalRequests) {
            this.totalRequests = totalRequests;
        }

        public int getSuccessCount() {
            return successCount;
        }

        public void setSuccessCount(int successCount) {
            this.successCount = successCount;
        }

        public int getFailureCount() {
            return failureCount;
        }

        public void setFailureCount(int failureCount) {
            this.failureCount = failureCount;
        }

        public long getTotalElapsedMs() {
            return totalElapsedMs;
        }

        public void setTotalElapsedMs(long totalElapsedMs) {
            this.totalElapsedMs = totalElapsedMs;
        }

        public long getP50Ms() {
            return p50Ms;
        }

        public void setP50Ms(long p50Ms) {
            this.p50Ms = p50Ms;
        }

        public long getP95Ms() {
            return p95Ms;
        }

        public void setP95Ms(long p95Ms) {
            this.p95Ms = p95Ms;
        }

        public long getP99Ms() {
            return p99Ms;
        }

        public void setP99Ms(long p99Ms) {
            this.p99Ms = p99Ms;
        }

        public double getThroughputPerSecond() {
            return throughputPerSecond;
        }

        public void setThroughputPerSecond(double throughputPerSecond) {
            this.throughputPerSecond = throughputPerSecond;
        }

        public List<String> getErrorSamples() {
            return errorSamples;
        }

        public void setErrorSamples(List<String> errorSamples) {
            this.errorSamples = errorSamples;
        }
    }

    static final class DrainMetrics {
        private int totalDefinitions;
        private int successCount;
        private int failureCount;
        private int timeoutCount;
        private long drainElapsedMs;
        private List<String> failureSamples = new ArrayList<String>();
        private Map<String, Integer> workerDistribution = new LinkedHashMap<String, Integer>();

        public int getTotalDefinitions() {
            return totalDefinitions;
        }

        public void setTotalDefinitions(int totalDefinitions) {
            this.totalDefinitions = totalDefinitions;
        }

        public int getSuccessCount() {
            return successCount;
        }

        public void setSuccessCount(int successCount) {
            this.successCount = successCount;
        }

        public int getFailureCount() {
            return failureCount;
        }

        public void setFailureCount(int failureCount) {
            this.failureCount = failureCount;
        }

        public int getTimeoutCount() {
            return timeoutCount;
        }

        public void setTimeoutCount(int timeoutCount) {
            this.timeoutCount = timeoutCount;
        }

        public long getDrainElapsedMs() {
            return drainElapsedMs;
        }

        public void setDrainElapsedMs(long drainElapsedMs) {
            this.drainElapsedMs = drainElapsedMs;
        }

        public List<String> getFailureSamples() {
            return failureSamples;
        }

        public void setFailureSamples(List<String> failureSamples) {
            this.failureSamples = failureSamples;
        }

        public Map<String, Integer> getWorkerDistribution() {
            return workerDistribution;
        }

        public void setWorkerDistribution(Map<String, Integer> workerDistribution) {
            this.workerDistribution = workerDistribution;
        }
    }
}
