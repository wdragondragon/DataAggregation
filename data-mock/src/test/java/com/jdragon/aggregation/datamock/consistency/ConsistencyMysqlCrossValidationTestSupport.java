package com.jdragon.aggregation.datamock.consistency;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.InsertDataDTO;
import com.jdragon.aggregation.datasource.mysql8.Mysql8SourcePlugin;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
import java.util.stream.Stream;

final class ConsistencyMysqlCrossValidationTestSupport {

    static final String AGGREGATION_HOME = "C:\\dev\\ideaProject\\DataAggregation\\package_all\\aggregation";

    private static final List<String> SOURCE_COLUMNS = Collections.unmodifiableList(Arrays.asList(
            "biz_id",
            "user_name",
            "age",
            "active_flag",
            "register_date",
            "last_login_at",
            "profile_text",
            "salary",
            "bonus_rate",
            "level_code",
            "note_text"
    ));

    private static final List<String> OUTPUT_COLUMNS = Collections.unmodifiableList(Arrays.asList(
            "rule_id",
            "record_id",
            "match_keys",
            "differences",
            "conflict_type",
            "payload"
    ));

    private static final int INSERT_CHUNK_SIZE = 5_000;
    private static final int WRITER_BATCH_SIZE = 1_000;
    private static final Mysql8SourcePlugin MYSQL_PLUGIN = new Mysql8SourcePlugin();

    private static final char[] LEVEL_CODES = new char[]{'A', 'B', 'C', 'D'};
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final LocalDate BASE_DATE = LocalDate.of(2024, 1, 1);
    private static final LocalDateTime BASE_DATE_TIME = LocalDateTime.of(2024, 1, 1, 8, 0, 0);
    private static final BigDecimal SALARY_BASE = new BigDecimal("1000.00");
    private static final BigDecimal SALARY_STEP = new BigDecimal("1.25");
    private static final BigDecimal BONUS_BASE = new BigDecimal("0.50");
    private static final BigDecimal BONUS_STEP = new BigDecimal("0.05");

    private ConsistencyMysqlCrossValidationTestSupport() {
    }

    static CrossValidationResult runCrossValidationScenario(String label,
                                                            int rowCount,
                                                            Path scenarioRoot,
                                                            boolean keepArtifacts,
                                                            ScenarioProfile profile) throws Exception {
        ensureAggregationHome();
        cleanupPath(scenarioRoot);
        Files.createDirectories(scenarioRoot);

        ScenarioContext scenario = null;
        try {
            scenario = prepareScenario(label, rowCount, scenarioRoot, profile != null ? profile : ScenarioProfile.FULL_VALIDATION);

            long exampleStart = System.nanoTime();
            ComparisonResult exampleResult = ConsistencyExample.executeRule(
                    buildRule(scenario, "example-rule", scenario.getTableNames().get("exampleTarget"), scenario.getTableNames().get("exampleReference")),
                    scenario.getScenarioRoot().resolve("example-results").toString()
            );
            long exampleElapsedMs = elapsedMillis(exampleStart);

            long readerStart = System.nanoTime();
            JobContainer container = new JobContainer(Configuration.from(scenario.getJobConfigFile().toFile()));
            container.start();
            long readerElapsedMs = elapsedMillis(readerStart);

            long verifyStart = System.nanoTime();
            TableSnapshot exampleTarget = queryTableSnapshot(scenario, scenario.getTableNames().get("exampleTarget"), rowCount <= 100);
            TableSnapshot exampleReference = queryTableSnapshot(scenario, scenario.getTableNames().get("exampleReference"), rowCount <= 100);
            TableSnapshot readerTarget = queryTableSnapshot(scenario, scenario.getTableNames().get("readerTarget"), rowCount <= 100);
            TableSnapshot readerReference = queryTableSnapshot(scenario, scenario.getTableNames().get("readerReference"), rowCount <= 100);
            WriterOutputSnapshot writerOutput = queryWriterOutputSnapshot(scenario);
            ComparisonResult readerResult = extractConsistencyResult(container);
            long verifyElapsedMs = elapsedMillis(verifyStart);

            CrossValidationResult result = new CrossValidationResult();
            result.setLabel(label);
            result.setRowCount(rowCount);
            result.setProfile(scenario.getProfile());
            result.setSetupElapsedMs(scenario.getSetupElapsedMs());
            result.setExampleElapsedMs(exampleElapsedMs);
            result.setReaderElapsedMs(readerElapsedMs);
            result.setVerifyElapsedMs(verifyElapsedMs);
            result.setExpectedMetrics(buildExpectedMetrics(rowCount, scenario.getProfile()));
            result.setExampleResult(exampleResult);
            result.setReaderResult(readerResult);
            result.setExampleTargetSnapshot(exampleTarget);
            result.setExampleReferenceSnapshot(exampleReference);
            result.setReaderTargetSnapshot(readerTarget);
            result.setReaderReferenceSnapshot(readerReference);
            result.setWriterOutputSnapshot(writerOutput);
            result.setJobConfigFile(scenario.getJobConfigFile());
            return result;
        } finally {
            if (scenario != null) {
                dropTables(scenario);
            }
            if (!keepArtifacts) {
                cleanupPath(scenarioRoot);
            }
        }
    }

    static Path benchmarkRoot(String child) {
        return Paths.get("target", "consistency-cross-validation-benchmark", child);
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
        Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .forEach(current -> {
                    try {
                        Files.deleteIfExists(current);
                    } catch (IOException ignored) {
                    }
                });
    }

    private static void copyDirectory(Path source, Path target) {
        try {
            deleteDirectory(target);
            Files.createDirectories(target);
            try (Stream<Path> stream = Files.walk(source)) {
                stream.forEach(current -> {
                    Path relative = source.relativize(current);
                    Path destination = target.resolve(relative.toString());
                    try {
                        if (Files.isDirectory(current)) {
                            Files.createDirectories(destination);
                        } else {
                            if (destination.getParent() != null) {
                                Files.createDirectories(destination.getParent());
                            }
                            Files.copy(current, destination, StandardCopyOption.REPLACE_EXISTING);
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException("Failed to copy plugin directory: " + source, e);
                    }
                });
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to prepare plugin directory: " + source, e);
        }
    }

    private static void deleteDirectory(Path target) throws IOException {
        if (!Files.exists(target)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(target)) {
            stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to clean plugin directory: " + target, e);
                }
            });
        }
    }

    private static void ensureAggregationHome() {
        System.setProperty("aggregation.home", AGGREGATION_HOME);
        ensureReaderPluginAvailable("consistency");
        Path pluginPath = Paths.get(AGGREGATION_HOME, "plugin", "writer", "mysql8writer", "plugin.json");
        if (!Files.exists(pluginPath)) {
            throw new IllegalStateException("mysql8 writer plugin not found under aggregation.home: " + pluginPath);
        }
    }

    private static void ensureReaderPluginAvailable(String pluginName) {
        String pluginDirectoryName = pluginName + "reader";
        Path pluginHome = Paths.get(AGGREGATION_HOME, "plugin", "reader", pluginDirectoryName);
        List<Path> candidates = Arrays.asList(
                Paths.get("..", "job-plugins", "reader", "consistencyreader", "target", "aggregation-bin", "plugin", "reader", pluginDirectoryName).normalize(),
                Paths.get("job-plugins", "reader", "consistencyreader", "target", "aggregation-bin", "plugin", "reader", pluginDirectoryName).normalize()
        );
        for (Path candidate : candidates) {
            if (Files.exists(candidate.resolve("plugin.json"))) {
                copyDirectory(candidate, pluginHome);
                return;
            }
        }
        throw new IllegalStateException("reader plugin not found under aggregation.home or module target: " + pluginName);
    }

    private static ScenarioContext prepareScenario(String label,
                                                   int rowCount,
                                                   Path scenarioRoot,
                                                   ScenarioProfile profile) throws Exception {
        ScenarioContext scenario = new ScenarioContext();
        scenario.setLabel(label);
        scenario.setRowCount(rowCount);
        scenario.setScenarioRoot(scenarioRoot);
        scenario.setProfile(profile);
        scenario.setTableNames(buildTableNames(label));
        scenario.setConnectionConfig(loadMysqlConnectionConfig());
        scenario.setDataSourceDTO(buildDataSourceDTO(scenario.getConnectionConfig()));

        long setupStart = System.nanoTime();
        createTables(scenario);
        populateTables(scenario);
        scenario.setJobConfigFile(buildReaderJobConfigFile(scenario));
        scenario.setSetupElapsedMs(elapsedMillis(setupStart));
        return scenario;
    }

    private static Map<String, String> buildTableNames(String label) {
        String sanitized = label.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
        String suffix = String.valueOf(System.currentTimeMillis() % 1_000_000L);
        Map<String, String> tableNames = new LinkedHashMap<String, String>();
        tableNames.put("exampleTarget", "cmv_" + sanitized + "_et_" + suffix);
        tableNames.put("exampleReference", "cmv_" + sanitized + "_er_" + suffix);
        tableNames.put("readerTarget", "cmv_" + sanitized + "_rt_" + suffix);
        tableNames.put("readerReference", "cmv_" + sanitized + "_rr_" + suffix);
        tableNames.put("output", "cmv_" + sanitized + "_out_" + suffix);
        return tableNames;
    }

    private static Configuration loadMysqlConnectionConfig() {
        File configFile = resolveDemoConfigFile();
        Configuration root = Configuration.from(configFile);
        Configuration connect = root.getConfiguration("reader.config.sources[0].config");
        if (connect == null) {
            connect = root.getConfiguration("writer.config.connect");
        }
        if (connect == null) {
            throw new IllegalStateException("MySQL connect config not found in fusion-mysql-demo.json");
        }
        return connect.clone();
    }

    private static File resolveDemoConfigFile() {
        Path moduleRelative = Paths.get("..", "core", "src", "main", "resources", "fusion-mysql-demo.json").normalize();
        if (Files.exists(moduleRelative)) {
            return moduleRelative.toFile();
        }
        Path rootRelative = Paths.get("core", "src", "main", "resources", "fusion-mysql-demo.json");
        if (Files.exists(rootRelative)) {
            return rootRelative.toFile();
        }
        throw new IllegalStateException("fusion-mysql-demo.json not found");
    }

    private static BaseDataSourceDTO buildDataSourceDTO(Configuration connectionConfig) {
        BaseDataSourceDTO dto = JSONObject.parseObject(connectionConfig.toJSON(), BaseDataSourceDTO.class);
        dto.setName("mysql8");
        dto.setType("mysql8");
        return dto;
    }

    private static void createTables(ScenarioContext scenario) {
        BaseDataSourceDTO dto = scenario.getDataSourceDTO();
        for (String tableName : scenario.getTableNames().values()) {
            MYSQL_PLUGIN.executeUpdate(dto, "DROP TABLE IF EXISTS " + tableName);
        }

        createSourceTable(dto, scenario.getTableNames().get("exampleTarget"));
        createSourceTable(dto, scenario.getTableNames().get("exampleReference"));
        createSourceTable(dto, scenario.getTableNames().get("readerTarget"));
        createSourceTable(dto, scenario.getTableNames().get("readerReference"));

        MYSQL_PLUGIN.executeUpdate(dto,
                "CREATE TABLE " + scenario.getTableNames().get("output") + " ("
                        + "id BIGINT PRIMARY KEY AUTO_INCREMENT,"
                        + "rule_id VARCHAR(128) NOT NULL,"
                        + "record_id VARCHAR(128) NOT NULL,"
                        + "match_keys TEXT NOT NULL,"
                        + "differences TEXT NOT NULL,"
                        + "conflict_type VARCHAR(64) NOT NULL,"
                        + "payload LONGTEXT NOT NULL"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    }

    private static void createSourceTable(BaseDataSourceDTO dto, String tableName) {
        MYSQL_PLUGIN.executeUpdate(dto,
                "CREATE TABLE " + tableName + " ("
                        + "biz_id BIGINT PRIMARY KEY,"
                        + "user_name VARCHAR(64) NOT NULL,"
                        + "age INT NOT NULL,"
                        + "active_flag TINYINT NOT NULL,"
                        + "register_date DATE NOT NULL,"
                        + "last_login_at DATETIME NOT NULL,"
                        + "profile_text TEXT NOT NULL,"
                        + "salary DECIMAL(18,2) NOT NULL,"
                        + "bonus_rate DOUBLE NOT NULL,"
                        + "level_code CHAR(1) NOT NULL,"
                        + "note_text TEXT NOT NULL"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    }

    private static void populateTables(ScenarioContext scenario) {
        populateSourcePair(
                scenario.getDataSourceDTO(),
                scenario.getTableNames().get("exampleTarget"),
                scenario.getTableNames().get("exampleReference"),
                scenario.getRowCount(),
                scenario.getProfile()
        );
        populateSourcePair(
                scenario.getDataSourceDTO(),
                scenario.getTableNames().get("readerTarget"),
                scenario.getTableNames().get("readerReference"),
                scenario.getRowCount(),
                scenario.getProfile()
        );
    }

    private static void populateSourcePair(BaseDataSourceDTO dto,
                                           String targetTable,
                                           String referenceTable,
                                           int rowCount,
                                           ScenarioProfile profile) {
        List<List<String>> targetBatch = new ArrayList<List<String>>(INSERT_CHUNK_SIZE);
        List<List<String>> referenceBatch = new ArrayList<List<String>>(INSERT_CHUNK_SIZE);
        for (long bizId = 1L; bizId <= rowCount; bizId++) {
            DifferenceCategory category = resolveCategory(bizId, rowCount, profile);
            switch (category) {
                case CONSISTENT:
                    targetBatch.add(buildCanonicalRow(bizId));
                    referenceBatch.add(buildCanonicalRow(bizId));
                    break;
                case UPDATE:
                    targetBatch.add(buildStaleRow(bizId));
                    referenceBatch.add(buildCanonicalRow(bizId));
                    break;
                case INSERT:
                    referenceBatch.add(buildCanonicalRow(bizId));
                    break;
                case DELETE:
                    targetBatch.add(buildCanonicalRow(bizId));
                    break;
                default:
                    break;
            }
            if (targetBatch.size() >= INSERT_CHUNK_SIZE) {
                flushInsertBatch(dto, targetTable, SOURCE_COLUMNS, targetBatch);
                targetBatch = new ArrayList<List<String>>(INSERT_CHUNK_SIZE);
            }
            if (referenceBatch.size() >= INSERT_CHUNK_SIZE) {
                flushInsertBatch(dto, referenceTable, SOURCE_COLUMNS, referenceBatch);
                referenceBatch = new ArrayList<List<String>>(INSERT_CHUNK_SIZE);
            }
        }
        if (!targetBatch.isEmpty()) {
            flushInsertBatch(dto, targetTable, SOURCE_COLUMNS, targetBatch);
        }
        if (!referenceBatch.isEmpty()) {
            flushInsertBatch(dto, referenceTable, SOURCE_COLUMNS, referenceBatch);
        }
    }

    private static void flushInsertBatch(BaseDataSourceDTO dto,
                                         String tableName,
                                         List<String> columns,
                                         List<List<String>> rows) {
        InsertDataDTO insertDataDTO = new InsertDataDTO(columns, rows);
        insertDataDTO.setBaseDataSourceDTO(dto);
        insertDataDTO.setTableName(tableName);
        insertDataDTO.setBatchSize(rows.size());
        insertDataDTO.setTruncate(false);
        MYSQL_PLUGIN.insertData(insertDataDTO);
    }

    private static List<String> buildCanonicalRow(long bizId) {
        return Arrays.asList(
                Long.toString(bizId),
                userName(bizId),
                Integer.toString(age(bizId)),
                Integer.toString(activeFlag(bizId)),
                registerDate(bizId),
                lastLoginAt(bizId),
                profileText(bizId),
                decimalText(salary(bizId)),
                decimalText(bonusRate(bizId)),
                levelCode(bizId),
                noteText(bizId)
        );
    }

    private static List<String> buildStaleRow(long bizId) {
        return Arrays.asList(
                Long.toString(bizId),
                userName(bizId) + "_stale",
                Integer.toString(age(bizId) + 3),
                Integer.toString(activeFlag(bizId) == 0 ? 1 : 0),
                registerDate(bizId + 5L),
                lastLoginAt(bizId + 120L),
                profileText(bizId) + "_stale",
                decimalText(salary(bizId).add(new BigDecimal("88.88"))),
                decimalText(bonusRate(bizId).add(new BigDecimal("0.15"))),
                rotateLevelCode(bizId),
                noteText(bizId) + "_stale"
        );
    }

    private static Path buildReaderJobConfigFile(ScenarioContext scenario) throws IOException {
        Configuration config = Configuration.newDefault();
        config.set("reader.type", "consistency");
        config.set("reader.config", buildReaderRuleConfig(scenario, buildRule(
                scenario,
                "reader-rule",
                scenario.getTableNames().get("readerTarget"),
                scenario.getTableNames().get("readerReference")
        )));
        config.set("writer.type", "mysql8");
        config.set("writer.config.connect", configurationAsMap(scenario.getConnectionConfig().clone()));
        config.set("writer.config.table", scenario.getTableNames().get("output"));
        config.set("writer.config.columns", OUTPUT_COLUMNS);
        config.set("writer.config.writeMode", "insert");
        config.set("writer.config.batchSize", WRITER_BATCH_SIZE);

        Path jobConfigFile = scenario.getScenarioRoot().resolve("consistency-job.json");
        Files.write(jobConfigFile, config.beautify().getBytes(StandardCharsets.UTF_8));
        return jobConfigFile;
    }

    private static ConsistencyRule buildRule(ScenarioContext scenario,
                                             String ruleId,
                                             String targetTable,
                                             String referenceTable) {
        ConsistencyRule rule = new ConsistencyRule();
        rule.setRuleId(ruleId);
        rule.setRuleName(ruleId);
        rule.setDescription("consistency mysql cross validation");
        rule.setEnabled(true);
        rule.setParallelFetch(true);
        rule.setToleranceThreshold(0.0D);
        rule.setConflictResolutionStrategy(ConflictResolutionStrategy.HIGH_CONFIDENCE);
        rule.setUpdateTargetSourceId("source-1");
        rule.setAutoApplyResolutions(true);
        rule.setAllowInsert(true);
        rule.setAllowDelete(true);
        rule.setSkipUnchangedUpdates(true);
        rule.setValidateBeforeUpdate(false);
        rule.setCompareFields(Arrays.asList(
                "user_name",
                "age",
                "active_flag",
                "register_date",
                "last_login_at",
                "profile_text",
                "salary",
                "bonus_rate",
                "level_code",
                "note_text"
        ));
        rule.setMatchKeys(Collections.singletonList("biz_id"));
        rule.setDataSources(Arrays.asList(
                buildSourceConfig("source-1", "target-source", targetTable, scenario.getConnectionConfig().clone(), true, 1.0D, 1),
                buildSourceConfig("source-2", "authoritative-source", referenceTable, scenario.getConnectionConfig().clone(), false, 2.0D, 2)
        ));

        OutputConfig outputConfig = new OutputConfig();
        outputConfig.setOutputPath(scenario.getScenarioRoot().resolve(ruleId + "-results").toString());
        outputConfig.setGenerateReport(true);
        outputConfig.setReportLanguage(OutputConfig.ReportLanguage.CHINESE);
        rule.setOutputConfig(outputConfig);

        ConsistencyRule.StreamCacheConfig cacheConfig = new ConsistencyRule.StreamCacheConfig();
        cacheConfig.setPartitionCount(16);
        cacheConfig.setRebalancePartitionMultiplier(4);
        cacheConfig.setSpillPath(scenario.getScenarioRoot().resolve(ruleId + "-spill").toString());
        cacheConfig.setKeepTempFiles(false);
        rule.setCacheConfig(cacheConfig);

        ConsistencyRule.StreamPerformanceConfig performanceConfig = new ConsistencyRule.StreamPerformanceConfig();
        performanceConfig.setParallelSourceCount(2);
        performanceConfig.setMemoryLimitMB(512);
        rule.setPerformanceConfig(performanceConfig);

        return rule;
    }

    private static DataSourceConfig buildSourceConfig(String sourceId,
                                                      String sourceName,
                                                      String tableName,
                                                      Configuration connectionConfig,
                                                      boolean updateTarget,
                                                      double confidenceWeight,
                                                      int priority) {
        DataSourceConfig config = new DataSourceConfig();
        config.setSourceId(sourceId);
        config.setSourceName(sourceName);
        config.setPluginName("mysql8");
        config.setConnectionConfig(connectionConfig);
        config.setTableName(tableName);
        config.setQuerySql(buildConsistencySourceQuery(tableName));
        config.setConfidenceWeight(confidenceWeight);
        config.setPriority(priority);
        config.setFieldMappings(new LinkedHashMap<String, String>());
        config.setExtConfig(Configuration.newDefault());
        config.setUpdateTarget(updateTarget);
        return config;
    }

    private static Map<String, Object> buildReaderRuleConfig(ScenarioContext scenario, ConsistencyRule rule) {
        Map<String, Object> ruleMap = ruleToMap(rule);
        List<Object> sourceConfigs = new ArrayList<Object>();
        sourceConfigs.add(buildReaderSourceConfig(
                "source-1",
                "target-source",
                scenario.getTableNames().get("readerTarget"),
                scenario.getConnectionConfig().clone(),
                true,
                1.0D,
                1
        ));
        sourceConfigs.add(buildReaderSourceConfig(
                "source-2",
                "authoritative-source",
                scenario.getTableNames().get("readerReference"),
                scenario.getConnectionConfig().clone(),
                false,
                2.0D,
                2
        ));
        ruleMap.put("dataSources", sourceConfigs);
        return ruleMap;
    }

    private static Map<String, Object> buildReaderSourceConfig(String sourceId,
                                                               String sourceName,
                                                               String tableName,
                                                               Configuration connectionConfig,
                                                               boolean updateTarget,
                                                               double confidenceWeight,
                                                               int priority) {
        Map<String, Object> source = new LinkedHashMap<String, Object>();
        source.put("sourceId", sourceId);
        source.put("sourceName", sourceName);
        source.put("pluginName", "mysql8");
        source.put("connectionConfig", configurationAsMap(connectionConfig));
        source.put("querySql", buildConsistencySourceQuery(tableName));
        source.put("tableName", tableName);
        source.put("confidenceWeight", confidenceWeight);
        source.put("priority", priority);
        source.put("fieldMappings", new LinkedHashMap<String, String>());
        source.put("extConfig", new LinkedHashMap<String, Object>());
        source.put("updateTarget", updateTarget);
        return source;
    }

    private static Map<String, Object> ruleToMap(ConsistencyRule rule) {
        Configuration config = rule.toConfig();
        Map<String, Object> values = config.getMap("");
        return values == null ? new LinkedHashMap<String, Object>() : new LinkedHashMap<String, Object>(values);
    }

    private static Map<String, Object> configurationAsMap(Configuration configuration) {
        Map<String, Object> values = configuration.getMap("");
        return values == null ? new LinkedHashMap<String, Object>() : new LinkedHashMap<String, Object>(values);
    }

    private static String buildConsistencySourceQuery(String tableName) {
        return "SELECT "
                + "CAST(biz_id AS CHAR) AS biz_id, "
                + "user_name AS user_name, "
                + "CAST(age AS CHAR) AS age, "
                + "CAST(active_flag AS CHAR) AS active_flag, "
                + "DATE_FORMAT(register_date, '%Y-%m-%d') AS register_date, "
                + "DATE_FORMAT(last_login_at, '%Y-%m-%d %H:%i:%s') AS last_login_at, "
                + "profile_text AS profile_text, "
                + decimalSql("salary", "salary") + ", "
                + decimalSql("ROUND(bonus_rate, 2)", "bonus_rate") + ", "
                + "level_code AS level_code, "
                + "note_text AS note_text "
                + "FROM " + tableName + " ORDER BY CAST(biz_id AS UNSIGNED)";
    }

    private static ComparisonResult extractConsistencyResult(JobContainer container) {
        if (container == null || container.getJobPointReporter() == null) {
            return null;
        }
        Communication communication = container.getJobPointReporter().getTrackCommunication();
        if (communication == null || communication.getMessage() == null) {
            return null;
        }
        List<String> results = communication.getMessage().get("consistency_result");
        if (results == null || results.isEmpty()) {
            return null;
        }
        return JSONObject.parseObject(results.get(results.size() - 1), ComparisonResult.class);
    }

    private static TableSnapshot queryTableSnapshot(ScenarioContext scenario, String tableName, boolean loadAllRows) {
        TableSnapshot snapshot = new TableSnapshot();
        snapshot.setAggregate(queryAggregate(scenario, tableName));
        snapshot.setSampleRows(querySampleRows(scenario, tableName, scenario.getRowCount(), scenario.getProfile()));
        if (loadAllRows) {
            snapshot.setOrderedRows(queryAllRows(scenario, tableName));
        }
        return snapshot;
    }

    private static AggregateResult queryAggregate(ScenarioContext scenario, String tableName) {
        String sql = "SELECT "
                + "CAST(COUNT(*) AS CHAR) AS row_count, "
                + "CAST(SUM(age) AS CHAR) AS total_age, "
                + "CAST(SUM(active_flag) AS CHAR) AS total_active_flag, "
                + decimalSql("SUM(salary)", "total_salary") + ", "
                + decimalSql("ROUND(SUM(bonus_rate), 2)", "total_bonus_rate") + " "
                + "FROM " + tableName;
        Table<Map<String, Object>> table = MYSQL_PLUGIN.executeQuerySql(scenario.getDataSourceDTO(), sql, true);
        Map<String, Object> row = table.getBodies().get(0);

        AggregateResult result = new AggregateResult();
        result.setRowCount(asString(row.get("row_count")));
        result.setTotalAge(asString(row.get("total_age")));
        result.setTotalActiveFlag(asString(row.get("total_active_flag")));
        result.setTotalSalary(asString(row.get("total_salary")));
        result.setTotalBonusRate(asString(row.get("total_bonus_rate")));
        return result;
    }

    private static Map<Long, List<String>> querySampleRows(ScenarioContext scenario,
                                                           String tableName,
                                                           int rowCount,
                                                           ScenarioProfile profile) {
        List<Long> keys = filteredSampleKeys(rowCount, profile);
        if (keys.isEmpty()) {
            return new LinkedHashMap<Long, List<String>>();
        }
        StringBuilder sql = new StringBuilder();
        sql.append(buildTableQuery(tableName));
        sql.append(" WHERE biz_id IN (");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(keys.get(i));
        }
        sql.append(") ORDER BY CAST(biz_id AS UNSIGNED)");
        List<List<String>> rows = queryRows(scenario, sql.toString());
        Map<Long, List<String>> sampleRows = new LinkedHashMap<Long, List<String>>();
        for (List<String> row : rows) {
            sampleRows.put(Long.valueOf(row.get(0)), row);
        }
        return sampleRows;
    }

    private static List<List<String>> queryAllRows(ScenarioContext scenario, String tableName) {
        return queryRows(scenario, buildTableQuery(tableName) + " ORDER BY CAST(biz_id AS UNSIGNED)");
    }

    private static String buildTableQuery(String tableName) {
        return "SELECT "
                + "CAST(biz_id AS CHAR) AS biz_id, "
                + "user_name AS user_name, "
                + "CAST(age AS CHAR) AS age, "
                + "CAST(active_flag AS CHAR) AS active_flag, "
                + "DATE_FORMAT(register_date, '%Y-%m-%d') AS register_date, "
                + "DATE_FORMAT(last_login_at, '%Y-%m-%d %H:%i:%s') AS last_login_at, "
                + "profile_text AS profile_text, "
                + decimalSql("salary", "salary") + ", "
                + decimalSql("ROUND(bonus_rate, 2)", "bonus_rate") + ", "
                + "level_code AS level_code, "
                + "note_text AS note_text "
                + "FROM " + tableName;
    }

    private static List<List<String>> queryRows(ScenarioContext scenario, String sql) {
        Table<Map<String, Object>> table = MYSQL_PLUGIN.executeQuerySql(scenario.getDataSourceDTO(), sql, true);
        List<List<String>> rows = new ArrayList<List<String>>();
        for (Map<String, Object> body : table.getBodies()) {
            rows.add(Arrays.asList(
                    asString(body.get("biz_id")),
                    asString(body.get("user_name")),
                    asString(body.get("age")),
                    asString(body.get("active_flag")),
                    asString(body.get("register_date")),
                    asString(body.get("last_login_at")),
                    asString(body.get("profile_text")),
                    asString(body.get("salary")),
                    asString(body.get("bonus_rate")),
                    asString(body.get("level_code")),
                    asString(body.get("note_text"))
            ));
        }
        return rows;
    }

    private static WriterOutputSnapshot queryWriterOutputSnapshot(ScenarioContext scenario) {
        String sql = "SELECT rule_id, record_id, match_keys, differences, conflict_type, payload "
                + "FROM " + scenario.getTableNames().get("output") + " ORDER BY id";
        Table<Map<String, Object>> table = MYSQL_PLUGIN.executeQuerySql(scenario.getDataSourceDTO(), sql, true);
        WriterOutputSnapshot snapshot = new WriterOutputSnapshot();
        snapshot.setRowCount(table.getBodies().size());
        Map<String, Integer> conflictTypeCounts = new LinkedHashMap<String, Integer>();
        Set<Long> diffKeys = new LinkedHashSet<Long>();
        List<List<String>> rows = new ArrayList<List<String>>();
        for (Map<String, Object> body : table.getBodies()) {
            String conflictType = asString(body.get("conflict_type"));
            conflictTypeCounts.put(conflictType, conflictTypeCounts.containsKey(conflictType)
                    ? conflictTypeCounts.get(conflictType) + 1
                    : 1);
            String matchKeys = asString(body.get("match_keys"));
            JSONObject keyJson = JSONObject.parseObject(matchKeys);
            if (keyJson != null && keyJson.get("biz_id") != null) {
                diffKeys.add(Long.valueOf(String.valueOf(keyJson.get("biz_id"))));
            }
            rows.add(Arrays.asList(
                    asString(body.get("rule_id")),
                    asString(body.get("record_id")),
                    matchKeys,
                    asString(body.get("differences")),
                    conflictType,
                    asString(body.get("payload"))
            ));
        }
        snapshot.setConflictTypeCounts(conflictTypeCounts);
        snapshot.setDiffKeys(diffKeys);
        snapshot.setRows(rows);
        return snapshot;
    }

    private static ExpectedMetrics buildExpectedMetrics(int rowCount, ScenarioProfile profile) {
        int updateCount = profile == ScenarioProfile.FULL_VALIDATION ? rowCount / 4 : 100;
        int insertCount = profile == ScenarioProfile.FULL_VALIDATION ? rowCount / 4 : 100;
        int deleteCount = profile == ScenarioProfile.FULL_VALIDATION ? rowCount / 4 : 100;
        int inconsistentCount = updateCount + insertCount + deleteCount;
        int consistentCount = rowCount - inconsistentCount;

        ExpectedMetrics metrics = new ExpectedMetrics();
        metrics.setTotalRecords(rowCount);
        metrics.setConsistentRecords(consistentCount);
        metrics.setInconsistentRecords(inconsistentCount);
        metrics.setResolvedRecords(inconsistentCount);
        metrics.setInsertCount(insertCount);
        metrics.setUpdateCount(updateCount);
        metrics.setDeleteCount(deleteCount);
        metrics.setFinalRowCount(rowCount - deleteCount);

        Map<String, Integer> conflictTypeCounts = new LinkedHashMap<String, Integer>();
        conflictTypeCounts.put("BINARY_CONFLICT", updateCount);
        conflictTypeCounts.put("MISSING", insertCount + deleteCount);
        metrics.setConflictTypeCounts(conflictTypeCounts);
        metrics.setDiffKeys(buildExpectedDiffKeys(rowCount, profile));
        return metrics;
    }

    private static Set<Long> buildExpectedDiffKeys(int rowCount, ScenarioProfile profile) {
        Set<Long> keys = new LinkedHashSet<Long>();
        for (long bizId = 1L; bizId <= rowCount; bizId++) {
            if (resolveCategory(bizId, rowCount, profile) != DifferenceCategory.CONSISTENT) {
                keys.add(bizId);
            }
        }
        return keys;
    }

    private static List<Long> filteredSampleKeys(int rowCount, ScenarioProfile profile) {
        List<Long> keys = new ArrayList<Long>();
        if (profile == ScenarioProfile.FULL_VALIDATION) {
            keys.addAll(Arrays.asList(1L, 10L, 25L, 26L, 40L, 50L, 51L, 60L, 75L));
            return filterExistingKeys(keys, rowCount, profile);
        }
        keys.addAll(Arrays.asList(
                1L,
                10L,
                1_000L,
                (long) rowCount - 299L,
                (long) rowCount - 250L,
                (long) rowCount - 150L,
                (long) rowCount - 101L
        ));
        return filterExistingKeys(keys, rowCount, profile);
    }

    private static List<Long> filterExistingKeys(List<Long> keys, int rowCount, ScenarioProfile profile) {
        List<Long> filtered = new ArrayList<Long>();
        for (Long key : keys) {
            if (key == null || key.longValue() < 1L || key.longValue() > rowCount) {
                continue;
            }
            if (resolveCategory(key.longValue(), rowCount, profile) == DifferenceCategory.DELETE) {
                continue;
            }
            filtered.add(key);
        }
        return filtered;
    }

    private static DifferenceCategory resolveCategory(long bizId, int rowCount, ScenarioProfile profile) {
        if (profile == ScenarioProfile.FULL_VALIDATION) {
            int quarter = rowCount / 4;
            if (bizId <= quarter) {
                return DifferenceCategory.CONSISTENT;
            }
            if (bizId <= quarter * 2L) {
                return DifferenceCategory.UPDATE;
            }
            if (bizId <= quarter * 3L) {
                return DifferenceCategory.INSERT;
            }
            return DifferenceCategory.DELETE;
        }

        long updateStart = rowCount - 299L;
        long updateEnd = rowCount - 200L;
        long insertStart = rowCount - 199L;
        long insertEnd = rowCount - 100L;
        long deleteStart = rowCount - 99L;
        if (bizId >= updateStart && bizId <= updateEnd) {
            return DifferenceCategory.UPDATE;
        }
        if (bizId >= insertStart && bizId <= insertEnd) {
            return DifferenceCategory.INSERT;
        }
        if (bizId >= deleteStart) {
            return DifferenceCategory.DELETE;
        }
        return DifferenceCategory.CONSISTENT;
    }

    private static String decimalSql(String expression, String alias) {
        return "CASE WHEN " + expression + " IS NULL THEN NULL "
                + "ELSE TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM CAST(" + expression + " AS CHAR))) END AS " + alias;
    }

    private static void dropTables(ScenarioContext scenario) {
        BaseDataSourceDTO dto = scenario.getDataSourceDTO();
        for (String tableName : scenario.getTableNames().values()) {
            try {
                MYSQL_PLUGIN.executeUpdate(dto, "DROP TABLE IF EXISTS " + tableName);
            } catch (Exception ignored) {
            }
        }
    }

    private static long elapsedMillis(long startNanoTime) {
        return (System.nanoTime() - startNanoTime) / 1_000_000L;
    }

    private static String userName(long bizId) {
        return String.format("user_%07d", bizId);
    }

    private static int age(long bizId) {
        return 18 + (int) (bizId % 50L);
    }

    private static int activeFlag(long bizId) {
        return (int) (bizId % 2L);
    }

    private static String registerDate(long bizId) {
        return BASE_DATE.plusDays((bizId - 1L) % 365L).format(DATE_FORMAT);
    }

    private static String lastLoginAt(long bizId) {
        return BASE_DATE_TIME.plusMinutes(bizId).format(DATE_TIME_FORMAT);
    }

    private static String profileText(long bizId) {
        return "profile_" + bizId + "_g" + (bizId % 7L);
    }

    private static BigDecimal salary(long bizId) {
        return SALARY_BASE.add(SALARY_STEP.multiply(BigDecimal.valueOf(bizId))).setScale(2, RoundingMode.HALF_UP);
    }

    private static BigDecimal bonusRate(long bizId) {
        BigDecimal step = BONUS_STEP.multiply(BigDecimal.valueOf(bizId % 20L));
        return BONUS_BASE.add(step).setScale(2, RoundingMode.HALF_UP);
    }

    private static String levelCode(long bizId) {
        int index = (int) ((bizId - 1L) % LEVEL_CODES.length);
        return String.valueOf(LEVEL_CODES[index]);
    }

    private static String rotateLevelCode(long bizId) {
        int index = (int) (bizId % LEVEL_CODES.length);
        return String.valueOf(LEVEL_CODES[index]);
    }

    private static String noteText(long bizId) {
        return "note_" + bizId + "_L" + levelCode(bizId);
    }

    private static String decimalText(BigDecimal value) {
        if (value == null) {
            return null;
        }
        BigDecimal normalized = value.stripTrailingZeros();
        if (normalized.scale() < 0) {
            normalized = normalized.setScale(0);
        }
        return normalized.toPlainString();
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    enum ScenarioProfile {
        FULL_VALIDATION,
        LARGE_PRESSURE
    }

    private enum DifferenceCategory {
        CONSISTENT,
        UPDATE,
        INSERT,
        DELETE
    }

    static final class CrossValidationResult {
        private String label;
        private int rowCount;
        private ScenarioProfile profile;
        private long setupElapsedMs;
        private long exampleElapsedMs;
        private long readerElapsedMs;
        private long verifyElapsedMs;
        private ExpectedMetrics expectedMetrics;
        private ComparisonResult exampleResult;
        private ComparisonResult readerResult;
        private TableSnapshot exampleTargetSnapshot;
        private TableSnapshot exampleReferenceSnapshot;
        private TableSnapshot readerTargetSnapshot;
        private TableSnapshot readerReferenceSnapshot;
        private WriterOutputSnapshot writerOutputSnapshot;
        private Path jobConfigFile;

        public String getLabel() { return label; }
        public void setLabel(String label) { this.label = label; }
        public int getRowCount() { return rowCount; }
        public void setRowCount(int rowCount) { this.rowCount = rowCount; }
        public ScenarioProfile getProfile() { return profile; }
        public void setProfile(ScenarioProfile profile) { this.profile = profile; }
        public long getSetupElapsedMs() { return setupElapsedMs; }
        public void setSetupElapsedMs(long setupElapsedMs) { this.setupElapsedMs = setupElapsedMs; }
        public long getExampleElapsedMs() { return exampleElapsedMs; }
        public void setExampleElapsedMs(long exampleElapsedMs) { this.exampleElapsedMs = exampleElapsedMs; }
        public long getReaderElapsedMs() { return readerElapsedMs; }
        public void setReaderElapsedMs(long readerElapsedMs) { this.readerElapsedMs = readerElapsedMs; }
        public long getVerifyElapsedMs() { return verifyElapsedMs; }
        public void setVerifyElapsedMs(long verifyElapsedMs) { this.verifyElapsedMs = verifyElapsedMs; }
        public ExpectedMetrics getExpectedMetrics() { return expectedMetrics; }
        public void setExpectedMetrics(ExpectedMetrics expectedMetrics) { this.expectedMetrics = expectedMetrics; }
        public ComparisonResult getExampleResult() { return exampleResult; }
        public void setExampleResult(ComparisonResult exampleResult) { this.exampleResult = exampleResult; }
        public ComparisonResult getReaderResult() { return readerResult; }
        public void setReaderResult(ComparisonResult readerResult) { this.readerResult = readerResult; }
        public TableSnapshot getExampleTargetSnapshot() { return exampleTargetSnapshot; }
        public void setExampleTargetSnapshot(TableSnapshot exampleTargetSnapshot) { this.exampleTargetSnapshot = exampleTargetSnapshot; }
        public TableSnapshot getExampleReferenceSnapshot() { return exampleReferenceSnapshot; }
        public void setExampleReferenceSnapshot(TableSnapshot exampleReferenceSnapshot) { this.exampleReferenceSnapshot = exampleReferenceSnapshot; }
        public TableSnapshot getReaderTargetSnapshot() { return readerTargetSnapshot; }
        public void setReaderTargetSnapshot(TableSnapshot readerTargetSnapshot) { this.readerTargetSnapshot = readerTargetSnapshot; }
        public TableSnapshot getReaderReferenceSnapshot() { return readerReferenceSnapshot; }
        public void setReaderReferenceSnapshot(TableSnapshot readerReferenceSnapshot) { this.readerReferenceSnapshot = readerReferenceSnapshot; }
        public WriterOutputSnapshot getWriterOutputSnapshot() { return writerOutputSnapshot; }
        public void setWriterOutputSnapshot(WriterOutputSnapshot writerOutputSnapshot) { this.writerOutputSnapshot = writerOutputSnapshot; }
        public Path getJobConfigFile() { return jobConfigFile; }
        public void setJobConfigFile(Path jobConfigFile) { this.jobConfigFile = jobConfigFile; }
    }

    static final class ExpectedMetrics {
        private int totalRecords;
        private int consistentRecords;
        private int inconsistentRecords;
        private int resolvedRecords;
        private int insertCount;
        private int updateCount;
        private int deleteCount;
        private int finalRowCount;
        private Map<String, Integer> conflictTypeCounts = new LinkedHashMap<String, Integer>();
        private Set<Long> diffKeys = new LinkedHashSet<Long>();

        public int getTotalRecords() { return totalRecords; }
        public void setTotalRecords(int totalRecords) { this.totalRecords = totalRecords; }
        public int getConsistentRecords() { return consistentRecords; }
        public void setConsistentRecords(int consistentRecords) { this.consistentRecords = consistentRecords; }
        public int getInconsistentRecords() { return inconsistentRecords; }
        public void setInconsistentRecords(int inconsistentRecords) { this.inconsistentRecords = inconsistentRecords; }
        public int getResolvedRecords() { return resolvedRecords; }
        public void setResolvedRecords(int resolvedRecords) { this.resolvedRecords = resolvedRecords; }
        public int getInsertCount() { return insertCount; }
        public void setInsertCount(int insertCount) { this.insertCount = insertCount; }
        public int getUpdateCount() { return updateCount; }
        public void setUpdateCount(int updateCount) { this.updateCount = updateCount; }
        public int getDeleteCount() { return deleteCount; }
        public void setDeleteCount(int deleteCount) { this.deleteCount = deleteCount; }
        public int getFinalRowCount() { return finalRowCount; }
        public void setFinalRowCount(int finalRowCount) { this.finalRowCount = finalRowCount; }
        public Map<String, Integer> getConflictTypeCounts() { return conflictTypeCounts; }
        public void setConflictTypeCounts(Map<String, Integer> conflictTypeCounts) { this.conflictTypeCounts = conflictTypeCounts; }
        public Set<Long> getDiffKeys() { return diffKeys; }
        public void setDiffKeys(Set<Long> diffKeys) { this.diffKeys = diffKeys; }
    }

    static final class TableSnapshot {
        private AggregateResult aggregate;
        private Map<Long, List<String>> sampleRows = new LinkedHashMap<Long, List<String>>();
        private List<List<String>> orderedRows = new ArrayList<List<String>>();

        public AggregateResult getAggregate() { return aggregate; }
        public void setAggregate(AggregateResult aggregate) { this.aggregate = aggregate; }
        public Map<Long, List<String>> getSampleRows() { return sampleRows; }
        public void setSampleRows(Map<Long, List<String>> sampleRows) { this.sampleRows = sampleRows; }
        public List<List<String>> getOrderedRows() { return orderedRows; }
        public void setOrderedRows(List<List<String>> orderedRows) { this.orderedRows = orderedRows; }
    }

    static final class WriterOutputSnapshot {
        private int rowCount;
        private Map<String, Integer> conflictTypeCounts = new LinkedHashMap<String, Integer>();
        private Set<Long> diffKeys = new LinkedHashSet<Long>();
        private List<List<String>> rows = new ArrayList<List<String>>();

        public int getRowCount() { return rowCount; }
        public void setRowCount(int rowCount) { this.rowCount = rowCount; }
        public Map<String, Integer> getConflictTypeCounts() { return conflictTypeCounts; }
        public void setConflictTypeCounts(Map<String, Integer> conflictTypeCounts) { this.conflictTypeCounts = conflictTypeCounts; }
        public Set<Long> getDiffKeys() { return diffKeys; }
        public void setDiffKeys(Set<Long> diffKeys) { this.diffKeys = diffKeys; }
        public List<List<String>> getRows() { return rows; }
        public void setRows(List<List<String>> rows) { this.rows = rows; }
    }

    static final class AggregateResult {
        private String rowCount;
        private String totalAge;
        private String totalActiveFlag;
        private String totalSalary;
        private String totalBonusRate;

        public String getRowCount() { return rowCount; }
        public void setRowCount(String rowCount) { this.rowCount = rowCount; }
        public String getTotalAge() { return totalAge; }
        public void setTotalAge(String totalAge) { this.totalAge = totalAge; }
        public String getTotalActiveFlag() { return totalActiveFlag; }
        public void setTotalActiveFlag(String totalActiveFlag) { this.totalActiveFlag = totalActiveFlag; }
        public String getTotalSalary() { return totalSalary; }
        public void setTotalSalary(String totalSalary) { this.totalSalary = totalSalary; }
        public String getTotalBonusRate() { return totalBonusRate; }
        public void setTotalBonusRate(String totalBonusRate) { this.totalBonusRate = totalBonusRate; }
    }

    private static final class ScenarioContext {
        private String label;
        private int rowCount;
        private long setupElapsedMs;
        private Path scenarioRoot;
        private Path jobConfigFile;
        private ScenarioProfile profile;
        private Map<String, String> tableNames = new LinkedHashMap<String, String>();
        private Configuration connectionConfig;
        private BaseDataSourceDTO dataSourceDTO;

        public String getLabel() { return label; }
        public void setLabel(String label) { this.label = label; }
        public int getRowCount() { return rowCount; }
        public void setRowCount(int rowCount) { this.rowCount = rowCount; }
        public long getSetupElapsedMs() { return setupElapsedMs; }
        public void setSetupElapsedMs(long setupElapsedMs) { this.setupElapsedMs = setupElapsedMs; }
        public Path getScenarioRoot() { return scenarioRoot; }
        public void setScenarioRoot(Path scenarioRoot) { this.scenarioRoot = scenarioRoot; }
        public Path getJobConfigFile() { return jobConfigFile; }
        public void setJobConfigFile(Path jobConfigFile) { this.jobConfigFile = jobConfigFile; }
        public ScenarioProfile getProfile() { return profile; }
        public void setProfile(ScenarioProfile profile) { this.profile = profile; }
        public Map<String, String> getTableNames() { return tableNames; }
        public void setTableNames(Map<String, String> tableNames) { this.tableNames = tableNames; }
        public Configuration getConnectionConfig() { return connectionConfig; }
        public void setConnectionConfig(Configuration connectionConfig) { this.connectionConfig = connectionConfig; }
        public BaseDataSourceDTO getDataSourceDTO() { return dataSourceDTO; }
        public void setDataSourceDTO(BaseDataSourceDTO dataSourceDTO) { this.dataSourceDTO = dataSourceDTO; }
    }
}
