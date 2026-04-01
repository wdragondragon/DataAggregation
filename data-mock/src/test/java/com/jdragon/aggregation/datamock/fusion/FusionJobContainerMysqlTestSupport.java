package com.jdragon.aggregation.datamock.fusion;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.core.sortmerge.AdaptiveMergeConfig;
import com.jdragon.aggregation.core.sortmerge.SortMergeStats;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

final class FusionJobContainerMysqlTestSupport {

    static final String AGGREGATION_HOME = "C:\\dev\\ideaProject\\DataAggregation\\package_all\\aggregation";
    static final String SCENARIO_TAG = "job_container";
    static final List<String> TARGET_COLUMNS = Collections.unmodifiableList(Arrays.asList(
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
            "note_text",
            "salary_double",
            "age_bucket",
            "scenario_tag"
    ));

    private static final List<String> SOURCE_A_COLUMNS = Collections.unmodifiableList(Arrays.asList(
            "biz_id",
            "user_name",
            "age",
            "active_flag",
            "register_date",
            "last_login_at",
            "profile_text"
    ));

    private static final List<String> SOURCE_B_COLUMNS = Collections.unmodifiableList(Arrays.asList(
            "biz_id",
            "salary",
            "bonus_rate",
            "level_code",
            "note_text"
    ));
    private static final List<String> SOURCE_A_INSERT_COLUMNS = Collections.unmodifiableList(Arrays.asList(
            "row_id",
            "biz_id",
            "user_name",
            "age",
            "active_flag",
            "register_date",
            "last_login_at",
            "profile_text"
    ));
    private static final List<String> SOURCE_B_INSERT_COLUMNS = Collections.unmodifiableList(Arrays.asList(
            "row_id",
            "biz_id",
            "salary",
            "bonus_rate",
            "level_code",
            "note_text"
    ));

    private static final List<Long> SAMPLE_KEYS = Collections.unmodifiableList(Arrays.asList(
            1L, 2L, 3L, 10L, 50L, 100L, 999L, 1_000L, 9_999L, 10_000L, 99_999L, 100_000L, 999_999L, 1_000_000L
    ));

    private static final char[] LEVEL_CODES = new char[]{'A', 'B', 'C', 'D'};
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final LocalDate BASE_DATE = LocalDate.of(2024, 1, 1);
    private static final LocalDateTime BASE_DATE_TIME = LocalDateTime.of(2024, 1, 1, 8, 0, 0);
    private static final BigDecimal SALARY_BASE = new BigDecimal("1000.00");
    private static final BigDecimal SALARY_STEP = new BigDecimal("1.25");
    private static final BigDecimal BONUS_BASE = new BigDecimal("0.50");
    private static final BigDecimal BONUS_STEP = new BigDecimal("0.05");
    private static final int INSERT_CHUNK_SIZE = 5_000;
    private static final int WRITER_BATCH_SIZE = 2_000;
    private static final Mysql8SourcePlugin MYSQL_PLUGIN = new Mysql8SourcePlugin();

    private FusionJobContainerMysqlTestSupport() {
    }

    static ScenarioResult runScenario(String label,
                                      int rowCount,
                                      Path scenarioRoot,
                                      boolean keepArtifacts) throws Exception {
        return runScenario(label, rowCount, scenarioRoot, keepArtifacts, ScenarioOptions.defaults());
    }

    static ScenarioResult runScenario(String label,
                                      int rowCount,
                                      Path scenarioRoot,
                                      boolean keepArtifacts,
                                      ScenarioOptions options) throws Exception {
        ensureAggregationHome();
        cleanupPath(scenarioRoot);
        Files.createDirectories(scenarioRoot);

        ScenarioContext scenario = null;
        try {
            scenario = prepareScenario(label, rowCount, scenarioRoot, options != null ? options : ScenarioOptions.defaults());

            long jobStart = System.nanoTime();
            JobContainer container = new JobContainer(Configuration.from(scenario.getJobConfigFile().toFile()));
            container.start();
            long jobElapsedMs = elapsedMillis(jobStart);

            long verifyStart = System.nanoTime();
            ActualOutput actualOutput = readActualOutput(scenario, rowCount <= 100);
            long verifyElapsedMs = elapsedMillis(verifyStart);

            ScenarioResult result = new ScenarioResult();
            result.setLabel(label);
            result.setRowCount(rowCount);
            result.setSetupElapsedMs(scenario.getSetupElapsedMs());
            result.setJobElapsedMs(jobElapsedMs);
            result.setVerifyElapsedMs(verifyElapsedMs);
            result.setExpectedMetrics(buildExpectedMetrics(rowCount));
            result.setActualOutput(actualOutput);
            result.setJobConfigFile(scenario.getJobConfigFile());
            result.setSortMergeStats(extractSortMergeStats(container));
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

    static FailureResult runScenarioExpectingFailure(String label,
                                                     int rowCount,
                                                     Path scenarioRoot,
                                                     boolean keepArtifacts,
                                                     ScenarioOptions options) throws Exception {
        ensureAggregationHome();
        cleanupPath(scenarioRoot);
        Files.createDirectories(scenarioRoot);

        ScenarioContext scenario = null;
        try {
            scenario = prepareScenario(label, rowCount, scenarioRoot, options != null ? options : ScenarioOptions.defaults());

            JobContainer container = new JobContainer(Configuration.from(scenario.getJobConfigFile().toFile()));
            Throwable failure = null;
            try {
                container.start();
            } catch (Throwable throwable) {
                failure = throwable;
            }
            if (failure == null) {
                throw new AssertionError("Expected JobContainer scenario to fail but it succeeded: " + label);
            }

            FailureResult result = new FailureResult();
            result.setLabel(label);
            result.setRowCount(rowCount);
            result.setSetupElapsedMs(scenario.getSetupElapsedMs());
            result.setJobConfigFile(scenario.getJobConfigFile());
            result.setFailure(failure);
            result.setFailureMessage(rootMessage(failure));
            result.setSortMergeStats(extractSortMergeStats(container));
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
        return Paths.get("target", "fusion-jobcontainer-benchmark", child);
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
                    } catch (IOException e) {
                        // Best-effort cleanup on Windows: Configuration.from(File)
                        // may keep the config file handle open until GC.
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
        ensureReaderPluginAvailable("fusion");
        Path pluginPath = Paths.get(AGGREGATION_HOME, "plugin", "writer", "mysql8writer", "plugin.json");
        if (!Files.exists(pluginPath)) {
            throw new IllegalStateException("mysql8 writer plugin not found under aggregation.home: " + pluginPath);
        }
    }

    private static void ensureReaderPluginAvailable(String pluginName) {
        String pluginDirectoryName = pluginName + "reader";
        Path pluginHome = Paths.get(AGGREGATION_HOME, "plugin", "reader", pluginDirectoryName);
        List<Path> candidates = Arrays.asList(
                Paths.get("..", "job-plugins", "reader", "fusionreader", "target", "aggregation-bin", "plugin", "reader", pluginDirectoryName).normalize(),
                Paths.get("job-plugins", "reader", "fusionreader", "target", "aggregation-bin", "plugin", "reader", pluginDirectoryName).normalize()
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
                                                   ScenarioOptions options) throws Exception {
        ScenarioContext scenario = new ScenarioContext();
        scenario.setLabel(label);
        scenario.setRowCount(rowCount);
        scenario.setScenarioRoot(scenarioRoot);
        scenario.setScenarioOptions(options);
        scenario.setTableNames(buildTableNames(label));
        scenario.setConnectionConfig(loadMysqlConnectionConfig());
        scenario.setDataSourceDTO(buildDataSourceDTO(scenario.getConnectionConfig()));

        long setupStart = System.nanoTime();
        createTables(scenario);
        populateTables(scenario, rowCount);
        Path jobConfigFile = buildJobConfigFile(scenario);
        scenario.setJobConfigFile(jobConfigFile);
        scenario.setSetupElapsedMs(elapsedMillis(setupStart));
        return scenario;
    }

    private static Map<String, String> buildTableNames(String label) {
        String sanitized = label.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
        String suffix = String.valueOf(System.currentTimeMillis() % 1_000_000L);
        Map<String, String> tableNames = new LinkedHashMap<String, String>();
        tableNames.put("sourceA", "fjc_" + sanitized + "_a_" + suffix);
        tableNames.put("sourceB", "fjc_" + sanitized + "_b_" + suffix);
        tableNames.put("target", "fjc_" + sanitized + "_t_" + suffix);
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
        AbstractDataSourcePlugin plugin = MYSQL_PLUGIN;
        BaseDataSourceDTO dto = scenario.getDataSourceDTO();
        for (String tableName : scenario.getTableNames().values()) {
            plugin.executeUpdate(dto, "DROP TABLE IF EXISTS " + tableName);
        }

        plugin.executeUpdate(dto,
                "CREATE TABLE " + scenario.getTableNames().get("sourceA") + " ("
                        + "row_id BIGINT PRIMARY KEY,"
                        + "biz_id BIGINT NOT NULL,"
                        + "user_name VARCHAR(64) NOT NULL,"
                        + "age INT NOT NULL,"
                        + "active_flag TINYINT NOT NULL,"
                        + "register_date DATE NOT NULL,"
                        + "last_login_at DATETIME NOT NULL,"
                        + "profile_text TEXT NOT NULL,"
                        + "KEY idx_biz_id (biz_id)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

        plugin.executeUpdate(dto,
                "CREATE TABLE " + scenario.getTableNames().get("sourceB") + " ("
                        + "row_id BIGINT PRIMARY KEY,"
                        + "biz_id BIGINT NOT NULL,"
                        + "salary DECIMAL(18,2) NOT NULL,"
                        + "bonus_rate DOUBLE NOT NULL,"
                        + "level_code CHAR(1) NOT NULL,"
                        + "note_text TEXT NOT NULL,"
                        + "KEY idx_biz_id (biz_id)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

        plugin.executeUpdate(dto,
                "CREATE TABLE " + scenario.getTableNames().get("target") + " ("
                        + "biz_id BIGINT PRIMARY KEY,"
                        + "user_name VARCHAR(64) NOT NULL,"
                        + "age INT NOT NULL,"
                        + "active_flag TINYINT NOT NULL,"
                        + "register_date VARCHAR(32) NOT NULL,"
                        + "last_login_at VARCHAR(32) NOT NULL,"
                        + "profile_text TEXT NOT NULL,"
                        + "salary DECIMAL(18,2) NOT NULL,"
                        + "bonus_rate DOUBLE NOT NULL,"
                        + "level_code CHAR(1) NOT NULL,"
                        + "note_text TEXT NOT NULL,"
                        + "salary_double DOUBLE NOT NULL,"
                        + "age_bucket VARCHAR(16) NOT NULL,"
                        + "scenario_tag VARCHAR(32) NOT NULL"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    }

    private static void populateTables(ScenarioContext scenario, int rowCount) {
        ScenarioOptions options = scenario.getScenarioOptions();
        insertSourceAData(
                scenario.getDataSourceDTO(),
                scenario.getTableNames().get("sourceA"),
                buildSourceOrder("sourceA", rowCount, options)
        );
        insertSourceBData(
                scenario.getDataSourceDTO(),
                scenario.getTableNames().get("sourceB"),
                buildSourceOrder("sourceB", rowCount, options)
        );
    }

    private static void insertSourceAData(BaseDataSourceDTO dto, String tableName, List<Long> sourceOrder) {
        List<List<String>> batch = new ArrayList<List<String>>(INSERT_CHUNK_SIZE);
        long rowId = 1L;
        for (Long bizId : sourceOrder) {
            batch.add(buildSourceARow(rowId++, bizId.longValue()));
            if (batch.size() >= INSERT_CHUNK_SIZE) {
                flushInsertBatch(dto, tableName, SOURCE_A_INSERT_COLUMNS, batch);
                batch = new ArrayList<List<String>>(INSERT_CHUNK_SIZE);
            }
        }
        if (!batch.isEmpty()) {
            flushInsertBatch(dto, tableName, SOURCE_A_INSERT_COLUMNS, batch);
        }
    }

    private static void insertSourceBData(BaseDataSourceDTO dto, String tableName, List<Long> sourceOrder) {
        List<List<String>> batch = new ArrayList<List<String>>(INSERT_CHUNK_SIZE);
        long rowId = 1L;
        for (Long bizId : sourceOrder) {
            batch.add(buildSourceBRow(rowId++, bizId.longValue()));
            if (batch.size() >= INSERT_CHUNK_SIZE) {
                flushInsertBatch(dto, tableName, SOURCE_B_INSERT_COLUMNS, batch);
                batch = new ArrayList<List<String>>(INSERT_CHUNK_SIZE);
            }
        }
        if (!batch.isEmpty()) {
            flushInsertBatch(dto, tableName, SOURCE_B_INSERT_COLUMNS, batch);
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

    private static List<String> buildSourceARow(long rowId, long bizId) {
        return Arrays.asList(
                Long.toString(rowId),
                Long.toString(bizId),
                userName(bizId),
                Integer.toString(age(bizId)),
                Integer.toString(activeFlag(bizId)),
                registerDate(bizId),
                lastLoginAt(bizId),
                profileText(bizId)
        );
    }

    private static List<String> buildSourceBRow(long rowId, long bizId) {
        return Arrays.asList(
                Long.toString(rowId),
                Long.toString(bizId),
                decimalText(salary(bizId)),
                decimalText(bonusRate(bizId)),
                levelCode(bizId),
                noteText(bizId)
        );
    }

    private static Path buildJobConfigFile(ScenarioContext scenario) throws IOException {
        Configuration config = Configuration.from(resolveDemoConfigFile());
        config.set("reader.type", "fusion");
        config.set("reader.config.sources", buildSourceConfigs(scenario));
        config.set("reader.config.join.keys", Collections.singletonList("biz_id"));
        config.set("reader.config.join.type", scenario.getScenarioOptions().getJoinType().name());
        config.set("reader.config.fieldMappings", buildFieldMappings());
        config.set("reader.config.errorMode", "LENIENT");
        config.set("reader.config.detailConfig.enabled", false);
        config.set("reader.config.detailConfig.savePath",
                scenario.getScenarioRoot().resolve("fusion_details").toAbsolutePath().toString());
        config.set("writer.type", "mysql8");
        config.set("writer.config.connect", scenario.getConnectionConfig().clone());
        config.set("writer.config.table", scenario.getTableNames().get("target"));
        config.set("writer.config.columns", TARGET_COLUMNS);
        config.set("writer.config.writeMode", "insert");
        config.set("writer.config.batchSize", WRITER_BATCH_SIZE);
        applyScenarioOptions(config, scenario);

        Path jobConfigFile = scenario.getScenarioRoot().resolve("fusion-job.json");
        Files.write(jobConfigFile, config.beautify().getBytes(StandardCharsets.UTF_8));
        return jobConfigFile;
    }

    private static List<Object> buildSourceConfigs(ScenarioContext scenario) {
        List<Object> sources = new ArrayList<Object>();
        sources.add(buildSourceConfig("sourceA", "profile_source", scenario.getConnectionConfig(), scenario.getTableNames().get("sourceA"),
                SOURCE_A_COLUMNS, 1.0D, 10, 0.95D));
        sources.add(buildSourceConfig("sourceB", "salary_source", scenario.getConnectionConfig(), scenario.getTableNames().get("sourceB"),
                SOURCE_B_COLUMNS, 1.2D, 8, 0.90D));
        return sources;
    }

    private static Map<String, Object> buildSourceConfig(String id,
                                                         String name,
                                                         Configuration connectionConfig,
                                                         String tableName,
                                                         List<String> columns,
                                                         double weight,
                                                         int priority,
                                                         double confidence) {
        Map<String, Object> source = new LinkedHashMap<String, Object>();
        source.put("id", id);
        source.put("name", name);
        source.put("type", "mysql8");
        source.put("config", configurationAsMap(connectionConfig.clone()));
        source.put("table", tableName);
        source.put("columns", new ArrayList<String>(columns));
        source.put("querySql", buildSourceQuerySql(tableName, columns));
        source.put("weight", weight);
        source.put("priority", priority);
        source.put("confidence", confidence);
        source.put("fieldMappings", new LinkedHashMap<String, String>());
        return source;
    }

    private static List<Object> buildFieldMappings() {
        List<Object> fieldMappings = new ArrayList<Object>();
        fieldMappings.add(directMapping("sourceA.biz_id", "biz_id"));
        fieldMappings.add(directMapping("sourceA.user_name", "user_name"));
        fieldMappings.add(directMapping("sourceA.age", "age"));
        fieldMappings.add(directMapping("sourceA.active_flag", "active_flag"));
        fieldMappings.add(directMapping("sourceA.register_date", "register_date"));
        fieldMappings.add(directMapping("sourceA.last_login_at", "last_login_at"));
        fieldMappings.add(directMapping("sourceA.profile_text", "profile_text"));
        fieldMappings.add(directMapping("sourceB.salary", "salary"));
        fieldMappings.add(directMapping("sourceB.bonus_rate", "bonus_rate"));
        fieldMappings.add(directMapping("sourceB.level_code", "level_code"));
        fieldMappings.add(directMapping("sourceB.note_text", "note_text"));
        fieldMappings.add(expressionMapping(
                "TO_NUMBER(${sourceB.salary, Double}) * 2",
                "salary_double",
                "DOUBLE"
        ));
        fieldMappings.add(conditionalMapping(
                "TO_NUMBER(${sourceA.age}) >= 40",
                "\"senior\"",
                "\"junior\"",
                "STRING",
                "age_bucket"
        ));
        fieldMappings.add(constantMapping(SCENARIO_TAG, "STRING", "scenario_tag"));
        return fieldMappings;
    }

    private static Map<String, Object> directMapping(String sourceField, String targetField) {
        Map<String, Object> mapping = new LinkedHashMap<String, Object>();
        mapping.put("type", "DIRECT");
        mapping.put("sourceField", sourceField);
        mapping.put("targetField", targetField);
        return mapping;
    }

    private static Map<String, Object> expressionMapping(String expression, String targetField, String resultType) {
        Map<String, Object> mapping = new LinkedHashMap<String, Object>();
        mapping.put("type", "EXPRESSION");
        mapping.put("expression", expression);
        mapping.put("targetField", targetField);
        mapping.put("resultType", resultType);
        return mapping;
    }

    private static Map<String, Object> conditionalMapping(String condition,
                                                          String trueValue,
                                                          String falseValue,
                                                          String resultType,
                                                          String targetField) {
        Map<String, Object> mapping = new LinkedHashMap<String, Object>();
        mapping.put("type", "CONDITIONAL");
        mapping.put("condition", condition);
        mapping.put("trueValue", trueValue);
        mapping.put("falseValue", falseValue);
        mapping.put("resultType", resultType);
        mapping.put("targetField", targetField);
        return mapping;
    }

    private static Map<String, Object> constantMapping(String value, String resultType, String targetField) {
        Map<String, Object> mapping = new LinkedHashMap<String, Object>();
        mapping.put("type", "CONSTANT");
        mapping.put("value", value);
        mapping.put("resultType", resultType);
        mapping.put("targetField", targetField);
        return mapping;
    }

    private static Map<String, Object> configurationAsMap(Configuration configuration) {
        Map<String, Object> internal = configuration.getMap("");
        if (internal == null) {
            return new HashMap<String, Object>();
        }
        return new LinkedHashMap<String, Object>(internal);
    }

    private static void applyScenarioOptions(Configuration config, ScenarioContext scenario) {
        ScenarioOptions options = scenario.getScenarioOptions();
        config.set("reader.config.cache.partitionCount", options.getPartitionCount());
        config.set("reader.config.cache.rebalancePartitionMultiplier", options.getRebalancePartitionMultiplier());
        config.set("reader.config.performance.parallelSourceCount", options.getParallelSourceCount());
        config.set("reader.config.performance.memoryLimitMB", options.getMemoryLimitMB());
        config.set("reader.config.adaptiveMerge.enabled", true);
        config.set("reader.config.adaptiveMerge.pendingKeyThreshold", options.getPendingKeyThreshold());
        config.set("reader.config.adaptiveMerge.pendingMemoryMB", options.getPendingMemoryMB());
        config.set("reader.config.adaptiveMerge.overflowPartitionCount", options.getOverflowPartitionCount());
        config.set("reader.config.adaptiveMerge.rebalancePartitionMultiplier", options.getRebalancePartitionMultiplier());
        config.set("reader.config.adaptiveMerge.overflowSpillPath",
                scenario.getScenarioRoot().resolve("fusion_spill").toAbsolutePath().toString());
        config.set("reader.config.adaptiveMerge.preferOrderedQuery", options.isPreferOrderedQuery());
        config.set("reader.config.adaptiveMerge.maxSpillBytesMB", options.getMaxSpillBytesMB());
        config.set("reader.config.adaptiveMerge.minFreeDiskMB", options.getMinFreeDiskMB());
    }

    private static String buildSourceQuerySql(String tableName, List<String> columns) {
        return "select " + String.join(",", columns) + " from " + tableName + " order by row_id";
    }

    private static List<Long> buildSourceOrder(String sourceId, int rowCount, ScenarioOptions options) {
        List<Long> order = new ArrayList<Long>(rowCount);
        for (long bizId = 1L; bizId <= rowCount; bizId++) {
            order.add(bizId);
        }
        SourceOrderPlan plan = options.getSourceOrderPlans().get(sourceId);
        if (plan == null) {
            return order;
        }
        if (plan.getPattern() == SourceOrderPattern.SPARSE_ADJACENT_SWAP) {
            int interval = Math.max(2, plan.getParameter());
            for (int index = interval - 1; index < order.size(); index += interval) {
                Collections.swap(order, index - 1, index);
            }
            return order;
        }
        if (plan.getPattern() == SourceOrderPattern.REVERSE_WINDOW) {
            int windowSize = Math.max(2, plan.getParameter());
            for (int start = 0; start < order.size(); start += windowSize) {
                int endExclusive = Math.min(order.size(), start + windowSize);
                Collections.reverse(order.subList(start, endExclusive));
            }
        }
        return order;
    }

    private static SortMergeStats extractSortMergeStats(JobContainer container) {
        if (container == null || container.getJobPointReporter() == null) {
            return null;
        }
        Communication communication = container.getJobPointReporter().getTrackCommunication();
        if (communication == null || communication.getMessage() == null) {
            return null;
        }
        List<String> summaries = communication.getMessage().get("fusion_sortmerge_summary");
        if (summaries == null || summaries.isEmpty()) {
            return null;
        }
        return JSONObject.parseObject(summaries.get(summaries.size() - 1), SortMergeStats.class);
    }

    private static String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current != null && current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current == null ? null : current.getMessage();
    }

    private static ActualOutput readActualOutput(ScenarioContext scenario, boolean loadAllRows) {
        ActualOutput output = new ActualOutput();
        output.setAggregate(queryAggregate(scenario));
        output.setSampleRows(querySampleRows(scenario, scenario.getRowCount()));
        if (loadAllRows) {
            output.setOrderedRows(queryAllRows(scenario));
        }
        return output;
    }

    private static AggregateResult queryAggregate(ScenarioContext scenario) {
        String tableName = scenario.getTableNames().get("target");
        String sql = "SELECT "
                + "CAST(COUNT(*) AS CHAR) AS row_count, "
                + "CAST(SUM(age) AS CHAR) AS total_age, "
                + "CAST(SUM(active_flag) AS CHAR) AS total_active_flag, "
                + decimalSql("SUM(salary)", "total_salary") + ", "
                + decimalSql("ROUND(SUM(salary_double), 2)", "total_salary_double") + ", "
                + "CAST(SUM(CASE WHEN age_bucket = 'senior' THEN 1 ELSE 0 END) AS CHAR) AS senior_count, "
                + "CAST(COUNT(DISTINCT scenario_tag) AS CHAR) AS scenario_tag_count, "
                + "MIN(scenario_tag) AS scenario_tag "
                + "FROM " + tableName;
        Table<Map<String, Object>> table = MYSQL_PLUGIN.executeQuerySql(scenario.getDataSourceDTO(), sql, true);
        Map<String, Object> row = table.getBodies().get(0);

        AggregateResult result = new AggregateResult();
        result.setRowCount(asString(row.get("row_count")));
        result.setTotalAge(asString(row.get("total_age")));
        result.setTotalActiveFlag(asString(row.get("total_active_flag")));
        result.setTotalSalary(asString(row.get("total_salary")));
        result.setTotalSalaryDouble(asString(row.get("total_salary_double")));
        result.setSeniorCount(asString(row.get("senior_count")));
        result.setScenarioTagCount(asString(row.get("scenario_tag_count")));
        result.setScenarioTag(asString(row.get("scenario_tag")));
        return result;
    }

    private static Map<Long, List<String>> querySampleRows(ScenarioContext scenario, int rowCount) {
        List<Long> keys = filteredSampleKeys(rowCount);
        if (keys.isEmpty()) {
            return new LinkedHashMap<Long, List<String>>();
        }
        StringBuilder sql = new StringBuilder();
        sql.append(buildOutputQuery(scenario.getTableNames().get("target")));
        sql.append(" WHERE biz_id IN (");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(keys.get(i));
        }
        sql.append(") ORDER BY CAST(biz_id AS UNSIGNED)");

        List<List<String>> rows = queryOutputRows(scenario, sql.toString());
        Map<Long, List<String>> sampleRows = new LinkedHashMap<Long, List<String>>();
        for (List<String> row : rows) {
            sampleRows.put(Long.valueOf(row.get(0)), row);
        }
        return sampleRows;
    }

    private static List<List<String>> queryAllRows(ScenarioContext scenario) {
        String sql = buildOutputQuery(scenario.getTableNames().get("target"))
                + " ORDER BY CAST(biz_id AS UNSIGNED)";
        return queryOutputRows(scenario, sql);
    }

    private static List<List<String>> queryOutputRows(ScenarioContext scenario, String sql) {
        Table<Map<String, Object>> table = MYSQL_PLUGIN.executeQuerySql(scenario.getDataSourceDTO(), sql, true);
        List<List<String>> rows = new ArrayList<List<String>>();
        for (Map<String, Object> row : table.getBodies()) {
            List<String> values = new ArrayList<String>(TARGET_COLUMNS.size());
            for (String column : TARGET_COLUMNS) {
                values.add(asString(row.get(column)));
            }
            rows.add(values);
        }
        return rows;
    }

    private static String buildOutputQuery(String tableName) {
        return "SELECT "
                + "CAST(biz_id AS CHAR) AS biz_id, "
                + "user_name AS user_name, "
                + "CAST(age AS CHAR) AS age, "
                + "CAST(active_flag AS CHAR) AS active_flag, "
                + "register_date AS register_date, "
                + "last_login_at AS last_login_at, "
                + "profile_text AS profile_text, "
                + decimalSql("salary", "salary") + ", "
                + decimalSql("ROUND(bonus_rate, 2)", "bonus_rate") + ", "
                + "level_code AS level_code, "
                + "note_text AS note_text, "
                + decimalSql("ROUND(salary_double, 2)", "salary_double") + ", "
                + "age_bucket AS age_bucket, "
                + "scenario_tag AS scenario_tag "
                + "FROM " + tableName;
    }

    private static String decimalSql(String expression, String alias) {
        return "CASE WHEN " + expression + " IS NULL THEN NULL "
                + "ELSE TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM CAST(" + expression + " AS CHAR))) "
                + "END AS " + alias;
    }

    private static ExpectedMetrics buildExpectedMetrics(int rowCount) {
        ExpectedMetrics metrics = new ExpectedMetrics();
        metrics.setRowCount(rowCount);
        metrics.setAggregate(buildExpectedAggregate(rowCount));
        metrics.setSampleRows(buildExpectedSampleRows(rowCount));
        if (rowCount <= 100) {
            metrics.setOrderedRows(buildExpectedRows(rowCount));
        }
        return metrics;
    }

    private static AggregateResult buildExpectedAggregate(int rowCount) {
        long totalAge = 0L;
        long totalActiveFlag = 0L;
        long seniorCount = 0L;
        BigDecimal totalSalary = BigDecimal.ZERO;
        BigDecimal totalSalaryDouble = BigDecimal.ZERO;

        for (long bizId = 1L; bizId <= rowCount; bizId++) {
            totalAge += age(bizId);
            totalActiveFlag += activeFlag(bizId);
            if (age(bizId) >= 40) {
                seniorCount++;
            }
            BigDecimal currentSalary = salary(bizId);
            totalSalary = totalSalary.add(currentSalary);
            totalSalaryDouble = totalSalaryDouble.add(currentSalary.multiply(new BigDecimal("2")));
        }

        AggregateResult result = new AggregateResult();
        result.setRowCount(Integer.toString(rowCount));
        result.setTotalAge(Long.toString(totalAge));
        result.setTotalActiveFlag(Long.toString(totalActiveFlag));
        result.setTotalSalary(decimalText(totalSalary));
        result.setTotalSalaryDouble(decimalText(totalSalaryDouble));
        result.setSeniorCount(Long.toString(seniorCount));
        result.setScenarioTagCount("1");
        result.setScenarioTag(SCENARIO_TAG);
        return result;
    }

    private static Map<Long, List<String>> buildExpectedSampleRows(int rowCount) {
        Map<Long, List<String>> rows = new LinkedHashMap<Long, List<String>>();
        for (Long key : filteredSampleKeys(rowCount)) {
            rows.put(key, expectedRow(key.longValue()));
        }
        return rows;
    }

    private static List<List<String>> buildExpectedRows(int rowCount) {
        List<List<String>> rows = new ArrayList<List<String>>(rowCount);
        for (long bizId = 1L; bizId <= rowCount; bizId++) {
            rows.add(expectedRow(bizId));
        }
        return rows;
    }

    private static List<Long> filteredSampleKeys(int rowCount) {
        List<Long> keys = new ArrayList<Long>();
        for (Long sampleKey : SAMPLE_KEYS) {
            if (sampleKey.longValue() <= rowCount) {
                keys.add(sampleKey);
            }
        }
        return keys;
    }

    private static List<String> expectedRow(long bizId) {
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
                noteText(bizId),
                decimalText(salary(bizId).multiply(new BigDecimal("2"))),
                age(bizId) >= 40 ? "senior" : "junior",
                SCENARIO_TAG
        );
    }

    private static void dropTables(ScenarioContext scenario) {
        AbstractDataSourcePlugin plugin = MYSQL_PLUGIN;
        BaseDataSourceDTO dto = scenario.getDataSourceDTO();
        for (String tableName : scenario.getTableNames().values()) {
            try {
                plugin.executeUpdate(dto, "DROP TABLE IF EXISTS " + tableName);
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
        return BASE_DATE.plusDays((bizId - 1L) % 365L).format(DATE_FORMAT) + " 00:00:00.000";
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

    static final class ScenarioResult {
        private String label;
        private int rowCount;
        private long setupElapsedMs;
        private long jobElapsedMs;
        private long verifyElapsedMs;
        private ExpectedMetrics expectedMetrics;
        private ActualOutput actualOutput;
        private Path jobConfigFile;
        private SortMergeStats sortMergeStats;

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public int getRowCount() {
            return rowCount;
        }

        public void setRowCount(int rowCount) {
            this.rowCount = rowCount;
        }

        public long getSetupElapsedMs() {
            return setupElapsedMs;
        }

        public void setSetupElapsedMs(long setupElapsedMs) {
            this.setupElapsedMs = setupElapsedMs;
        }

        public long getJobElapsedMs() {
            return jobElapsedMs;
        }

        public void setJobElapsedMs(long jobElapsedMs) {
            this.jobElapsedMs = jobElapsedMs;
        }

        public long getVerifyElapsedMs() {
            return verifyElapsedMs;
        }

        public void setVerifyElapsedMs(long verifyElapsedMs) {
            this.verifyElapsedMs = verifyElapsedMs;
        }

        public ExpectedMetrics getExpectedMetrics() {
            return expectedMetrics;
        }

        public void setExpectedMetrics(ExpectedMetrics expectedMetrics) {
            this.expectedMetrics = expectedMetrics;
        }

        public ActualOutput getActualOutput() {
            return actualOutput;
        }

        public void setActualOutput(ActualOutput actualOutput) {
            this.actualOutput = actualOutput;
        }

        public Path getJobConfigFile() {
            return jobConfigFile;
        }

        public void setJobConfigFile(Path jobConfigFile) {
            this.jobConfigFile = jobConfigFile;
        }

        public SortMergeStats getSortMergeStats() {
            return sortMergeStats;
        }

        public void setSortMergeStats(SortMergeStats sortMergeStats) {
            this.sortMergeStats = sortMergeStats;
        }
    }

    static final class FailureResult {
        private String label;
        private int rowCount;
        private long setupElapsedMs;
        private Path jobConfigFile;
        private Throwable failure;
        private String failureMessage;
        private SortMergeStats sortMergeStats;

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public int getRowCount() {
            return rowCount;
        }

        public void setRowCount(int rowCount) {
            this.rowCount = rowCount;
        }

        public long getSetupElapsedMs() {
            return setupElapsedMs;
        }

        public void setSetupElapsedMs(long setupElapsedMs) {
            this.setupElapsedMs = setupElapsedMs;
        }

        public Path getJobConfigFile() {
            return jobConfigFile;
        }

        public void setJobConfigFile(Path jobConfigFile) {
            this.jobConfigFile = jobConfigFile;
        }

        public Throwable getFailure() {
            return failure;
        }

        public void setFailure(Throwable failure) {
            this.failure = failure;
        }

        public String getFailureMessage() {
            return failureMessage;
        }

        public void setFailureMessage(String failureMessage) {
            this.failureMessage = failureMessage;
        }

        public SortMergeStats getSortMergeStats() {
            return sortMergeStats;
        }

        public void setSortMergeStats(SortMergeStats sortMergeStats) {
            this.sortMergeStats = sortMergeStats;
        }

        public boolean containsMessage(String fragment) {
            if (fragment == null || failure == null) {
                return false;
            }
            Throwable current = failure;
            while (current != null) {
                if (current.getMessage() != null && current.getMessage().contains(fragment)) {
                    return true;
                }
                current = current.getCause();
            }
            return false;
        }
    }

    static final class ExpectedMetrics {
        private int rowCount;
        private AggregateResult aggregate;
        private Map<Long, List<String>> sampleRows = new LinkedHashMap<Long, List<String>>();
        private List<List<String>> orderedRows = new ArrayList<List<String>>();

        public int getRowCount() {
            return rowCount;
        }

        public void setRowCount(int rowCount) {
            this.rowCount = rowCount;
        }

        public AggregateResult getAggregate() {
            return aggregate;
        }

        public void setAggregate(AggregateResult aggregate) {
            this.aggregate = aggregate;
        }

        public Map<Long, List<String>> getSampleRows() {
            return sampleRows;
        }

        public void setSampleRows(Map<Long, List<String>> sampleRows) {
            this.sampleRows = sampleRows;
        }

        public List<List<String>> getOrderedRows() {
            return orderedRows;
        }

        public void setOrderedRows(List<List<String>> orderedRows) {
            this.orderedRows = orderedRows;
        }
    }

    static final class ActualOutput {
        private AggregateResult aggregate;
        private Map<Long, List<String>> sampleRows = new LinkedHashMap<Long, List<String>>();
        private List<List<String>> orderedRows = new ArrayList<List<String>>();

        public AggregateResult getAggregate() {
            return aggregate;
        }

        public void setAggregate(AggregateResult aggregate) {
            this.aggregate = aggregate;
        }

        public Map<Long, List<String>> getSampleRows() {
            return sampleRows;
        }

        public void setSampleRows(Map<Long, List<String>> sampleRows) {
            this.sampleRows = sampleRows;
        }

        public List<List<String>> getOrderedRows() {
            return orderedRows;
        }

        public void setOrderedRows(List<List<String>> orderedRows) {
            this.orderedRows = orderedRows;
        }
    }

    static final class AggregateResult {
        private String rowCount;
        private String totalAge;
        private String totalActiveFlag;
        private String totalSalary;
        private String totalSalaryDouble;
        private String seniorCount;
        private String scenarioTagCount;
        private String scenarioTag;

        public String getRowCount() {
            return rowCount;
        }

        public void setRowCount(String rowCount) {
            this.rowCount = rowCount;
        }

        public String getTotalAge() {
            return totalAge;
        }

        public void setTotalAge(String totalAge) {
            this.totalAge = totalAge;
        }

        public String getTotalActiveFlag() {
            return totalActiveFlag;
        }

        public void setTotalActiveFlag(String totalActiveFlag) {
            this.totalActiveFlag = totalActiveFlag;
        }

        public String getTotalSalary() {
            return totalSalary;
        }

        public void setTotalSalary(String totalSalary) {
            this.totalSalary = totalSalary;
        }

        public String getTotalSalaryDouble() {
            return totalSalaryDouble;
        }

        public void setTotalSalaryDouble(String totalSalaryDouble) {
            this.totalSalaryDouble = totalSalaryDouble;
        }

        public String getSeniorCount() {
            return seniorCount;
        }

        public void setSeniorCount(String seniorCount) {
            this.seniorCount = seniorCount;
        }

        public String getScenarioTagCount() {
            return scenarioTagCount;
        }

        public void setScenarioTagCount(String scenarioTagCount) {
            this.scenarioTagCount = scenarioTagCount;
        }

        public String getScenarioTag() {
            return scenarioTag;
        }

        public void setScenarioTag(String scenarioTag) {
            this.scenarioTag = scenarioTag;
        }
    }

    private static final class ScenarioContext {
        private String label;
        private int rowCount;
        private long setupElapsedMs;
        private Path scenarioRoot;
        private Path jobConfigFile;
        private Map<String, String> tableNames = new LinkedHashMap<String, String>();
        private Configuration connectionConfig;
        private BaseDataSourceDTO dataSourceDTO;
        private ScenarioOptions scenarioOptions;

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public int getRowCount() {
            return rowCount;
        }

        public void setRowCount(int rowCount) {
            this.rowCount = rowCount;
        }

        public long getSetupElapsedMs() {
            return setupElapsedMs;
        }

        public void setSetupElapsedMs(long setupElapsedMs) {
            this.setupElapsedMs = setupElapsedMs;
        }

        public Path getScenarioRoot() {
            return scenarioRoot;
        }

        public void setScenarioRoot(Path scenarioRoot) {
            this.scenarioRoot = scenarioRoot;
        }

        public Path getJobConfigFile() {
            return jobConfigFile;
        }

        public void setJobConfigFile(Path jobConfigFile) {
            this.jobConfigFile = jobConfigFile;
        }

        public Map<String, String> getTableNames() {
            return tableNames;
        }

        public void setTableNames(Map<String, String> tableNames) {
            this.tableNames = tableNames;
        }

        public Configuration getConnectionConfig() {
            return connectionConfig;
        }

        public void setConnectionConfig(Configuration connectionConfig) {
            this.connectionConfig = connectionConfig;
        }

        public BaseDataSourceDTO getDataSourceDTO() {
            return dataSourceDTO;
        }

        public void setDataSourceDTO(BaseDataSourceDTO dataSourceDTO) {
            this.dataSourceDTO = dataSourceDTO;
        }

        public ScenarioOptions getScenarioOptions() {
            return scenarioOptions;
        }

        public void setScenarioOptions(ScenarioOptions scenarioOptions) {
            this.scenarioOptions = scenarioOptions;
        }
    }

    enum SourceOrderPattern {
        SPARSE_ADJACENT_SWAP,
        REVERSE_WINDOW
    }

    static final class SourceOrderPlan {
        private final SourceOrderPattern pattern;
        private final int parameter;

        private SourceOrderPlan(SourceOrderPattern pattern, int parameter) {
            this.pattern = pattern;
            this.parameter = parameter;
        }

        public SourceOrderPattern getPattern() {
            return pattern;
        }

        public int getParameter() {
            return parameter;
        }
    }

    static final class ScenarioOptions {
        private int partitionCount = 10;
        private int rebalancePartitionMultiplier = 4;
        private int parallelSourceCount = 2;
        private int memoryLimitMB = 512;
        private int pendingKeyThreshold = 4096;
        private int pendingMemoryMB = 256;
        private int overflowPartitionCount = 10;
        private boolean preferOrderedQuery = true;
        private int maxSpillBytesMB = 512;
        private int minFreeDiskMB = 256;
        private FusionConfig.JoinType joinType = FusionConfig.JoinType.LEFT;
        private final Map<String, SourceOrderPlan> sourceOrderPlans =
                new LinkedHashMap<String, SourceOrderPlan>();

        static ScenarioOptions defaults() {
            return new ScenarioOptions();
        }

        ScenarioOptions withPartitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        ScenarioOptions withRebalancePartitionMultiplier(int rebalancePartitionMultiplier) {
            this.rebalancePartitionMultiplier = rebalancePartitionMultiplier;
            return this;
        }

        ScenarioOptions withParallelSourceCount(int parallelSourceCount) {
            this.parallelSourceCount = parallelSourceCount;
            return this;
        }

        ScenarioOptions withMemoryLimitMB(int memoryLimitMB) {
            this.memoryLimitMB = memoryLimitMB;
            return this;
        }

        ScenarioOptions withPendingKeyThreshold(int pendingKeyThreshold) {
            this.pendingKeyThreshold = pendingKeyThreshold;
            return this;
        }

        ScenarioOptions withPendingMemoryMB(int pendingMemoryMB) {
            this.pendingMemoryMB = pendingMemoryMB;
            return this;
        }

        ScenarioOptions withOverflowPartitionCount(int overflowPartitionCount) {
            this.overflowPartitionCount = overflowPartitionCount;
            return this;
        }

        ScenarioOptions withPreferOrderedQuery(boolean preferOrderedQuery) {
            this.preferOrderedQuery = preferOrderedQuery;
            return this;
        }

        ScenarioOptions withMaxSpillBytesMB(int maxSpillBytesMB) {
            this.maxSpillBytesMB = maxSpillBytesMB;
            return this;
        }

        ScenarioOptions withMinFreeDiskMB(int minFreeDiskMB) {
            this.minFreeDiskMB = minFreeDiskMB;
            return this;
        }

        ScenarioOptions withJoinType(FusionConfig.JoinType joinType) {
            this.joinType = joinType == null ? FusionConfig.JoinType.LEFT : joinType;
            return this;
        }

        ScenarioOptions enableSparseOutOfOrder(String sourceId, int interval) {
            this.sourceOrderPlans.put(sourceId, new SourceOrderPlan(SourceOrderPattern.SPARSE_ADJACENT_SWAP, interval));
            return this;
        }

        ScenarioOptions enableWindowReverseDisorder(String sourceId, int windowSize) {
            this.sourceOrderPlans.put(sourceId, new SourceOrderPlan(SourceOrderPattern.REVERSE_WINDOW, windowSize));
            return this;
        }

        int getPartitionCount() {
            return partitionCount;
        }

        int getRebalancePartitionMultiplier() {
            return rebalancePartitionMultiplier;
        }

        int getParallelSourceCount() {
            return parallelSourceCount;
        }

        int getMemoryLimitMB() {
            return memoryLimitMB;
        }

        int getPendingKeyThreshold() {
            return pendingKeyThreshold;
        }

        int getPendingMemoryMB() {
            return pendingMemoryMB;
        }

        int getOverflowPartitionCount() {
            return overflowPartitionCount;
        }

        boolean isPreferOrderedQuery() {
            return preferOrderedQuery;
        }

        int getMaxSpillBytesMB() {
            return maxSpillBytesMB;
        }

        int getMinFreeDiskMB() {
            return minFreeDiskMB;
        }

        FusionConfig.JoinType getJoinType() {
            return joinType;
        }

        Map<String, SourceOrderPlan> getSourceOrderPlans() {
            return sourceOrderPlans;
        }
    }
}
