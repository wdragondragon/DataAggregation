package com.jdragon.aggregation.datamock.sortmerge;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import com.jdragon.aggregation.core.consistency.service.AdaptiveSortMergeConsistencyExecutor;
import com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager;
import com.jdragon.aggregation.core.consistency.service.ResultRecorder;
import com.jdragon.aggregation.core.fusion.AdaptiveSortMergeFusionExecutor;
import com.jdragon.aggregation.core.fusion.FusionContext;
import com.jdragon.aggregation.core.fusion.config.DirectFieldMapping;
import com.jdragon.aggregation.core.fusion.config.FieldMapping;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.core.fusion.config.FusionDetailConfig;
import com.jdragon.aggregation.core.fusion.config.SourceConfig;
import com.jdragon.aggregation.core.fusion.strategy.FusionStrategyFactory;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.sortmerge.AdaptiveMergeConfig;
import com.jdragon.aggregation.core.sortmerge.OrderedKeyType;
import com.jdragon.aggregation.core.sortmerge.SortMergeStats;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.mysql8.Mysql8SourcePlugin;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

final class AdaptiveSortMergeTestSupport {

    static final String TEST_PLUGIN_NAME = "mysql8";
    static final List<Long> SAMPLE_KEYS = Arrays.asList(
            1L, 50L, 70L, 90L, 120L, 125L, 135L,
            333L, 350L, 500L,
            9_999L, 10_000L,
            99_999L, 100_000L,
            999_950L, 999_990L, 1_000_000L
    );

    private static final String[] DEPARTMENTS = {
            "engineering", "finance", "marketing", "sales", "support", "ops"
    };
    private static final String[] STATUSES = {
            "NEW", "ACTIVE", "HOLD", "CLOSED"
    };
    private static final BigInteger DIGEST_MOD = BigInteger.ONE.shiftLeft(256);

    private static final Mysql8SourcePlugin MYSQL_PLUGIN = new Mysql8SourcePlugin();

    private AdaptiveSortMergeTestSupport() {
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
        cleanupPath(scenarioRoot);
        Files.createDirectories(scenarioRoot);

        DatasetContext dataset = null;
        try {
            dataset = createDataset(label, rowCount, scenarioRoot, options != null ? options : ScenarioOptions.defaults());
            DatasetExpectations expectations = calculateExpectations(rowCount);
            ConsistencyExecution consistencyExecution = executeConsistency(dataset);
            FusionExecution fusionExecution = executeFusion(dataset);

            ScenarioResult result = new ScenarioResult();
            result.setLabel(label);
            result.setRowCount(rowCount);
            result.setSetupElapsedMs(dataset.getSetupElapsedMs());
            result.setExpectations(expectations);
            result.setConsistencyExecution(consistencyExecution);
            result.setFusionExecution(fusionExecution);
            return result;
        } finally {
            if (dataset != null) {
                dropTables(dataset);
            }
            if (!keepArtifacts) {
                cleanupPath(scenarioRoot);
            }
        }
    }

    static Path benchmarkRoot(String child) {
        return Paths.get("target", "sortmerge-benchmark", child);
    }

    static Path reportPath(String fileName) {
        return Paths.get("validation-reports", fileName);
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
                        throw new RuntimeException("Failed to delete " + current, e);
                    }
                });
    }

    private static DatasetContext createDataset(String label,
                                                int rowCount,
                                                Path scenarioRoot,
                                                ScenarioOptions options) throws Exception {
        DatasetContext dataset = new DatasetContext();
        dataset.setLabel(label);
        dataset.setRowCount(rowCount);
        dataset.setScenarioRoot(scenarioRoot);
        dataset.setScenarioOptions(options);
        dataset.setTableNames(buildTableNames(label));
        dataset.setConnectionConfig(loadMysqlConnectionConfig());
        dataset.setBaseDataSourceDTO(buildBaseDataSourceDTO(dataset.getConnectionConfig()));

        long start = System.nanoTime();
        try (Connection connection = openConnection(dataset)) {
            createTables(connection, dataset.getTableNames());
            populateTables(connection, dataset.getTableNames(), rowCount);
        }
        dataset.setSetupElapsedMs(elapsedMillis(start));
        return dataset;
    }

    private static Map<String, String> buildTableNames(String label) {
        String sanitized = label.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
        Map<String, String> tableNames = new LinkedHashMap<String, String>();
        tableNames.put("sourceA", "sm_" + sanitized + "_a");
        tableNames.put("sourceB", "sm_" + sanitized + "_b");
        tableNames.put("sourceC", "sm_" + sanitized + "_c");
        return tableNames;
    }

    private static Configuration loadMysqlConnectionConfig() {
        File configFile = resolveDemoConfigFile();
        Configuration root = Configuration.from(configFile);
        List<Configuration> sources = root.getListConfiguration("reader.config.sources");
        if (sources == null || sources.isEmpty()) {
            throw new IllegalStateException("reader.config.sources is empty in fusion-mysql-demo.json");
        }
        Configuration connect = sources.get(0).getConfiguration("config");
        if (connect == null) {
            throw new IllegalStateException("Missing reader.config.sources[0].config in fusion-mysql-demo.json");
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

    private static BaseDataSourceDTO buildBaseDataSourceDTO(Configuration connectionConfig) {
        DataSourceConfig dataSourceConfig = new DataSourceConfig();
        dataSourceConfig.setPluginName(TEST_PLUGIN_NAME);
        dataSourceConfig.setConnectionConfig(connectionConfig.clone());
        return DataSourcePluginManager.createDataSourceDTO(dataSourceConfig);
    }

    private static Connection openConnection(DatasetContext dataset) {
        return MYSQL_PLUGIN.getConnection(dataset.getBaseDataSourceDTO());
    }

    private static void createTables(Connection connection, Map<String, String> tableNames) throws SQLException {
        for (String tableName : tableNames.values()) {
            executeSql(connection, "DROP TABLE IF EXISTS " + tableName);
            executeSql(connection, "CREATE TABLE " + tableName + " ("
                    + "row_id BIGINT PRIMARY KEY,"
                    + "biz_id BIGINT NOT NULL,"
                    + "fusion_name VARCHAR(128),"
                    + "age INT,"
                    + "salary DECIMAL(18,2),"
                    + "department VARCHAR(64),"
                    + "status VARCHAR(32),"
                    + "updated_at DATETIME,"
                    + "KEY idx_" + tableName + "_biz_id (biz_id)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        }
    }

    private static void dropTables(DatasetContext dataset) {
        try (Connection connection = openConnection(dataset)) {
            for (String tableName : dataset.getTableNames().values()) {
                executeSql(connection, "DROP TABLE IF EXISTS " + tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to drop benchmark tables", e);
        }
    }

    private static void executeSql(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void populateTables(Connection connection,
                                       Map<String, String> tableNames,
                                       int rowCount) throws SQLException {
        connection.setAutoCommit(false);

        String insertSql = "INSERT INTO %s "
                + "(row_id, biz_id, fusion_name, age, salary, department, status, updated_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        long rowIdA = 1L;
        long rowIdB = 1L;
        long rowIdC = 1L;

        try (PreparedStatement insertA = connection.prepareStatement(String.format(insertSql, tableNames.get("sourceA")));
             PreparedStatement insertB = connection.prepareStatement(String.format(insertSql, tableNames.get("sourceB")));
             PreparedStatement insertC = connection.prepareStatement(String.format(insertSql, tableNames.get("sourceC")))) {

            int batchCount = 0;
            for (long bizId = 1; bizId <= rowCount; bizId++) {
                bindRow(insertA, rowIdA++, baseRow("A", bizId));

                if (!isSourceBMissing(bizId)) {
                    RowData rowB = sourceBRow(bizId);
                    bindRow(insertB, rowIdB++, rowB);
                    if (hasSourceBDuplicate(bizId)) {
                        bindRow(insertB, rowIdB++, rowB);
                    }
                }

                if (!isSourceCMissing(bizId)) {
                    RowData rowC = sourceCRow(bizId);
                    bindRow(insertC, rowIdC++, rowC);
                    if (hasSourceCDuplicate(bizId)) {
                        bindRow(insertC, rowIdC++, rowC);
                    }
                }

                batchCount++;
                if (batchCount % 5000 == 0) {
                    executeBatches(insertA, insertB, insertC, connection);
                }
            }
            executeBatches(insertA, insertB, insertC, connection);
        }
        connection.setAutoCommit(true);
    }

    private static void executeBatches(PreparedStatement insertA,
                                       PreparedStatement insertB,
                                       PreparedStatement insertC,
                                       Connection connection) throws SQLException {
        insertA.executeBatch();
        insertB.executeBatch();
        insertC.executeBatch();
        connection.commit();
    }

    private static void bindRow(PreparedStatement statement, long rowId, RowData row) throws SQLException {
        statement.setLong(1, rowId);
        statement.setLong(2, row.getBizId());
        statement.setString(3, row.getFusionName());
        statement.setInt(4, row.getAge());
        statement.setBigDecimal(5, row.getSalary());
        statement.setString(6, row.getDepartment());
        statement.setString(7, row.getStatus());
        statement.setTimestamp(8, row.getUpdatedAt());
        statement.addBatch();
    }

    private static ConsistencyExecution executeConsistency(DatasetContext dataset) {
        MysqlTestDataSourcePluginManager pluginManager = new MysqlTestDataSourcePluginManager();
        ComparisonResult comparisonResult;
        long start = System.nanoTime();
        HeapUsageSampler sampler = new HeapUsageSampler();
        sampler.start();
        try {
            AdaptiveSortMergeConsistencyExecutor executor =
                    new AdaptiveSortMergeConsistencyExecutor(pluginManager, new NoOpResultRecorder());
            comparisonResult = executor.execute(buildConsistencyRule(dataset));
        } finally {
            sampler.stop();
        }

        ConsistencyExecution execution = new ConsistencyExecution();
        execution.setElapsedMs(elapsedMillis(start));
        execution.setPeakHeapBytes(sampler.getPeakHeapBytes());
        execution.setComparisonResult(comparisonResult);
        execution.setExecutionEngine(comparisonResult.getExecutionEngine());
        execution.setMergeResolvedKeyCount(getLongSummary(comparisonResult, "mergeResolvedKeyCount"));
        execution.setMergeSpilledKeyCount(getLongSummary(comparisonResult, "mergeSpilledKeyCount"));
        execution.setDuplicateIgnoredCount(getLongSummary(comparisonResult, "duplicateIgnoredCount"));
        execution.setLocalReorderedGroupCount(getLongSummary(comparisonResult, "localReorderedGroupCount"));
        execution.setOrderRecoveryCount(getLongSummary(comparisonResult, "orderRecoveryCount"));
        execution.setSpillBytes(getLongSummary(comparisonResult, "spillBytes"));
        execution.setSpillGuardTriggered(getBooleanSummary(comparisonResult, "spillGuardTriggered"));
        execution.setSpillGuardReason(getStringSummary(comparisonResult, "spillGuardReason"));
        execution.setFallbackReason(getStringSummary(comparisonResult, "fallbackReason"));
        execution.setErrorMessage(getStringSummary(comparisonResult, "error"));
        return execution;
    }

    private static FusionExecution executeFusion(DatasetContext dataset) throws Exception {
        MysqlTestDataSourcePluginManager pluginManager = new MysqlTestDataSourcePluginManager();
        FusionBundle fusionBundle = buildFusionBundle(dataset);
        CollectingRecordSender recordSender = new CollectingRecordSender(
                fusionBundle.getTargetColumns(),
                SAMPLE_KEYS,
                dataset.getRowCount() <= 5_000
        );

        FusionStrategyFactory.initDefaultStrategies();
        HeapUsageSampler sampler = new HeapUsageSampler();
        long start = System.nanoTime();
        sampler.start();
        SortMergeStats stats = null;
        String errorMessage = null;
        try {
            AdaptiveSortMergeFusionExecutor executor = new AdaptiveSortMergeFusionExecutor(
                    pluginManager,
                    fusionBundle.getFusionConfig(),
                    fusionBundle.getFusionContext()
            );
            stats = executor.execute(recordSender);
        } catch (Exception e) {
            errorMessage = e.getMessage();
        } finally {
            sampler.stop();
        }

        FusionExecution execution = new FusionExecution();
        execution.setElapsedMs(elapsedMillis(start));
        execution.setPeakHeapBytes(sampler.getPeakHeapBytes());
        execution.setOutputCount(recordSender.getOutputCount());
        execution.setDigest(recordSender.getDigestHex());
        execution.setContentDigest(recordSender.getContentDigestHex());
        execution.setSampleRows(recordSender.getSampleRows());
        execution.setFirstOutputKeys(recordSender.getFirstOutputKeys());
        execution.setOrderedRows(recordSender.getOrderedRows());
        execution.setOutputOrdered(recordSender.isOutputOrdered());
        execution.setOrderViolationSample(recordSender.getOrderViolationSample());
        execution.setStats(stats);
        execution.setErrorMessage(errorMessage);
        return execution;
    }

    private static ConsistencyRule buildConsistencyRule(DatasetContext dataset) {
        ConsistencyRule rule = new ConsistencyRule();
        rule.setRuleId("adaptive-sortmerge-consistency-" + dataset.getLabel());
        rule.setRuleName("adaptive-sortmerge-consistency-" + dataset.getLabel());
        rule.setEnabled(true);
        rule.setParallelFetch(true);
        rule.setConflictResolutionStrategy(ConflictResolutionStrategy.NO_RESOLVE);
        rule.setToleranceThreshold(0.0);
        rule.setMatchKeys(Collections.singletonList("biz_id"));
        rule.setCompareFields(Arrays.asList("age", "salary", "department", "status"));
        rule.setDataSources(Arrays.asList(
                buildConsistencySource(dataset, "sourceA", dataset.getTableNames().get("sourceA"), dataset.getConnectionConfig(), 1.0, 10),
                buildConsistencySource(dataset, "sourceB", dataset.getTableNames().get("sourceB"), dataset.getConnectionConfig(), 0.9, 30),
                buildConsistencySource(dataset, "sourceC", dataset.getTableNames().get("sourceC"), dataset.getConnectionConfig(), 0.8, 20)
        ));

        OutputConfig outputConfig = new OutputConfig();
        outputConfig.setGenerateReport(false);
        rule.setOutputConfig(outputConfig);

        ConsistencyRule.StreamCacheConfig cacheConfig = new ConsistencyRule.StreamCacheConfig();
        cacheConfig.setPartitionCount(8);
        cacheConfig.setSpillPath(dataset.getScenarioRoot().resolve("consistency-spill").toAbsolutePath().normalize().toString());
        rule.setCacheConfig(cacheConfig);

        ConsistencyRule.StreamPerformanceConfig performanceConfig = new ConsistencyRule.StreamPerformanceConfig();
        performanceConfig.setParallelSourceCount(3);
        performanceConfig.setMemoryLimitMB(256);
        rule.setPerformanceConfig(performanceConfig);

        rule.setAdaptiveMergeConfig(buildAdaptiveMergeConfig(
                dataset,
                dataset.getScenarioRoot().resolve("consistency-overflow").toAbsolutePath().normalize().toString()
        ));
        return rule;
    }

    private static DataSourceConfig buildConsistencySource(DatasetContext dataset,
                                                           String sourceId,
                                                           String tableName,
                                                           Configuration connectionConfig,
                                                           double confidence,
                                                           int priority) {
        DataSourceConfig sourceConfig = new DataSourceConfig();
        sourceConfig.setSourceId(sourceId);
        sourceConfig.setSourceName(sourceId);
        sourceConfig.setPluginName(TEST_PLUGIN_NAME);
        sourceConfig.setConnectionConfig(connectionConfig.clone());
        sourceConfig.setQuerySql(buildSourceQuery(dataset, sourceId, tableName));
        sourceConfig.setConfidenceWeight(confidence);
        sourceConfig.setPriority(priority);
        sourceConfig.setExtConfig(buildScanExtConfig());
        return sourceConfig;
    }

    private static FusionBundle buildFusionBundle(DatasetContext dataset) {
        FusionConfig fusionConfig = new FusionConfig();
        fusionConfig.setJoinType(FusionConfig.JoinType.LEFT);
        fusionConfig.setJoinKeys(Collections.singletonList("biz_id"));
        fusionConfig.setDefaultStrategy("PRIORITY");
        fusionConfig.setSources(Arrays.asList(
                buildFusionSource(dataset, "sourceA", dataset.getTableNames().get("sourceA"), dataset.getConnectionConfig(), 1.0, 10),
                buildFusionSource(dataset, "sourceB", dataset.getTableNames().get("sourceB"), dataset.getConnectionConfig(), 0.9, 30),
                buildFusionSource(dataset, "sourceC", dataset.getTableNames().get("sourceC"), dataset.getConnectionConfig(), 0.8, 20)
        ));

        List<FieldMapping> fieldMappings = new ArrayList<FieldMapping>();
        fieldMappings.add(directMapping("biz_id", "${sourceA.biz_id}", null));
        fieldMappings.add(directMapping("chosen_name", "${fusion_name}", "PRIORITY"));
        fieldMappings.add(directMapping("age", "${age}", "PRIORITY"));
        fieldMappings.add(directMapping("salary", "${salary}", "PRIORITY"));
        fieldMappings.add(directMapping("department", "${department}", "PRIORITY"));
        fieldMappings.add(directMapping("status", "${status}", "PRIORITY"));
        fusionConfig.setFieldMappings(fieldMappings);

        FusionConfig.CacheConfig cacheConfig = new FusionConfig.CacheConfig();
        cacheConfig.setPartitionCount(8);
        fusionConfig.setCacheConfig(cacheConfig);

        FusionConfig.PerformanceConfig performanceConfig = new FusionConfig.PerformanceConfig();
        performanceConfig.setParallelSourceCount(3);
        performanceConfig.setMemoryLimitMB(256);
        fusionConfig.setPerformanceConfig(performanceConfig);

        fusionConfig.setAdaptiveMergeConfig(buildAdaptiveMergeConfig(
                dataset,
                dataset.getScenarioRoot().resolve("fusion-overflow").toAbsolutePath().normalize().toString()
        ));

        FusionDetailConfig detailConfig = new FusionDetailConfig();
        detailConfig.setEnabled(false);
        fusionConfig.setDetailConfig(detailConfig);

        FusionContext fusionContext = new FusionContext(fusionConfig);
        List<String> targetColumns = Arrays.asList("biz_id", "chosen_name", "age", "salary", "department", "status");
        fusionContext.setTargetColumns(targetColumns);
        fusionContext.updateIncrValue();
        return new FusionBundle(fusionConfig, fusionContext, targetColumns);
    }

    private static SourceConfig buildFusionSource(DatasetContext dataset,
                                                  String sourceId,
                                                  String tableName,
                                                  Configuration connectionConfig,
                                                  double confidence,
                                                  int priority) {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setSourceId(sourceId);
        sourceConfig.setSourceName(sourceId);
        sourceConfig.setPluginType(TEST_PLUGIN_NAME);
        sourceConfig.setPluginConfig(connectionConfig.clone());
        sourceConfig.setQuerySql(buildSourceQuery(dataset, sourceId, tableName));
        sourceConfig.setConfidence(confidence);
        sourceConfig.setWeight(confidence);
        sourceConfig.setPriority(priority);
        sourceConfig.setExtConfig(buildScanExtConfig());
        return sourceConfig;
    }

    private static AdaptiveMergeConfig buildAdaptiveMergeConfig(DatasetContext dataset, String overflowSpillPath) {
        ScenarioOptions options = dataset.getScenarioOptions() != null
                ? dataset.getScenarioOptions()
                : ScenarioOptions.defaults();

        AdaptiveMergeConfig adaptiveMergeConfig = new AdaptiveMergeConfig();
        adaptiveMergeConfig.setEnabled(true);
        adaptiveMergeConfig.setPendingKeyThreshold(options.getPendingKeyThreshold() != null
                ? options.getPendingKeyThreshold()
                : 2048);
        adaptiveMergeConfig.setPendingMemoryMB(options.getPendingMemoryMB() != null
                ? options.getPendingMemoryMB()
                : 64);
        adaptiveMergeConfig.setOverflowPartitionCount(options.getOverflowPartitionCount() != null
                ? options.getOverflowPartitionCount()
                : 8);
        adaptiveMergeConfig.setOverflowSpillPath(overflowSpillPath);
        adaptiveMergeConfig.setPreferOrderedQuery(options.getPreferOrderedQuery() == null || options.getPreferOrderedQuery());
        adaptiveMergeConfig.setValidateSourceOrder(options.getValidateSourceOrder() == null || options.getValidateSourceOrder());
        adaptiveMergeConfig.setLocalDisorderEnabled(options.getLocalDisorderEnabled() == null || options.getLocalDisorderEnabled());
        if (options.getLocalDisorderMaxGroups() != null) {
            adaptiveMergeConfig.setLocalDisorderMaxGroups(options.getLocalDisorderMaxGroups());
        }
        if (options.getLocalDisorderMaxMemoryMB() != null) {
            adaptiveMergeConfig.setLocalDisorderMaxMemoryMB(options.getLocalDisorderMaxMemoryMB());
        }
        if (options.getMaxSpillBytesMB() != null) {
            adaptiveMergeConfig.setMaxSpillBytesMB(options.getMaxSpillBytesMB());
        }
        if (options.getMinFreeDiskMB() != null) {
            adaptiveMergeConfig.setMinFreeDiskMB(options.getMinFreeDiskMB());
        }
        if (options.getOnOrderViolation() != null) {
            adaptiveMergeConfig.setOnOrderViolation(options.getOnOrderViolation());
        }
        if (options.getOnMemoryExceeded() != null) {
            adaptiveMergeConfig.setOnMemoryExceeded(options.getOnMemoryExceeded());
        }
        adaptiveMergeConfig.getKeyTypes().put("biz_id", OrderedKeyType.NUMBER);
        return adaptiveMergeConfig;
    }

    private static String buildSourceQuery(DatasetContext dataset, String sourceId, String tableName) {
        ScenarioOptions options = dataset.getScenarioOptions();
        if (options != null && options.hasSparseLocalDisorder(sourceId)) {
            return buildSparseLocalDisorderQuery(tableName, options.getSparseLocalDisorderEvery());
        }
        return "SELECT biz_id, fusion_name, age, salary, department, status, updated_at FROM " + tableName;
    }

    private static String buildSparseLocalDisorderQuery(String tableName, long disorderEvery) {
        long effectiveDisorderEvery = Math.max(2L, disorderEvery);
        return "SELECT biz_id, fusion_name, age, salary, department, status, updated_at FROM " + tableName
                + " ORDER BY CASE"
                + " WHEN MOD(biz_id, " + effectiveDisorderEvery + ") = 0 THEN biz_id + 1"
                + " WHEN MOD(biz_id, " + effectiveDisorderEvery + ") = 1 THEN biz_id - 1"
                + " ELSE biz_id END, biz_id";
    }

    private static DirectFieldMapping directMapping(String targetField, String sourceField, String strategy) {
        DirectFieldMapping mapping = new DirectFieldMapping();
        mapping.setMappingType(FieldMapping.MappingType.DIRECT);
        mapping.setTargetField(targetField);
        mapping.setSourceField(sourceField);
        mapping.setFusionStrategy(strategy);
        return mapping;
    }

    private static Configuration buildScanExtConfig() {
        Configuration extConfig = Configuration.newDefault();
        extConfig.set("fetchSize", "1000");
        return extConfig;
    }

    static DatasetExpectations calculateExpectations(int rowCount) {
        DatasetExpectations expectations = new DatasetExpectations();
        expectations.setTotalKeys(rowCount);
        expectations.setFusionOutputCount(rowCount);

        long inconsistentKeys = 0L;
        long duplicateIgnoredCount = 0L;
        MessageDigest digest = newDigest();
        BigInteger contentDigest = BigInteger.ZERO;
        Map<Long, List<String>> sampleRows = new LinkedHashMap<Long, List<String>>();
        List<List<String>> orderedRows = rowCount <= 5_000 ? new ArrayList<List<String>>(rowCount) : Collections.<List<String>>emptyList();
        Set<Long> sampleSet = new LinkedHashSet<Long>(SAMPLE_KEYS);

        for (long bizId = 1; bizId <= rowCount; bizId++) {
            boolean inconsistent = isSourceBMissing(bizId)
                    || isSourceCMissing(bizId)
                    || hasSourceBAgeDifference(bizId)
                    || hasSourceBSalaryDifference(bizId)
                    || hasSourceCDepartmentDifference(bizId)
                    || hasSourceCStatusDifference(bizId);
            if (inconsistent) {
                inconsistentKeys++;
            }

            if (hasSourceBDuplicate(bizId)) {
                duplicateIgnoredCount++;
            }
            if (hasSourceCDuplicate(bizId)) {
                duplicateIgnoredCount++;
            }

            RowData chosenRow = chooseFusionRow(bizId);
            List<String> values = toFusionValues(chosenRow);
            updateDigest(digest, values);
            contentDigest = updateContentDigest(contentDigest, values);
            if (rowCount <= 5_000) {
                orderedRows.add(values);
            }
            if (sampleSet.contains(bizId)) {
                sampleRows.put(bizId, values);
            }
        }

        expectations.setExpectedInconsistentKeys(inconsistentKeys);
        expectations.setExpectedDuplicateIgnoredCount(duplicateIgnoredCount);
        expectations.setExpectedFusionDigest(toHex(digest.digest()));
        expectations.setExpectedFusionContentDigest(toHex(contentDigest));
        expectations.setExpectedFusionSamples(sampleRows);
        expectations.setExpectedOrderedRows(orderedRows);
        return expectations;
    }

    private static RowData chooseFusionRow(long bizId) {
        if (!isSourceBMissing(bizId)) {
            return sourceBRow(bizId);
        }
        if (!isSourceCMissing(bizId)) {
            return sourceCRow(bizId);
        }
        return baseRow("A", bizId);
    }

    private static List<String> toFusionValues(RowData row) {
        return Arrays.asList(
                String.valueOf(row.getBizId()),
                row.getFusionName(),
                String.valueOf(row.getAge()),
                normalizeDecimal(row.getSalary()),
                row.getDepartment(),
                row.getStatus()
        );
    }

    private static String normalizeDecimal(BigDecimal value) {
        if (value == null) {
            return "null";
        }
        return String.valueOf(value.doubleValue());
    }

    private static RowData baseRow(String sourceTag, long bizId) {
        RowData row = new RowData();
        row.setBizId(bizId);
        row.setFusionName(sourceTag + "_name_" + String.format("%07d", bizId));
        row.setAge(20 + (int) (bizId % 37));
        row.setSalary(BigDecimal.valueOf(1_000_000L + bizId * 17L, 2));
        row.setDepartment(DEPARTMENTS[(int) (bizId % DEPARTMENTS.length)]);
        row.setStatus(STATUSES[(int) (bizId % STATUSES.length)]);
        row.setUpdatedAt(Timestamp.valueOf(LocalDateTime.of(2026, 1, 1, 0, 0).plusSeconds(bizId)));
        return row;
    }

    private static RowData sourceBRow(long bizId) {
        RowData row = baseRow("B", bizId);
        if (hasSourceBAgeDifference(bizId)) {
            row.setAge(row.getAge() + 1);
        }
        if (hasSourceBSalaryDifference(bizId)) {
            row.setSalary(row.getSalary().add(BigDecimal.valueOf(2500L, 2)));
        }
        return row;
    }

    private static RowData sourceCRow(long bizId) {
        RowData row = baseRow("C", bizId);
        if (hasSourceCDepartmentDifference(bizId)) {
            row.setDepartment("alt_" + row.getDepartment());
        }
        if (hasSourceCStatusDifference(bizId)) {
            row.setStatus("ARCHIVED");
        }
        return row;
    }

    private static boolean isSourceBMissing(long bizId) {
        return bizId % 50 == 0;
    }

    private static boolean isSourceCMissing(long bizId) {
        return bizId % 70 == 0;
    }

    private static boolean hasSourceBAgeDifference(long bizId) {
        return !isSourceBMissing(bizId) && bizId % 90 == 0;
    }

    private static boolean hasSourceBSalaryDifference(long bizId) {
        return !isSourceBMissing(bizId) && bizId % 125 == 0;
    }

    private static boolean hasSourceCDepartmentDifference(long bizId) {
        return !isSourceCMissing(bizId) && bizId % 120 == 0;
    }

    private static boolean hasSourceCStatusDifference(long bizId) {
        return !isSourceCMissing(bizId) && bizId % 135 == 0;
    }

    private static boolean hasSourceBDuplicate(long bizId) {
        return !isSourceBMissing(bizId) && bizId % 333 == 0;
    }

    private static boolean hasSourceCDuplicate(long bizId) {
        return !isSourceCMissing(bizId) && bizId % 500 == 0;
    }

    private static long elapsedMillis(long startNano) {
        return (System.nanoTime() - startNano) / 1_000_000L;
    }

    private static long getLongSummary(ComparisonResult result, String key) {
        if (result == null || result.getSummary() == null) {
            return 0L;
        }
        Object value = result.getSummary().get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String && !((String) value).trim().isEmpty()) {
            return Long.parseLong((String) value);
        }
        return 0L;
    }

    private static String getStringSummary(ComparisonResult result, String key) {
        if (result == null || result.getSummary() == null) {
            return null;
        }
        Object value = result.getSummary().get(key);
        return value == null ? null : String.valueOf(value);
    }

    private static boolean getBooleanSummary(ComparisonResult result, String key) {
        if (result == null || result.getSummary() == null) {
            return false;
        }
        Object value = result.getSummary().get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return value != null && Boolean.parseBoolean(String.valueOf(value));
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static void updateDigest(MessageDigest digest, List<String> values) {
        String joined = String.join("|", values) + "\n";
        digest.update(joined.getBytes(StandardCharsets.UTF_8));
    }

    private static BigInteger updateContentDigest(BigInteger current, List<String> values) {
        MessageDigest rowDigest = newDigest();
        updateDigest(rowDigest, values);
        return current.add(new BigInteger(1, rowDigest.digest())).mod(DIGEST_MOD);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte current : bytes) {
            builder.append(String.format("%02x", current));
        }
        return builder.toString();
    }

    private static String toHex(BigInteger value) {
        String hex = value.toString(16);
        StringBuilder builder = new StringBuilder(64);
        for (int index = hex.length(); index < 64; index++) {
            builder.append('0');
        }
        builder.append(hex);
        return builder.toString();
    }

    static final class ScenarioOptions {
        private Integer pendingKeyThreshold;
        private Integer pendingMemoryMB;
        private Integer overflowPartitionCount;
        private Boolean preferOrderedQuery;
        private Boolean validateSourceOrder;
        private Boolean localDisorderEnabled;
        private Integer localDisorderMaxGroups;
        private Integer localDisorderMaxMemoryMB;
        private Integer maxSpillBytesMB;
        private Integer minFreeDiskMB;
        private AdaptiveMergeConfig.OrderViolationAction onOrderViolation;
        private AdaptiveMergeConfig.MemoryExceededAction onMemoryExceeded;
        private final Set<String> sparseLocalDisorderSources = new LinkedHashSet<String>();
        private long sparseLocalDisorderEvery = 128L;

        static ScenarioOptions defaults() {
            return new ScenarioOptions();
        }

        ScenarioOptions withPendingKeyThreshold(Integer pendingKeyThreshold) {
            this.pendingKeyThreshold = pendingKeyThreshold;
            return this;
        }

        ScenarioOptions withPendingMemoryMB(Integer pendingMemoryMB) {
            this.pendingMemoryMB = pendingMemoryMB;
            return this;
        }

        ScenarioOptions withOverflowPartitionCount(Integer overflowPartitionCount) {
            this.overflowPartitionCount = overflowPartitionCount;
            return this;
        }

        ScenarioOptions withPreferOrderedQuery(Boolean preferOrderedQuery) {
            this.preferOrderedQuery = preferOrderedQuery;
            return this;
        }

        ScenarioOptions withValidateSourceOrder(Boolean validateSourceOrder) {
            this.validateSourceOrder = validateSourceOrder;
            return this;
        }

        ScenarioOptions withLocalDisorderEnabled(Boolean localDisorderEnabled) {
            this.localDisorderEnabled = localDisorderEnabled;
            return this;
        }

        ScenarioOptions withLocalDisorderMaxGroups(Integer localDisorderMaxGroups) {
            this.localDisorderMaxGroups = localDisorderMaxGroups;
            return this;
        }

        ScenarioOptions withLocalDisorderMaxMemoryMB(Integer localDisorderMaxMemoryMB) {
            this.localDisorderMaxMemoryMB = localDisorderMaxMemoryMB;
            return this;
        }

        ScenarioOptions withMaxSpillBytesMB(Integer maxSpillBytesMB) {
            this.maxSpillBytesMB = maxSpillBytesMB;
            return this;
        }

        ScenarioOptions withMinFreeDiskMB(Integer minFreeDiskMB) {
            this.minFreeDiskMB = minFreeDiskMB;
            return this;
        }

        ScenarioOptions withOnOrderViolation(AdaptiveMergeConfig.OrderViolationAction onOrderViolation) {
            this.onOrderViolation = onOrderViolation;
            return this;
        }

        ScenarioOptions withOnMemoryExceeded(AdaptiveMergeConfig.MemoryExceededAction onMemoryExceeded) {
            this.onMemoryExceeded = onMemoryExceeded;
            return this;
        }

        ScenarioOptions enableSparseLocalDisorder(long disorderEvery, String... sourceIds) {
            this.sparseLocalDisorderEvery = disorderEvery;
            this.sparseLocalDisorderSources.clear();
            if (sourceIds != null) {
                this.sparseLocalDisorderSources.addAll(Arrays.asList(sourceIds));
            }
            return this;
        }

        boolean hasSparseLocalDisorder(String sourceId) {
            return sparseLocalDisorderSources.contains(sourceId);
        }

        public Integer getPendingKeyThreshold() {
            return pendingKeyThreshold;
        }

        public Integer getPendingMemoryMB() {
            return pendingMemoryMB;
        }

        public Integer getOverflowPartitionCount() {
            return overflowPartitionCount;
        }

        public Boolean getPreferOrderedQuery() {
            return preferOrderedQuery;
        }

        public Boolean getValidateSourceOrder() {
            return validateSourceOrder;
        }

        public Boolean getLocalDisorderEnabled() {
            return localDisorderEnabled;
        }

        public Integer getLocalDisorderMaxGroups() {
            return localDisorderMaxGroups;
        }

        public Integer getLocalDisorderMaxMemoryMB() {
            return localDisorderMaxMemoryMB;
        }

        public Integer getMaxSpillBytesMB() {
            return maxSpillBytesMB;
        }

        public Integer getMinFreeDiskMB() {
            return minFreeDiskMB;
        }

        public AdaptiveMergeConfig.OrderViolationAction getOnOrderViolation() {
            return onOrderViolation;
        }

        public AdaptiveMergeConfig.MemoryExceededAction getOnMemoryExceeded() {
            return onMemoryExceeded;
        }

        public long getSparseLocalDisorderEvery() {
            return sparseLocalDisorderEvery;
        }
    }

    static final class DatasetContext {
        private String label;
        private int rowCount;
        private Path scenarioRoot;
        private Configuration connectionConfig;
        private BaseDataSourceDTO baseDataSourceDTO;
        private Map<String, String> tableNames;
        private long setupElapsedMs;
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

        public Path getScenarioRoot() {
            return scenarioRoot;
        }

        public void setScenarioRoot(Path scenarioRoot) {
            this.scenarioRoot = scenarioRoot;
        }

        public Configuration getConnectionConfig() {
            return connectionConfig;
        }

        public void setConnectionConfig(Configuration connectionConfig) {
            this.connectionConfig = connectionConfig;
        }

        public BaseDataSourceDTO getBaseDataSourceDTO() {
            return baseDataSourceDTO;
        }

        public void setBaseDataSourceDTO(BaseDataSourceDTO baseDataSourceDTO) {
            this.baseDataSourceDTO = baseDataSourceDTO;
        }

        public Map<String, String> getTableNames() {
            return tableNames;
        }

        public void setTableNames(Map<String, String> tableNames) {
            this.tableNames = tableNames;
        }

        public long getSetupElapsedMs() {
            return setupElapsedMs;
        }

        public void setSetupElapsedMs(long setupElapsedMs) {
            this.setupElapsedMs = setupElapsedMs;
        }

        public ScenarioOptions getScenarioOptions() {
            return scenarioOptions;
        }

        public void setScenarioOptions(ScenarioOptions scenarioOptions) {
            this.scenarioOptions = scenarioOptions;
        }
    }

    static final class DatasetExpectations {
        private long totalKeys;
        private long expectedInconsistentKeys;
        private long expectedDuplicateIgnoredCount;
        private long fusionOutputCount;
        private String expectedFusionDigest;
        private String expectedFusionContentDigest;
        private Map<Long, List<String>> expectedFusionSamples = new LinkedHashMap<Long, List<String>>();
        private List<List<String>> expectedOrderedRows = new ArrayList<List<String>>();

        public long getTotalKeys() {
            return totalKeys;
        }

        public void setTotalKeys(long totalKeys) {
            this.totalKeys = totalKeys;
        }

        public long getExpectedInconsistentKeys() {
            return expectedInconsistentKeys;
        }

        public void setExpectedInconsistentKeys(long expectedInconsistentKeys) {
            this.expectedInconsistentKeys = expectedInconsistentKeys;
        }

        public long getExpectedDuplicateIgnoredCount() {
            return expectedDuplicateIgnoredCount;
        }

        public void setExpectedDuplicateIgnoredCount(long expectedDuplicateIgnoredCount) {
            this.expectedDuplicateIgnoredCount = expectedDuplicateIgnoredCount;
        }

        public long getFusionOutputCount() {
            return fusionOutputCount;
        }

        public void setFusionOutputCount(long fusionOutputCount) {
            this.fusionOutputCount = fusionOutputCount;
        }

        public String getExpectedFusionDigest() {
            return expectedFusionDigest;
        }

        public void setExpectedFusionDigest(String expectedFusionDigest) {
            this.expectedFusionDigest = expectedFusionDigest;
        }

        public String getExpectedFusionContentDigest() {
            return expectedFusionContentDigest;
        }

        public void setExpectedFusionContentDigest(String expectedFusionContentDigest) {
            this.expectedFusionContentDigest = expectedFusionContentDigest;
        }

        public Map<Long, List<String>> getExpectedFusionSamples() {
            return expectedFusionSamples;
        }

        public void setExpectedFusionSamples(Map<Long, List<String>> expectedFusionSamples) {
            this.expectedFusionSamples = expectedFusionSamples;
        }

        public List<List<String>> getExpectedOrderedRows() {
            return expectedOrderedRows;
        }

        public void setExpectedOrderedRows(List<List<String>> expectedOrderedRows) {
            this.expectedOrderedRows = expectedOrderedRows;
        }
    }

    static final class ConsistencyExecution {
        private ComparisonResult comparisonResult;
        private long elapsedMs;
        private long peakHeapBytes;
        private String executionEngine;
        private long mergeResolvedKeyCount;
        private long mergeSpilledKeyCount;
        private long duplicateIgnoredCount;
        private long localReorderedGroupCount;
        private long orderRecoveryCount;
        private long spillBytes;
        private boolean spillGuardTriggered;
        private String spillGuardReason;
        private String fallbackReason;
        private String errorMessage;

        public ComparisonResult getComparisonResult() {
            return comparisonResult;
        }

        public void setComparisonResult(ComparisonResult comparisonResult) {
            this.comparisonResult = comparisonResult;
        }

        public long getElapsedMs() {
            return elapsedMs;
        }

        public void setElapsedMs(long elapsedMs) {
            this.elapsedMs = elapsedMs;
        }

        public long getPeakHeapBytes() {
            return peakHeapBytes;
        }

        public void setPeakHeapBytes(long peakHeapBytes) {
            this.peakHeapBytes = peakHeapBytes;
        }

        public String getExecutionEngine() {
            return executionEngine;
        }

        public void setExecutionEngine(String executionEngine) {
            this.executionEngine = executionEngine;
        }

        public long getMergeResolvedKeyCount() {
            return mergeResolvedKeyCount;
        }

        public void setMergeResolvedKeyCount(long mergeResolvedKeyCount) {
            this.mergeResolvedKeyCount = mergeResolvedKeyCount;
        }

        public long getMergeSpilledKeyCount() {
            return mergeSpilledKeyCount;
        }

        public void setMergeSpilledKeyCount(long mergeSpilledKeyCount) {
            this.mergeSpilledKeyCount = mergeSpilledKeyCount;
        }

        public long getDuplicateIgnoredCount() {
            return duplicateIgnoredCount;
        }

        public void setDuplicateIgnoredCount(long duplicateIgnoredCount) {
            this.duplicateIgnoredCount = duplicateIgnoredCount;
        }

        public long getLocalReorderedGroupCount() {
            return localReorderedGroupCount;
        }

        public void setLocalReorderedGroupCount(long localReorderedGroupCount) {
            this.localReorderedGroupCount = localReorderedGroupCount;
        }

        public long getOrderRecoveryCount() {
            return orderRecoveryCount;
        }

        public void setOrderRecoveryCount(long orderRecoveryCount) {
            this.orderRecoveryCount = orderRecoveryCount;
        }

        public long getSpillBytes() {
            return spillBytes;
        }

        public void setSpillBytes(long spillBytes) {
            this.spillBytes = spillBytes;
        }

        public boolean isSpillGuardTriggered() {
            return spillGuardTriggered;
        }

        public void setSpillGuardTriggered(boolean spillGuardTriggered) {
            this.spillGuardTriggered = spillGuardTriggered;
        }

        public String getSpillGuardReason() {
            return spillGuardReason;
        }

        public void setSpillGuardReason(String spillGuardReason) {
            this.spillGuardReason = spillGuardReason;
        }

        public String getFallbackReason() {
            return fallbackReason;
        }

        public void setFallbackReason(String fallbackReason) {
            this.fallbackReason = fallbackReason;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }
    }

    static final class FusionExecution {
        private long elapsedMs;
        private long peakHeapBytes;
        private long outputCount;
        private String digest;
        private String contentDigest;
        private Map<Long, List<String>> sampleRows = new LinkedHashMap<Long, List<String>>();
        private List<Long> firstOutputKeys = new ArrayList<Long>();
        private List<List<String>> orderedRows = new ArrayList<List<String>>();
        private boolean outputOrdered = true;
        private String orderViolationSample;
        private SortMergeStats stats;
        private String errorMessage;

        public long getElapsedMs() {
            return elapsedMs;
        }

        public void setElapsedMs(long elapsedMs) {
            this.elapsedMs = elapsedMs;
        }

        public long getPeakHeapBytes() {
            return peakHeapBytes;
        }

        public void setPeakHeapBytes(long peakHeapBytes) {
            this.peakHeapBytes = peakHeapBytes;
        }

        public long getOutputCount() {
            return outputCount;
        }

        public void setOutputCount(long outputCount) {
            this.outputCount = outputCount;
        }

        public String getDigest() {
            return digest;
        }

        public void setDigest(String digest) {
            this.digest = digest;
        }

        public String getContentDigest() {
            return contentDigest;
        }

        public void setContentDigest(String contentDigest) {
            this.contentDigest = contentDigest;
        }

        public Map<Long, List<String>> getSampleRows() {
            return sampleRows;
        }

        public void setSampleRows(Map<Long, List<String>> sampleRows) {
            this.sampleRows = sampleRows;
        }

        public List<Long> getFirstOutputKeys() {
            return firstOutputKeys;
        }

        public void setFirstOutputKeys(List<Long> firstOutputKeys) {
            this.firstOutputKeys = firstOutputKeys;
        }

        public List<List<String>> getOrderedRows() {
            return orderedRows;
        }

        public void setOrderedRows(List<List<String>> orderedRows) {
            this.orderedRows = orderedRows;
        }

        public boolean isOutputOrdered() {
            return outputOrdered;
        }

        public void setOutputOrdered(boolean outputOrdered) {
            this.outputOrdered = outputOrdered;
        }

        public String getOrderViolationSample() {
            return orderViolationSample;
        }

        public void setOrderViolationSample(String orderViolationSample) {
            this.orderViolationSample = orderViolationSample;
        }

        public SortMergeStats getStats() {
            return stats;
        }

        public void setStats(SortMergeStats stats) {
            this.stats = stats;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }
    }

    static final class ScenarioResult {
        private String label;
        private int rowCount;
        private long setupElapsedMs;
        private DatasetExpectations expectations;
        private ConsistencyExecution consistencyExecution;
        private FusionExecution fusionExecution;

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

        public DatasetExpectations getExpectations() {
            return expectations;
        }

        public void setExpectations(DatasetExpectations expectations) {
            this.expectations = expectations;
        }

        public ConsistencyExecution getConsistencyExecution() {
            return consistencyExecution;
        }

        public void setConsistencyExecution(ConsistencyExecution consistencyExecution) {
            this.consistencyExecution = consistencyExecution;
        }

        public FusionExecution getFusionExecution() {
            return fusionExecution;
        }

        public void setFusionExecution(FusionExecution fusionExecution) {
            this.fusionExecution = fusionExecution;
        }
    }

    private static final class FusionBundle {
        private final FusionConfig fusionConfig;
        private final FusionContext fusionContext;
        private final List<String> targetColumns;

        private FusionBundle(FusionConfig fusionConfig, FusionContext fusionContext, List<String> targetColumns) {
            this.fusionConfig = fusionConfig;
            this.fusionContext = fusionContext;
            this.targetColumns = targetColumns;
        }

        public FusionConfig getFusionConfig() {
            return fusionConfig;
        }

        public FusionContext getFusionContext() {
            return fusionContext;
        }

        public List<String> getTargetColumns() {
            return targetColumns;
        }
    }

    private static final class RowData {
        private long bizId;
        private String fusionName;
        private int age;
        private BigDecimal salary;
        private String department;
        private String status;
        private Timestamp updatedAt;

        public long getBizId() {
            return bizId;
        }

        public void setBizId(long bizId) {
            this.bizId = bizId;
        }

        public String getFusionName() {
            return fusionName;
        }

        public void setFusionName(String fusionName) {
            this.fusionName = fusionName;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public BigDecimal getSalary() {
            return salary;
        }

        public void setSalary(BigDecimal salary) {
            this.salary = salary;
        }

        public String getDepartment() {
            return department;
        }

        public void setDepartment(String department) {
            this.department = department;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public Timestamp getUpdatedAt() {
            return updatedAt;
        }

        public void setUpdatedAt(Timestamp updatedAt) {
            this.updatedAt = updatedAt;
        }
    }

    private static final class CollectingRecordSender implements RecordSender {
        private static final int MAX_TRACKED_OUTPUT_KEYS = 20;
        private final List<String> targetColumns;
        private final Set<Long> sampleKeys;
        private final boolean captureAllRows;
        private final Map<Long, List<String>> sampleRows = new LinkedHashMap<Long, List<String>>();
        private final List<Long> firstOutputKeys = new ArrayList<Long>();
        private final List<List<String>> orderedRows = new ArrayList<List<String>>();
        private final MessageDigest digest = newDigest();
        private BigInteger contentDigest = BigInteger.ZERO;
        private boolean outputOrdered = true;
        private Long previousBizId;
        private String orderViolationSample;
        private long outputCount;

        private CollectingRecordSender(List<String> targetColumns, List<Long> sampleKeys, boolean captureAllRows) {
            this.targetColumns = targetColumns;
            this.sampleKeys = new LinkedHashSet<Long>(sampleKeys);
            this.captureAllRows = captureAllRows;
        }

        @Override
        public Record createRecord() {
            return new DefaultRecord();
        }

        @Override
        public void sendToWriter(Record record) {
            List<String> values = new ArrayList<String>(targetColumns.size());
            for (int index = 0; index < targetColumns.size(); index++) {
                Column column = record.getColumn(index);
                values.add(column == null ? "null" : column.asString());
            }
            outputCount++;
            updateDigest(digest, values);
            contentDigest = updateContentDigest(contentDigest, values);
            if (captureAllRows) {
                orderedRows.add(new ArrayList<String>(values));
            }
            if (!values.isEmpty() && !"null".equals(values.get(0))) {
                Long bizId = Long.parseLong(values.get(0));
                if (previousBizId != null && bizId.longValue() < previousBizId.longValue() && outputOrdered) {
                    outputOrdered = false;
                    orderViolationSample = previousBizId + " -> " + bizId;
                }
                previousBizId = bizId;
                if (firstOutputKeys.size() < MAX_TRACKED_OUTPUT_KEYS) {
                    firstOutputKeys.add(bizId);
                }
                if (sampleKeys.contains(bizId)) {
                    sampleRows.put(bizId, new ArrayList<String>(values));
                }
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void terminate() {
        }

        @Override
        public void shutdown() {
        }

        long getOutputCount() {
            return outputCount;
        }

        String getDigestHex() {
            return toHex(digest.digest());
        }

        String getContentDigestHex() {
            return toHex(contentDigest);
        }

        Map<Long, List<String>> getSampleRows() {
            return sampleRows;
        }

        List<Long> getFirstOutputKeys() {
            return firstOutputKeys;
        }

        List<List<String>> getOrderedRows() {
            return orderedRows;
        }

        boolean isOutputOrdered() {
            return outputOrdered;
        }

        String getOrderViolationSample() {
            return orderViolationSample;
        }
    }

    private static final class HeapUsageSampler {
        private final AtomicBoolean running = new AtomicBoolean(false);
        private Thread worker;
        private volatile long peakHeapBytes;

        void start() {
            peakHeapBytes = usedHeapBytes();
            running.set(true);
            worker = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (running.get()) {
                        peakHeapBytes = Math.max(peakHeapBytes, usedHeapBytes());
                        try {
                            Thread.sleep(50L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    peakHeapBytes = Math.max(peakHeapBytes, usedHeapBytes());
                }
            }, "sortmerge-heap-sampler");
            worker.setDaemon(true);
            worker.start();
        }

        void stop() {
            running.set(false);
            if (worker != null) {
                try {
                    worker.join(1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        long getPeakHeapBytes() {
            return peakHeapBytes;
        }

        private long usedHeapBytes() {
            Runtime runtime = Runtime.getRuntime();
            return runtime.totalMemory() - runtime.freeMemory();
        }
    }

    private static final class NoOpResultRecorder implements ResultRecorder {
        @Override
        public void recordComparisonResult(ComparisonResult result) {
        }

        @Override
        public String recordDifferences(List<DifferenceRecord> differences) {
            return null;
        }

        @Override
        public List<ResolutionResult> recordResolutionResults(ComparisonResult result, List<DifferenceRecord> resolvedDifferences) {
            return new ArrayList<ResolutionResult>();
        }

        @Override
        public String generateReport(ComparisonResult result, List<DifferenceRecord> differences) {
            return null;
        }

        @Override
        public String generateReport(ComparisonResult result, List<DifferenceRecord> differences, OutputConfig outputConfig) {
            return null;
        }
    }

    private static final class MysqlTestDataSourcePluginManager extends DataSourcePluginManager {
        private final AbstractDataSourcePlugin mysqlPlugin = MYSQL_PLUGIN;

        @Override
        public AbstractDataSourcePlugin getDataSourcePlugin(String pluginName) {
            if (!TEST_PLUGIN_NAME.equals(pluginName)) {
                throw new IllegalArgumentException("Unsupported test plugin: " + pluginName);
            }
            return mysqlPlugin;
        }

        @Override
        public void clearCache() {
        }
    }
}
