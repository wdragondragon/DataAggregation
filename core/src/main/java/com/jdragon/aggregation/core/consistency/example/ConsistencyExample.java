package com.jdragon.aggregation.core.consistency.example;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.*;
import com.jdragon.aggregation.core.consistency.service.DataConsistencyService;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ConsistencyExample {

    public static void main(String[] args) {
        log.info("Starting Data Consistency Example");

        try {
            ConsistencyRule rule = createSampleRule();
            logResult(executeRule(rule, "./consistency-results"));

        } catch (Exception e) {
            log.error("Error during consistency check", e);
        }

        log.info("Example completed");
    }

    public static ComparisonResult executeRule(ConsistencyRule rule, String outputDirectory) {
        DataConsistencyService service = new DataConsistencyService(outputDirectory);
        try {
            service.addRule(rule);
            log.info("Executing consistency rule: {}", rule.getRuleName());
            return service.executeRule(rule.getRuleId());
        } finally {
            service.shutdown();
        }
    }

    public static void logResult(ComparisonResult result) {
        if (result == null) {
            log.warn("Consistency result is null");
            return;
        }
        log.info("Execution completed. Status: {}", result.getStatus());
        log.info("Total Records: {}", result.getTotalRecords());
        log.info("Consistent Records: {}", result.getConsistentRecords());
        log.info("Inconsistent Records: {}", result.getInconsistentRecords());
        log.info("Resolved Records: {}", result.getResolvedRecords());
        if (result.getUpdateResult() != null) {
            log.info("Update Summary: inserts={}, updates={}, deletes={}, skips={}",
                    result.getUpdateResult().getInsertCount(),
                    result.getUpdateResult().getUpdateCount(),
                    result.getUpdateResult().getDeleteCount(),
                    result.getUpdateResult().getSkipCount());
        }
        if (result.getReportPath() != null) {
            log.info("Report generated: {}", result.getReportPath());
        }
    }

    private static ConsistencyRule createSampleRule() {
        ConsistencyRule rule = new ConsistencyRule();
        rule.setRuleId("rule-001");
        rule.setRuleName("用户数据一致性检查");
        rule.setDescription("检查来自不同系统的用户基本信息一致性");
        rule.setEnabled(true);
        rule.setParallelFetch(true);
        rule.setToleranceThreshold(0.01);
        rule.setConflictResolutionStrategy(ConflictResolutionStrategy.MAJORITY_VOTE);
        rule.setUpdateTargetSourceId("source-1");
        rule.setAutoApplyResolutions(true);

        rule.setCompareFields(Arrays.asList("age", "salary", "department", "email"));
        rule.setMatchKeys(Arrays.asList("user_id", "username"));

        rule.setDataSources(Arrays.asList(
                createDataSourceConfig("source-1", "主数据库", "mysql8",
                        createConnectionConfig("192.168.188.128", "3306", "agg_test", "root", "951753"),
                        "SELECT user_id, username, age, salary, department, email FROM users_1",
                        1.0, 1, Configuration.newDefault()),

                createDataSourceConfig("source-2", "备份数据库", "mysql8",
                        createConnectionConfig("192.168.188.128", "3306", "agg_test", "root", "951753"),
                        "SELECT user_id, username, age, salary, department, email FROM users_2",
                        1.8, 2, Configuration.newDefault()),

                createDataSourceConfig("source-3", "数据仓库", "minio",
                        createFileConfig("http://192.168.188.128:9000", "minioadmin", "minioadmin", "test"),
                        "/test/users_3.csv",
                        0.9, 3, Configuration.from(Collections.singletonMap("file.format", "csv")))
        ));

        Map<String, Object> resolutionParams = new HashMap<>();
        resolutionParams.put("notes", "使用高可信度源策略，主数据库权重最高");
        rule.setResolutionParams(resolutionParams);

        OutputConfig outputConfig = new OutputConfig();
        outputConfig.setOutputPath("./consistency-results");
        outputConfig.setGenerateReport(true);
        // 支持三种语言模式: ENGLISH, CHINESE, BILINGUAL (英文/中文双语)
        outputConfig.setReportLanguage(OutputConfig.ReportLanguage.CHINESE);
        rule.setOutputConfig(outputConfig);

        return rule;
    }

    private static DataSourceConfig createDataSourceConfig(
            String sourceId, String sourceName, String pluginName,
            Configuration connectionConfig, String querySql,
            double confidenceWeight, int priority, Configuration extConfig) {

        DataSourceConfig config = new DataSourceConfig();
        config.setSourceId(sourceId);
        config.setSourceName(sourceName);
        config.setPluginName(pluginName);
        config.setConnectionConfig(connectionConfig);
        config.setQuerySql(querySql);
        config.setConfidenceWeight(confidenceWeight);
        config.setPriority(priority);
        config.setExtConfig(extConfig);

        Map<String, String> fieldMappings = new HashMap<>();
        if ("source-1".equals(sourceId)) {
            config.setTableName("users_1");
        }
        if ("source-3".equals(sourceId)) {
            fieldMappings.put("dept", "department");
        }
        config.setFieldMappings(fieldMappings);

        return config;
    }

    private static Configuration createConnectionConfig(
            String host, String port, String database, String username, String password) {

        Configuration config = Configuration.newDefault();
        config.set("host", host);
        config.set("port", port);
        config.set("database", database);
        config.set("username", username);
        config.set("password", password);

        Map<String, String> otherParams = new HashMap<>();
        otherParams.put("useSSL", "false");
        otherParams.put("serverTimezone", "UTC");
        config.set("other", JSONObject.toJSONString(otherParams));

        return config;
    }

    private static Configuration createFileConfig(String endpoint, String accessKey, String secretKey, String bucket) {
        Configuration config = Configuration.newDefault();
        config.set("endpoint", endpoint);
        config.set("accessKey", accessKey);
        config.set("secretKey", secretKey);
        config.set("bucket", bucket);

        return config;
    }
}
