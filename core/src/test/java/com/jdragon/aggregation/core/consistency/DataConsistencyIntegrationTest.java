package com.jdragon.aggregation.core.consistency;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.*;
import com.jdragon.aggregation.core.consistency.service.DataConsistencyService;
import com.jdragon.aggregation.core.consistency.test.MockDataGenerator;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DataConsistencyIntegrationTest {

    private static final String TEST_HOST = "localhost";
    private static final String TEST_PORT = "3306";
    private static final String TEST_USERNAME = "root";
    private static final String TEST_PASSWORD = "password";

    private static final String MYSQL_PLUGIN_NAME = "mysql-source";

    public static void main(String[] args) {
        log.info("开始数据一致性集成测试");

        // 检查MySQL连接是否可用
        if (!isMySqlAvailable()) {
            log.warn("MySQL数据库不可用，跳过集成测试");
            log.warn("请确保MySQL正在运行，并且连接配置正确：");
            log.warn("主机: {}, 端口: {}, 用户名: {}, 密码: {}",
                    TEST_HOST, TEST_PORT, TEST_USERNAME, "******");
            return;
        }

        MockDataGenerator dataGenerator = new MockDataGenerator(MYSQL_PLUGIN_NAME);
        DataConsistencyService consistencyService = new DataConsistencyService("./test-results");

        try {
            // 步骤1: 生成测试数据
            log.info("步骤1: 生成测试数据...");
            dataGenerator.generateTestDatabases(TEST_HOST, TEST_PORT, TEST_USERNAME, TEST_PASSWORD);

            // 步骤2: 创建一致性规则
            log.info("步骤2: 创建一致性规则...");
            ConsistencyRule rule = createTestRule();
            consistencyService.addRule(rule);

            // 步骤3: 执行一致性检查
            log.info("步骤3: 执行一致性检查...");
            ComparisonResult result = consistencyService.executeRule(rule.getRuleId());

            // 步骤4: 验证结果
            log.info("步骤4: 验证结果...");
            printTestResult(result);

            // 步骤5: 运行不同冲突解决策略的测试
            log.info("步骤5: 测试不同冲突解决策略...");
            testDifferentResolutionStrategies();

            log.info("集成测试完成！");

        } catch (Exception e) {
            log.error("集成测试失败", e);
        } finally {
            // 清理资源
            dataGenerator.shutdown();
            consistencyService.shutdown();

            // 可选：清理测试数据库
            // dataGenerator.cleanupTestDatabases(TEST_HOST, TEST_PORT, TEST_USERNAME, TEST_PASSWORD);
        }
    }

    private static boolean isMySqlAvailable() {
        try {
            MockDataGenerator generator = new MockDataGenerator(MYSQL_PLUGIN_NAME);
            // 尝试创建测试数据库来验证连接
            Configuration connectionConfig = Configuration.newDefault();
            connectionConfig.set("host", TEST_HOST);
            connectionConfig.set("port", TEST_PORT);
            connectionConfig.set("database", "mysql"); // 使用系统数据库
            connectionConfig.set("username", TEST_USERNAME);
            connectionConfig.set("password", TEST_PASSWORD);

            com.jdragon.aggregation.core.consistency.model.DataSourceConfig dsConfig =
                    new com.jdragon.aggregation.core.consistency.model.DataSourceConfig();
            dsConfig.setPluginName(MYSQL_PLUGIN_NAME);
            dsConfig.setConnectionConfig(connectionConfig);

            com.jdragon.aggregation.datasource.BaseDataSourceDTO dto =
                    com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager.createDataSourceDTO(dsConfig);
            com.jdragon.aggregation.datasource.AbstractDataSourcePlugin plugin =
                    generator.getPluginManager().getDataSourcePlugin(MYSQL_PLUGIN_NAME);

            // 尝试执行简单查询
            plugin.executeQuerySql(dto, "SELECT 1", true);

            generator.shutdown();
            return true;
        } catch (Exception e) {
            log.warn("MySQL连接测试失败: {}", e.getMessage());
            return false;
        }
    }

    private static ConsistencyRule createTestRule() {
        ConsistencyRule rule = new ConsistencyRule();
        rule.setRuleId("integration-test-rule");
        rule.setRuleName("集成测试一致性规则");
        rule.setDescription("测试多个数据源用户数据的一致性");
        rule.setEnabled(true);
        rule.setToleranceThreshold(0.01);
        rule.setConflictResolutionStrategy(ConflictResolutionStrategy.HIGH_CONFIDENCE);

        rule.setCompareFields(Arrays.asList("age", "salary", "department"));
        rule.setMatchKeys(Arrays.asList("user_id", "username"));

        // 配置三个数据源
        rule.setDataSources(Arrays.asList(
                createDataSourceConfig("primary", "主数据库", 1.0, 1),
                createDataSourceConfig("backup", "备份数据库", 0.8, 2),
                createDataSourceConfig("warehouse", "数据仓库", 0.9, 3)
        ));

        Map<String, Object> resolutionParams = new HashMap<>();
        resolutionParams.put("test", "集成测试");
        rule.setResolutionParams(resolutionParams);

        OutputConfig outputConfig = new OutputConfig();
        outputConfig.setOutputType(OutputConfig.OutputType.FILE);
        outputConfig.setOutputPath("./test-results");
        outputConfig.setStoreDifferences(true);
        outputConfig.setStoreResolutionResults(true);
        outputConfig.setGenerateReport(true);
        outputConfig.setReportFormat(OutputConfig.ReportFormat.HTML);
        rule.setOutputConfig(outputConfig);

        return rule;
    }

    private static DataSourceConfig createDataSourceConfig(String sourceId, String sourceName,
                                                           double confidenceWeight, int priority) {
        DataSourceConfig config = new DataSourceConfig();
        config.setSourceId(sourceId);
        config.setSourceName(sourceName);
        config.setPluginName(MYSQL_PLUGIN_NAME);
        config.setConfidenceWeight(confidenceWeight);
        config.setPriority(priority);

        Configuration connectionConfig = Configuration.newDefault();
        connectionConfig.set("host", TEST_HOST);
        connectionConfig.set("port", TEST_PORT);
        connectionConfig.set("username", TEST_USERNAME);
        connectionConfig.set("password", TEST_PASSWORD);

        Map<String, String> otherParams = new HashMap<>();
        otherParams.put("useSSL", "false");
        otherParams.put("serverTimezone", "UTC");
        connectionConfig.set("other", otherParams);

        // 设置数据库名和查询SQL
        if ("primary".equals(sourceId)) {
            connectionConfig.set("database", "primary_db");
            config.setQuerySql("SELECT user_id, username, age, salary, department, email FROM users");
        } else if ("backup".equals(sourceId)) {
            connectionConfig.set("database", "backup_db");
            config.setQuerySql("SELECT user_id, username, age, salary, department, email FROM users");
        } else if ("warehouse".equals(sourceId)) {
            connectionConfig.set("database", "warehouse_db");
            // 数据仓库字段名不同，需要映射
            config.setQuerySql("SELECT user_id, username, age, salary, dept as department, email FROM users");
        }

        config.setConnectionConfig(connectionConfig);

        // 字段映射（针对数据仓库）
        if ("warehouse".equals(sourceId)) {
            Map<String, String> fieldMappings = new HashMap<>();
            fieldMappings.put("dept", "department");
            config.setFieldMappings(fieldMappings);
        }

        return config;
    }

    private static void printTestResult(ComparisonResult result) {
        log.info("==========================================");
        log.info("一致性检查结果:");
        log.info("规则ID: {}", result.getRuleId());
        log.info("状态: {}", result.getStatus());
        log.info("执行时间: {}", result.getExecutionTime());
        log.info("总记录数: {}", result.getTotalRecords());
        log.info("一致记录数: {}", result.getConsistentRecords());
        log.info("不一致记录数: {}", result.getInconsistentRecords());
        log.info("已解决记录数: {}", result.getResolvedRecords());

        if (result.getFieldDiscrepancies() != null && !result.getFieldDiscrepancies().isEmpty()) {
            log.info("字段差异统计:");
            for (Map.Entry<String, Integer> entry : result.getFieldDiscrepancies().entrySet()) {
                log.info("  字段 '{}': {} 处差异", entry.getKey(), entry.getValue());
            }
        }

        if (result.getReportPath() != null) {
            log.info("报告路径: {}", result.getReportPath());
        }
        log.info("==========================================");

        // 验证测试预期
        int expectedInconsistent = 8; // 8个用户中有多个不一致
        if (result.getInconsistentRecords() >= expectedInconsistent) {
            log.info("测试通过: 成功检测到数据不一致");
        } else {
            log.warn("测试警告: 检测到的不一致记录数少于预期");
        }
    }

    private static void testDifferentResolutionStrategies() {
        log.info("测试不同冲突解决策略...");

        String[] strategies = {"HIGH_CONFIDENCE", "WEIGHTED_AVERAGE", "MAJORITY_VOTE"};

        for (String strategyName : strategies) {
            try {
                log.info("测试策略: {}", strategyName);

                DataConsistencyService service = new DataConsistencyService("./test-results-" + strategyName.toLowerCase());

                ConsistencyRule rule = createTestRule();
                rule.setRuleId("strategy-test-" + strategyName.toLowerCase());
                rule.setConflictResolutionStrategy(ConflictResolutionStrategy.valueOf(strategyName));

                service.addRule(rule);
                ComparisonResult result = service.executeRule(rule.getRuleId());

                log.info("策略 {} 结果: {} 不一致记录, {} 已解决",
                        strategyName, result.getInconsistentRecords(), result.getResolvedRecords());

                service.shutdown();

            } catch (Exception e) {
                log.error("策略 {} 测试失败: {}", strategyName, e.getMessage());
            }
        }
    }

    // 注意：MockDataGenerator现在提供了getPluginManager()方法，可以直接使用
}