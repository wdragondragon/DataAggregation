package com.jdragon.aggregation.core.consistency.test;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MockDataGenerator {

    private final DataSourcePluginManager pluginManager;

    private final String pluginName;

    public MockDataGenerator(String pluginName) {
        this.pluginManager = new DataSourcePluginManager();
        this.pluginName = pluginName;
    }

    public MockDataGenerator() {
        this("mysql8");  // 默认使用mysql插件
    }

    public void generateTestDatabases(String host, String port, String username, String password) {
        log.info("开始生成测试数据库和数据...");

        try {
            // 创建主数据库
            createAndPopulateDatabase(host, port, username, password, "agg_test", getPrimaryDbScripts());
            log.info("主数据库创建完成: primary_db");

            // 创建备份数据库
            createAndPopulateDatabase(host, port, username, password, "agg_test", getBackupDbScripts());
            log.info("备份数据库创建完成: backup_db");

            // 创建数据仓库数据库
            createAndPopulateDatabase(host, port, username, password, "agg_test", getWarehouseDbScripts());
            log.info("数据仓库创建完成: warehouse_db");

            log.info("所有测试数据库和数据生成完成");

        } catch (Exception e) {
            log.error("生成测试数据失败", e);
            throw new RuntimeException("生成测试数据失败", e);
        }
    }

    private void createAndPopulateDatabase(String host, String port, String username, String password,
                                           String databaseName, List<String> sqlScripts) {
        // 然后执行该数据库的所有SQL脚本
        executeSqlScripts(host, port, username, password, databaseName, sqlScripts);
    }

    private void executeSqlScripts(String host, String port, String username, String password,
                                   String databaseName, List<String> sqlScripts) {

        Configuration connectionConfig = Configuration.newDefault();
        connectionConfig.set("host", host);
        connectionConfig.set("port", port);
        connectionConfig.set("database", databaseName);
        connectionConfig.set("username", username);
        connectionConfig.set("password", password);

        DataSourceConfig dsConfig = new DataSourceConfig();
        dsConfig.setPluginName(pluginName);
        dsConfig.setConnectionConfig(connectionConfig);

        BaseDataSourceDTO dto = DataSourcePluginManager.createDataSourceDTO(dsConfig);
        AbstractDataSourcePlugin plugin = pluginManager.getDataSourcePlugin(pluginName);

        for (String sql : sqlScripts) {
            if (sql.trim().isEmpty() || sql.trim().startsWith("--")) {
                continue;
            }

            try {
                plugin.executeUpdate(dto, sql);
                log.debug("SQL执行成功: {}", sql.substring(0, Math.min(sql.length(), 100)) + "...");
            } catch (Exception e) {
                log.error("SQL执行失败: {}", sql, e);
                throw new RuntimeException("执行SQL失败: " + sql, e);
            }
        }
    }

    private List<String> getPrimaryDbScripts() {
        List<String> scripts = new ArrayList<>();
        // 插入用户数据（主数据库 - 最准确的数据）
        scripts.add("INSERT INTO users_1 (user_id, username, age, salary, department, email) VALUES " +
                "(1, 'zhangsan', 30, 15000.00, 'Technology', 'zhangsan@company.com')," +
                "(2, 'lisi', 28, 12000.00, 'Marketing', 'lisi@company.com')," +
                "(3, 'wangwu', 35, 18000.00, 'Sales', 'wangwu@company.com')," +
                "(4, 'zhaoliu', 32, 16000.00, 'HR', 'zhaoliu@company.com')," +
                "(5, 'sunqi', 29, 13000.00, 'Finance', 'sunqi@company.com')," +
                "(6, 'zhouba', 31, 14000.00, 'Technology', 'zhouba@company.com')," +
                "(7, 'wujiu', 27, 11000.00, 'Marketing', 'wujiu@company.com')," +
                "(8, 'zhengshi', 33, 17000.00, 'Sales', 'zhengshi@company.com') " +
                "ON DUPLICATE KEY UPDATE " +
                "username=VALUES(username), age=VALUES(age), salary=VALUES(salary), " +
                "department=VALUES(department), email=VALUES(email)");
        return scripts;
    }

    private List<String> getBackupDbScripts() {
        List<String> scripts = new ArrayList<>();

        // 插入用户数据（备份数据库 - 部分不一致）
        scripts.add("INSERT INTO users_2 (user_id, username, age, salary, department, email) VALUES " +
                "(1, 'zhangsan', 31, 15000.00, 'Technology', 'zhangsan@company.com')," +        // 年龄不一致：30 vs 31
                "(2, 'lisi', 28, 12500.00, 'Marketing', 'lisi@company.com')," +                // 工资不一致：12000 vs 12500
                "(3, 'wangwu', 35, 18000.00, 'Sales', 'wangwu@company.com')," +                // 一致
                "(4, 'zhaoliu', 32, 16000.00, 'HR', 'zhaoliu@company.com')," +                 // 一致
                "(5, 'sunqi', 29, 13000.00, 'Finance', 'sunqi@company.com')," +                // 一致
                "(6, 'zhouba', 31, 14200.00, 'Technology', 'zhouba@company.com')," +           // 工资不一致：14000 vs 14200
                "(7, 'wujiu', 27, 11000.00, 'Marketing', 'wujiu@company.com')," +              // 一致
                "(8, 'zhengshi', 33, 17000.00, 'Sales', 'zhengshi@company.com') " +            // 一致
                "ON DUPLICATE KEY UPDATE " +
                "username=VALUES(username), age=VALUES(age), salary=VALUES(salary), " +
                "department=VALUES(department), email=VALUES(email)");
        return scripts;
    }

    private List<String> getWarehouseDbScripts() {
        List<String> scripts = new ArrayList<>();
        // 插入用户数据（数据仓库 - 更多不一致）
        scripts.add("INSERT INTO users_3 (user_id, username, age, salary, dept, email) VALUES " +
                "(1, 'zhangsan', 30, 15000.00, 'Tech', 'zhangsan@company.com')," +             // 部门不一致：Technology vs Tech
                "(2, 'lisi', 28, 12000.00, 'Marketing', 'lisi@company.com')," +                // 一致
                "(3, 'wangwu', 36, 18200.00, 'Sales', 'wangwu@company.com')," +                // 年龄和工资都不一致：35/18000 vs 36/18200
                "(4, 'zhaoliu', 32, 16000.00, 'Human Resource', 'zhaoliu@company.com')," +     // 部门不一致：HR vs Human Resource
                "(5, 'sunqi', 29, 13000.00, 'Finance', 'sunqi@company.com')," +                // 一致
                "(6, 'zhouba', 31, 14000.00, 'Technology', 'zhouba@company.com')," +           // 一致
                "(7, 'wujiu', 27, 11500.00, 'Marketing', 'wujiu@company.com')," +              // 工资不一致：11000 vs 11500
                "(8, 'zhengshi', NULL, 17000.00, 'Sales', 'zhengshi@company.com') " +          // 年龄为NULL
                "ON DUPLICATE KEY UPDATE " +
                "username=VALUES(username), age=VALUES(age), salary=VALUES(salary), " +
                "dept=VALUES(dept), email=VALUES(email)");
        return scripts;
    }

    public void shutdown() {
        pluginManager.clearCache();
        log.info("MockDataGenerator已关闭");
    }

    /**
     * 获取插件管理器（主要用于测试）
     */
    public com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager getPluginManager() {
        return pluginManager;
    }

    // 使用示例
    public static void main(String[] args) {
        // 配置数据库连接信息
        String host = "192.168.188.128";
        String port = "3306";
        String username = "root";
        String password = "951753";

        MockDataGenerator generator = new MockDataGenerator();

        try {
            // 生成测试数据
            generator.generateTestDatabases(host, port, username, password);

            System.out.println("测试数据生成成功！");
            System.out.println("数据库列表：");
            System.out.println("1. primary_db - 主数据库（高可信度）");
            System.out.println("2. backup_db - 备份数据库（中等可信度）");
            System.out.println("3. warehouse_db - 数据仓库（低可信度，字段名不同）");

        } catch (Exception e) {
            System.err.println("测试数据生成失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            generator.shutdown();
        }
    }
}