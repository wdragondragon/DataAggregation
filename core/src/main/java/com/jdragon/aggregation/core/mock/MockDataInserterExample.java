package com.jdragon.aggregation.core.mock;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class MockDataInserterExample {
    
    public static void main(String[] args) {
        log.info("开始Mock数据插入器示例");
        
        // 配置数据库连接信息
        String host = "localhost";
        String port = "3306";
        String username = "root";
        String password = "password";
        String database = "test_db";
        
        // 创建数据源配置
        DataSourceConfig dataSourceConfig = createDataSourceConfig(
                host, port, database, username, password, "mysql-source");
        
        MockDataInserter inserter = new MockDataInserter();
        
        try {
            // 示例1: 基本插入
            exampleBasicInsert(inserter, dataSourceConfig);
            
            // 示例2: 使用自定义配置插入
            exampleCustomInsert(inserter, dataSourceConfig);
            
            // 示例3: 清空表并插入
            exampleTruncateAndInsert(inserter, dataSourceConfig);
            
            // 示例4: 使用自定义生成器
            exampleCustomGenerator(inserter, dataSourceConfig);
            
            log.info("所有示例执行完成！");
            
        } catch (Exception e) {
            log.error("示例执行失败", e);
        } finally {
            inserter.shutdown();
        }
    }
    
    private static void exampleBasicInsert(MockDataInserter inserter, DataSourceConfig dataSourceConfig) {
        log.info("=== 示例1: 基本插入 ===");
        
        try {
            // 创建测试表（如果不存在）
            createTestTableIfNotExists(dataSourceConfig);
            
            // 插入100条用户数据
            inserter.insertMockData(dataSourceConfig, "users", 100);
            
            log.info("基本插入示例完成: 成功插入100条用户数据");
            
        } catch (Exception e) {
            log.error("基本插入示例失败", e);
        }
    }
    
    private static void exampleCustomInsert(MockDataInserter inserter, DataSourceConfig dataSourceConfig) {
        log.info("=== 示例2: 自定义配置插入 ===");
        
        try {
            // 创建订单表（如果不存在）
            createOrderTableIfNotExists(dataSourceConfig);
            
            // 自定义配置：固定某些字段的值
            Map<String, Object> customConfig = new HashMap<>();
            customConfig.put("status", "PENDING");
            customConfig.put("currency", "CNY");
            customConfig.put("payment_method", "ALIPAY");
            
            // 插入50条订单数据
            inserter.insertMockData(dataSourceConfig, "orders", 50, customConfig);
            
            log.info("自定义插入示例完成: 成功插入50条订单数据");
            
        } catch (Exception e) {
            log.error("自定义插入示例失败", e);
        }
    }
    
    private static void exampleTruncateAndInsert(MockDataInserter inserter, DataSourceConfig dataSourceConfig) {
        log.info("=== 示例3: 清空表并插入 ===");
        
        try {
            // 创建产品表（如果不存在）
            createProductTableIfNotExists(dataSourceConfig);
            
            // 清空表并插入30条产品数据
            inserter.truncateAndInsert(dataSourceConfig, "products", 30);
            
            log.info("清空并插入示例完成: 成功清空并插入30条产品数据");
            
        } catch (Exception e) {
            log.error("清空并插入示例失败", e);
        }
    }
    
    private static void exampleCustomGenerator(MockDataInserter inserter, DataSourceConfig dataSourceConfig) {
        log.info("=== 示例4: 自定义生成器插入 ===");
        
        try {
            // 创建员工表（如果不存在）
            createEmployeeTableIfNotExists(dataSourceConfig);
            
            // 定义自定义生成器
            Map<String, MockDataInserter.ColumnValueGenerator> customGenerators = new HashMap<>();
            
            // 为employee_id生成特定格式的值
            customGenerators.put("employee_id", (column, rowIndex) -> 
                    String.format("EMP%06d", rowIndex + 1));
            
            // 为department生成特定部门的轮换
            customGenerators.put("department", (column, rowIndex) -> {
                String[] departments = {"HR", "IT", "Finance", "Sales", "Marketing", "Operations"};
                return departments[rowIndex % departments.length];
            });
            
            // 为hire_date生成递增的日期
            customGenerators.put("hire_date", (column, rowIndex) -> {
                java.time.LocalDate date = java.time.LocalDate.now()
                        .minusYears(1)
                        .plusDays(rowIndex * 7); // 每周入职一人
                return date.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            });
            
            // 使用自定义生成器插入数据
            inserter.insertWithCustomGenerator(dataSourceConfig, "employees", 20, customGenerators);
            
            log.info("自定义生成器示例完成: 成功插入20条员工数据");
            
        } catch (Exception e) {
            log.error("自定义生成器示例失败", e);
        }
    }
    
    private static DataSourceConfig createDataSourceConfig(String host, String port, String database,
                                                         String username, String password, String pluginName) {
        
        Configuration connectionConfig = Configuration.newDefault();
        connectionConfig.set("host", host);
        connectionConfig.set("port", port);
        connectionConfig.set("database", database);
        connectionConfig.set("username", username);
        connectionConfig.set("password", password);
        
        Map<String, String> otherParams = new HashMap<>();
        otherParams.put("useSSL", "false");
        otherParams.put("serverTimezone", "UTC");
        connectionConfig.set("other", otherParams);
        
        DataSourceConfig config = new DataSourceConfig();
        config.setPluginName(pluginName);
        config.setConnectionConfig(connectionConfig);
        
        return config;
    }
    
    private static void createTestTableIfNotExists(DataSourceConfig dataSourceConfig) {
        try {
            MockDataInserter inserter = new MockDataInserter();
            
            // 简单的建表SQL（仅用于示例）
            String createTableSql = "CREATE TABLE IF NOT EXISTS users (" +
                    "    user_id INT PRIMARY KEY AUTO_INCREMENT," +
                    "    username VARCHAR(50) NOT NULL," +
                    "    email VARCHAR(100)," +
                    "    age INT," +
                    "    salary DECIMAL(10, 2)," +
                    "    department VARCHAR(50)," +
                    "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                    "    is_active TINYINT(1) DEFAULT 1" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
            
            // 获取插件并执行SQL
            com.jdragon.aggregation.datasource.AbstractDataSourcePlugin plugin = 
                    inserter.getPluginManager().getDataSourcePlugin(dataSourceConfig.getPluginName());
            com.jdragon.aggregation.datasource.BaseDataSourceDTO dto = 
                    com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager.createDataSourceDTO(dataSourceConfig);
            
            plugin.executeUpdate(dto, createTableSql);
            log.debug("测试表创建成功: users");
            
            inserter.shutdown();
            
        } catch (Exception e) {
            log.warn("创建测试表失败，可能表已存在: {}", e.getMessage());
        }
    }
    
    private static void createOrderTableIfNotExists(DataSourceConfig dataSourceConfig) {
        try {
            MockDataInserter inserter = new MockDataInserter();
            
            String createTableSql = "CREATE TABLE IF NOT EXISTS orders (" +
                    "    order_id INT PRIMARY KEY AUTO_INCREMENT," +
                    "    order_number VARCHAR(50) UNIQUE NOT NULL," +
                    "    customer_id INT," +
                    "    amount DECIMAL(10, 2) NOT NULL," +
                    "    status VARCHAR(20) DEFAULT 'PENDING'," +
                    "    currency VARCHAR(10) DEFAULT 'CNY'," +
                    "    payment_method VARCHAR(20)," +
                    "    order_date DATE," +
                    "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
            
            com.jdragon.aggregation.datasource.AbstractDataSourcePlugin plugin = 
                    inserter.getPluginManager().getDataSourcePlugin(dataSourceConfig.getPluginName());
            com.jdragon.aggregation.datasource.BaseDataSourceDTO dto = 
                    com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager.createDataSourceDTO(dataSourceConfig);
            
            plugin.executeUpdate(dto, createTableSql);
            log.debug("订单表创建成功: orders");
            
            inserter.shutdown();
            
        } catch (Exception e) {
            log.warn("创建订单表失败，可能表已存在: {}", e.getMessage());
        }
    }
    
    private static void createProductTableIfNotExists(DataSourceConfig dataSourceConfig) {
        try {
            MockDataInserter inserter = new MockDataInserter();
            
            String createTableSql = "CREATE TABLE IF NOT EXISTS products (" +
                    "    product_id INT PRIMARY KEY AUTO_INCREMENT," +
                    "    product_name VARCHAR(100) NOT NULL," +
                    "    sku VARCHAR(50) UNIQUE," +
                    "    price DECIMAL(10, 2) NOT NULL," +
                    "    stock_quantity INT DEFAULT 0," +
                    "    category VARCHAR(50)," +
                    "    description TEXT," +
                    "    is_available TINYINT(1) DEFAULT 1," +
                    "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
            
            com.jdragon.aggregation.datasource.AbstractDataSourcePlugin plugin = 
                    inserter.getPluginManager().getDataSourcePlugin(dataSourceConfig.getPluginName());
            com.jdragon.aggregation.datasource.BaseDataSourceDTO dto = 
                    com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager.createDataSourceDTO(dataSourceConfig);
            
            plugin.executeUpdate(dto, createTableSql);
            log.debug("产品表创建成功: products");
            
            inserter.shutdown();
            
        } catch (Exception e) {
            log.warn("创建产品表失败，可能表已存在: {}", e.getMessage());
        }
    }
    
    private static void createEmployeeTableIfNotExists(DataSourceConfig dataSourceConfig) {
        try {
            MockDataInserter inserter = new MockDataInserter();
            
            String createTableSql = "CREATE TABLE IF NOT EXISTS employees (" +
                    "    employee_id VARCHAR(20) PRIMARY KEY," +
                    "    first_name VARCHAR(50) NOT NULL," +
                    "    last_name VARCHAR(50) NOT NULL," +
                    "    email VARCHAR(100) UNIQUE," +
                    "    department VARCHAR(50)," +
                    "    position VARCHAR(50)," +
                    "    salary DECIMAL(10, 2)," +
                    "    hire_date DATE," +
                    "    manager_id VARCHAR(20)," +
                    "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
            
            com.jdragon.aggregation.datasource.AbstractDataSourcePlugin plugin = 
                    inserter.getPluginManager().getDataSourcePlugin(dataSourceConfig.getPluginName());
            com.jdragon.aggregation.datasource.BaseDataSourceDTO dto = 
                    com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager.createDataSourceDTO(dataSourceConfig);
            
            plugin.executeUpdate(dto, createTableSql);
            log.debug("员工表创建成功: employees");
            
            inserter.shutdown();
            
        } catch (Exception e) {
            log.warn("创建员工表失败，可能表已存在: {}", e.getMessage());
        }
    }
    
    // 获取插件管理器的方法（需要修改MockDataInserter的访问权限或添加getter）
    private static com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager getPluginManager(MockDataInserter inserter) {
        // 注意：这里需要通过反射或修改MockDataInserter类来获取pluginManager
        // 为了示例，我们创建一个新的
        return new com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager();
    }
}