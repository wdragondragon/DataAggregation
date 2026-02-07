package com.jdragon.aggregation.core.mock;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager;
import com.jdragon.aggregation.datasource.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Slf4j
public class MockDataInserter {

    @Getter
    private final DataSourcePluginManager pluginManager;

    private final Random random = new Random();

    public MockDataInserter() {
        this.pluginManager = new DataSourcePluginManager();
    }

    public MockDataInserter(DataSourcePluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    public void insertMockData(DataSourceConfig dataSourceConfig, String tableName, int recordCount) {
        insertMockData(dataSourceConfig, tableName, recordCount, null);
    }

    public void insertMockData(DataSourceConfig dataSourceConfig, String tableName,
                               int recordCount, Map<String, Object> customConfig) {

        log.info("开始生成Mock数据: 表={}, 记录数={}", tableName, recordCount);

        try {
            // 1. 加载数据源插件
            String pluginName = dataSourceConfig.getPluginName();
            AbstractDataSourcePlugin plugin = pluginManager.getDataSourcePlugin(pluginName);

            // 2. 获取表结构信息
            BaseDataSourceDTO dto = DataSourcePluginManager.createDataSourceDTO(dataSourceConfig);
            List<ColumnInfo> columns = plugin.getColumns(dto, tableName);

            if (columns == null || columns.isEmpty()) {
                throw new RuntimeException("无法获取表结构信息: " + tableName);
            }

            log.info("表 {} 共有 {} 个字段", tableName, columns.size());

            // 3. 过滤掉自增字段
            List<ColumnInfo> insertColumns = columns.stream()
                    .filter(col -> !"YES".equalsIgnoreCase(col.getIsAutoincrement()))
                    .collect(Collectors.toList());

            List<String> fieldNames = insertColumns.stream()
                    .map(ColumnInfo::getColumnName)
                    .collect(Collectors.toList());

            log.info("将插入 {} 个字段 (跳过自增字段)", fieldNames.size());

            // 4. 生成Mock数据
            List<List<String>> data = generateMockData(insertColumns, recordCount, customConfig);

            // 5. 构建插入DTO
            InsertDataDTO insertDataDTO = new InsertDataDTO(fieldNames, data);
            insertDataDTO.setBaseDataSourceDTO(dto);
            insertDataDTO.setTableName(tableName);
            insertDataDTO.setBatchSize(1000); // 批次大小
            insertDataDTO.setTruncate(false); // 不截断表

            // 6. 执行插入
            log.info("开始插入数据到表 {}...", tableName);
            plugin.insertData(insertDataDTO);

            log.info("成功插入 {} 条记录到表 {}", recordCount, tableName);

        } catch (Exception e) {
            log.error("插入Mock数据失败", e);
            throw new RuntimeException("插入Mock数据失败: " + e.getMessage(), e);
        }
    }

    private List<List<String>> generateMockData(List<ColumnInfo> columns, int recordCount,
                                                Map<String, Object> customConfig) {

        List<List<String>> data = new ArrayList<>();

        for (int i = 0; i < recordCount; i++) {
            List<String> row = new ArrayList<>();

            for (ColumnInfo column : columns) {
                String value = generateColumnValue(column, i, customConfig);
                row.add(value);
            }

            data.add(row);

            // 每1000条记录打印一次进度
            if ((i + 1) % 1000 == 0) {
                log.debug("已生成 {} 条记录", i + 1);
            }
        }

        return data;
    }

    private String generateColumnValue(ColumnInfo column, int rowIndex, Map<String, Object> customConfig) {

        // 检查自定义配置
        if (customConfig != null) {
            String columnName = column.getColumnName();
            Object customValue = customConfig.get(columnName);
            if (customValue != null) {
                return customValue.toString();
            }
        }

        // 处理可为空字段：有一定概率返回null
        if ("YES".equalsIgnoreCase(column.getIsNullable()) && random.nextDouble() < 0.1) {
            return null;
        }

        // 根据数据类型生成值
        String typeName = column.getTypeName().toUpperCase();
        int dataType = column.getDataType();
        int columnSize = column.getColumnSize();

        // 检查是否为主键，如果是则生成唯一值
        boolean isPrimaryKey = "YES".equalsIgnoreCase(column.getIsPrimaryKey());

        try {
            switch (dataType) {
                case Types.INTEGER:
                case Types.SMALLINT:
                case Types.TINYINT:
                    if (isPrimaryKey) {
                        return String.valueOf(rowIndex + 1);
                    }
                    return String.valueOf(random.nextInt(10000));

                case Types.BIGINT:
                    if (isPrimaryKey) {
                        return String.valueOf(rowIndex + 1);
                    }
                    return String.valueOf(random.nextLong() % 1000000L);

                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    BigDecimal decimalValue = BigDecimal.valueOf(random.nextDouble() * 10000);
                    if (column.getDecimalDigits() > 0) {
                        decimalValue = decimalValue.setScale(column.getDecimalDigits(), BigDecimal.ROUND_HALF_UP);
                    } else {
                        decimalValue = decimalValue.setScale(2, BigDecimal.ROUND_HALF_UP);
                    }
                    return decimalValue.toString();

                case Types.VARCHAR:
                case Types.CHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.NCHAR:
                case Types.LONGNVARCHAR:
                    return generateStringValue(column, rowIndex, isPrimaryKey);

                case Types.DATE:
                    LocalDate date = LocalDate.now()
                            .minusDays(random.nextInt(365 * 5))
                            .plusDays(random.nextInt(30));
                    return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

                case Types.TIMESTAMP:
                case Types.TIME:
                case Types.TIME_WITH_TIMEZONE:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    LocalDateTime datetime = LocalDateTime.now()
                            .minusDays(random.nextInt(365 * 2))
                            .plusHours(random.nextInt(24))
                            .plusMinutes(random.nextInt(60));
                    return datetime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

                case Types.BOOLEAN:
                case Types.BIT:
                    return random.nextBoolean() ? "1" : "0";

                case Types.BLOB:
                case Types.CLOB:
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    // 对于二进制类型，生成简单的base64编码数据
                    return generateBinaryData(columnSize);

                default:
                    // 默认处理：根据类型名称
                    return handleByTypeName(typeName, column, rowIndex, isPrimaryKey);
            }
        } catch (Exception e) {
            log.warn("生成字段 {} 的值失败，使用默认值", column.getColumnName(), e);
            return generateDefaultValue(column);
        }
    }

    private String generateStringValue(ColumnInfo column, int rowIndex, boolean isPrimaryKey) {
        int maxLength = column.getColumnSize();
        if (maxLength <= 0) {
            maxLength = 50; // 默认长度
        }

        // 如果是主键，生成唯一值
        if (isPrimaryKey) {
            return "PK_" + column.getColumnName().toLowerCase() + "_" + (rowIndex + 1);
        }

        // 根据字段名猜测内容类型
        String columnName = column.getColumnName().toLowerCase();

        if (columnName.contains("name") || columnName.contains("title")) {
            return generateName(maxLength);
        } else if (columnName.contains("email")) {
            return generateEmail(maxLength);
        } else if (columnName.contains("phone") || columnName.contains("tel")) {
            return generatePhoneNumber();
        } else if (columnName.contains("address")) {
            return generateAddress(maxLength);
        } else if (columnName.contains("url") || columnName.contains("website")) {
            return generateUrl(maxLength);
        } else if (columnName.contains("description") || columnName.contains("remark")) {
            return generateDescription(maxLength);
        } else if (columnName.contains("code") || columnName.contains("no") || columnName.contains("number")) {
            return generateCode(columnName, maxLength, rowIndex);
        } else {
            // 默认生成随机字符串
            return generateRandomString(maxLength);
        }
    }

    private String handleByTypeName(String typeName, ColumnInfo column, int rowIndex, boolean isPrimaryKey) {
        if (typeName.contains("INT")) {
            if (isPrimaryKey) {
                return String.valueOf(rowIndex + 1);
            }
            return String.valueOf(random.nextInt(10000));
        } else if (typeName.contains("CHAR") || typeName.contains("TEXT")) {
            return generateStringValue(column, rowIndex, isPrimaryKey);
        } else if (typeName.contains("DECIMAL") || typeName.contains("NUMERIC") ||
                typeName.contains("FLOAT") || typeName.contains("DOUBLE")) {
            BigDecimal value = BigDecimal.valueOf(random.nextDouble() * 10000)
                    .setScale(2, BigDecimal.ROUND_HALF_UP);
            return value.toString();
        } else if (typeName.contains("DATE") || typeName.contains("TIME")) {
            LocalDate date = LocalDate.now()
                    .minusDays(random.nextInt(365 * 2))
                    .plusDays(random.nextInt(30));
            return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        } else if (typeName.contains("BOOL")) {
            return random.nextBoolean() ? "1" : "0";
        } else {
            return generateDefaultValue(column);
        }
    }

    private String generateDefaultValue(ColumnInfo column) {
        // 生成简单的默认值
        String columnName = column.getColumnName().toLowerCase();
        if (columnName.contains("id") || columnName.contains("no") || columnName.contains("code")) {
            return "DEFAULT_" + columnName.toUpperCase() + "_" + random.nextInt(1000);
        } else if (columnName.contains("date") || columnName.contains("time")) {
            return LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        } else if (columnName.contains("amount") || columnName.contains("price") ||
                columnName.contains("salary") || columnName.contains("cost")) {
            return String.format("%.2f", random.nextDouble() * 10000);
        } else {
            return "测试数据_" + columnName + "_" + random.nextInt(1000);
        }
    }

    // ========== 生成具体类型数据的辅助方法 ==========

    private String generateName(int maxLength) {
        String[] firstNames = {"张", "王", "李", "赵", "刘", "陈", "杨", "黄", "周", "吴"};
        String[] lastNames = {"伟", "芳", "娜", "秀英", "敏", "静", "丽", "强", "磊", "军"};

        String name = firstNames[random.nextInt(firstNames.length)] +
                lastNames[random.nextInt(lastNames.length)];

        // 如果长度超过限制，截断
        if (name.length() > maxLength) {
            return name.substring(0, maxLength);
        }
        return name;
    }

    private String generateEmail(int maxLength) {
        String[] domains = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com"};
        String username = "user" + random.nextInt(10000);
        String email = username + "@" + domains[random.nextInt(domains.length)];

        if (email.length() > maxLength) {
            return email.substring(0, maxLength);
        }
        return email;
    }

    private String generatePhoneNumber() {
        String[] prefixes = {"130", "131", "132", "133", "134", "135", "136", "137", "138", "139",
                "150", "151", "152", "153", "155", "156", "157", "158", "159",
                "170", "171", "172", "173", "174", "175", "176", "177", "178", "179",
                "180", "181", "182", "183", "184", "185", "186", "187", "188", "189"};
        String prefix = prefixes[random.nextInt(prefixes.length)];
        String suffix = String.format("%08d", random.nextInt(100000000));
        return prefix + suffix.substring(0, 8);
    }

    private String generateAddress(int maxLength) {
        String[] provinces = {"北京市", "上海市", "广州市", "深圳市", "杭州市", "南京市", "武汉市", "成都市", "西安市", "重庆市"};
        String[] districts = {"朝阳区", "海淀区", "浦东新区", "福田区", "西湖区", "鼓楼区", "江汉区", "武侯区", "雁塔区", "渝中区"};
        String[] streets = {"中山路", "人民路", "解放路", "建设路", "和平路", "新华路", "胜利路", "文化路", "青年路", "东风路"};

        String address = provinces[random.nextInt(provinces.length)] +
                districts[random.nextInt(districts.length)] +
                streets[random.nextInt(streets.length)] +
                (random.nextInt(200) + 1) + "号";

        if (address.length() > maxLength) {
            return address.substring(0, maxLength);
        }
        return address;
    }

    private String generateUrl(int maxLength) {
        String[] protocols = {"http://", "https://"};
        String[] domains = {"example", "test", "demo", "sample", "mock"};
        String[] tlds = {".com", ".cn", ".net", ".org", ".io"};

        String url = protocols[random.nextInt(protocols.length)] +
                domains[random.nextInt(domains.length)] +
                tlds[random.nextInt(tlds.length)] +
                "/path/" + random.nextInt(1000);

        if (url.length() > maxLength) {
            return url.substring(0, maxLength);
        }
        return url;
    }

    private String generateDescription(int maxLength) {
        String[] adjectives = {"优秀的", "高效的", "可靠的", "创新的", "专业的", "智能的", "安全的", "稳定的", "灵活的", "易用的"};
        String[] nouns = {"产品", "服务", "系统", "方案", "平台", "工具", "应用", "软件", "框架", "组件"};
        String[] verbs = {"提供", "实现", "支持", "优化", "提升", "保障", "简化", "加速", "增强", "改善"};

        StringBuilder desc = new StringBuilder();
        int sentences = random.nextInt(3) + 1;

        for (int i = 0; i < sentences; i++) {
            desc.append(adjectives[random.nextInt(adjectives.length)])
                    .append(nouns[random.nextInt(nouns.length)])
                    .append(verbs[random.nextInt(verbs.length)])
                    .append(adjectives[random.nextInt(adjectives.length)])
                    .append("功能。");

            if (desc.length() > maxLength) {
                break;
            }
        }

        if (desc.length() > maxLength) {
            return desc.substring(0, maxLength);
        }
        return desc.toString();
    }

    private String generateCode(String columnName, int maxLength, int rowIndex) {
        if (columnName.contains("id")) {
            return "ID_" + String.format("%06d", rowIndex + 1);
        } else if (columnName.contains("no")) {
            return "NO_" + String.format("%08d", random.nextInt(100000000));
        } else if (columnName.contains("code")) {
            String prefix = columnName.toUpperCase().replace("_CODE", "").replace("CODE", "");
            if (prefix.length() > 3) {
                prefix = prefix.substring(0, 3);
            }
            return prefix + "_" + String.format("%05d", random.nextInt(100000));
        } else {
            return "CODE_" + String.format("%06d", rowIndex + 1);
        }
    }

    private String generateRandomString(int maxLength) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        int length = random.nextInt(maxLength / 2) + maxLength / 4;
        if (length < 1) length = 1;
        if (length > maxLength) length = maxLength;

        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(characters.charAt(random.nextInt(characters.length())));
        }
        return sb.toString();
    }

    private String generateBinaryData(int size) {
        if (size <= 0) {
            size = 100;
        }
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    // ========== 高级功能方法 ==========

    public void truncateAndInsert(DataSourceConfig dataSourceConfig, String tableName, int recordCount) {
        truncateAndInsert(dataSourceConfig, tableName, recordCount, null);
    }

    public void truncateAndInsert(DataSourceConfig dataSourceConfig, String tableName,
                                  int recordCount, Map<String, Object> customConfig) {

        log.info("清空表 {} 并插入新数据", tableName);

        try {
            // 1. 加载插件
            String pluginName = dataSourceConfig.getPluginName();
            AbstractDataSourcePlugin plugin = pluginManager.getDataSourcePlugin(pluginName);
            BaseDataSourceDTO dto = DataSourcePluginManager.createDataSourceDTO(dataSourceConfig);

            // 2. 清空表
            String truncateSql = getTruncateSql(plugin, tableName);
            plugin.executeUpdate(dto, truncateSql);
            log.info("表 {} 已清空", tableName);

            // 3. 插入数据
            insertMockData(dataSourceConfig, tableName, recordCount, customConfig);

        } catch (Exception e) {
            log.error("清空并插入数据失败", e);
            throw new RuntimeException("清空并插入数据失败: " + e.getMessage(), e);
        }
    }

    private String getTruncateSql(AbstractDataSourcePlugin plugin, String tableName) {
        // 根据插件类型生成不同的清空表语句
        return "TRUNCATE TABLE " + tableName;
    }

    public void insertWithCustomGenerator(DataSourceConfig dataSourceConfig, String tableName,
                                          int recordCount, Map<String, ColumnValueGenerator> customGenerators) {

        log.info("使用自定义生成器插入数据: 表={}, 记录数={}", tableName, recordCount);

        try {
            // 1. 加载插件
            String pluginName = dataSourceConfig.getPluginName();
            AbstractDataSourcePlugin plugin = pluginManager.getDataSourcePlugin(pluginName);

            // 2. 获取表结构
            BaseDataSourceDTO dto = DataSourcePluginManager.createDataSourceDTO(dataSourceConfig);
            List<ColumnInfo> columns = plugin.getColumns(dto, tableName);

            if (columns == null || columns.isEmpty()) {
                throw new RuntimeException("无法获取表结构信息: " + tableName);
            }

            // 3. 过滤掉自增字段
            List<ColumnInfo> insertColumns = columns.stream()
                    .filter(col -> !"YES".equalsIgnoreCase(col.getIsAutoincrement()))
                    .collect(Collectors.toList());

            List<String> fieldNames = insertColumns.stream()
                    .map(ColumnInfo::getColumnName)
                    .collect(Collectors.toList());

            // 4. 使用自定义生成器生成数据
            List<List<String>> data = new ArrayList<>();

            for (int i = 0; i < recordCount; i++) {
                List<String> row = new ArrayList<>();

                for (ColumnInfo column : insertColumns) {
                    String columnName = column.getColumnName();
                    ColumnValueGenerator generator = customGenerators.get(columnName);

                    if (generator != null) {
                        // 使用自定义生成器
                        row.add(generator.generate(column, i));
                    } else {
                        // 使用默认生成器
                        row.add(generateColumnValue(column, i, null));
                    }
                }

                data.add(row);
            }

            // 5. 插入数据
            InsertDataDTO insertDataDTO = new InsertDataDTO(fieldNames, data);
            insertDataDTO.setBaseDataSourceDTO(dto);
            insertDataDTO.setTableName(tableName);
            insertDataDTO.setBatchSize(1000);
            insertDataDTO.setTruncate(false);

            plugin.insertData(insertDataDTO);
            log.info("成功插入 {} 条记录到表 {}", recordCount, tableName);

        } catch (Exception e) {
            log.error("使用自定义生成器插入数据失败", e);
            throw new RuntimeException("使用自定义生成器插入数据失败: " + e.getMessage(), e);
        }
    }

    public void shutdown() {
        pluginManager.clearCache();
        log.info("MockDataInserter已关闭");
    }

    // ========== 接口定义 ==========

    @FunctionalInterface
    public interface ColumnValueGenerator {
        String generate(ColumnInfo column, int rowIndex);
    }

    // ========== 使用示例 ==========

    public static void main(String[] args) {
        // 示例：插入测试数据到users表

        // 1. 配置数据源
        Configuration connectionConfig = Configuration.newDefault();
        connectionConfig.set("host", "localhost");
        connectionConfig.set("port", "3306");
        connectionConfig.set("database", "test_db");
        connectionConfig.set("username", "root");
        connectionConfig.set("password", "password");

        Map<String, String> otherParams = new HashMap<>();
        otherParams.put("useSSL", "false");
        otherParams.put("serverTimezone", "UTC");
        connectionConfig.set("other", otherParams);

        DataSourceConfig dataSourceConfig = new DataSourceConfig();
        dataSourceConfig.setPluginName("mysql-source");
        dataSourceConfig.setConnectionConfig(connectionConfig);

        // 2. 创建插入器
        MockDataInserter inserter = new MockDataInserter();

        try {
            // 3. 插入100条测试数据
            inserter.insertMockData(dataSourceConfig, "users", 100);

            // 4. 使用自定义配置插入数据
            Map<String, Object> customConfig = new HashMap<>();
            customConfig.put("status", "ACTIVE");
            customConfig.put("type", "USER");

            inserter.insertMockData(dataSourceConfig, "orders", 50, customConfig);

            System.out.println("Mock数据插入完成！");

        } catch (Exception e) {
            System.err.println("Mock数据插入失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            inserter.shutdown();
        }
    }
}