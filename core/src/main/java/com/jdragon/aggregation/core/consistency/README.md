# 数据一致性对比系统

基于DataAggregation插件架构的数据一致性对比系统，支持多数据源数据一致性检查和冲突解决。

## 架构设计

系统采用服务层架构，不依赖于reader-transformer-writer管道模式，主要组件包括：

### 核心组件
1. **DataSourcePluginManager** - 数据源插件管理器
   - 加载和管理数据源插件
   - 插件缓存和生命周期管理

2. **ConsistencyRuleManager** - 规则管理器
   - 一致性规则的CRUD操作
   - 规则存储和加载

3. **DataFetcher** - 数据获取器
   - 从多个数据源并行获取数据
   - 支持字段映射和SQL查询

4. **DataComparator** - 数据比较器
   - 基于匹配键分组比较数据
   - 支持容错阈值和差异检测

5. **ConflictResolver** - 冲突解决器
   - 高可信度源策略
   - 加权平均值策略
   - 多数投票策略
   - 自定义规则策略
   - 人工审核策略

6. **ResultRecorder** - 结果记录器
   - 对比结果记录
   - 差异数据存储
   - 冲突解决结果保存
   - HTML报告生成

7. **DataConsistencyService** - 主服务
   - 协调整个对比流程
   - 规则执行调度
   - 错误处理和日志记录

## 使用示例

### 1. 创建数据一致性服务
```java
DataConsistencyService service = new DataConsistencyService("./consistency-results");
```

### 2. 定义一致性规则
```java
ConsistencyRule rule = new ConsistencyRule();
rule.setRuleId("rule-001");
rule.setRuleName("用户数据一致性检查");
rule.setDescription("检查来自不同系统的用户基本信息一致性");
rule.setEnabled(true);
rule.setToleranceThreshold(0.01);
rule.setConflictResolutionStrategy(ConflictResolutionStrategy.HIGH_CONFIDENCE);

// 设置比较字段和匹配键
rule.setCompareFields(Arrays.asList("age", "salary", "department"));
rule.setMatchKeys(Arrays.asList("user_id", "username"));

// 配置数据源
List<DataSourceConfig> dataSources = Arrays.asList(
    createDataSourceConfig("source-1", "主数据库", "mysql-source", ...),
    createDataSourceConfig("source-2", "备份数据库", "mysql-source", ...)
);
rule.setDataSources(dataSources);

// 添加规则
service.addRule(rule);
```

### 3. 执行一致性检查
```java
ComparisonResult result = service.executeRule("rule-001");

System.out.println("Status: " + result.getStatus());
System.out.println("Total Records: " + result.getTotalRecords());
System.out.println("Inconsistent Records: " + result.getInconsistentRecords());
System.out.println("Resolved Records: " + result.getResolvedRecords());
```

### 4. 查看结果
- 对比结果JSON文件: `comparison_result_rule-001_时间戳.json`
- 差异数据文件: `differences_时间戳.json`
- 解决结果文件: `resolutions_时间戳.json`
- HTML报告: `consistency_report_rule-001_时间戳.html`

## 数据源插件支持

系统通过DataAggregation的插件机制支持多种数据源：

### 已支持的数据源类型
- MySQL/MariaDB
- PostgreSQL
- Oracle
- SQL Server
- DM (达梦数据库)
- Hive
- ClickHouse

### 插件配置示例
```java
Configuration connectionConfig = new Configuration();
connectionConfig.set("host", "localhost");
connectionConfig.set("port", "3306");
connectionConfig.set("database", "test_db");
connectionConfig.set("username", "root");
connectionConfig.set("password", "password");

DataSourceConfig config = new DataSourceConfig();
config.setSourceId("mysql-1");
config.setSourceName("生产MySQL");
config.setPluginName("mysql-source");  // 插件名称
config.setConnectionConfig(connectionConfig);
config.setQuerySql("SELECT * FROM users");
config.setConfidenceWeight(1.0);
config.setPriority(1);
```

## 冲突解决策略

### 1. 高可信度源 (HIGH_CONFIDENCE)
- 选择置信权重最高的数据源
- 权重相同时选择优先级高的源
- 适用于有明显主从关系的数据源

### 2. 加权平均值 (WEIGHTED_AVERAGE)
- 对数值字段计算加权平均值
- 权重来自数据源的confidenceWeight配置
- 适用于数值型数据的融合

### 3. 多数投票 (MAJORITY_VOTE)
- 选择出现次数最多的值
- 支持权重投票（加权多数）
- 适用于分类数据或枚举值

### 4. 自定义规则 (CUSTOM_RULE)
- 通过实现ConflictResolver接口自定义逻辑
- 在resolutionParams中指定自定义类名

### 5. 人工审核 (MANUAL_REVIEW)
- 标记需要人工处理的冲突
- 生成待审核清单

## 配置参数

### 一致性规则配置
| 参数 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| ruleId | String | 规则唯一标识 | 必填 |
| ruleName | String | 规则名称 | 必填 |
| description | String | 规则描述 | 可选 |
| enabled | boolean | 是否启用 | true |
| parallelFetch | boolean | 是否并行获取数据 | true |
| toleranceThreshold | double | 数值字段容错阈值 | 0.0 |
| conflictResolutionStrategy | enum | 冲突解决策略 | HIGH_CONFIDENCE |
| compareFields | List<String> | 需要比较的字段 | 必填 |
| matchKeys | List<String> | 数据匹配键 | 必填 |

### 数据源配置
| 参数 | 类型 | 说明 |
|------|------|------|
| sourceId | String | 数据源唯一标识 |
| sourceName | String | 数据源名称 |
| pluginName | String | 数据源插件名称 |
| connectionConfig | Configuration | 连接配置 |
| querySql | String | 查询SQL |
| confidenceWeight | double | 置信权重(0.0-1.0) |
| priority | int | 优先级(数字越大优先级越低) |
| fieldMappings | Map<String,String> | 字段映射关系 |

### 输出配置
| 参数 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| outputType | enum | 输出类型 (FILE, DATABASE, MEMORY) | FILE |
| outputPath | String | 输出文件路径 | 必填 |
| storeDifferences | boolean | 是否存储差异数据 | true |
| storeResolutionResults | boolean | 是否存储解决结果 | true |
| generateReport | boolean | 是否生成报告 | true |
| reportFormat | enum | 报告格式 (JSON, HTML, CSV) | JSON |
| reportLanguage | enum | 报告语言 (ENGLISH, CHINESE, BILINGUAL) | ENGLISH |

## 多语言报告

系统支持三种报告语言模式：

### 1. 英文报告 (ENGLISH)
- 所有界面文本使用英文
- 适合国际团队或英文环境
- 默认报告语言

### 2. 中文报告 (CHINESE)
- 所有界面文本使用中文
- 适合中文用户环境
- 使用中文字体优化显示

### 3. 双语报告 (BILINGUAL)
- 同时显示英文和中文文本
- 格式：英文 / 中文
- 适合跨国团队或需要双语对照的场景

### 4. HTML 报告模板
系统使用 Freemarker 模板引擎生成 HTML 报告，提供更灵活的样式和布局控制。模板位于 `src/main/resources/templates/consistency_report.ftl`。

#### 新特性
1. **字段差异详细显示**：每个字段的差异现在显示所有数据源的具体值，格式为 `字段名: {数据源1: 值1, 数据源2: 值2, ...}`
2. **解决后数据行**：冲突解决后的完整数据行现在以 JSON 格式显示在报告中，便于下游系统使用
3. **改进的样式**：新增 `.resolved-row` 和 `.field-diff` CSS 类，提供更好的视觉区分
4. **模板回退机制**：如果 Freemarker 模板不可用，系统会自动回退到字符串拼接方式生成报告

#### 自定义模板
您可以通过修改 `consistency_report.ftl` 文件来自定义报告样式和布局。模板支持所有标准 Freemarker 语法和数据模型。

### 配置示例
```java
OutputConfig outputConfig = new OutputConfig();
outputConfig.setOutputType(OutputConfig.OutputType.FILE);
outputConfig.setOutputPath("./consistency-results");
outputConfig.setGenerateReport(true);
outputConfig.setReportFormat(OutputConfig.ReportFormat.HTML);
outputConfig.setReportLanguage(OutputConfig.ReportLanguage.CHINESE); // 设置为中文报告
```

## 扩展开发

### 添加新的冲突解决策略
1. 实现`ConflictResolver`接口
2. 在`ConflictResolverFactory`中注册
3. 通过`resolutionParams`传递自定义参数

```java
public class CustomResolver extends BaseConflictResolver {
    @Override
    public ResolutionResult resolve(DifferenceRecord differenceRecord) {
        // 自定义解决逻辑
    }
    
    @Override
    public ConflictResolutionStrategy getStrategy() {
        return ConflictResolutionStrategy.CUSTOM_RULE;
    }
}
```

### 自定义结果记录器
1. 实现`ResultRecorder`接口
2. 支持不同的输出目标（数据库、消息队列等）

```java
public class DatabaseResultRecorder implements ResultRecorder {
    @Override
    public void recordComparisonResult(ComparisonResult result) {
        // 保存到数据库
    }
}
```

## 注意事项

1. **插件依赖**: 确保所需的数据源插件已正确安装和配置
2. **性能考虑**: 大数据量时考虑分页查询和分批处理
3. **错误处理**: 单个数据源失败不影响其他数据源的对比
4. **内存管理**: 大结果集考虑流式处理或分块处理
5. **并发安全**: 服务本身是线程安全的，支持并发执行多个规则

## 故障排查

### 常见问题
1. **插件加载失败**: 检查插件路径和配置文件
2. **数据源连接失败**: 检查网络连接和认证信息
3. **字段映射错误**: 确认字段名大小写和类型匹配
4. **内存溢出**: 调整查询分页大小或增加JVM内存

### 日志查看
系统使用SLF4J记录日志，关键日志级别：
- INFO: 规则执行进度和结果摘要
- DEBUG: 详细数据对比过程
- ERROR: 错误和异常信息