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

// 配置自动更新（可选）
rule.setUpdateTargetSourceId("source-1"); // 更新到主数据库
rule.setAutoApplyResolutions(true); // 自动应用解决结果
rule.setUpdateBufferSize(50); // 批量更新大小
rule.setUpdateRetryAttempts(3); // 更新失败时重试3次
rule.setUpdateRetryDelayMs(2000L); // 初始重试延迟2秒
rule.setUpdateRetryBackoffMultiplier(2.0); // 退避乘数

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
- 支持三种注册方式：SPI自动发现、编程式注册、配置式注册（向后兼容）
- 在resolutionParams中指定customClass参数（配置式注册）
- 可通过 `strategyName` 参数使用插件注册的自定义策略名
- 详情参见"插件化冲突解决架构"章节

### 5. 人工审核 (MANUAL_REVIEW)
- 标记需要人工处理的冲突
- 生成待审核清单

## 自动更新与重试

系统支持将冲突解决结果自动更新到目标数据源，并包含智能重试机制：

### 1. 自动更新配置
- **目标数据源**：指定需要更新的数据源ID（`updateTargetSourceId`）
- **自动应用**：启用/禁用自动更新（`autoApplyResolutions`）
- **批量大小**：控制批量更新的缓冲区大小（`updateBufferSize`）

### 2. 重试机制
- **重试次数**：配置更新失败时的重试次数（`updateRetryAttempts`）
- **初始延迟**：第一次重试前的等待时间（`updateRetryDelayMs`）
- **退避乘数**：每次重试后延迟时间的增长倍数（`updateRetryBackoffMultiplier`）

### 3. 执行流程
1. 冲突解决完成后，系统检查是否启用自动更新
2. 构建针对目标数据源的UPDATE语句（使用匹配键作为WHERE条件）
3. 尝试批量更新，失败时回退到单条更新
4. 单条更新失败时，根据配置进行重试
5. 记录更新结果（成功/失败统计）并包含在报告中

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
| conflictResolutionStrategy | String | 冲突解决策略（支持内置枚举或插件注册的自定义策略名） | HIGH_CONFIDENCE |
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

### 更新配置
| 参数 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| updateTargetSourceId | String | 目标数据源ID（用于自动更新） | 无 |
| autoApplyResolutions | boolean | 是否自动应用解决结果到目标数据源 | false |
| updateBufferSize | Integer | 批量更新缓冲区大小 | 100 |
| updateRetryAttempts | Integer | 更新重试次数 | 0 |
| updateRetryDelayMs | Long | 重试初始延迟毫秒数 | 1000 |
| updateRetryBackoffMultiplier | Double | 重试退避乘数 | 1.5 |

### 输出配置
| 参数 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| outputPath | String | 输出文件路径 | 必填 |
| generateReport | boolean | 是否生成报告 | true |
| reportLanguage | enum | 报告语言 (ENGLISH, CHINESE, BILINGUAL) | ENGLISH |
| maxDifferencesToDisplay | Integer | HTML报告中最大显示差异数量 | 100 |

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
1. **表格化字段差异显示**：每个差异记录现在以表格形式显示，包含匹配键和所有字段的值，差异字段高亮显示
2. **字段差异高亮**：差异字段在表格中使用黄色背景突出显示，表头也相应高亮
3. **解决后数据行**：冲突解决后的完整数据行现在以 JSON 格式显示在报告中，便于下游系统使用
4. **改进的样式**：新增 `.resolved-row`、`.field-diff`、`.field-with-diff`、`.field-with-diff-header` CSS 类，提供更好的视觉区分
5. **可配置的显示限制**：通过 `maxDifferencesToDisplay` 参数控制HTML报告中显示的最大差异数量，避免页面过大
6. **模板回退机制**：如果 Freemarker 模板不可用，系统会自动回退到字符串拼接方式生成报告

#### 自定义模板
您可以通过修改 `consistency_report.ftl` 文件来自定义报告样式和布局。模板支持所有标准 Freemarker 语法和数据模型。

### 配置示例
```java
OutputConfig outputConfig = new OutputConfig();
outputConfig.setOutputPath("./consistency-results");
outputConfig.setGenerateReport(true);
outputConfig.setReportLanguage(OutputConfig.ReportLanguage.CHINESE); // 设置为中文报告
outputConfig.setMaxDifferencesToDisplay(50); // 限制HTML报告中最多显示50条差异
```

## 扩展开发

### 插件化冲突解决架构

系统支持插件化冲突解决架构，允许从外部注册自定义冲突解决器而无需修改核心代码。提供三种注册方式：

1. **SPI自动发现**：实现`ConflictResolverProvider`接口，通过Java ServiceLoader自动发现
2. **编程式注册**：运行时调用`ConflictResolverRegistry.registerResolverClass()`或`registerResolverInstance()`
3. **配置式注册**：使用`CUSTOM_RULE`策略并在`resolutionParams`中指定`customClass`参数（向后兼容）

#### 核心组件
- **ConflictResolverRegistry**：冲突解决器注册中心，管理所有已注册的解决器
- **ConflictResolverProvider**：SPI接口，用于外部模块提供自定义解决器
- **ConflictResolverFactory**：工厂类，提供向后兼容的API

### 添加新的冲突解决策略

#### 方法一：SPI自动发现（推荐）
1. 实现`ConflictResolverProvider`接口
2. 在`META-INF/services/com.jdragon.aggregation.core.consistency.service.plugin.ConflictResolverProvider`文件中注册实现类
3. 系统启动时自动加载

```java
public class MyResolverProvider implements ConflictResolverProvider {
    @Override
    public String getStrategyName() {
        return "MY_STRATEGY";  // 自定义策略名称
    }
    
    @Override
    public String getDescription() {
        return "我的自定义解决策略";
    }
    
    @Override
    public ConflictResolver createResolver(List<DataSourceConfig> dataSourceConfigs,
                                          Map<String, Object> resolutionParams) {
        return new MyCustomResolver(dataSourceConfigs, resolutionParams);
    }
}

// 在 META-INF/services 文件中添加：
// com.example.MyResolverProvider
```

#### 方法二：编程式注册
1. 实现`ConflictResolver`接口
2. 在应用程序启动时注册到`ConflictResolverRegistry`

```java
// 注册类（延迟初始化）
ConflictResolverRegistry.getInstance().registerResolverClass(
    "MY_STRATEGY", 
    "com.example.MyCustomResolver"
);

// 或注册实例（立即初始化）
ConflictResolver resolver = new MyCustomResolver();
ConflictResolverRegistry.getInstance().registerResolverInstance(
    "MY_STRATEGY", 
    resolver
);
```

#### 方法三：配置式注册（向后兼容）
1. 实现`ConflictResolver`接口
2. 在规则配置中使用`CUSTOM_RULE`策略，并在`resolutionParams`中指定`customClass`参数

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

// 在规则配置中：
rule.setConflictResolutionStrategy(ConflictResolutionStrategy.CUSTOM_RULE);
Map<String, Object> params = new HashMap<>();
params.put("customClass", "com.example.CustomResolver");
rule.setResolutionParams(params);
```

#### 内置策略
系统已内置以下策略，无需额外注册：
- `HIGH_CONFIDENCE`：高可信度源策略
- `WEIGHTED_AVERAGE`：加权平均值策略  
- `MAJORITY_VOTE`：多数投票策略
- `MANUAL_REVIEW`：人工审核策略

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
