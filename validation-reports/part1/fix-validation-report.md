# 流式一致性 / 流式融合修复验证报告

## 1. 背景

本次验证针对两类真实问题展开：

1. 一致性比对在 `ConsistencyExample` 场景下，缺失于目标库 `users_1` 的记录 `user_id=7, username=wujiu` 没有被自动补插。
2. 融合在 `FusionExample` 场景下，首次运行存在 `No operations allowed after connection closed`，表现为作业失败或不稳定。

本轮修复完成后，按示例场景重新回归，直接以：

- 结果文件
- 运行日志
- 目标库数据

三类证据判定修复是否生效。

## 2. 修复内容

### 2.1 一致性修复

涉及文件：

- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/UpdateExecutor.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/BaseConflictResolver.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/HighConfidenceResolver.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/MajorityVoteResolver.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/WeightedAverageResolver.java`
- `core/src/test/java/com/jdragon/aggregation/core/consistency/service/UpdateExecutorTest.java`

修复点：

1. `HighConfidenceResolver`、`MajorityVoteResolver`、`WeightedAverageResolver` 对缺失源的判断不再依赖“整行是否全 null”，而是优先使用 `DifferenceRecord.missingSources`。
2. `UpdateExecutor.determineOperationType(...)` 增加防御逻辑：
   - 若 `winningSource` 指向缺失源，但仍存在其他真实来源，则不再误判为 `DELETE/UPDATE`。
   - 支持优先使用显式 `operationType`。
3. 增加回归单测，覆盖“winningSource 错指缺失源但真实记录存在”的场景。

### 2.2 融合修复

涉及文件：

- `data-source-plugins/data-source-handler-rdbms/src/main/java/com/jdragon/aggregation/datasource/rdbms/DatasourceCache.java`
- `data-source-plugins/data-source-handler-rdbms/src/test/java/com/jdragon/aggregation/datasource/rdbms/DatasourceCacheTest.java`
- `data-source-plugins/data-source-handler-rdbms/pom.xml`

修复点：

1. `DatasourceCache.get(...)` 从“`getIfPresent -> create -> put`”改为 Caffeine 原子 `get(key, mappingFunction)`。
2. 消除并行扫描同一 JDBC 配置时重复创建 `HikariDataSource` 的竞态。
3. 避免后创建的数据源覆盖前一个实例，并通过 removal listener 提前关闭正在使用的连接池。
4. 增加并发单测，验证并发访问只生成一个 `DataSource` 实例。

## 3. 编译与单测验证

执行命令：

```bash
mvn -q -pl core,data-source-plugins/data-source-handler-rdbms -am test -DfailIfNoTests=false
```

结果：

- `core` 模块测试全部通过
- `data-source-handler-rdbms` 模块新增并发缓存测试通过

## 4. 一致性场景验证

### 4.1 测试前基线

基线清理：

```sql
DELETE FROM users_1 WHERE user_id = 7 AND username = 'wujiu';
```

测试前查询：

```sql
SELECT user_id, username, age, salary, department, email FROM users_1 ORDER BY user_id;
```

测试前结果：

- `users_1` 共 6 行
- 不存在 `user_id=7 / username=wujiu`

### 4.2 运行方式

通过与 `ConsistencyExample` 同配置的反射辅助程序执行：

```bash
java -cp <classpath> validation.ExampleOps consistency <aggregation.home> <outputDir>
```

输出目录：

- `consistency-results-fix`

日志文件：

- `validation-reports/logs/consistency-fix.log`

### 4.3 关键运行证据

日志关键片段：

- `Generated 1 SQL statements for target data source source-1`
- `Update execution completed: 1 successful, 0 failed, 0 skipped`

最终返回结果：

```json
{
  "status":"PARTIAL_SUCCESS",
  "totalRecords":7,
  "consistentRecords":0,
  "inconsistentRecords":7,
  "resolvedRecords":7,
  "updateResult":{
    "targetSourceId":"source-1",
    "totalUpdates":1,
    "successfulUpdates":1,
    "failedUpdates":0,
    "insertCount":1,
    "updateCount":0,
    "deleteCount":0,
    "skipCount":6
  }
}
```

修复后的 `resolutions_20260327_133249.json` 中，`wujiu` 记录已经变为：

- `operationType = INSERT`
- `winningSource = source-3`
- `resolvedValues = { user_id=7, username=wujiu, age=27, salary=11500, department=Marketing, email=wujiu@company.com }`

### 4.4 目标库核验

执行：

```sql
SELECT * FROM users_1 WHERE username='wujiu' OR user_id=7;
```

结果：

```json
[{
  "user_id":7,
  "username":"wujiu",
  "age":27,
  "salary":11500.00,
  "department":"Marketing",
  "email":"wujiu@company.com"
}]
```

结论：

- 修复前：缺失记录未插入
- 修复后：缺失记录成功插入，值与解决结果一致

一致性问题已修复。

## 5. 融合场景验证

### 5.1 测试前基线

基线清理：

```sql
TRUNCATE TABLE fusion_3;
```

测试前查询：

```sql
SELECT COUNT(*) AS cnt FROM fusion_3;
```

结果：

```json
[{"cnt":0}]
```

### 5.2 运行方式

通过与 `FusionExample` 等效的配置执行：

```bash
java -cp <classpath> validation.ExampleOps fusion <aggregation.home> <configPath> <detailDir>
```

详情输出目录：

- `fusion_details_fix`

日志文件：

- `validation-reports/logs/fusion-fix.log`

### 5.3 关键运行证据

本次运行中：

- 未再出现 `No operations allowed after connection closed`
- `JobContainer` 最终状态为 `SUCCEEDED`
- `summary.totalRecords = 7`
- `summary.successfulRecords = 7`
- `summary.errorRecords = 0`

日志关键片段：

- 两个并行扫描线程同时使用同一个连接池 key
- 作业最终输出：`completed it's job. status is SUCCEEDED`

### 5.4 目标库核验

执行：

```sql
SELECT * FROM fusion_3 ORDER BY user_id;
```

结果共 7 行，核心值如下：

```json
[
  {"user_id":1,"username":"zhangsan","age":31,"salary":15000.00,"dept":"Technology","email":"zhangsan@company.com","double_salary":30000.00,"ste":"zhangsan@company.com_test","condition":"31_old","constant":"用户","constant_int":123},
  {"user_id":2,"username":"lisi","age":28,"salary":12500.00,"dept":"Marketing","email":"lisi@company.com","double_salary":25000.00,"ste":"lisi@company.com_test","condition":"28_young","constant":"用户","constant_int":123},
  {"user_id":3,"username":"wangwu","age":35,"salary":18000.00,"dept":"Sales","email":"wangwu@company.com","double_salary":36000.00,"ste":"wangwu@company.com_test","condition":"35_old","constant":"用户","constant_int":123},
  {"user_id":4,"username":"zhaoliu","age":32,"salary":16000.00,"dept":"HR","email":"zhaoliu@company.com","double_salary":32000.00,"ste":"zhaoliu@company.com_test","condition":"32_old","constant":"用户","constant_int":123},
  {"user_id":5,"username":"sunqi","age":29,"salary":13000.00,"dept":"Finance","email":"sunqi@company.com","double_salary":26000.00,"ste":"sunqi@company.com_test","condition":"29_young","constant":"用户","constant_int":123},
  {"user_id":6,"username":"zhouba","age":31,"salary":14200.00,"dept":"Technology","email":"zhouba@company.com","double_salary":28400.00,"ste":"zhouba@company.com_test","condition":"31_old","constant":"用户","constant_int":123},
  {"user_id":7,"username":"huangsan","age":32,"double_salary":0.00,"ste":"no_email_test","condition":"32_old","constant":"用户","constant_int":123}
]
```

说明：

- `user_id=7` 在 `fusion_2` 缺失，因此 `salary/dept/email` 为空是符合 `LEFT JOIN + LENIENT` 预期的。
- `double_salary=0.00`、`ste=no_email_test`、`condition=32_old` 与配置中的表达式和条件逻辑一致。

结论：

- 修复前：首次运行可能连接池被提前关闭，作业失败
- 修复后：并行扫描稳定，作业成功，目标表结果正确

融合问题已修复。

## 6. 最终结论

本轮两个问题均已完成修复并通过示例场景验证：

1. 一致性自动修复现在可以正确补插目标库缺失记录。
2. 融合流式扫描中的 JDBC 连接池竞态已消除，`FusionExample` 可以稳定成功执行。

## 7. 证据文件

- 一致性运行日志：`validation-reports/logs/consistency-fix.log`
- 融合运行日志：`validation-reports/logs/fusion-fix.log`
- 一致性结果目录：`consistency-results-fix`
- 融合详情目录：`fusion_details_fix`
- 本报告：`validation-reports/fix-validation-report.md`