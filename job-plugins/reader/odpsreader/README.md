# ODPS Reader 分区读取

`autoPartitionScan` 为可选布尔参数，默认 `false`。使用 Tunnel 读取分区表时，配置该开关可以在不指定 `partitionSpec` 的情况下采集全部分区。

```json
{
  "readMode": "tunnel",
  "table": "orders",
  "partitionSpec": "",
  "autoPartitionScan": true,
  "columns": ["*"],
  "includePartitionColumns": true,
  "offset": 0,
  "maxRows": 0
}
```

以上字段放入现有作业的 `reader.config`，连接信息继续使用原有 `connect` 配置。

## 动态函数

`partitionSpec` 和 `selectSql` 在 Reader 准备执行时复用质量规则 SQL 模板底层的 `DynamicFunctionResolver`，使用相同的严格解析规则。支持 `$getCurrentTime`、`$getTheMonthLastDay`、`$getTimeUnitValue`、`$subStr`、`$subString`、`$toLower` 和 `$toUpper`。

时间基准在本次 Reader 准备执行时取一次，所有函数使用同一基准，沿用 Worker 系统时间及默认时区。每次重新执行任务重新计算，不以原定调度时间计算。

Tunnel 指定分区示例：

```text
dt='$getCurrentTime("yyyyMMdd", "-1d")'
```

SQL 模式示例：

```sql
select * from orders where dt = '$getCurrentTime("yyyyMMdd", "-1d")'
```

动态函数先于分区条件解析执行，因此函数参数中的逗号可以正常使用。动态函数返回字符串时，SQL 或分区条件中的引号仍需由配置保留。未知函数、参数错误和不完整表达式会在任务准备阶段失败；原始配置不会被改写，任务重试时会按新的执行时间重新计算。

SQL 模式没有配置 `selectSql` 时，先替换 `partitionSpec`，再生成 SQL；配置了 `selectSql` 时仅处理该 SQL，忽略 `partitionSpec`。`auto` 模式沿用原有模式选择。MaxCompute 原生 SQL 函数由 MaxCompute 执行；本次接入仅替换 `$函数名(...)`，不增加 `${bizdate}` 等变量、函数嵌套或分区范围表达式。

本次动态函数接入只需升级 ODPS Reader 插件，Worker 运行时需已有带 `DynamicFunctionResolver` 的 aggregation commons；现有 release_1106/release_1112 基线构建已包含该类。此项能力无需新增数据库迁移或修改前端。

## 分区读取行为

- 显式配置 `partitionSpec` 时优先读取指定分区，不自动枚举。SQL 模式沿用原有逻辑，自动遍历开关仅作用于 Tunnel。
- 未指定分区时，Reader 探查表类型；普通表直接读取，分区表需要开启自动遍历，否则明确报错。
- 自动遍历通过 SDK 分页取得全部分区，按表分区字段顺序规范化并按分区规格排序，再串行读取。没有分区数量上限；内存中仅保留规格清单，记录流仍逐条输出。
- `offset` 在所有分区之间全局跳过记录，`maxRows` 限制全局输出条数；`offset=0/maxRows=0` 读取全部枚举分区。分区内记录顺序由 Tunnel 下载会话决定，不能将偏移量用作跨次执行的持久化游标。
- 正常枚举得到空集合时成功输出 0 条；元数据枚举或任一分区读取失败时作业失败，已写入目标端的数据不回滚。
- 本次读取使用枚举完成后的分区清单；下一次执行重新枚举。该清单不提供跨分区数据快照，也不提供断点续传。

Studio 通过 ODPS Reader 参数元数据显示“自动遍历全部分区”开关，复用现有 `readerOptions` 保存与执行配置组装。先升级 Worker 的 ODPS Reader 插件，再更新 Studio 参数元数据。已有任务默认关闭；目标端使用更新写入时，需要确保业务唯一键能够区分应保留的跨分区记录。

## 验证与上线

使用 JDK 17，在 DataAggregation 根目录执行 Reader 回归：

```powershell
mvn -pl job-plugins/reader/odpsreader -am '-Dtest=DynamicFunctionResolverTest,OdpsReaderTest,OdpsReaderPartitionTest' '-Dsurefire.failIfNoSpecifiedTests=false' test
```

分区回归使用本地 HTTP 服务驱动实际 SDK 的表元数据、分页迭代器和下载会话，记录流注入可观测的测试实现；不访问真实 ODPS。Studio 的元数据升级检查入口为 `data-aggregation-studio/backend/scripts/test-studio-incremental-migrations.mjs`，使用独立 MySQL 容器验证新安装、存量升级与重复执行。

存量 MySQL 环境执行 `data-aggregation-studio/backend/studio-server/src/main/resources/update/20261112/20261112-odps-auto-partition-scan.sql`；该增量只补缺失字段，保留当前字段定制、逻辑删除和历史版本。SQLite / 本地环境沿用现有 Java 参数初始化。无需重新构建前端。

部署后使用独立 ODPS → MySQL 测试任务验收：

1. 准备源数据稳定的分区表，记录全部分区及源端总条数；目标表使用独立测试表，并配置能够区分跨分区记录的唯一键。
2. 在 Studio 参数表单开启“自动遍历全部分区”，清空 `partitionSpec`，设置 `offset=0`、`maxRows=0`，保存后重新打开确认开关回显。
3. 执行任务，核对日志中的分区总数、各分区输出及 Reader 总输出，确认覆盖本次枚举的所有分区。
4. 核对目标记录及更新模式；Reader 输出条数和目标最终行数可能因唯一键合并而不同，不能只以最终行数判断漏采。
5. 已有生产任务维持默认关闭，仅对需要全分区采集的任务显式启用。
