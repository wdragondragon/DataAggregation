# ConsistencyReader 配置参数说明

本文档只说明 `consistencyreader` 在 `JobContainer` 场景下可配置的参数，重点覆盖：

- 是否必填
- 默认值
- 配置后实际效果
- 推荐值
- 当前版本的兼容性注意事项

结论先说在前面：

- 推荐入口写法：`reader.type = consistency`
- `consistencyreader` 当前真正读取的是 `reader.config`
- 部分字段虽然能被解析，但在当前 reader 实现里并不会真正生效，文档里已单独标明

## 1. 推荐入口

推荐配置骨架：

```json
{
  "reader": {
    "type": "consistency",
    "config": {
      "ruleId": "user-check",
      "ruleName": "用户主数据一致性校验",
      "matchKeys": ["user_id"],
      "compareFields": ["username", "age", "department"],
      "dataSources": []
    }
  }
}
```

说明：

- `JobContainer` 会把 `reader.type` 拼成插件目录名去加载，所以这里应写 `consistency`，最终会加载 `consistencyreader`
- `job-plugins/reader/consistencyreader/src/main/resources/plugin_job_template.json` 里仍然写着 `consistency-sortmerge`，这是历史模板残留，不建议继续照抄

## 2. 顶层参数

`reader.config` 顶层参数如下。

| 路径 | 类型 | 是否必填 | 默认值 | 配置效果 | 建议值 |
| --- | --- | --- | --- | --- | --- |
| `ruleId` | string | 是 | 无 | 规则唯一标识；会进入结果文件名、报告名、writer 输出 | 用稳定英文 ID，如 `user-master-check` |
| `ruleName` | string | 否 | `name`，再退化为 `ruleId` | 规则显示名；主要用于日志和报告 | 写中文业务名，便于排查 |
| `name` | string | 否 | 无 | `ruleName` 的兼容别名 | 只在兼容旧配置时使用 |
| `description` | string | 否 | 无 | 规则说明；仅用于展示和结果元数据 | 建议填写一句业务背景 |
| `enabled` | boolean | 否 | `true` | 当前 `consistencyreader` 不会因为它是 `false` 而跳过执行 | 保留默认即可，不要依赖它停任务 |
| `parallelFetch` | boolean | 否 | `true` | 当前 `consistencyreader` 解析该参数，但执行链路未实际使用 | 保留默认即可 |
| `toleranceThreshold` | double | 否 | `0.0` | 传入比较器，但当前字段差异判断仍是严格相等，容差逻辑未真正接入 | 先按 `0.0` 理解，别依赖它做数值容差 |
| `conflictResolutionStrategy` | enum | 否 | `HIGH_CONFIDENCE` | 决定差异如何生成 `resolutionResult`，并影响自动修复计划 | 主从场景优先用 `HIGH_CONFIDENCE` |
| `resolutionParams` | object | 条件必填 | 空 | 给冲突解决器透传参数；主要给 `CUSTOM_RULE` 使用 | 只有自定义策略时再配 |
| `compareFields` | array<string> | 强烈建议填写 | 空数组 | 指定真正参与比对的字段；不填时只会发现“缺源”，不会做字段差异比对 | 明确列出需要比较的业务字段 |
| `matchKeys` | array<string> | 是 | 空数组 | 一致性匹配键；决定如何把多源记录对齐 | 必须是稳定业务主键或联合唯一键 |
| `join.keys` | array<string> | 否 | 无 | `matchKeys` 的兼容别名 | 新配置请统一写 `matchKeys` |
| `dataSources` | array<object> | 是 | 空数组 | 参与一致性校验的数据源列表 | 至少 2 个源 |
| `sources` | array<object> | 否 | 无 | `dataSources` 的兼容别名 | 新配置请统一写 `dataSources` |
| `outputConfig` | object | 否 | 见下文 | 控制差异文件、resolution 文件、HTML 报告输出 | 测试环境建议开启报告 |
| `cache` | object | 否 | 见下文 | 控制 spill 分区、临时文件等 | 默认可用，大数据量再调 |
| `performance` | object | 否 | 见下文 | 控制内存预算；部分并发字段当前未生效 | 通常只需要调 `memoryLimitMB` |
| `adaptiveMerge` | object | 否 | 见下文 | 控制 sort-merge 自适应窗口和 spill 策略 | 建议保留启用 |
| `updateTargetSourceId` | string | 条件必填 | 无 | 自动修复写回到哪个 `sourceId` | 只在 `autoApplyResolutions=true` 时填写 |
| `autoApplyResolutions` | boolean | 否 | `false` | 是否把解决结果自动回写到目标源 | 生产环境谨慎开启 |
| `validateBeforeUpdate` | boolean | 否 | `false` | 回写前是否先校验目标记录是否存在 | 生产环境建议 `true` |
| `updateBufferSize` | int | 否 | `1024` | 批量回写的缓冲条数 | 常见取值 `200 ~ 1000` |
| `updateRetryAttempts` | int | 否 | `0` | 回写失败时重试次数 | 常见取值 `1 ~ 3` |
| `updateRetryDelayMs` | long | 否 | `1000` | 首次重试等待毫秒数 | 常见取值 `1000 ~ 5000` |
| `updateRetryBackoffMultiplier` | double | 否 | `1.5` | 每次重试后的退避倍率 | 常见取值 `1.5 ~ 2.0` |
| `allowInsert` | boolean | 否 | `true` | 自动修复时是否允许补插目标端缺失数据 | 生产环境若只允许修正不允许补数，可设 `false` |
| `allowDelete` | boolean | 否 | `true` | 自动修复时是否允许删除目标端多余数据 | 生产环境通常建议先设 `false` |
| `skipUnchangedUpdates` | boolean | 否 | `true` | 回写前若目标值已一致，则跳过无效更新 | 建议保持 `true` |

## 3. `dataSources[]` 参数

每个数据源对象支持如下参数。

| 路径 | 类型 | 是否必填 | 默认值 | 配置效果 | 建议值 |
| --- | --- | --- | --- | --- | --- |
| `dataSources[].sourceId` | string | 是 | 无 | 数据源唯一标识；会参与报告、冲突记录、自动修复 | 用短且稳定的英文 ID，如 `master`/`slave` |
| `dataSources[].id` | string | 否 | 无 | `sourceId` 的兼容别名 | 新配置请统一写 `sourceId` |
| `dataSources[].sourceName` | string | 否 | `sourceId` | 展示名称 | 写业务可读名，如“主库” |
| `dataSources[].name` | string | 否 | 无 | `sourceName` 的兼容别名 | 兼容旧配置时再用 |
| `dataSources[].pluginName` | string | 是 | 无 | 数据源插件名，例如 `mysql8`、`postgres`、`minio` | 写实际 source 插件名 |
| `dataSources[].type` | string | 否 | 无 | `pluginName` 的兼容别名 | 新配置请统一写 `pluginName` |
| `dataSources[].connectionConfig` | object | 条件必填 | 无 | 连接配置；数据库/文件源都会使用 | 建议统一写这个键 |
| `dataSources[].config` | object | 条件必填 | 无 | `connectionConfig` 的兼容别名 | 兼容旧配置时再用 |
| `dataSources[].connect` | object | 条件必填 | 无 | `connectionConfig` 的兼容别名 | 兼容旧配置时再用 |
| `dataSources[].querySql` | string | 条件必填 | 无 | 直接指定读取 SQL；文件源场景下会被当成文件路径 | 数据库源优先推荐写它 |
| `dataSources[].selectSql` | string | 否 | 无 | `querySql` 的兼容别名 | 新配置请统一写 `querySql` |
| `dataSources[].table` | string | 条件必填 | 无 | 未提供 `querySql` 时自动拼 `SELECT * FROM table`；自动修复目标源也依赖它生成 DML | 作为目标修复源时务必填写 |
| `dataSources[].tableName` | string | 否 | 无 | `table` 的兼容别名 | 新配置请统一写 `table` |
| `dataSources[].confidenceWeight` | double | 否 | `1.0` | 高可信度/加权平均/多数投票等策略的权重输入 | 主源可设 `1.0 ~ 2.0`，次源 `0.5 ~ 0.9` |
| `dataSources[].priority` | int | 否 | `0` | 权重相同或策略需要时的次级优先级，值越大越优先 | 主源高于备源，如 `10`/`5` |
| `dataSources[].maxRecords` | int | 否 | 无 | 限制单源扫描条数；文件源按条截断，数据库源会尝试追加 `LIMIT n` | 仅测试抽样时使用 |
| `dataSources[].fieldMappings` | map<string,string> | 否 | 空 | 把源字段名映射到内部统一字段名；回写时也会反向用于列名转换 | 字段名不一致时必须配 |
| `dataSources[].extConfig` | object | 否 | 空对象 | 扩展参数；文件源会透传给 `readFile`，数据库源会并入 `extraParams` | 文件源建议把格式等放这里 |
| `dataSources[].updateTarget` | boolean | 否 | `false` | 当前会被解析，但执行链路没有直接使用 | 无需依赖 |

### 3.1 `connectionConfig` 常见子参数

以下字段会被 `consistencyreader` 直接抽取并写入 `BaseDataSourceDTO`。

| 路径 | 类型 | 是否必填 | 默认值 | 配置效果 | 建议值 |
| --- | --- | --- | --- | --- | --- |
| `connectionConfig.host` | string | 条件必填 | 无 | 数据库主机地址 | 填实际地址 |
| `connectionConfig.port` | string/int | 条件必填 | 无 | 数据库端口 | 填实际端口 |
| `connectionConfig.database` | string | 条件必填 | 无 | 数据库名/库名 | 填实际库名 |
| `connectionConfig.username` | string | 条件必填 | 无 | 用户名 | 与插件实际要求保持一致 |
| `connectionConfig.userName` | string | 条件必填 | 无 | `username` 的兼容别名 | 新配置优先写 `userName` 或 `username` 二选一即可 |
| `connectionConfig.password` | string | 条件必填 | 无 | 密码 | 建议走密文或外部注入 |
| `connectionConfig.usePool` | boolean | 否 | `false` | 是否启用连接池 | 数据库源建议 `true` |
| `connectionConfig.jdbcUrl` | string | 否 | 无 | 自定义 JDBC URL | 有特殊连接需求时填写 |
| `connectionConfig.driverClassName` | string | 否 | 无 | 驱动类名 | 自定义驱动时填写 |
| `connectionConfig.extraParams` | object | 否 | 空 | 透传到 DTO 的 `extraParams` | JDBC 附加参数写这里 |
| `connectionConfig.other` | string/object | 否 | 无 | 透传到 DTO 的 `other` 字段；对象会转成 JSON 字符串 | 历史插件兼容参数写这里 |

说明：

- 数据库型 source 插件实际拿到的是 `BaseDataSourceDTO`，所以额外数据库参数建议优先放在 `extraParams` 或 `other`
- 文件型 source（如 `ftp`、`sftp`、`hdfs`、`minio`）会直接使用整段 `connectionConfig`

## 4. `outputConfig` 参数

| 路径 | 类型 | 是否必填 | 默认值 | 配置效果 | 建议值 |
| --- | --- | --- | --- | --- | --- |
| `outputConfig.outputPath` | string | 否 | 最终回退到 `./consistency-results` | 差异 JSON、resolution JSON、HTML 报告输出目录 | 建议按环境/规则单独分目录 |
| `outputConfig.generateReport` | boolean | 否 | `true` | 是否生成 HTML 报告 | 联调阶段 `true`，批量定时任务可按需关闭 |
| `outputConfig.reportLanguage` | enum | 否 | `ENGLISH` | 报告语言，支持 `ENGLISH` / `CHINESE` / `BILINGUAL` | 中文团队建议 `CHINESE` |
| `outputConfig.maxDifferencesToDisplay` | int | 否 | `100` | HTML 报告中最多展示多少条差异 | 常见取值 `100 ~ 500` |

## 5. `cache` 参数

| 路径 | 类型 | 是否必填 | 默认值 | 配置效果 | 建议值 |
| --- | --- | --- | --- | --- | --- |
| `cache.partitionCount` | int | 否 | `16` | spill 顶层分区数；也会影响 adaptive overflow 默认分区数 | 数据量大时可升到 `32` 或 `64` |
| `cache.rebalancePartitionMultiplier` | int | 否 | `4` | 分区过大时再均衡的倍数 | 通常保持 `4` |
| `cache.spillPath` | string | 否 | 无 | spill 临时文件目录 | 建议指向本地 SSD、空间充足目录 |
| `cache.keepTempFiles` | boolean | 否 | `false` | 执行结束后是否保留临时 spill 文件 | 排查问题时临时设 `true` |

## 6. `performance` 参数

| 路径 | 类型 | 是否必填 | 默认值 | 配置效果 | 建议值 |
| --- | --- | --- | --- | --- | --- |
| `performance.parallelSourceCount` | int | 否 | `1` | 当前 `consistencyreader` 会解析但未实际用于执行并发控制 | 无需依赖 |
| `performance.memoryLimitMB` | int | 否 | `512` | sort-merge 内存预算来源，也会影响 `adaptiveMerge.pendingMemoryMB` 默认值 | 常见取值 `512`、`1024`、`2048` |

补充：

- 运行时最小会按 `64MB` 做保护
- 当前最值得调的是 `memoryLimitMB`，不是 `parallelSourceCount`

## 7. `adaptiveMerge` 参数

| 路径 | 类型 | 是否必填 | 默认值 | 配置效果 | 建议值 |
| --- | --- | --- | --- | --- | --- |
| `adaptiveMerge.enabled` | boolean | 否 | `true` | 是否启用自适应 sort-merge | 建议保持开启 |
| `adaptiveMerge.pendingKeyThreshold` | int | 否 | `4096` | 内存窗口最多保留多少个待决 key | 大 key 基数场景可适度调大 |
| `adaptiveMerge.pendingMemoryMB` | int | 否 | `max(64, performance.memoryLimitMB / 2)` | 内存窗口的预算大小 | 通常让它跟随默认推导即可 |
| `adaptiveMerge.overflowPartitionCount` | int | 否 | `cache.partitionCount`，若未配 cache 则为 `16` | overflow spill 的初始分区数 | 建议与 `cache.partitionCount` 保持一致 |
| `adaptiveMerge.rebalancePartitionMultiplier` | int | 否 | `4` | overflow 分区继续扩容时的倍数 | 通常保持 `4` |
| `adaptiveMerge.overflowSpillPath` | string | 否 | `cache.spillPath` | overflow spill 目录 | 不配时继承 `cache.spillPath` 即可 |
| `adaptiveMerge.preferOrderedQuery` | boolean | 否 | `true` | 尝试把源查询改写成带 `ORDER BY` 的包装 SQL，降低 pending window 压力 | 数据库源建议保持 `true` |
| `adaptiveMerge.maxSpillBytesMB` | int | 否 | `512` | spill 总写入预算上限 | 大任务可适当调大 |
| `adaptiveMerge.minFreeDiskMB` | int | 否 | `256` | 最低剩余磁盘空间保护阈值 | 至少高于操作系统安全线 |
| `adaptiveMerge.keyTypes.<field>` | enum | 否 | `AUTO` | 显式指定匹配键类型，影响 key 排序与比较 | 键类型稳定时建议显式配置 |

`adaptiveMerge.keyTypes.<field>` 当前只接受：

- `AUTO`
- `STRING`
- `NUMBER`
- `DATETIME`

建议：

- 数字主键如 `user_id` 用 `NUMBER`
- 日期时间键用 `DATETIME`
- 复杂编码或混合字符键用 `STRING`

## 8. 冲突解决策略建议

### 8.1 `HIGH_CONFIDENCE`

默认策略，最适合“主源明确、其他源服从主源”的场景。

建议：

- 主源 `confidenceWeight` 最高
- 权重相同再用 `priority` 拉开
- 如果要自动修复，从库要修谁，`updateTargetSourceId` 就指向谁，不要指向权威源本身

### 8.2 `WEIGHTED_AVERAGE`

适合数值字段融合。当前实现里：

- 数值字段会按 `confidenceWeight` 做加权平均
- 非数值字段会退回“取最常见值”

建议：

- 只在数值冲突占主导时使用
- 小数精度要求高时先自行评估下游字段类型

### 8.3 `MAJORITY_VOTE`

适合 3 个及以上来源的离散值判定。

建议：

- 数据源数量至少 3 个时更有意义
- 若 2 个源冲突，它会退化为“谁票数/权重高选谁”

### 8.4 `CUSTOM_RULE`

如果用自定义解决器，配置上应写：

```json
{
  "conflictResolutionStrategy": "CUSTOM_RULE",
  "resolutionParams": {
    "customClass": "com.example.CustomResolver"
  }
}
```

或者：

```json
{
  "conflictResolutionStrategy": "CUSTOM_RULE",
  "resolutionParams": {
    "strategyName": "MY_STRATEGY"
  }
}
```

说明：

- 配置层依然必须写 `CUSTOM_RULE`
- 不能把 `conflictResolutionStrategy` 直接写成任意自定义名字，因为当前配置解析先走枚举 `valueOf`

### 8.5 `MANUAL_REVIEW`

适合只出报告、不自动改数的场景。

注意：

- 即便 `autoApplyResolutions=true`，该策略生成的更新计划也是 `SKIP`

### 8.6 `NO_RESOLVE`

只记录差异，不生成解决结果。

注意：

- 没有 `resolutionResult` 时，自然也无法做自动修复

## 9. 自动修复参数组合建议

如果要启用自动修复，至少同时检查这几项：

| 参数 | 是否建议 | 原因 |
| --- | --- | --- |
| `autoApplyResolutions=true` | 必须 | 开启自动回写 |
| `updateTargetSourceId` | 必须 | 指定回写目标源 |
| `dataSources[].table` | 目标源必须填写 | `UpdateExecutor` 需要它生成 `INSERT/UPDATE/DELETE` |
| `matchKeys` | 必须稳定且唯一 | 回写 SQL 的 `WHERE` 依赖它 |
| `validateBeforeUpdate=true` | 生产建议 | 降低误更新/误删除风险 |
| `allowDelete=false` | 生产建议起步值 | 先避免误删 |
| `skipUnchangedUpdates=true` | 强烈建议 | 避免无效写入 |

推荐起步组合：

```json
{
  "autoApplyResolutions": true,
  "validateBeforeUpdate": true,
  "allowInsert": true,
  "allowDelete": false,
  "skipUnchangedUpdates": true,
  "updateBufferSize": 500,
  "updateRetryAttempts": 2,
  "updateRetryDelayMs": 2000,
  "updateRetryBackoffMultiplier": 2.0
}
```

## 10. Writer 侧可用输出列

`consistencyreader` 最终投给 writer 的列名支持如下几类：

- `rule_id`
- `record_id`
- `match_keys`
- `conflict_type`
- `differences`
- `payload`

说明：

- 如果 writer 配了 `columns`，reader 会按列名逐列投影
- 如果 writer 没配 `columns`，reader 会退化为只输出一列 `payload`，内容是整个 `DifferenceRecord` 的 JSON

## 11. 当前版本的重要坑位

下面这些点非常值得在项目里显式备注。

### 11.1 `reader.type` 请写 `consistency`

原因：

- `JobContainer` 会把它拼成 `consistencyreader`
- 模板里的 `consistency-sortmerge` 是旧值，直接照抄会尝试加载不存在的 `consistency-sortmergereader`

### 11.2 `enabled=false` 当前不会阻止 reader 执行

原因：

- `DataConsistencyService` 会检查它
- 但 `SortMergeConsistencyReader` 入口不会检查它

### 11.3 `parallelFetch` 当前没有实际执行效果

原因：

- 当前 reader 走的是 `AdaptiveSortMergeConsistencyExecutor + OrderedSourceCursor`
- 该链路没有消费 `parallelFetch`

### 11.4 `toleranceThreshold` 当前没有真正参与字段差异判定

原因：

- 比较器里虽然保留了数值容差辅助方法
- 但当前字段比对实际仍走严格相等判断

### 11.5 `performance.parallelSourceCount` 当前也基本无效

原因：

- 参数会被读入 `StreamExecutionOptions`
- 但当前 consistency sort-merge 代码没有实际用它控制并发数

### 11.6 历史键名 `performanceConfig` / `adaptiveMergeConfig` 当前不会被读取

正确写法应为：

- `performance`
- `adaptiveMerge`

### 11.7 `adaptiveMerge.keyTypes` 的枚举值不是旧文档里的 `LONG/DOUBLE/DATE`

当前真实可用值只有：

- `AUTO`
- `STRING`
- `NUMBER`
- `DATETIME`

### 11.8 `maxRecords` 对数据库源会直接补 `LIMIT n`

影响：

- 对 MySQL / PostgreSQL 这类兼容 `LIMIT` 的数据库通常没问题
- 对 Oracle 等不兼容 `LIMIT` 的数据库，建议不要依赖 `maxRecords`，而是在 `querySql` 里自己写原生分页/限流语句

### 11.9 `dataSources[].updateTarget` 当前没有直接执行效果

它会被解析并带到扫描配置里，但当前执行链路没有基于这个字段做逻辑分支。

## 12. 最小推荐示例

```json
{
  "reader": {
    "type": "consistency",
    "config": {
      "ruleId": "user-master-check",
      "ruleName": "用户主数据一致性校验",
      "description": "比较主库与从库的用户基础信息",
      "conflictResolutionStrategy": "HIGH_CONFIDENCE",
      "matchKeys": ["user_id"],
      "compareFields": ["username", "age", "department", "email"],
      "dataSources": [
        {
          "sourceId": "master",
          "sourceName": "主库",
          "pluginName": "mysql8",
          "connectionConfig": {
            "host": "127.0.0.1",
            "port": "3306",
            "database": "agg_test",
            "userName": "root",
            "password": "password",
            "usePool": true
          },
          "querySql": "select user_id, username, age, department, email from users_master",
          "confidenceWeight": 1.0,
          "priority": 10
        },
        {
          "sourceId": "slave",
          "sourceName": "从库",
          "pluginName": "mysql8",
          "connectionConfig": {
            "host": "127.0.0.1",
            "port": "3306",
            "database": "agg_test",
            "userName": "root",
            "password": "password",
            "usePool": true
          },
          "querySql": "select user_id, username, age, department, email from users_slave",
          "confidenceWeight": 0.8,
          "priority": 5
        }
      ],
      "outputConfig": {
        "outputPath": "./consistency-results",
        "generateReport": true,
        "reportLanguage": "CHINESE",
        "maxDifferencesToDisplay": 200
      },
      "cache": {
        "partitionCount": 16,
        "rebalancePartitionMultiplier": 4,
        "keepTempFiles": false
      },
      "performance": {
        "memoryLimitMB": 1024
      },
      "adaptiveMerge": {
        "enabled": true,
        "preferOrderedQuery": true,
        "keyTypes": {
          "user_id": "NUMBER"
        }
      }
    }
  }
}
```

## 13. 推荐实践总结

- `matchKeys` 一定要选业务唯一键；不填会把所有记录压成同一组，结果失真
- `compareFields` 一定要显式列出；不填几乎只会看到“缺源”，看不到字段不一致
- 主从修复场景优先用 `HIGH_CONFIDENCE`
- 自动修复先从 `allowDelete=false` 开始
- 数据库量大时先调 `performance.memoryLimitMB`、`cache.partitionCount`
- 非 MySQL 类数据库做抽样时优先手写 `querySql`，不要盲配 `maxRecords`

