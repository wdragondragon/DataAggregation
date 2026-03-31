# Fusion / Consistency 配置参考

本文档只列出当前代码里仍然保留、且有明确用途的 fusion / consistency 相关配置。

适用入口：

- `reader.type = fusion-sortmerge`
- `reader.type = consistency-sortmerge`

说明：

- `writer` 仍然是 DataAggregation 的通用 writer 配置，本文只在示例 JSON 里给出最小可读示例，不展开所有 writer 插件参数。
- 文中的“默认值”以当前代码解析逻辑为准。
- 文中的“作用范围”会区分 `sort-merge` 与旧的 `streaming` 路径，避免把共享模型字段误认为当前 reader 一定生效。

## 已删除的预留字段

这批字段已经从模型或示例中移除，不再建议继续使用：

- fusion: `sources[].isPrimary`
- fusion: `cache.enabled`
- fusion: `cache.maxSize`
- fusion: `cache.type`
- fusion: `cache.externalStorageType`
- fusion: `performance.batchSize`
- fusion: `performance.enableLazyLoading`
- fusion: `fieldStrategies`
- consistency: `cache.maxSize`
- consistency: `performance.batchSize`
- consistency: `outputConfig.outputType`
- consistency: `outputConfig.databaseTable`
- consistency: `outputConfig.databaseConnection`
- consistency: `outputConfig.storeDifferences`
- consistency: `outputConfig.storeResolutionResults`
- consistency: `outputConfig.reportFormat`

## 一、Fusion Sort-Merge

### 1. 作业结构

```json
{
  "reader": {
    "type": "fusion-sortmerge",
    "config": {
    }
  },
  "writer": {
  }
}
```

### 2. `reader.config` 顶层配置

| 路径 | 类型 | 默认值 | 作用 |
|------|------|--------|------|
| `sources` | array | 必填 | 参与融合的数据源列表。 |
| `join.keys` | array<string> | 必填 | 融合匹配键。 |
| `join.type` | enum | `INNER` | 连接方式，支持 `INNER` / `LEFT` / `RIGHT` / `FULL`。 |
| `joinKeys` | array<string> | 无 | `join.keys` 的兼容别名。 |
| `joinType` | enum | 无 | `join.type` 的兼容别名。 |
| `fieldMappings` | array | 建议配置 | 目标字段生成规则。为空时会退回 legacy 融合逻辑。 |
| `defaultStrategy` | string | `WEIGHTED_AVERAGE` | 默认融合策略名。 |
| `errorMode` | enum | `LENIENT` | 全局错误处理模式，支持 `STRICT` / `LENIENT` / `MIXED`。 |
| `errorModes.fieldOverrides.<targetField>` | map | 无 | 单个目标字段的错误模式覆盖。 |
| `cache.partitionCount` | int | `10` | 旧 streaming spill 分区数；同时作为 adaptive overflow 的默认分区数来源。 |
| `cache.rebalancePartitionMultiplier` | int | `4` | rebalance 子分区倍率，子层分区数会严格大于父层。 |
| `performance.parallelSourceCount` | int | `2` | 旧 `StreamingFusionExecutor` 的并行扫描度；`fusion-sortmerge` 当前不直接消费它。 |
| `performance.memoryLimitMB` | int | `1024` | 内存预算，驱动 sort-merge pending 窗口和 `maxKeysPerPartition` 估算。 |
| `adaptiveMerge` | object | 见下表 | 自适应 sort-merge 主配置。 |
| `detailConfig` | object | 见下表 | 融合详情落盘配置。 |

### 3. `sources[]` 配置

| 路径 | 类型 | 默认值 | 作用 |
|------|------|--------|------|
| `sources[].id` | string | 必填 | 数据源唯一标识。 |
| `sources[].name` | string | 同 `id` | 展示名称。 |
| `sources[].type` | string | 必填 | 数据源插件类型，如 `mysql8`。 |
| `sources[].config` / `sources[].connect` | object | 必填 | 插件连接参数。 |
| `sources[].querySql` | string | 无 | 直接指定查询 SQL。 |
| `sources[].selectSql` | string | 无 | `querySql` 的兼容别名。 |
| `sources[].table` / `sources[].tableName` | string | 无 | 不写 `querySql` 时可用表名自动拼 SQL。 |
| `sources[].columns` | array<string> | `*` | 自动拼 SQL 时的列清单。 |
| `sources[].weight` | double | `1.0` | 融合权重。 |
| `sources[].priority` | int | `0` | 优先级策略使用。 |
| `sources[].confidence` | double | `1.0` | 高可信度策略使用。 |
| `sources[].maxRecords` | int | 无 | 限制该 source 最多扫描的记录数。 |
| `sources[].fieldMappings` | map<string,string> | 空 | 源字段名到内部字段名的映射。 |
| `sources[].extConfig` | object | 空 | 扩展参数透传。 |
| `sources[].incrColumn` | string | 无 | 增量字段。 |
| `sources[].incrModel` | string | `>` | 增量比较符。 |
| `sources[].pkValue` | string | 无 | 与 `incrColumn` 配合，拼接增量 where 条件。 |

### 4. `fieldMappings[]` 通用字段

| 路径 | 类型 | 默认值 | 作用 |
|------|------|--------|------|
| `fieldMappings[].type` | enum | `DIRECT` | 映射类型：`DIRECT` / `EXPRESSION` / `CONDITIONAL` / `CONSTANT` / `GROOVY`。 |
| `fieldMappings[].targetField` | string | 必填 | 输出目标字段名。 |
| `fieldMappings[].resultType` | string | 类型相关 | 结果列类型。 |
| `fieldMappings[].errorMode` | string | `DEFAULT` | 单字段错误处理模式。 |
| `fieldMappings[].strategy` | string | 无 | 单字段融合策略，优先级高于 `defaultStrategy`。 |

各类型专属字段：

| 类型 | 额外字段 | 作用 |
|------|----------|------|
| `DIRECT` | `sourceField` | 直接取值，支持 `${sourceId.field}` 或 `${field}`。 |
| `EXPRESSION` | `expression` | Janino 表达式求值。 |
| `CONDITIONAL` | `condition` / `trueValue` / `falseValue` | 条件表达式分支。 |
| `CONSTANT` | `value` | 固定值。 |
| `GROOVY` | `script` | Groovy 脚本求值。 |

### 5. `adaptiveMerge` 配置

| 路径 | 类型 | 默认值 | 作用 |
|------|------|--------|------|
| `adaptiveMerge.enabled` | boolean | `true` | 是否启用 adaptive sort-merge。 |
| `adaptiveMerge.pendingKeyThreshold` | int | `4096` | coordinator 内存 pending key 上限。 |
| `adaptiveMerge.pendingMemoryMB` | int | `max(64, memoryLimitMB / 2)` | pending 窗口内存预算。 |
| `adaptiveMerge.overflowPartitionCount` | int | `cache.partitionCount` | overflow 顶层分区数。 |
| `adaptiveMerge.rebalancePartitionMultiplier` | int | `4` | overflow rebalance 的子分区倍率。 |
| `adaptiveMerge.overflowSpillPath` | string | 无 | overflow 分区文件目录。 |
| `adaptiveMerge.preferOrderedQuery` | boolean | `true` | 优先改写 source 查询为带 `ORDER BY`，用于降低 pending window 压力，不再作为正确性前提。 |
| `adaptiveMerge.maxSpillBytesMB` | int | `512` | spill guard 最大累计写入预算。 |
| `adaptiveMerge.minFreeDiskMB` | int | `256` | spill guard 最小剩余磁盘空间。 |
| `adaptiveMerge.keyTypes.<field>` | map | 空 | 显式指定匹配键类型，值可为 `STRING` / `LONG` / `DOUBLE` / `DATE` / `AUTO` 等。 |

### 6. `detailConfig` 配置

| 路径 | 类型 | 默认值 | 作用 |
|------|------|--------|------|
| `detailConfig.enabled` | boolean | `false`（整个节不存在时）/ `true`（节存在但未写时） | 是否记录融合详情。 |
| `detailConfig.savePath` | string | `./fusion_details` | 详情 JSON 保存目录或完整文件名。 |
| `detailConfig.samplingRate` | double | `1.0` | 采样率，范围 `0.0 ~ 1.0`。 |
| `detailConfig.maxRecords` | int | `10000` | 最多保留多少条详情。 |
| `detailConfig.includeSourceData` | boolean | `false` | 是否带源数据快照。 |
| `detailConfig.includeFieldDetails` | boolean | `false` | 是否输出字段级详情。 |
| `detailConfig.saveHtml` | boolean | `true` | 是否同时生成 HTML 浏览页。 |
| `detailConfig.fileNamePattern` | string | `fusion_details_{timestamp}.json` | 文件名模板。 |

## 二、Consistency Sort-Merge

### 1. 作业结构

```json
{
  "reader": {
    "type": "consistency-sortmerge",
    "config": {
    }
  },
  "writer": {
  }
}
```

### 2. `reader.config` 顶层配置

| 路径 | 类型 | 默认值 | 作用 |
|------|------|--------|------|
| `ruleId` | string | 必填 | 规则唯一标识。 |
| `ruleName` | string | `name` 或 `ruleId` | 规则显示名称。 |
| `name` | string | 无 | `ruleName` 兼容别名。 |
| `description` | string | 无 | 规则说明。 |
| `enabled` | boolean | `true` | 旧 `DataConsistencyService` 会检查此字段。 |
| `parallelFetch` | boolean | `true` | 是否并行拉取 source 数据。 |
| `toleranceThreshold` | double | `0.0` | 数值字段比较容差。 |
| `conflictResolutionStrategy` | string | `HIGH_CONFIDENCE` | 冲突解决策略。 |
| `resolutionParams` | object | 空 | 传给 resolver 的扩展参数。 |
| `compareFields` | array<string> | 空 | 要比较的字段列表。 |
| `matchKeys` | array<string> | 空 | 一致性匹配键。 |
| `join.keys` | array<string> | 空 | `matchKeys` 的兼容别名。 |
| `dataSources` / `sources` | array | 必填 | 参与对比的数据源列表。 |
| `outputConfig` | object | 见下表 | 对比结果落盘与报告配置。 |
| `cache` | object | 见下表 | spill / 分区配置。 |
| `performance` | object | 见下表 | 内存和并发参数。 |
| `adaptiveMerge` | object | 见上文 | 与 fusion 共用同一套 adaptive merge 配置。 |
| `updateTargetSourceId` | string | 无 | 自动应用解决结果时的目标源。 |
| `autoApplyResolutions` | boolean | `false` | 是否自动回写解决结果。 |
| `validateBeforeUpdate` | boolean | `false` | 回写前是否做校验。 |
| `updateBufferSize` | int | `1024` | 更新批次大小。 |
| `updateRetryAttempts` | int | `0` | 回写失败重试次数。 |
| `updateRetryDelayMs` | long | `1000` | 初始重试延迟。 |
| `updateRetryBackoffMultiplier` | double | `1.5` | 重试退避倍数。 |
| `allowInsert` | boolean | `true` | 是否允许自动插入。 |
| `allowDelete` | boolean | `true` | 是否允许自动删除。 |
| `skipUnchangedUpdates` | boolean | `true` | 是否跳过无变化更新。 |

### 3. `dataSources[]` 配置

| 路径 | 类型 | 默认值 | 作用 |
|------|------|--------|------|
| `dataSources[].sourceId` / `dataSources[].id` | string | 必填 | 数据源 ID。 |
| `dataSources[].sourceName` / `dataSources[].name` | string | 同 `sourceId` | 展示名。 |
| `dataSources[].pluginName` / `dataSources[].type` | string | 必填 | 数据源插件名。 |
| `dataSources[].connectionConfig` / `dataSources[].config` / `dataSources[].connect` | object | 必填 | 插件连接配置。 |
| `dataSources[].querySql` / `dataSources[].selectSql` | string | 无 | 查询 SQL。 |
| `dataSources[].table` / `dataSources[].tableName` | string | 无 | 表名。 |
| `dataSources[].confidenceWeight` | double | `1.0` | 冲突解决时的权重。 |
| `dataSources[].priority` | int | `0` | 策略优先级。 |
| `dataSources[].maxRecords` | int | 无 | 最大扫描条数。 |
| `dataSources[].fieldMappings` | map<string,string> | 空 | 源字段映射。 |
| `dataSources[].extConfig` | object | 空 | 扩展参数透传。 |
| `dataSources[].updateTarget` | boolean | `false` | 透传给扫描配置，供更新链路标记目标端。 |

### 4. `outputConfig` 配置

| 路径 | 类型 | 默认值 | 作用 |
|------|------|--------|------|
| `outputConfig.outputPath` | string | reader 初始化时回退到 `./consistency-results` | 差异、resolution、report 的落盘目录。 |
| `outputConfig.generateReport` | boolean | `true` | 是否生成 HTML 报告。 |
| `outputConfig.reportLanguage` | enum | `ENGLISH` | 报告语言：`ENGLISH` / `CHINESE` / `BILINGUAL`。 |
| `outputConfig.maxDifferencesToDisplay` | int | `100` | HTML 报告中最多展示多少条差异。 |

### 5. `cache` / `performance` 配置

| 路径 | 类型 | 默认值 | 作用 |
|------|------|--------|------|
| `cache.partitionCount` | int | `16` | spill 顶层分区数。 |
| `cache.rebalancePartitionMultiplier` | int | `4` | 子分区倍率。 |
| `cache.spillPath` | string | 无 | spill 文件目录。 |
| `cache.keepTempFiles` | boolean | `false` | 是否保留临时 spill 文件。 |
| `performance.parallelSourceCount` | int | `1` | 旧 `StreamingConsistencyExecutor` 的并行度；`consistency-sortmerge` 当前不直接消费它。 |
| `performance.memoryLimitMB` | int | `512` | 分区 key 数估算和 sort-merge 内存预算来源。 |

## 三、Fusion 示例 JSON

```json
{
  "reader": {
    "type": "fusion-sortmerge",
    "config": {
      "sources": [
        {
          "id": "base",
          "name": "基础信息",
          "type": "mysql8",
          "config": {
            "host": "127.0.0.1",
            "port": 3306,
            "database": "agg_test",
            "userName": "root",
            "password": "password",
            "usePool": true
          },
          "weight": 1.0,
          "priority": 10,
          "confidence": 0.9,
          "querySql": "select user_id, username, age from fusion_1"
        },
        {
          "id": "extra",
          "name": "额外信息",
          "type": "mysql8",
          "config": {
            "host": "127.0.0.1",
            "port": 3306,
            "database": "agg_test",
            "userName": "root",
            "password": "password",
            "usePool": true
          },
          "weight": 1.5,
          "priority": 5,
          "confidence": 0.8,
          "querySql": "select user_id, salary, department, email from fusion_2"
        }
      ],
      "join": {
        "keys": ["user_id"],
        "type": "LEFT"
      },
      "fieldMappings": [
        {
          "type": "DIRECT",
          "sourceField": "base.user_id",
          "targetField": "user_id"
        },
        {
          "type": "DIRECT",
          "sourceField": "base.username",
          "targetField": "username"
        },
        {
          "type": "DIRECT",
          "sourceField": "base.age",
          "targetField": "age"
        },
        {
          "type": "DIRECT",
          "sourceField": "extra.salary",
          "targetField": "salary"
        },
        {
          "type": "DIRECT",
          "sourceField": "extra.department",
          "targetField": "dept"
        },
        {
          "type": "EXPRESSION",
          "expression": "TO_NUMBER(IFNULL(${extra.salary, Double},0)) * 2",
          "targetField": "double_salary"
        },
        {
          "type": "CONSTANT",
          "value": "用户",
          "resultType": "STRING",
          "targetField": "category"
        }
      ],
      "defaultStrategy": "WEIGHTED_AVERAGE",
      "errorMode": "LENIENT",
      "cache": {
        "partitionCount": 10,
        "rebalancePartitionMultiplier": 4
      },
      "performance": {
        "parallelSourceCount": 2,
        "memoryLimitMB": 1024
      },
      "adaptiveMerge": {
        "enabled": true,
        "overflowPartitionCount": 10,
        "rebalancePartitionMultiplier": 4,
        "preferOrderedQuery": true,
        "maxSpillBytesMB": 512,
        "minFreeDiskMB": 256,
        "keyTypes": {
          "user_id": "LONG"
        }
      },
      "detailConfig": {
        "enabled": true,
        "savePath": "./fusion_details",
        "samplingRate": 1.0,
        "maxRecords": 1000,
        "includeSourceData": false,
        "includeFieldDetails": false,
        "saveHtml": true,
        "fileNamePattern": "fusion_details_{datetime}.json"
      }
    }
  },
  "writer": {
    "type": "mysql8",
    "config": {
      "connect": {
        "host": "127.0.0.1",
        "port": 3306,
        "database": "agg_test",
        "userName": "root",
        "password": "password",
        "usePool": true
      },
      "table": "fusion_result",
      "columns": [
        "user_id",
        "username",
        "age",
        "salary",
        "dept",
        "double_salary",
        "category"
      ],
      "writeMode": "insert"
    }
  }
}
```

## 四、Consistency 示例 JSON

```json
{
  "reader": {
    "type": "consistency-sortmerge",
    "config": {
      "ruleId": "user-consistency-rule",
      "ruleName": "用户主数据一致性检查",
      "description": "检查多个系统中的用户主数据是否一致",
      "enabled": true,
      "parallelFetch": true,
      "toleranceThreshold": 0.01,
      "conflictResolutionStrategy": "HIGH_CONFIDENCE",
      "compareFields": [
        "username",
        "age",
        "department",
        "email"
      ],
      "matchKeys": [
        "user_id"
      ],
      "dataSources": [
        {
          "sourceId": "master",
          "sourceName": "主库",
          "pluginName": "mysql8",
          "config": {
            "host": "127.0.0.1",
            "port": 3306,
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
          "config": {
            "host": "127.0.0.1",
            "port": 3306,
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
        "maxDifferencesToDisplay": 100
      },
      "cache": {
        "partitionCount": 16,
        "rebalancePartitionMultiplier": 4,
        "spillPath": "./spill/consistency",
        "keepTempFiles": false
      },
      "performance": {
        "parallelSourceCount": 1,
        "memoryLimitMB": 512
      },
      "adaptiveMerge": {
        "enabled": true,
        "overflowPartitionCount": 16,
        "rebalancePartitionMultiplier": 4,
        "preferOrderedQuery": true,
        "maxSpillBytesMB": 512,
        "minFreeDiskMB": 256,
        "keyTypes": {
          "user_id": "LONG"
        }
      },
      "updateTargetSourceId": "master",
      "autoApplyResolutions": false,
      "validateBeforeUpdate": false,
      "updateBufferSize": 1024,
      "updateRetryAttempts": 0,
      "updateRetryDelayMs": 1000,
      "updateRetryBackoffMultiplier": 1.5,
      "allowInsert": true,
      "allowDelete": true,
      "skipUnchangedUpdates": true
    }
  },
  "writer": {
    "type": "mysql8",
    "config": {
      "connect": {
        "host": "127.0.0.1",
        "port": 3306,
        "database": "agg_test",
        "userName": "root",
        "password": "password",
        "usePool": true
      },
      "table": "consistency_diff_result",
      "columns": [
        "rule_id",
        "record_id",
        "match_keys",
        "conflict_type",
        "differences",
        "payload"
      ],
      "writeMode": "insert"
    }
  }
}
```

### Consistency writer 列说明

`consistency-sortmerge` 投给 writer 的常用列名如下：

- `rule_id`
- `record_id`
- `match_keys`
- `conflict_type`
- `differences`
- `payload`

如果 writer 没配置 `columns`，projector 会退回单列 `payload` 输出。
