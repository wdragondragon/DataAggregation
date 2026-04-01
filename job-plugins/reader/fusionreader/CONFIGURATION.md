# fusionreader 配置参数说明

本文档基于当前仓库中 `job-plugins/reader/fusionreader` 与 `core/sortmerge` 的实现整理，目标是说明 `fusionreader` 现在实际支持哪些参数、哪些参数必填、默认值是什么、配置后会产生什么效果，以及推荐怎么配。

## 1. 使用入口

- 作业里通常写 `reader.type = fusion`
- 插件目录名是 `plugin/reader/fusionreader`
- `plugin_job_template.json` 里仍然写着 `fusion-sortmerge`，但仓库中的示例、测试和常规使用方式都在走 `fusion`

下面只是结构示意，不是可直接运行的最小示例：

```json
{
  "reader": {
    "type": "fusion",
    "config": {
      "sources": [],
      "join": {
        "keys": ["user_id"],
        "type": "LEFT"
      },
      "fieldMappings": []
    }
  },
  "writer": {
    "type": "mysql8",
    "config": {
      "columns": ["user_id"]
    }
  }
}
```

## 2. 先看结论

### 2.1 当前版本必须满足的条件

- `sources` 必填，且至少 1 个
- `sources[].id` 必填，且不能重复
- `sources[].type` 必填
- `sources[].config` 或 `sources[].connect` 必填
- 每个 `source` 至少要提供一组取数入口：
  - `querySql` / `selectSql`
  - 或 `table` / `tableName`
- `join.keys` 或 `joinKeys` 必填
- `fieldMappings` 在当前 `fusionreader` 中是必填
- `fieldMappings[].targetField` 必须同时出现在 `writer.config.columns` 中，否则这个字段不会输出到 writer

### 2.2 当前版本“能写但不要指望生效”的参数

这些字段源码里会解析，但在当前 `fusionreader` 主链路里基本没有实际效果，或者效果和字段名不完全一致：

| 参数 | 当前状态 | 说明 |
|---|---|---|
| `defaultStrategy` | 基本不生效 | 当前 reader 会先校验 `fieldMappings` 非空，因此不会进入依赖 `defaultStrategy` 的 legacy 融合分支。 |
| `performance.parallelSourceCount` | 基本不生效 | 当前执行器会为每个 source 启一个 `OrderedSourceCursor` 线程，这个参数不会真正限制并发数。 |
| `adaptiveMerge.enabled` | 当前无切换效果 | 参数会被解析，但当前 reader 始终走 adaptive sort-merge 执行链。 |
| `fieldMappings[].errorMode` | 当前不生效 | 当前真正生效的是顶层 `errorMode` 和 `errorModes.fieldOverrides.<targetField>`。 |
| `fieldMappings[].strategy` | 仅部分场景生效 | 只有 `DIRECT` 且 `sourceField` 没写 `sourceId`、需要在多个源里择值时才会用到。 |
| `fieldMappings[].resultType` | `DIRECT` 时不生效 | `DIRECT` 直接返回源列对象，不做类型转换。 |
| `sources[].confidence` | 当前基本不影响融合结果 | `HIGH_CONFIDENCE` 策略当前实现里实际比较的是 `weight`，不是 `confidence`。 |

## 3. reader.config 顶层参数

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `sources` | 是 | 无 | 定义参与融合的所有数据源。 | 至少 2 个；第 1 个 source 建议放主表。 |
| `join.keys` | 是 | 无 | 定义关联键；sort-merge 和 join 判断都依赖它。 | 推荐使用清洗后的统一字段名。 |
| `joinKeys` | 否 | 无 | `join.keys` 的兼容别名。 | 新配置优先用 `join.keys`。 |
| `join.type` | 否 | `INNER` | 指定 `INNER` / `LEFT` / `RIGHT` / `FULL`。`LEFT` 以 `sources[0]` 为主表，`RIGHT` 以最后一个 source 为主表。 | 业务主表驱动时推荐 `LEFT`。 |
| `joinType` | 否 | `INNER` | `join.type` 的兼容别名。 | 新配置优先用 `join.type`。 |
| `fieldMappings` | 是 | 空列表 | 定义输出字段怎么生成。当前 reader 初始化会校验它非空。 | 每个 writer 列都显式配一条。 |
| `defaultStrategy` | 否 | `WEIGHTED_AVERAGE` | 在当前 validated 路径里基本不生效。 | 不建议依赖。真正需要时在 `DIRECT` 映射里显式写 `strategy`。 |
| `errorMode` | 否 | `LENIENT` | 控制字段映射报错后的全局行为。支持 `STRICT` / `LENIENT` / `MIXED`。 | 生产推荐 `LENIENT`，联调或验数推荐 `STRICT`。 |
| `errorModes.fieldOverrides.<targetField>` | 否 | 无 | 按目标字段覆盖错误模式。 | 对关键字段单独设 `STRICT`。 |
| `cache.partitionCount` | 否 | `10` | 影响 overflow spill 的顶层分区数默认值。 | 小数据量 `8~16`，大数据量 `16~64`。 |
| `cache.rebalancePartitionMultiplier` | 否 | `4` | overflow rebalance 的子分区倍率。 | 通常保持 `4`。 |
| `performance.parallelSourceCount` | 否 | `2` | 当前基本不影响实际并发。 | 保持默认即可。 |
| `performance.memoryLimitMB` | 否 | `1024` | 影响 pending window 的默认内存预算，以及 overflow rebalance 的内存估算。 | 常规 `512~2048`；大作业可更高。 |
| `adaptiveMerge` | 否 | 见第 7 节 | 控制 pending window、spill、排序查询改写等行为。 | 大多数大表融合都建议显式配置。 |
| `detailConfig` | 否 | 节点不存在时整体关闭 | 控制融合详情输出。 | 排障时打开，日常批跑建议关闭或采样。 |

## 4. sources[] 参数

### 4.1 source 基础参数

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `sources[].id` | 是 | 无 | source 唯一标识；字段引用、日志、详情输出都用它。 | 用短而稳定的英文名，如 `base`、`extra`。 |
| `sources[].name` | 否 | 同 `id` | 展示名称。 | 可写中文描述，便于排障。 |
| `sources[].type` | 是 | 无 | 数据源插件类型，如 `mysql8`、`postgres`、`ftp`。 | 与实际 source 插件保持一致。 |
| `sources[].config` | 是（二选一） | 无 | 数据源连接参数。 | 新配置常用这个。 |
| `sources[].connect` | 是（二选一） | 无 | `config` 的兼容写法。 | 老配置兼容可保留。 |
| `sources[].querySql` | 条件必填 | 无 | 直接指定查询 SQL；优先级高于 `table`。 | 复杂筛选、聚合、预处理都用它。 |
| `sources[].selectSql` | 否 | 无 | `querySql` 的兼容别名。 | 新配置优先用 `querySql`。 |
| `sources[].table` | 条件必填 | 无 | 当没写 `querySql` 时自动拼 `select ... from table`。 | 简单全表或按列读取时用。 |
| `sources[].tableName` | 否 | 无 | `table` 的兼容别名。 | 新配置优先用 `table`。 |
| `sources[].columns` | 否 | `*` | 仅在没写 `querySql` 时生效，用来拼 `select col1,col2 from table`。 | 不要无脑 `*`，建议只选 join 和输出所需列。 |
| `sources[].maxRecords` | 否 | 不限 | 限制单个 source 最多读取多少行。DB 源会尽量追加 `LIMIT`。 | 联调 `100~1000`；生产一般不配。 |
| `sources[].fieldMappings` | 否 | 空 | 源字段重命名，方向是“原字段名 -> 融合内部字段名”。join 和后续字段映射看到的是重命名后的字段。 | 多源同义字段先在这里统一命名。 |
| `sources[].extConfig` | 否 | 空对象 | 文件源会原样透传给 file helper；DB 源会合并进 `extraParams`。 | 文件源常配格式参数；DB 源少量特殊参数可放这里。 |

### 4.2 融合策略相关参数

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `sources[].weight` | 否 | `1.0` | 影响 `WEIGHTED_AVERAGE`、`MAJORITY_VOTE`，以及 `PRIORITY` 并列时的 tie-break。当前 `HIGH_CONFIDENCE` 也实际按它比较。 | 默认 `1.0`；更可信源可设更高，如 `2.0`。 |
| `sources[].priority` | 否 | `0` | 影响 `PRIORITY` 策略，数值越大优先级越高。 | 主源建议比其他源高，如 `10`。 |
| `sources[].confidence` | 否 | `1.0` | 当前会被解析并透传，但当前 fusion 选值逻辑基本不消费它。 | 先按业务填，但不要指望它直接改变结果。 |

约束：

- `weight` 必须 `> 0`
- `confidence` 必须在 `0 ~ 1`

### 4.3 增量读取相关参数

这组参数只有在 **没写 `querySql`，而是走 `table` 自动拼 SQL** 时才会解析并生效。

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `sources[].incrColumn` | 否 | 无 | 指定增量列。 | 用单调递增主键或时间列。 |
| `sources[].incrModel` | 否 | `>` | 增量条件操作符。 | 常用 `>`。 |
| `sources[].pkValue` | 否 | 无 | 若提供，则自动拼成 `where incrColumn incrModel 'pkValue'`。 | 配合作业断点续跑使用。 |

额外说明：

- 处理完成后，reader 会把每个 source 的增量最大值回写成 `pkValue_<sourceId>` 到作业埋点里
- 如果你已经自己写了 `querySql`，这三项当前不会帮你二次改写 SQL

### 4.4 sources[].config / connect 通用子字段

`fusionreader` 本身不强校验连接参数结构，真正消费这些字段的是底层 source 插件。对数据库类 source，框架层通用识别这些键：

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `host` | 通常必填 | 无 | 连接主机。 | 写 IP 或域名。 |
| `port` | 通常必填 | 无 | 连接端口。 | 与插件类型匹配。 |
| `database` | 视插件而定 | 无 | 库名。 | 只写当前业务库。 |
| `username` / `userName` | 视插件而定 | 无 | 用户名。 | 两种写法都兼容，推荐统一 `userName`。 |
| `password` | 视插件而定 | 无 | 密码。 | 建议走安全配置。 |
| `usePool` | 否 | `false` | 是否启用连接池。 | 生产 DB 源一般 `true`。 |
| `jdbcUrl` | 否 | 无 | 自定义 JDBC URL。 | 非标准连接场景再用。 |
| `driverClassName` | 否 | 无 | 自定义驱动类。 | 非标准驱动场景再用。 |
| `extraParams` | 否 | 空 | 额外连接参数。 | JDBC 特殊参数放这里。 |
| `other` | 否 | 无 | 原样透传给 DTO。 | 历史兼容参数。 |

## 5. fieldMappings[] 参数

### 5.1 通用参数

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `fieldMappings[].type` | 否 | `DIRECT` | 指定映射类型：`DIRECT` / `EXPRESSION` / `CONDITIONAL` / `CONSTANT` / `GROOVY`。 | 大多数普通字段用 `DIRECT`。 |
| `fieldMappings[].targetField` | 是 | 无 | 目标字段名。writer 侧会按 `writer.config.columns` 找它的位置。 | 与 writer 列名严格一致。 |
| `fieldMappings[].resultType` | 否 | 类型相关 | 控制输出列类型。`DIRECT` 当前不生效；`EXPRESSION` / `CONDITIONAL` / `GROOVY` 默认为 `STRING`；`CONSTANT` 默认为 `STRING`。 | 只要不是纯字符串，都建议显式写。 |
| `fieldMappings[].strategy` | 否 | 无 | 仅在 `DIRECT` 且 `sourceField` 未指定 `sourceId`、需要跨源选值时生效。 | 这种模糊取值场景才写；否则不必写。 |
| `fieldMappings[].errorMode` | 否 | `DEFAULT` | 当前实现里基本不生效。 | 不建议依赖；改用顶层 `errorModes.fieldOverrides`。 |

支持的策略名：

- `PRIORITY`
- `WEIGHTED_AVERAGE`
- `HIGH_CONFIDENCE`
- `MAJORITY_VOTE`

注意：

- `HIGH_CONFIDENCE` 当前实现实际上按 `weight` 选值，不是按 `confidence`
- `strategy` 对 `EXPRESSION` / `CONDITIONAL` / `CONSTANT` / `GROOVY` 当前没有实际影响

### 5.2 DIRECT

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `fieldMappings[].sourceField` | 是 | 无 | 指向源字段。支持 `sourceId.fieldName`、`${sourceId.fieldName}`、`fieldName`、`${fieldName}`。 | 强烈建议始终写成带 `sourceId` 的全限定形式。 |

效果说明：

- 写成 `base.user_id` 这类全限定形式时，直接取指定 source 的值
- 写成 `user_id` 这类不带 sourceId 的形式时，会从所有 source 收集同名字段，再按 `strategy` 或内部回退逻辑择值
- 如果没写 `strategy`，当前 `DIRECT` 的模糊取值会回退到 `PRIORITY`，不会读取顶层 `defaultStrategy`

推荐：

- 普通映射统一写 `sourceId.field`
- 只有当多个 source 真的是同义字段、且你明确要在 reader 侧做择值时，才使用不带 `sourceId` 的写法

### 5.3 EXPRESSION

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `fieldMappings[].expression` | 是 | 无 | 使用 Janino 表达式计算目标值。 | 数值、拼接、条件判断都适合。 |

推荐写法：

```json
{
  "type": "EXPRESSION",
  "targetField": "salary_x2",
  "expression": "TO_NUMBER(IFNULL(${extra.salary, Double}, 0)) * 2",
  "resultType": "DOUBLE"
}
```

表达式字段引用建议：

- 推荐使用 `${sourceId.fieldName}`
- 需要强制类型时可写 `${sourceId.fieldName, Double}`、`${sourceId.fieldName, Integer}`、`${sourceId.fieldName, String}`、`${sourceId.fieldName, Boolean}`
- 不建议写裸字段 `${fieldName}`，当前表达式上下文不会像 `DIRECT` 那样自动去所有 source 里找同名字段

常用内置函数：

- 字符串：`CONCAT` `SUBSTR` `UPPER` `LOWER` `TRIM`
- 数值：`ROUND` `ABS` `POW` `SQRT` `MAX` `MIN` `SUM` `AVG`
- 逻辑：`IF` `IFNULL` `COALESCE` `ISNULL` `ISNOTNULL`
- 类型转换：`TO_NUMBER` `TO_INTEGER` `TO_STRING` `TO_DATE` `TO_BOOLEAN`

### 5.4 CONDITIONAL

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `fieldMappings[].condition` | 是 | 无 | 条件表达式。 | 明确返回布尔结果。 |
| `fieldMappings[].trueValue` | 是 | 无 | 条件为真时返回的表达式。 | 字符串字面量要自己加引号。 |
| `fieldMappings[].falseValue` | 是 | 无 | 条件为假时返回的表达式。 | 与 `trueValue` 类型保持一致。 |

说明：

- `condition`、`trueValue`、`falseValue` 都按表达式处理，不是纯文本模板
- 如果返回字符串，请像 `"adult"` 这样显式加引号

### 5.5 CONSTANT

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `fieldMappings[].value` | 是 | 无 | 固定值。 | 适合补常量字段、批次标签、来源标识。 |

说明：

- 默认 `resultType = STRING`
- 如果你写了 `INT` / `LONG` / `DOUBLE` / `BOOL` / `DATE`，当前会尝试把 `value` 转成对应类型

示例：

```json
{
  "type": "CONSTANT",
  "targetField": "source_tag",
  "value": "fusion_job",
  "resultType": "STRING"
}
```

### 5.6 GROOVY

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `fieldMappings[].script` | 是 | 无 | 用 Groovy 脚本返回目标值。 | 只在 Janino 表达式不够用时使用。 |

Groovy 可用变量：

- `fields`：扁平 map，键格式为 `sourceId_fieldName`
- `sources`：嵌套 map，结构为 `sources[sourceId][fieldName]`
- `targetField`：当前目标字段名

推荐示例：

```json
{
  "type": "GROOVY",
  "targetField": "full_name",
  "script": "return (sources['base']['first_name'] ?: '') + ' ' + (sources['base']['last_name'] ?: '')",
  "resultType": "STRING"
}
```

## 6. errorMode 与字段级报错控制

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `errorMode` | 否 | `LENIENT` | 全局报错模式。 | 生产先用 `LENIENT`。 |
| `errorModes.fieldOverrides.<targetField>` | 否 | 无 | 覆盖单个目标字段的报错模式。 | 主键、金额、时间等关键字段可设 `STRICT`。 |

当前可安全使用的值建议：

- `STRICT`：报错即终止
- `LENIENT`：记录错误并写 `null`

关于 `MIXED`：

- 顶层 `errorMode = MIXED` 可以理解成“全局宽松，但允许字段级覆盖”
- 但运行时它最终仍会落到 `LENIENT`
- 因此真正决定单字段严格度的，还是 `errorModes.fieldOverrides.<targetField>`

## 7. adaptiveMerge 参数

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `adaptiveMerge.enabled` | 否 | `true` | 当前仅被解析，不能关闭当前 reader 的 sort-merge 主链路。 | 保持默认即可。 |
| `adaptiveMerge.pendingKeyThreshold` | 否 | `4096` | pending window 中最多允许保留多少个未凑齐的 key；超出后开始 spill。 | 常规 `4096~20000`。 |
| `adaptiveMerge.pendingMemoryMB` | 否 | `max(64, memoryLimitMB / 2)` | pending window 的内存预算。超出后开始 spill。 | 通常设为总 reader 内存预算的一半。 |
| `adaptiveMerge.overflowPartitionCount` | 否 | `cache.partitionCount` | spill 到磁盘时的顶层分区数。 | 大量 spill 建议 `16~64`。 |
| `adaptiveMerge.rebalancePartitionMultiplier` | 否 | `4` | 溢出分区继续 rebalance 时的子分区倍率。 | 通常保持 `4`。 |
| `adaptiveMerge.overflowSpillPath` | 否 | 系统临时目录下的 `aggregation-stream` | spill 文件落盘目录。 | 生产建议显式指向本地 SSD。 |
| `adaptiveMerge.preferOrderedQuery` | 否 | `true` | 对数据库 source 尝试改写成 `SELECT * FROM (...) ORDER BY joinKeys`，降低 pending window 压力。文件源不改写。 | 能接受排序 SQL 时保持 `true`。 |
| `adaptiveMerge.maxSpillBytesMB` | 否 | `512` | spill guard 最大累计写盘预算。超限会抛异常。 | 大作业建议按磁盘容量提高。 |
| `adaptiveMerge.minFreeDiskMB` | 否 | `256` | 磁盘剩余空间保护线。低于它会拒绝继续 spill。 | 生产建议按机器磁盘情况提高。 |
| `adaptiveMerge.keyTypes.<field>` | 否 | `AUTO` | 显式指定 join key 的排序类型。支持 `AUTO` / `STRING` / `NUMBER` / `DATETIME`。 | 数字键和时间键建议显式写。 |

补充说明：

- `preferOrderedQuery = true` 时，数据库 source 可能会被改写成包一层子查询再 `ORDER BY join keys`
- `LEFT` / `RIGHT` 的主表判断依赖 `sources` 顺序，不依赖 SQL 中的表别名
- `keyTypes` 建议与实际 join key 数据类型一致，否则比较阶段可能抛类型不兼容异常

## 8. detailConfig 参数

### 8.1 参数表

| 参数路径 | 是否必填 | 默认值 | 配置后效果 | 建议值 / 建议做法 |
|---|---|---|---|---|
| `detailConfig.enabled` | 否 | `false`（整个节点不存在时） / `true`（节点存在但未显式填写时） | 是否开启融合详情记录。 | 日常批跑关闭；排障时开启。 |
| `detailConfig.savePath` | 否 | `./fusion_details` | 保存目录或完整 JSON 文件路径。以 `.json` 结尾时视为完整文件名。 | 推荐目录方式。 |
| `detailConfig.samplingRate` | 否 | `1.0` | 详情采样率，范围 `0.0 ~ 1.0`。 | 大作业排障建议 `0.01~0.1`。 |
| `detailConfig.maxRecords` | 否 | `10000` | 最多保存多少条采样详情。达到上限后，后续详情直接丢弃。 | 大作业可限制在 `1000~5000`。 |
| `detailConfig.includeSourceData` | 否 | `false`（节点存在时） | 是否在详情里附带源行快照。 | 仅排障时开启。 |
| `detailConfig.includeFieldDetails` | 否 | `false`（节点存在时） | 是否记录字段级详情。当前实现里它实际上决定“是否真的把逐行 detail 写入输出”。 | 想看逐条融合细节时必须设 `true`。 |
| `detailConfig.saveHtml` | 否 | `true` | 是否同时生成 HTML 查看页。 | 排障时保留默认。 |
| `detailConfig.fileNamePattern` | 否 | `fusion_details_{timestamp}.json` | 目录模式下的输出文件名模板。 | 建议带 `{datetime}` 或 `{jobId}`。 |

### 8.2 fileNamePattern 支持的占位符

- `{timestamp}`
- `{datetime}`
- `{jobId}`
- `{sourceCount}`
- `{recordCount}`

### 8.3 detailConfig 的几个关键坑

- `includeFieldDetails = false` 时，当前实现通常只会输出元数据和摘要，不会真正落逐行详情
- `maxRecords` 达到后不会做 FIFO 轮转，后续采样记录会直接不再保存
- `samplingRate = 1.0` 在大表上可能非常占内存和磁盘

推荐排障配置：

```json
{
  "detailConfig": {
    "enabled": true,
    "savePath": "./fusion_details",
    "samplingRate": 0.05,
    "maxRecords": 2000,
    "includeSourceData": true,
    "includeFieldDetails": true,
    "saveHtml": true,
    "fileNamePattern": "fusion_details_{datetime}_{jobId}.json"
  }
}
```

## 9. 推荐配置模板

```json
{
  "reader": {
    "type": "fusion",
    "config": {
      "sources": [
        {
          "id": "base",
          "name": "主表",
          "type": "mysql8",
          "config": {
            "host": "127.0.0.1",
            "port": 3306,
            "database": "agg_test",
            "userName": "root",
            "password": "password",
            "usePool": true
          },
          "querySql": "select user_id, username, age from fusion_1",
          "priority": 10,
          "weight": 1.0
        },
        {
          "id": "extra",
          "name": "补充表",
          "type": "mysql8",
          "config": {
            "host": "127.0.0.1",
            "port": 3306,
            "database": "agg_test",
            "userName": "root",
            "password": "password",
            "usePool": true
          },
          "querySql": "select user_id, salary, department from fusion_2",
          "priority": 5,
          "weight": 1.5
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
          "sourceField": "extra.department",
          "targetField": "department"
        },
        {
          "type": "EXPRESSION",
          "expression": "TO_NUMBER(IFNULL(${extra.salary, Double}, 0)) * 2",
          "targetField": "salary_x2",
          "resultType": "DOUBLE"
        }
      ],
      "errorMode": "LENIENT",
      "errorModes": {
        "fieldOverrides": {
          "user_id": "STRICT"
        }
      },
      "cache": {
        "partitionCount": 16,
        "rebalancePartitionMultiplier": 4
      },
      "performance": {
        "memoryLimitMB": 1024
      },
      "adaptiveMerge": {
        "pendingKeyThreshold": 4096,
        "pendingMemoryMB": 512,
        "overflowPartitionCount": 16,
        "rebalancePartitionMultiplier": 4,
        "preferOrderedQuery": true,
        "maxSpillBytesMB": 1024,
        "minFreeDiskMB": 1024,
        "keyTypes": {
          "user_id": "NUMBER"
        }
      }
    }
  }
}
```

## 10. 实战建议

- 主表放在 `sources[0]`，并优先使用 `join.type = LEFT`
- 先在 `sources[].fieldMappings` 把多源同义列统一命名，再写 `join.keys`
- `fieldMappings` 尽量全部写成显式 `sourceId.field`，不要依赖模糊取值
- 数值计算字段务必写 `resultType`
- 大表融合时显式配置 `adaptiveMerge.keyTypes`
- 需要排障时再开 `detailConfig`，并配采样率
- 如果用 `querySql`，就不要再指望 `incrColumn/pkValue` 帮你自动拼 where
