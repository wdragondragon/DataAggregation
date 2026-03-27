# 高置信度更新规则回归报告

## 规则定义

在 `HIGH_CONFIDENCE` 策略下，若：

- `source-1.confidence = 1`
- `source-2.confidence = 1.8`
- `source-3.confidence = 0.9`
- `updateTargetSourceId = source-1`

则自动更新阶段应以 `source-2` 作为 `source-1` 的对齐基准，而不是任意非缺失源。

对应业务语义：

1. `source-2` 有、`source-1` 没有：对 `source-1` 执行 `INSERT`
2. `source-2` 没有、`source-1` 有：对 `source-1` 执行 `DELETE`
3. `source-2` 有、`source-1` 也有但值不一致：对 `source-1` 执行 `UPDATE`
4. `source-2` 没有、`source-1` 也没有：`SKIP`
5. `source-3` 仅参与一致性比对展示，不参与 `source-1` 自动更新决策

## 本次实现

新增了“高置信度更新规划”步骤，在分区处理阶段直接把每条 `DifferenceRecord` 规范成明确的 `operationType`：

- `INSERT`
- `UPDATE`
- `DELETE`
- `SKIP`

并把 `winningSource` 固定为更新基准源 `source-2`。

涉及核心文件：

- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/HighConfidenceUpdatePlanner.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/ConsistencyPartitionProcessor.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/UpdateExecutor.java`

## 单测结果

执行：

```bash
mvn -q -pl core -am test -DfailIfNoTests=false
```

结果：通过。

新增单测：

- `HighConfidenceUpdatePlannerTest`

覆盖场景：

- authoritative source 存在且 target 缺失 -> `INSERT`
- authoritative source 缺失且 target 存在 -> `DELETE`
- authoritative source 与 target 同时缺失 -> `SKIP`

## 示例回放

### 基线处理

先删除 `users_1` 中手工补入的 `wujiu`：

```sql
DELETE FROM users_1 WHERE user_id = 7 AND username = 'wujiu';
```

### 运行结果

使用 `ConsistencyExample` 同配置重新回放，日志：

- `validation-reports/logs/consistency-rule2.log`

返回摘要：

```json
{
  "status":"PARTIAL_SUCCESS",
  "totalRecords":8,
  "inconsistentRecords":8,
  "resolvedRecords":8,
  "updateResult":{
    "targetSourceId":"source-1",
    "totalUpdates":2,
    "successfulUpdates":2,
    "failedUpdates":0,
    "insertCount":1,
    "updateCount":0,
    "deleteCount":1,
    "skipCount":6
  }
}
```

## resolution 结果核验

`resolutions_20260327_135000.json` 的关键记录如下：

### 1. `wujiu`

```json
{
  "matchKeyValues":{"user_id":"7","username":"wujiu"},
  "operationType":"SKIP",
  "winningSource":"source-2"
}
```

解释：

- `source-2` 中没有 `wujiu`
- `source-1` 中也没有 `wujiu`
- 即使 `source-3` 有该记录，也不应自动插入到 `source-1`
- 结果为 `SKIP`

这与规则一致。

### 2. 用户 `user_id=1`

`source-1` 与 `source-2` 的匹配键发生分裂：

- `source-1` 中存在旧键：`(1, zhangsan11)`
- `source-2` 中存在新键：`(1, zhangsan)`

因此生成了两条操作：

```json
{
  "matchKeyValues":{"user_id":"1","username":"zhangsan11"},
  "operationType":"DELETE",
  "winningSource":"source-2"
}
```

```json
{
  "matchKeyValues":{"user_id":"1","username":"zhangsan"},
  "operationType":"INSERT",
  "winningSource":"source-2"
}
```

这与“以 source-2 为准”的语义一致：

- 旧键删除
- 新键插入

## 目标表核验

回放后执行：

```sql
SELECT user_id, username FROM users_1 ORDER BY user_id;
```

结果：

```json
[
  {"user_id":1,"username":"zhangsan"},
  {"user_id":2,"username":"lisi"},
  {"user_id":3,"username":"wangwu"},
  {"user_id":4,"username":"zhaoliu"},
  {"user_id":5,"username":"sunqi"},
  {"user_id":6,"username":"zhouba"}
]
```

说明：

- `user_id=1` 已经按 `source-2` 修正为 `zhangsan`
- `wujiu` 没有被错误插入到 `source-1`

## 结论

本次修复后，`HIGH_CONFIDENCE + updateTargetSourceId=source-1` 的自动更新语义已经符合你补充的规则：

- 以 `source-2` 作为更新基准
- `source-2` 缺失 -> 删除 `source-1`
- `source-2` 多出 -> 插入 `source-1`
- `source-2` 与 `source-1` 不一致 -> 更新/重建 `source-1`
- `source-3` 不再错误触发对 `source-1` 的补插