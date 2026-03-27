# DataAggregation 会话总结

## 1. 本次工作的背景与目标

本次会话围绕 `DataAggregation` 项目中的两条核心链路展开：

1. 数据一致性比对
   - 包路径：`com.jdragon.aggregation.core.consistency`
   - 原问题：依赖 `DataFetcher` 全量拉取后再按 `matchKey` 分组，数据量大时容易内存不足，并且与项目现有 `reader-transformer-writer` 流式架构不一致。

2. 数据融合
   - 包路径：`com.jdragon.aggregation.core.fusion`
   - 原问题：依赖全量拉取后再按 `joinKey` 分组融合，存在同样的内存瓶颈，也不符合流式处理架构。

核心目标是：

- 将“全量拉取 -> 全量分组 -> 全量输出”重构为“逐行扫描 -> 分桶落盘/缓存 -> 分桶处理 -> 增量输出”
- 保持原有功能配置兼容
- 让一致性比对和融合都支持大数据量下的流式执行
- 补齐与自动更新、冲突解决策略相关的行为问题

---

## 2. 本次会话中完成的主要工作

### 2.1 设计并确认流式比对 / 流式融合方案

先产出了一份完整实施方案，明确了以下方向：

- 增加统一的 streaming 底座
- 一致性和融合共用“按 key 分桶处理”的执行思路
- 使用 spill 文件 / 缓存削峰，避免把全量数据都放进内存
- 输出端改成增量写出
- 保持现有配置、现有服务入口、现有 reader 兼容

该方案随后被实现为实际代码。

---

### 2.2 实现共享 streaming 底座

新增了 `core/streaming` 相关能力，用于支撑一致性和融合的统一流式处理。核心思想是：

- 每读取一条记录就计算复合 key
- 按 key hash 写入不同 partition 文件
- 每次只处理一个 partition
- 单 partition 内再按“每个 source + 每个 key 只保留第一条记录”的既有语义处理

这部分的目的是把峰值内存占用从“全量数据量”降到“单桶 key 数量级”。

---

### 2.3 重构一致性比对主链路为流式执行

完成了一致性比对的流式改造，主要包括：

- 新增 `StreamingConsistencyExecutor`
- 新增 `ConsistencyPartitionProcessor`
- `DataConsistencyService` 切换到 streaming executor
- `DataComparator` 支持按单 key 组进行比对
- `FileResultRecorder` 改为更适合大结果集的流式输出方式
- `ComparisonResult` 的大集合结果改用 spill-backed list 承接

目标是让一致性比对不再依赖先把所有 sourceData 和 groupedData 全部装入内存。

---

### 2.4 为一致性能力补齐 reader 入口

新增了：

- `ConsistencyReader`
- `ConsistencyRecordProjector`

这样一致性能力不仅可以通过 service 入口运行，也可以更自然地接入项目现有的 `reader-transformer-writer` 流式作业体系。

---

### 2.5 重构融合链路为流式执行

完成了融合侧的流式重构，主要包括：

- `FusionReader` 不再先拉全量数据再统一返回，而是边处理边向 writer 发送数据
- 新增 `StreamingFusionExecutor`
- 新增 `FusionPartitionProcessor`
- `FusionEngine` 收缩为按单 key 组做融合
- `FusionDetailRecorder` 改造成更适合大结果量的记录方式

这一部分的目标，是让融合过程也符合项目整体的流式设计方向。

---

### 2.6 扩展数据源层以支持真正的流式读取

为数据源插件补了流式扫描能力：

- `AbstractDataSourcePlugin` 增加默认 `scanQuery(...)` 扩展点
- `RdbmsSourcePlugin` 增加基于 `ResultSet + fetchSize` 的流式扫描实现
- `DataSourcePluginManager.createDataSourceDTO(...)` 补齐 `usePool / extraParams / extConfig` 等映射

这一步是 streaming 执行能真正落地的前提。

---

## 3. 本次会话中修复过的问题

### 3.1 修复了一致性自动更新缺失插入的问题

早期验证 `ConsistencyExample` 时，发现自动更新并没有把目标库缺失的数据补进去。

修复结果：

- 一致性自动更新能够正确把目标源中缺失的记录补插
- 相关修复集中在 `UpdateExecutor` 及各类 conflict resolver 上

对应验证文档：

- `validation-reports/part1/example-validation-report.md`
- `validation-reports/part1/fix-validation-report.md`

---

### 3.2 修复了融合运行时的 JDBC 连接提前关闭问题

早期验证 `FusionExample` 时，出现过：

- `No operations allowed after connection closed`

这说明流式读取链路下 JDBC 资源生命周期管理有问题。

修复结果：

- 重新梳理并修正了 RDBMS 侧连接 / statement / resultSet 的生命周期
- 修复后 `FusionExample` 可正常运行并写入目标表

相关修复文件包括：

- `data-source-handler-rdbms/.../DatasourceCache.java`

---

### 3.3 修复了高可信度策略下 update/insert/delete 语义错误的问题

后续用户补充了更明确的业务规则：

- 假设 `source-1 / source-2 / source-3` 的置信度分别为 `1 / 1.8 / 0.9`
- 若启用 `HIGH_CONFIDENCE`
- 且更新目标为 `source-1`
- 则实际用于对齐 `source-1` 的权威源应为 `source-2`

规则明确为：

- `source-2` 有、`source-1` 没有 -> `INSERT`
- `source-2` 没有、`source-1` 有 -> `DELETE`
- `source-2` 与 `source-1` 都有但值不同 -> `UPDATE`
- `source-2` 与 `source-1` 都没有 -> `SKIP`
- `source-3` 只参与比对展示，不参与对 `source-1` 的 DML 决策

对此进行了一轮修复，解决了：

- 原先把 `source-3` 错误用于补插/更新判断的问题
- `wujiu` 这类仅存在于低置信度非权威源的数据被误插入目标源的问题

对应验证文档：

- `validation-reports/part1/high-confidence-target-rule-report.md`

---

### 3.4 修复了“自动更新语义硬编码在高可信度处理器里”的设计问题

用户进一步指出：

- 冲突解决策略应由 `ConsistencyRule.setConflictResolutionStrategy(...)` 决定
- 不能只为 `HIGH_CONFIDENCE` 单独做一个 planner 后，导致实现割裂
- 同时，`WEIGHTED_AVERAGE` 和 `MAJORITY_VOTE` 必须保持原有自动更新行为不变

因此又做了一轮架构修复：

- 新增 `UpdatePlan`
- 新增 `UpdatePlanningStrategy` 体系
- 将“冲突如何解决”和“目标源如何执行 DML”彻底拆开
- `StreamingConsistencyExecutor` 统一生成 `UpdatePlan`
- `UpdateExecutor` 优先消费 `UpdatePlan`
- 无 `UpdatePlan` 时仍回退旧逻辑

本轮改造后：

- `HIGH_CONFIDENCE` 走新的专属 planner
- `WEIGHTED_AVERAGE`、`MAJORITY_VOTE`、`CUSTOM_RULE` 默认走 legacy planner，保持旧行为
- `MANUAL_REVIEW` 默认 `SKIP`
- 主流程不再硬编码某一种 conflict strategy 的自动更新语义

---

## 4. 本次新增 / 重点改动的核心类

### 4.1 consistency 侧新增 / 重构

- `core/src/main/java/com/jdragon/aggregation/core/consistency/ConsistencyReader.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/ConsistencyRecordProjector.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/model/UpdatePlan.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/model/DifferenceRecord.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/StreamingConsistencyExecutor.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/ConsistencyPartitionProcessor.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/UpdateExecutor.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/FileResultRecorder.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/UpdatePlanningStrategy.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/UpdatePlanningStrategyFactory.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/UpdatePlanningStrategyRegistry.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/HighConfidenceUpdatePlanningStrategy.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/LegacyResolvedValuesUpdatePlanningStrategy.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/LegacyUpdatePlanningSupport.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/ManualReviewUpdatePlanningStrategy.java`

### 4.2 fusion 侧新增 / 重构

- `core/src/main/java/com/jdragon/aggregation/core/fusion/StreamingFusionExecutor.java`
- `core/src/main/java/com/jdragon/aggregation/core/fusion/FusionPartitionProcessor.java`
- `core/src/main/java/com/jdragon/aggregation/core/fusion/FusionReader.java`
- `core/src/main/java/com/jdragon/aggregation/core/fusion/FusionEngine.java`
- `core/src/main/java/com/jdragon/aggregation/core/fusion/detail/FusionDetailRecorder.java`

### 4.3 streaming 底座

- `core/src/main/java/com/jdragon/aggregation/core/streaming/...`

### 4.4 数据源层

- `data-source-plugins/data-source-handler-abstract/.../AbstractDataSourcePlugin.java`
- `data-source-plugins/data-source-handler-rdbms/.../RdbmsSourcePlugin.java`
- `data-source-plugins/data-source-handler-rdbms/.../DatasourceCache.java`

---

## 5. 本次补充的测试

新增 / 更新了多组测试，重点覆盖自动更新策略解耦和行为兼容：

- `HighConfidenceUpdatePlanningStrategyTest`
- `LegacyResolvedValuesUpdatePlanningStrategyTest`
- `ManualReviewUpdatePlanningStrategyTest`
- `UpdatePlanningStrategyFactoryTest`
- `UpdateExecutorTest`

执行过的验证命令：

```bash
mvn -q -pl core -am test -DfailIfNoTests=false
```

结果：

- `56` 个测试全部通过

---

## 6. 本次已经形成的验证文档

当前工作区内已有以下验证文档：

- `validation-reports/part1/example-validation-report.md`
- `validation-reports/part1/fix-validation-report.md`
- `validation-reports/part1/high-confidence-target-rule-report.md`

对应日志：

- `validation-reports/logs/consistency-example.log`
- `validation-reports/logs/consistency-fix.log`
- `validation-reports/logs/consistency-rule2.log`
- `validation-reports/logs/fusion-example.log`
- `validation-reports/logs/fusion-fix.log`

---

## 7. 当前确认已完成的修复结论

到本次会话结束，已经确认完成的修复包括：

1. 一致性和融合的主执行链路已从全量模式重构为以流式分桶处理为核心的模式
2. 结果输出端不再强依赖全量内存集合
3. 一致性能力已补齐 reader 入口
4. 融合能力已支持真正边读边发 writer
5. RDBMS 流式扫描的连接生命周期问题已修复
6. 高可信度策略下“目标源对齐权威源”的 update/insert/delete/skip 语义已按补充规则修正
7. 自动更新逻辑已从 conflict resolver 主链路中解耦出来
8. `WEIGHTED_AVERAGE`、`MAJORITY_VOTE` 已通过 legacy planner 保持原自动更新行为

---

## 8. 当前仍未完全解决的问题

用户在最后反馈：

- 运行 `ConsistencyExample` 仍出现 `Duplicate entry key` 异常

这说明虽然前面已经修复了若干自动更新问题，但“一致性自动更新在某些场景下的 insert / update / delete 区分”仍没有彻底收口，至少还有一类会导致：

- 本应走 `UPDATE` 或 `DELETE + INSERT` 的场景
- 实际执行成了直接 `INSERT`
- 从而触发唯一键冲突

这属于当前明确遗留的问题，需要下一轮继续排查。

从现状推断，排查重点大概率包括：

1. `WEIGHTED_AVERAGE` 或其他 legacy 策略下，planner 与旧 `UpdateExecutor` 语义之间是否还有边界差异
2. 当 `matchKeys` 包含会变化的业务键时，当前 DML 规划是否仍错误地下发了 `INSERT`
3. 目标表唯一约束键与 `matchKeys` 的关系是否未被完全纳入规划
4. `skipUnchangedUpdates / validateBeforeUpdate / allowInsert / allowDelete` 与 planner 决策组合后是否存在未覆盖分支

换句话说：

- “内存模型”和“架构解耦”这两条线已经完成了主要重构
- “自动更新在所有冲突场景下都能正确挑选 DML 类型”这条线还未完全结束

---

## 9. 建议的下一步工作

建议下一轮直接围绕 `Duplicate entry key` 做专项修复，顺序如下：

1. 复现当前 `ConsistencyExample` 的异常
2. 打印触发异常的 `DifferenceRecord`、`ResolutionResult`、`UpdatePlan`
3. 对比：
   - planner 产出的 `operationType`
   - `UpdateExecutor` 实际执行的 SQL
   - 目标表唯一键定义
4. 判断问题属于：
   - planner 规划错误
   - `UpdateExecutor` 对 `UpdatePlan` 消费错误
   - 还是 `matchKeys` 与唯一键约束不一致导致的语义漏洞
5. 增补一个专门覆盖 `Duplicate entry key` 场景的回归测试

---

## 10. 结论

本次会话已经完成了大规模、跨模块的重构与修复，重点成果是：

- 把一致性和融合从“全量内存处理”推进到了“流式分桶处理”
- 修复了部分自动更新逻辑错误
- 修复了融合流式执行中的连接生命周期问题
- 把自动更新策略从硬编码逻辑中抽离出来，形成了统一的 planner 体系

但当前并不能宣称“一致性自动更新已经完全正确”。

原因是：

- 用户最新反馈的 `Duplicate entry key` 异常表明，自动更新在某些 DML 规划分支上仍有遗漏

因此，本次工作的真实状态应表述为：

- 架构级重构已基本完成
- 多项关键问题已修复
- 但一致性自动更新仍存在一个未闭环的唯一键冲突问题，需要下一轮继续修复
