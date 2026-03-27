# Fusion / Consistency 自适应排序归并实现进度交接

生成日期: 2026-03-27

## 1. 本文件用途

本文件用于承接 2026-03-27 这次会话中，围绕 `fusion` 与 `consistency` 的“自适应排序归并实验方案”所做的实现与当前状态总结。

下次会话如果需要继续推进，只需要先阅读：

1. `validation-reports/part1/session-summary-20260327.md`
2. `validation-reports/part2/implementation-progress.md`
3. `validation-reports/part2/testing-usage-and-conclusion.md`
4. `validation-reports/part2/sortmerge-memory-flow.md`

其中：

- `session-summary-20260327.md` 记录的是上一阶段“流式分桶 / spill / planner”方向的整体重构背景
- 本文件记录的是本次新增的“排序归并 + 超阈值降级文件桶”实验实现

配套文档：

- `sortmerge-memory-flow.md`
  - 专门描述 `sortmerge` 链路里数据在内存中的流转，以及 spill / hybrid 路径的切换方式

## 2. 本次会话目标

本次会话的重点不是替换原有实现，而是在保持原配置总体兼容、方便回滚的前提下，新增一条实验链路：

- 默认优先走“多路排序归并”
- 当 pending window 超出阈值时，局部 spill 到 overflow bucket
- 当无法继续稳定有序处理时，退化为 hybrid / bucket 风格处理

目标是避免“先全量拉取、再全量落盘、再统一分组”的高内存和高磁盘压力。

## 3. 本次实现的核心结果

### 3.1 新增共享 sort-merge 内核

新增包：

- `core/src/main/java/com/jdragon/aggregation/core/sortmerge`

其中关键类包括：

- `AdaptiveMergeConfig`
- `AdaptiveMergeCoordinator`
- `OrderedKey`
- `OrderedKeyGroup`
- `OrderedKeySchema`
- `OrderedKeyType`
- `OrderedSourceCursor`
- `PendingWindow`
- `OverflowBucketStore`
- `SortMergeStats`

职责概览：

- `OrderedSourceCursor`
  - 对每个 source 进行有序流式扫描
  - 把连续同 key 的记录折叠成一个 group
  - 保留首条记录参与业务逻辑
  - 额外重复记录只累计 duplicate ignored 统计

- `AdaptiveMergeCoordinator`
  - 维护多源 head group
  - 维护 pending window
  - 负责“立即归并 / 延后等待 / spill unresolved keys / 退化 hybrid”

- `OverflowBucketStore`
  - 只持久化未决 key 或后续降级数据
  - 不再默认把所有原始源数据都完整拷贝一遍

### 3.2 Fusion 新增实验 reader 与执行器

新增类：

- `core/src/main/java/com/jdragon/aggregation/core/fusion/AdaptiveSortMergeFusionExecutor.java`
- `core/src/main/java/com/jdragon/aggregation/core/fusion/SortMergeFusionReader.java`

集成点：

- `JobContainer` 已内建注册 `fusion-sortmerge`

行为特点：

- 优先走多源有序归并
- 仍复用原融合语义
- 配置仍通过 `FusionConfig` 解析
- 新增 `adaptiveMerge` 配置块
- summary 里会输出 `executionEngine`、`mergeResolvedKeyCount`、`mergeSpilledKeyCount`、`duplicateIgnoredCount`、`fallbackReason`

### 3.3 Consistency 新增实验 reader 与执行器

新增类：

- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/AdaptiveSortMergeConsistencyExecutor.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/OrderedDataConsistencyService.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/SortMergeConsistencyReader.java`

集成点：

- `JobContainer` 已内建注册 `consistency-sortmerge`

行为特点：

- 优先走多源排序归并
- pending 超阈值后允许 spill unresolved keys
- 仍复用原 `ComparisonResult`、冲突解决、自动更新链路
- 通过 `ConsistencyRule` 新增 `adaptiveMerge` 配置块

### 3.4 配置模型已补齐 adaptiveMerge

已补充到：

- `core/src/main/java/com/jdragon/aggregation/core/fusion/config/FusionConfig.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/model/ConsistencyRule.java`

当前可用的核心配置语义：

- `adaptiveMerge.enabled`
- `adaptiveMerge.pendingKeyThreshold`
- `adaptiveMerge.pendingMemoryMB`
- `adaptiveMerge.overflowPartitionCount`
- `adaptiveMerge.overflowSpillPath`
- `adaptiveMerge.validateSourceOrder`
- `adaptiveMerge.onOrderViolation`
- `adaptiveMerge.onMemoryExceeded`
- `adaptiveMerge.keyTypes`

## 4. 当前实验链路的真实状态

### 4.1 小中数据量状态

在真实 MySQL 数据源测试下：

- `100` 条场景：`consistency=sortmerge`，`fusion=sortmerge`
- `10000` 条场景：`consistency=sortmerge`，`fusion=sortmerge`

说明：

- 小中数据量下，新链路可以稳定纯 sortmerge 运行
- 未发生 spill
- 结果内容校验通过

### 4.2 百万级数据量状态

在 `1000000` 条场景下：

- `consistency=hybrid`
- `fusion=hybrid`
- `fusion spilled keys = 879450`

这说明：

- 当前 adaptive 阈值下，百万级数据会触发“局部 spill + hybrid 收尾”
- 内容正确性仍然通过
- 但已经不再是全程纯 sortmerge

### 4.3 大数据 hybrid 场景的关键结论

百万级测试中确认到一个非常重要的现象：

- hybrid 场景下，融合结果“内容正确”
- 但“全局输出顺序不再严格保持有序”

也就是说：

- 如果验收标准是“结果集内容正确”，当前实现已经通过
- 如果验收标准进一步要求“spill 之后最终输出仍保持全局有序”，当前实现还没有做到

当前测试中已经观测到顺序回退样例：

- `1000000 -> 92889`

这不是内容错误，而是 overflow / hybrid 收尾阶段导致的输出顺序变化。

## 5. 本次新增的测试承接模块

为避免把测试型数据源或大规模 mock 逻辑塞进 `core`，本次新增了独立模块：

- `data-mock`

模块位置：

- `data-mock/pom.xml`
- `data-mock/src/test/java/com/jdragon/aggregation/datamock/sortmerge`

当前测试直接复用框架自己的 MySQL 数据源能力，并读取：

- `core/src/main/resources/fusion-mysql-demo.json`

不会在 `core` 中引入额外插件化测试依赖。

## 6. 当前交付成果清单

本次已经完成并验证过的成果包括：

1. `fusion-sortmerge` 实验 reader 已接入
2. `consistency-sortmerge` 实验 reader 已接入
3. 自适应排序归并内核已实现
4. `adaptiveMerge` 配置模型已补齐
5. MySQL 真实数据源链路验证已跑通
6. 百条 / 万条 / 百万条三档测试已完成
7. 测试报告已生成

## 7. 当前已知边界与风险

### 7.1 已知边界

- 文件 / 数据源若无法保持有序，最终仍可能进入 hybrid
- hybrid 后结果内容正确，但当前不保证全局输出顺序
- 百万级场景下 heap 仍然较高，说明参数和内部对象占用仍有继续优化空间

### 7.2 本轮未做的事情

- 没有实现“对 overflow bucket 再做全局有序归并输出”
- 没有做更激进的内存压缩
- 没有把 benchmark runner 做成生产功能，只是测试模块

## 8. 下次会话建议直接衔接的方向

如果下次要继续推进，建议优先在以下方向中选一个：

### 方向 A: 保证 hybrid 后最终输出仍全局有序

适合诉求：

- 用户要求即使发生 spill，最终输出也必须是严格 key 有序

建议排查点：

- `OverflowBucketStore` 写出格式
- hybrid 收尾阶段是否按 bucket 顺序直接输出
- 是否需要再做一层“overflow 结果有序归并”

### 方向 B: 降低百万级场景内存峰值

适合诉求：

- 当前 heap 峰值偏高，需要进一步压内存

建议排查点：

- `pendingKeyThreshold`
- `pendingMemoryMB`
- `OrderedKeyGroup` 保留内容是否仍过重
- bucket spill 是否可以更早触发

### 方向 C: 把实验配置整理成对外示例

适合诉求：

- 需要让业务方直接试跑 `fusion-sortmerge` / `consistency-sortmerge`

建议产出：

- 示例 JSON
- 参数说明
- 何时会进入 hybrid 的说明

## 9. 下次会话的最小上下文提示词

下次可以直接引用下面这段上下文继续：

> 我们已经在 `core.sortmerge` 下实现了自适应排序归并实验链路，并在 `fusion-sortmerge`、`consistency-sortmerge` reader 中接入。`data-mock` 模块已经基于 `fusion-mysql-demo.json` 跑过百条、万条、百万条 MySQL 实测。当前结论是：小中数据量可纯 sortmerge，百万级会进入 hybrid，内容正确但输出顺序不再全局有序。请在此基础上继续推进。
