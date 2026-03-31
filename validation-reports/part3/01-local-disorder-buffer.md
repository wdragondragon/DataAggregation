# 批次 01：局部乱序缓冲

## 本批次目标
在 `OrderedSourceCursor` 增加 source 内部的有界局部乱序缓冲，让“轻微顺序倒退”优先在 source 侧被吸收，而不是立刻交给 `coordinator` 触发全局降级。

## 目标文件
- `core/src/main/java/com/jdragon/aggregation/core/sortmerge/AdaptiveMergeConfig.java`
- `core/src/main/java/com/jdragon/aggregation/core/sortmerge/OrderedSourceCursor.java`
- `core/src/main/java/com/jdragon/aggregation/core/fusion/AdaptiveSortMergeFusionExecutor.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/AdaptiveSortMergeConsistencyExecutor.java`

## 计划改动点
- 新增局部乱序缓冲配置：
  - `localDisorderEnabled`
  - `localDisorderMaxGroups`
  - `localDisorderMaxMemoryMB`
- 在 `OrderedSourceCursor` 引入 `LocalDisorderBuffer`
- `GroupAccumulator.emitCurrent()` 改为“先入本地缓冲，再按 key 顺序释放”
- 支持同 key 合并 `duplicateCount` / `scannedRecords`
- end-of-stream 时按 key 顺序清空缓冲

## 关键不变量
- queue 里的事件顺序代表“cursor 已决定对外发布的顺序”
- 本地缓冲对象是 `OrderedKeyGroup`，不是原始 row
- 同 key 只保留首条 `firstRow`
- 本地缓冲必须有上限，不能无界增长

## 风险与回退点
- 风险：缓冲释放策略不当会造成 group 丢失或重复发布
- 风险：同 key 合并若错误，可能破坏 `duplicateCount` 统计
- 回退点：保留 `localDisorderEnabled=false` 时直接沿用当前行为

## 本批次完成标准
- `OrderedSourceCursor` 在轻微局部倒退场景下可按 key 有序输出 group
- 未开启缓冲时行为与当前一致
- 不引入 `coordinator` 语义变化

## 执行记录
- 2026-03-30：开始实施。当前切入点已确认：
  - `AdaptiveMergeConfig.fromConfig(...)` 负责 sortmerge 新配置默认值
  - `AdaptiveSortMergeFusionExecutor` / `AdaptiveSortMergeConsistencyExecutor` 负责把 `keySchema` 与 `adaptiveMergeConfig` 传给 `OrderedSourceCursor`
  - `OrderedSourceCursor.GroupAccumulator.emitCurrent()` 将改为“先进入 `LocalDisorderBuffer`，再按上限释放最小 key”
> 2026-03-31 注：该设计已淘汰。当前实现不再使用 `LocalDisorderBuffer`，而是由 pending window 按 key 合并并在超窗时整体 spill。本文仅保留为历史记录。
