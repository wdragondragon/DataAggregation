# 批次 03：spill guard

## 本批次目标
为 sortmerge overflow 和 rebalance 的所有落盘路径增加共享 spill 护栏，超过配额或磁盘余量阈值时快速失败，避免 Pod 小磁盘被无限打满。

## 目标文件
- `core/src/main/java/com/jdragon/aggregation/core/sortmerge/AdaptiveMergeConfig.java`
- `core/src/main/java/com/jdragon/aggregation/core/streaming/PartitionedSpillStore.java`
- `core/src/main/java/com/jdragon/aggregation/core/streaming/SpillGuard.java`
- `core/src/main/java/com/jdragon/aggregation/core/streaming/SpillLimitExceededException.java`
- `core/src/main/java/com/jdragon/aggregation/core/sortmerge/OverflowBucketStore.java`
- `core/src/main/java/com/jdragon/aggregation/core/sortmerge/AdaptiveMergeCoordinator.java`
- `core/src/main/java/com/jdragon/aggregation/core/fusion/AdaptiveSortMergeFusionExecutor.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/AdaptiveSortMergeConsistencyExecutor.java`

## 计划改动点
- 新增：
  - `maxSpillBytesMB`
  - `minFreeDiskMB`
- `PartitionedSpillStore.append(...)` 写盘前先向 `SpillGuard` 申请配额
- `OverflowBucketStore` 支持持有共享 guard
- `AdaptiveSortMergeFusionExecutor` / `AdaptiveSortMergeConsistencyExecutor` 在一次执行开始时创建共享 guard
- overflow store 与递归 rebalance store 共用同一个 guard

## 关键不变量
- spill 配额必须是一次 sortmerge 执行内的共享总量，而不是单个 partition store 的局部统计
- 任何真正落盘前都必须先完成 guard 校验
- guard 触发后必须停止继续扩大 spill

## 风险与回退点
- 风险：只拦 overflow、不拦 rebalance，会留下漏网写盘路径
- 风险：可用磁盘查询失败时若处理不当，会误判为无限可写
- 回退点：保留原 4 参数 `PartitionedSpillStore` 构造，非 sortmerge 链路不受影响

## 本批次完成标准
- sortmerge overflow 和 rebalance 都走共享 `SpillGuard`
- 超 `maxSpillBytesMB` 或低于 `minFreeDiskMB` 时快速失败
- 失败原因可被 executor 捕获并向上层 summary 或异常信息透出

## 执行记录
- 2026-03-30：开始实施。当前已确认：
  - `OverflowBucketStore` 是 coordinator spill 的统一入口
  - `AdaptiveSortMergeFusionExecutor` 与 `AdaptiveSortMergeConsistencyExecutor` 里还有递归 rebalance 的 `PartitionedSpillStore` 分支，需要与 overflow 共享同一 guard
