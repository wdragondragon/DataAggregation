# 批次 02：coordinator 局部恢复

## 本批次目标
将当前“乱序即全局 bucket”的处理方式改为“优先局部恢复”，只把受污染前缀切入 overflow，未污染的后续 key 继续走内存 sortmerge。

## 目标文件
- `core/src/main/java/com/jdragon/aggregation/core/sortmerge/AdaptiveMergeConfig.java`
- `core/src/main/java/com/jdragon/aggregation/core/sortmerge/AdaptiveMergeCoordinator.java`
- `core/src/main/java/com/jdragon/aggregation/core/sortmerge/SortMergeStats.java`
- `core/src/test/java/com/jdragon/aggregation/core/sortmerge/AdaptiveMergeCoordinatorTest.java`

## 计划改动点
- `OrderViolationAction` 新增 `RECOVER_LOCAL`
- 默认 `onOrderViolation` 调整为 `RECOVER_LOCAL`
- 在 coordinator 中区分：
  - `maxObservedKeyBySource`
  - 用于判定可释放窗口的单调边界
- 对超出本地缓冲能力的乱序：
  - 计算恢复上界
  - spill `PendingWindow` 中受污染前缀
  - 将 offending group 直接写入 overflow
  - 提升 `bucketUpperBound`
  - 保持 `bucketMode=false`

## 关键不变量
- `key <= bucketUpperBound` 必须继续直达 overflow
- 局部恢复不能让同一 key 同时走内存和 overflow 两条路径
- 显式配置 `BUCKET` / `FAIL` 时保持原行为

## 风险与回退点
- 风险：推进边界语义拆分不当，会让 `isResolvable(...)` 判断失真
- 风险：恢复上界过大，会扩大 spill 区间
- 回退点：保留 `BUCKET` / `FAIL` 原实现分支

## 本批次完成标准
- 局部乱序不再直接触发全局 `bucketMode`
- 受污染前缀进入 overflow，后续更大 key 仍可继续走内存路径
- 统计中能区分发生了局部恢复

## 执行记录
- 2026-03-30：开始实施。当前已确认：
  - `lastReadKeyBySource` 需要升级为单调边界，避免残余乱序把已推进边界向后拉回
  - `RECOVER_LOCAL` 将只 spill `PendingWindow` 的受污染前缀，并保留后续内存 sortmerge 能力
  - 现有 `BUCKET` / `FAIL` 测试会保留，新逻辑额外补 `RECOVER_LOCAL` 场景
> 2026-03-31 注：该设计已淘汰。当前 coordinator 不再依赖 `RECOVER_LOCAL` / 最小 key 可判定模型，而是改为按 key 等待 source 到齐并按 first-seen 顺序驱逐。本文仅保留为历史记录。
