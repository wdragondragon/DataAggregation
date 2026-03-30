# Part3 当前机器接手记录

生成日期: 2026-03-30

## 1. 本文件用途

本文件用于记录 `part3` 在当前机器上的实际接手结果，重点说明：

- `part3` 相关源码是否已经同步到当前工作区
- 当前机器上的最小可用验证命令
- 历史编译阻塞是否仍然存在

下次会话若继续推进 `part3`，建议优先阅读：

1. `validation-reports/part3/05-handoff-20260330.md`
2. `validation-reports/part3/06-current-machine-takeover-20260330.md`
3. `validation-reports/part3/07-benchmark-and-validation-20260330.md`

## 2. 当前机器上的源码状态

已确认当前工作区已经包含 `part3` 对应实现，而不是只有文档：

- `AdaptiveMergeConfig`
  - 已包含：
    - `localDisorderEnabled`
    - `localDisorderMaxGroups`
    - `localDisorderMaxMemoryMB`
    - `maxSpillBytesMB`
    - `minFreeDiskMB`
    - `rebalancePartitionMultiplier`
    - `preferOrderedQuery`
    - `onOrderViolation = RECOVER_LOCAL`

- `OrderedSourceCursor`
  - 已包含 source 侧局部乱序缓冲统计：
    - `localReorderedGroupCount`
    - `localMergedDuplicateGroupCount`

- `AdaptiveMergeCoordinator`
  - 已包含：
    - `maxObservedKeyBySource`
    - `RECOVER_LOCAL`
    - 受污染前缀 spill
    - `bucketUpperBound` 路由约束
    - 共享 `SpillGuard` 接入

- `SortMergeStats`
  - 已包含：
    - `localReorderedGroupCount`
    - `localMergedDuplicateGroupCount`
    - `orderRecoveryCount`
    - `spillBytes`
    - `activeSpillBytes`
    - `spillGuardTriggered`
    - `spillGuardReason`

- `data-mock`
  - 已包含：
    - `ScenarioOptions`
    - `preferOrderedQuery`
    - 稀疏局部倒退场景
    - 小 spill 配额快速失败场景

## 3. 编译基线结论

### 3.1 历史阻塞与当前状态

历史上曾观察到：直接执行下面命令时，

```powershell
mvn -q -pl core "-Dtest=AdaptiveMergeCoordinatorTest,SpillGuardTest" test -DfailIfNoTests=false
```

会先在 `core/src/main/java/com/jdragon/aggregation/core/streaming/SourceRowScanner.java`
看到 `scanQuery(...)` 相关编译错误。

但当前状态已经更新：

- `SourceRowScanner` 已改为直接走 `executeQuerySql(...)`
- 该假性接口漂移阻塞已不再是当前机器上的必现问题

### 3.2 历史根因判断

这不是 `part3` 源码本身缺失，而是：

- `core` 依赖 sibling module `data-source-handler-abstract`
- 当只单独构建 `core` 且不带 `-am` 时，Maven 可能直接使用本地仓库里较旧的已安装构件
- 于是出现“源码里有 `scanQuery(...)`，但当前编译拿到的依赖类却没有该方法”的假性接口漂移

### 3.3 当前机器上的验证方式

当前 `core` 定向测试可以直接运行；若同时涉及 sibling module 变更，仍建议使用 reactor 方式把依赖模块一起带上：

```powershell
mvn -q -pl core -am "-Dtest=AdaptiveMergeCoordinatorTest,SpillGuardTest" test -DfailIfNoTests=false
```

结论：

- 当前机器上的 `part3` 编译基线已对齐
- 旧的 `scanQuery(...)` 假性阻塞说明应视为历史备注，不再是当前结论
- 后续凡是只验证 `core`、但又依赖 sibling module 新接口时，仍优先使用 `-am`

## 4. 当前机器上的实际验证结果

### 4.1 core 定向验证

执行命令：

```powershell
mvn -q -pl core "-Dtest=AdaptiveMergeCoordinatorTest,SpillGuardTest,StreamExecutionOptionsTest,PartitionedSpillStoreTest" test -DfailIfNoTests=false
```

结果：

- `AdaptiveMergeCoordinatorTest`：7 个测试通过
- `SpillGuardTest`：5 个测试通过
- `StreamExecutionOptionsTest`：4 个测试通过
- `PartitionedSpillStoreTest`：3 个测试通过
- 总计：19 个测试全部通过

说明：

- `RECOVER_LOCAL`
- `BUCKET`
- `FAIL`
- `bucketUpperBound`
- spill guard
- rebalance multiplier
- eager parent cleanup / active spill quota release

以上 core 级语义在当前机器上已通过最小验证。

### 4.2 data-mock / MySQL 定向验证

执行命令：

```powershell
mvn -q -pl data-mock -am "-Dtest=AdaptiveSortMergeMysqlIntegrationTest" test -DfailIfNoTests=false
```

结果：

- `AdaptiveSortMergeMysqlIntegrationTest`：3 个测试通过

本次已实际跑通的场景包括：

1. `shouldRunConsistencyAndFusionAgainstMysqlDemoSource`
2. `shouldAbsorbSparseLocalDisorderWithoutEarlyBucket`
3. `shouldFailFastWhenSpillQuotaTooSmall`

说明：

- 当前机器可访问 `fusion-mysql-demo.json` 中配置的 MySQL
- 稀疏局部倒退场景已验证 source 侧缓冲可吸收，不会过早进入全局 bucket
- 小 spill 配额场景已验证会快速失败，并透出 `Spill limit exceeded`

### 4.3 benchmark 收尾状态

当前机器上已经补跑并落档：

- `mysql_small`
- `mysql_medium`
- `mysql_sparse_local_disorder`
- `mysql_small_spill_guard`

对应报告：

- `validation-reports/part3/07-benchmark-and-validation-20260330.md`

同时，本轮也确认了一个新的现实边界：

- `mysql_large` 在当前机器重跑时没有在合理窗口内结束
- 它不是立即失败，而是进入了明显更长的 overflow / 长尾运行阶段
- 追加的单场景补跑进一步显示：
  - 日志在 ordered query 启动后很快停住
  - 但 `consistency-overflow` 与 `consistency-spill` 目录仍持续增长
  - 在 90 分钟窗口内依然没有进入 fusion 阶段
- 因此 `part3` 当前应被视为“功能闭环完成，但百万级 benchmark 仍有新的性能风险待排查”

## 5. 当前接手结论

可以认为：

- `part3` 已在当前机器完成源码接手
- `part3` 的 core 语义已验证
- `part3` 的 MySQL 集成层关键场景也已验证

当前不再把 `part3` 视为“另一台电脑上的未确认增量”，而是当前工作区中的已同步、已验证能力。

但需要额外补一句：

- 上述“已验证”主要针对功能正确性与小 / 中 / 定向场景
- 对于百万级 benchmark，本轮已确认存在新的长尾风险，详见 `07-benchmark-and-validation-20260330.md`

## 6. 后续继续推进时的注意事项

- 若只跑 `core`，优先使用 `-am`，避免误用本地旧构件导致假性编译错误
- 当前验证只覆盖了 `part3` 自身目标，不包含：
  - hybrid 后全局输出顺序保持
  - 更大规模 benchmark 重新扩跑
  - `part4+` 新能力

如果下次继续，可以直接把下面这段上下文贴给新会话：

> `part3` 已在当前机器完成接手并验证。当前 core 层已通过 `AdaptiveMergeCoordinatorTest`、`SpillGuardTest`、`StreamExecutionOptionsTest`、`PartitionedSpillStoreTest` 共 19 个测试；`SourceRowScanner -> scanQuery(...)` 的历史假性编译阻塞已移除。若涉及 sibling module 变更，验证时仍优先使用 `-am`。当前也已通过 `AdaptiveSortMergeMysqlIntegrationTest` 的 3 个场景。
