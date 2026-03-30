# Part3 SortMerge 基准与验证报告

生成日期: 2026-03-30

## 1. 本文件用途

本文件用于承接 `part3` 在当前机器上的收尾验证结果，覆盖：

- core 基线测试
- MySQL integration 场景
- benchmark 小 / 中 / 定向场景
- `mysql_large` 本轮重新扩跑时的新发现

配套交接文档：

- `validation-reports/part3/05-handoff-20260330.md`
- `validation-reports/part3/06-current-machine-takeover-20260330.md`

## 1.1 后续补充说明

在本报告生成后，core 层又补了一轮与 spill/rebalance 直接相关的代码与测试：

- 新增 `rebalancePartitionMultiplier`
  - legacy streaming 走 `cache.rebalancePartitionMultiplier`
  - adaptive sort-merge 走 `adaptiveMerge.rebalancePartitionMultiplier`
- 新增 `activeSpillBytes`
  - 继续保留原 `spillBytes` 作为累计写盘字节
  - `activeSpillBytes` 表示当前尚未释放的 active spill 配额
- consumed partition 文件支持 eager cleanup
  - 读取完成并关闭 reader 后可立即删除
  - 删除时同步释放 `SpillGuard` active quota

因此，下面 benchmark 表中的 `spillBytes` 仍应理解为累计值；若后续重新跑 benchmark，建议同时补采 `activeSpillBytes`。

## 2. 执行环境与命令

执行环境：

- Java: `1.8.0_401`
- 数据源：复用 `core/src/main/resources/fusion-mysql-demo.json` 中的 `mysql8`
- 数据库：`agg_test`

本轮实际执行命令：

```powershell
mvn -q -pl core "-Dtest=AdaptiveMergeCoordinatorTest,SpillGuardTest,StreamExecutionOptionsTest,PartitionedSpillStoreTest" test -DfailIfNoTests=false
mvn -q -pl data-mock -am "-Dtest=AdaptiveSortMergeMysqlIntegrationTest" test -DfailIfNoTests=false
```

benchmark 入口命令：

```powershell
mvn -q -pl data-mock -am -DrunSortMergeMysqlBenchmarks=true "-Dtest=AdaptiveSortMergeMysqlBenchmarkTest" test -DfailIfNoTests=false
```

本轮由于 `mysql_large` 长时间未完成，实际额外使用了分场景补跑：

```powershell
mvn -q -pl data-mock -am -DrunSortMergeMysqlBenchmarks=true "-DsortMergeScenario=mysql_small" "-DsortMergeReportDir=target/part3-report/mysql_small" "-Dtest=AdaptiveSortMergeMysqlBenchmarkTest" test -DfailIfNoTests=false
mvn -q -pl data-mock -am -DrunSortMergeMysqlBenchmarks=true "-DsortMergeScenario=mysql_medium" "-DsortMergeReportDir=target/part3-report/mysql_medium" "-Dtest=AdaptiveSortMergeMysqlBenchmarkTest" test -DfailIfNoTests=false
mvn -q -pl data-mock -am -DrunSortMergeMysqlBenchmarks=true "-DsortMergeScenario=mysql_sparse_local_disorder" "-DsortMergeReportDir=target/part3-report/mysql_sparse_local_disorder" "-Dtest=AdaptiveSortMergeMysqlBenchmarkTest" test -DfailIfNoTests=false
mvn -q -pl data-mock -am -DrunSortMergeMysqlBenchmarks=true "-DsortMergeScenario=mysql_small_spill_guard" "-DsortMergeReportDir=target/part3-report/mysql_small_spill_guard" "-Dtest=AdaptiveSortMergeMysqlBenchmarkTest" test -DfailIfNoTests=false
mvn -q -pl data-mock -am -DrunSortMergeMysqlBenchmarks=true "-DsortMergeScenario=mysql_large" "-DsortMergeReportDir=target/part3-report/mysql_large" "-Dtest=AdaptiveSortMergeMysqlBenchmarkTest" test -DfailIfNoTests=false
```

## 3. 基线验证结果

### 3.1 core 定向测试

- `AdaptiveMergeCoordinatorTest`：7 个测试通过
- `SpillGuardTest`：5 个测试通过
- `StreamExecutionOptionsTest`：4 个测试通过
- `PartitionedSpillStoreTest`：3 个测试通过
- 总计：19 个测试全部通过

### 3.2 MySQL integration 场景

- `AdaptiveSortMergeMysqlIntegrationTest`：3 个测试通过

通过场景：

1. `shouldRunConsistencyAndFusionAgainstMysqlDemoSource`
2. `shouldAbsorbSparseLocalDisorderWithoutEarlyBucket`
3. `shouldFailFastWhenSpillQuotaTooSmall`

结论：

- source 侧局部乱序缓冲在真实 MySQL 链路下可吸收稀疏倒退
- spill guard 在真实 MySQL 链路下可快速失败

## 4. benchmark 结果

### 4.1 三档基准结果

| 场景 | 数据量 | consistency total | consistency inconsistent | duplicateIgnored | fusion output | fusion content match | consistency engine | fusion engine | spilled keys | localReorderedGroupCount | orderRecoveryCount | spillBytes | spillGuardTriggered | ordered output | 结果 |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| mysql_small | 100 | 100 | 4 | 0 | 100 | PASS | sortmerge | sortmerge | 0 | 0 | 0 | 0 | NO | YES | PASS |
| mysql_medium | 10000 | 10000 | 506 | 48 | 10000 | PASS | sortmerge | sortmerge | 0 | 0 | 0 | 0 | NO | YES | PASS |
| mysql_large | 1000000 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 本轮 full benchmark 中长时间未完成 |

### 4.2 定向场景结果

| 场景 | 数据量 | 场景目标 | consistency status | consistency engine | fusion engine | localReorderedGroupCount | orderRecoveryCount | spillBytes | spillGuardTriggered | spillGuardReason | fusion error | 结果 |
| --- | ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | --- | --- | --- |
| mysql_sparse_local_disorder | 2000 | 稀疏局部倒退应由 source 缓冲吸收 | PARTIAL_SUCCESS | sortmerge | sortmerge | 15 | 0 | 0 | NO |  |  | PASS |
| mysql_small_spill_guard | 5000 | spill guard 应快速失败 | FAILED |  |  | 0 | 0 | 1048505 | YES | Spill limit exceeded: requested=192, reserved=1048505, max=1048576 | Spill limit exceeded: requested=190, reserved=1048564, max=1048576 | PASS |

说明：

- `mysql_sparse_local_disorder` 中 `localReorderedGroupCount = 15`，说明确实观测到了局部倒退，但 `orderRecoveryCount = 0`，表示 source 侧缓冲已吸收，无需 coordinator 做前缀恢复。
- `mysql_small_spill_guard` 的关键观察点是 `spillBytes` 与 `spillGuardTriggered`，本轮未观测到局部乱序恢复动作。

## 5. 性能结果

### 5.1 三档基准性能

| 场景 | 数据量 | 建表造数耗时(ms) | consistency耗时(ms) | consistency吞吐(keys/s) | consistency峰值堆MB | fusion耗时(ms) | fusion吞吐(rows/s) | fusion峰值堆MB | spilled keys | ordered output |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| mysql_small | 100 | 2525 | 251 | 398.41 | 32.34 | 123 | 813.01 | 44.18 | 0 | YES |
| mysql_medium | 10000 | 4556 | 811 | 12330.46 | 239.48 | 615 | 16260.16 | 389.20 | 0 | YES |
| mysql_large | 1000000 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 | 未完成 |

### 5.2 定向场景执行耗时

| 场景 | 数据量 | 建表造数耗时(ms) | consistency耗时(ms) | consistency峰值堆MB | fusion耗时(ms) | fusion峰值堆MB | 说明 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| mysql_sparse_local_disorder | 2000 | 3260 | 498 | 86.34 | 311 | 93.54 | 正常完成，属于局部乱序恢复验证 |
| mysql_small_spill_guard | 5000 | 2765 | 18318 | 126.99 | 15981 | 140.81 | 触发失败耗时，属于 spill guard 快速失败验证 |

## 6. mysql_large 本轮新发现

本轮完整 benchmark 在 `mysql_large` 上没有在合理窗口内完成。

已确认的事实：

- 第一轮 full benchmark 进程于 `2026-03-30 08:59:18` 启动
- 第一轮日志显示 `mysql_large` 于 `2026-03-30 09:01:03` 开始读取三源有序数据
- 第一轮到 `2026-03-30 09:39:01`，`data-mock/target/sortmerge-benchmark/mysql_large/consistency-overflow/...` 下已写出 8 个分区文件
- 第一轮每个分区文件大小约 `16.8 MB`
- 第一轮在观察超过 `55` 分钟后仍未自然结束，因此人工停止
- 第二轮又单独执行了 `mysql_large` 场景：
  - 命令启动时间约为 `2026-03-30 10:04:59`
  - `aggregation-10_05_05.254.log` 最后更新时间停在 `2026-03-30 10:06:38`
  - 但 `data-mock/target/sortmerge-benchmark/mysql_large/consistency-overflow/...` 在 `2026-03-30 11:36:08 ~ 11:36:09` 仍持续写出了 8 个分区文件
  - 第二轮每个分区文件大小约 `42.4 MB`
  - `consistency-spill/consistency-differences-*.spill` 增长到约 `4.0 MB`
  - 顶层目录始终只有 `consistency-overflow` 与 `consistency-spill`，未进入 fusion 阶段产物
- 第二轮在 `90` 分钟窗口内仍未自然结束，因此再次人工停止，不再继续占用当前机器与 MySQL

这说明：

- `part3` 的小 / 中 / 定向场景闭环已经完成
- 但 `mysql_large` 在当前机器上存在新的性能或长尾问题
- 该问题不是功能正确性立即失败，而是 consistency 阶段在 overflow / spill 回放路径上长时间运行、且明显超出 part2 历史基线的新风险

作为参考，`part2` 历史报告中的 `mysql_large` 曾给出：

- `consistency = 72723 ms`
- `fusion = 81538 ms`
- `fusion spilled keys = 879450`
- `ordered output = NO`

本轮尚未拿到新的 `mysql_large` 完整终值，因此不能直接与 part2 做定量对比，只能确认“当前机器重跑时大场景明显变慢”。

## 7. 结论

当前可以确定：

1. `part3` 的功能性交付已完成，并在当前机器上通过了 core 与 integration 验证。
2. `part3` 的新增语义已通过 benchmark 定向场景确认：
   - 局部乱序缓冲有效
   - spill guard 可快速失败
3. `mysql_small` 与 `mysql_medium` 的纯 sortmerge 基准已重新跑通。
4. `mysql_large` 本轮两次重跑都暴露了新的长时间运行问题，这是当前 `part3` 收尾阶段的主要剩余风险。

因此，`part3` 当前的真实状态应描述为：

- 小 / 中 / 定向场景：已闭环
- 百万级 benchmark：功能历史上已验证，但本轮当前机器重跑存在长尾性能风险，需单独作为下一步排查主题
