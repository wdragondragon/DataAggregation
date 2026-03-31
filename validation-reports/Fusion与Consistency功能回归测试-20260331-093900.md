# Fusion与Consistency功能回归测试

测试时间：2026-03-31 09:35:00 - 2026-03-31 09:38:09  
测试目录：`core`、`data-mock`  
运行环境：`C:\dev\ideaProject\DataAggregation`  
聚合运行目录：`-Daggregation.home=C:\dev\ideaProject\DataAggregation\package_all\aggregation`

## 1. 测试目标

本轮回归聚焦 `fusion` 与 `consistency` 的功能正确性，重点验证：

- 新的 pending window 按 key 等待合并模型是否稳定工作
- `fusion` 在 `JobContainer`、稀疏乱序、紧窗口 spill 场景下是否仍能得到正确结果
- `consistency` 在 `Example` 与 `Reader` 两条链路下，`insert / update / delete` 三类差异是否仍能被正确识别、输出和修复
- `core` 到 `data-mock` 的关键回归链路是否保持通过

本轮不包含新的 100w 压测执行，已有压测结果仍以历史专项报告为准。

## 2. 本轮执行命令

### 2.1 core 回归

```bash
mvn -pl core "-Dtest=AdaptiveMergeCoordinatorTest" test
```

### 2.2 data-mock 回归

```bash
mvn -pl data-mock -am "-Dtest=FusionJobContainerMysqlIntegrationTest,FusionJobContainerMysqlAdaptiveBehaviorTest,AdaptiveSortMergeMysqlIntegrationTest,ConsistencyMysqlCrossValidationIntegrationTest" test -DfailIfNoTests=false
```

## 3. 回归范围

本轮实际覆盖的测试类如下：

- `core/src/test/java/com/jdragon/aggregation/core/sortmerge/AdaptiveMergeCoordinatorTest.java`
- `data-mock/src/test/java/com/jdragon/aggregation/datamock/fusion/FusionJobContainerMysqlIntegrationTest.java`
- `data-mock/src/test/java/com/jdragon/aggregation/datamock/fusion/FusionJobContainerMysqlAdaptiveBehaviorTest.java`
- `data-mock/src/test/java/com/jdragon/aggregation/datamock/sortmerge/AdaptiveSortMergeMysqlIntegrationTest.java`
- `data-mock/src/test/java/com/jdragon/aggregation/datamock/consistency/ConsistencyMysqlCrossValidationIntegrationTest.java`

## 4. 执行结果汇总

| 模块 | 测试类 | 用例数 | 耗时 | 结果 | 主要覆盖点 |
| --- | --- | ---: | ---: | --- | --- |
| `core` | `AdaptiveMergeCoordinatorTest` | 3 | 1.562s | 通过 | key 到齐即发、超窗 spill、晚到 key 旁路 overflow、源结束 drain |
| `data-mock` | `FusionJobContainerMysqlIntegrationTest` | 1 | 0.331s | 通过 | 100 条数据下 `JobContainer` 融合正确性、样本值与全量结果对齐 |
| `data-mock` | `FusionJobContainerMysqlAdaptiveBehaviorTest` | 3 | 119.584s | 通过 | 稀疏乱序无 spill、紧窗口 spill、active spill quota 清理释放 |
| `data-mock` | `AdaptiveSortMergeMysqlIntegrationTest` | 3 | 37.349s | 通过 | MySQL 多源融合与一致性联测、乱序输入、紧窗口 spill 行为 |
| `data-mock` | `ConsistencyMysqlCrossValidationIntegrationTest` | 1 | 7.093s | 通过 | `insert/update/delete` 三类差异、Example/Reader 四路交叉验证 |

汇总结果：

- `core`：`3` 条用例全部通过
- `data-mock`：`8` 条用例全部通过
- 总计：`11` 条用例全部通过
- 失败数：`0`
- 错误数：`0`
- 跳过数：`0`

## 5. 关键验证点

### 5.1 Pending Window 回归

`AdaptiveMergeCoordinatorTest` 本轮验证了新的等待窗口语义：

- 不再依赖“最小 key 可判定”推进
- 当同一个 key 的所有 source 到齐后，立即下发处理
- 当窗口过小导致 key 无法继续等待时，最早进入窗口的未完成 key 会整体 spill
- 已 spill key 的晚到数据不会重新回到内存窗口，而是直接进入 overflow
- 所有 source 结束后，窗口中剩余未 spill 的 key 可以正常 drain

### 5.2 Fusion 正确性回归

`FusionJobContainerMysqlIntegrationTest` 继续验证了标准 `JobContainer` 融合链路：

- 两张 MySQL 源表融合到一张目标表
- 100 条数据下聚合指标、样本行、全量结果都与预期一致
- 该验证仍使用 `fusion-mysql-demo.json` 的连接配置与测试支撑动态建表/灌数

### 5.3 Fusion 乱序与 Spill 回归

`FusionJobContainerMysqlAdaptiveBehaviorTest` 覆盖了 3 个关键场景：

1. `shouldMergeSparseOutOfOrderKeysThroughJobContainer`
   - `sourceB` 稀疏乱序
   - 期待结果：仍保持 `sortmerge`
   - 实际结果：通过，且 `windowImmediateResolvedKeyCount > 0`、`spillBytes = 0`

2. `shouldSpillAndStillFuseCorrectlyWhenDisorderExceedsLocalBuffer`
   - 使用极小 `pendingKeyThreshold`
   - 期待结果：触发 spill，但融合结果仍正确
   - 实际结果：通过，执行引擎进入 `hybrid`，并产生 `spillBytes` 与 `windowEvictedKeyCount`

3. `shouldReleaseActiveSpillQuotaAfterHybridSpillCompletes`
   - 关注 spill 清理后的配额释放
   - 期待结果：作业结束后 `activeSpillBytes = 0`
   - 实际结果：通过，说明 eager cleanup 与 active quota 释放链路正常

### 5.4 Adaptive SortMerge MySQL 联测

`AdaptiveSortMergeMysqlIntegrationTest` 验证了数据源级 MySQL 场景下的融合与一致性联合行为：

- 小规模标准场景：`shouldRunConsistencyAndFusionAgainstMysqlDemoSource`
- 稀疏乱序场景：`shouldMergeSparseOutOfOrderKeysWithoutSpill`
- 紧窗口 spill 场景：`shouldSpillWhenPendingWindowIsTight`

本轮结果表明：

- 稀疏乱序输入不会破坏最终结果
- 当 pending window 变小后，系统可以退化到 spill/hybrid 路径
- spill 场景下，至少一条执行路径会产生 spill 且总体结果仍正确

### 5.5 Consistency 功能回归

`ConsistencyMysqlCrossValidationIntegrationTest` 继续验证 consistency 的核心能力：

- 场景规模：100 条
- 差异类型：`INSERT / UPDATE / DELETE`
- 验证链路：
  - `ConsistencyExample -> DataConsistencyService`
  - `JobContainer -> ConsistencyReader`
- 交叉验证结果：
  - `ComparisonResult`
  - Example 执行后的目标表快照
  - Reader 执行后的 `output` 差异表
  - Reader 执行后的目标表快照

本轮结果保持通过，说明：

- 三类差异的识别、输出与修复仍正常
- `Example` 与 `Reader` 两条链路结果一致
- 四路结果之间没有出现回归偏差

## 6. 本轮结论

截至 2026-03-31 09:38，本轮 `fusion` 与 `consistency` 功能回归结果为：

- 新的 pending window 按 key 等待合并模型回归通过
- `fusion` 的标准 `JobContainer` 融合链路回归通过
- `fusion` 的稀疏乱序、紧窗口 spill、spill 清理释放链路回归通过
- `consistency` 的 `insert / update / delete` 三类差异交叉验证回归通过
- `core` 与 `data-mock` 关键链路均未发现新的功能性回归

## 7. 备注

- 本轮 Maven 构建过程中仍存在项目既有 warning，例如部分 `pom` 缺少插件版本、`settings.xml` 中存在非标准 `release` 标签；这些 warning 未阻断本轮测试执行。
- 本轮未重新执行 100w 压测；若需要更新性能结论，应单独执行 benchmark 并生成专项报告。
- 当前工作区中存在其他未纳入本轮回归范围的本地改动，本报告仅记录本轮实际执行的回归测试结果。
