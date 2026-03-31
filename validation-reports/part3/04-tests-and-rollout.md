# 批次 04：测试与验收

## 本批次目标
补齐局部乱序恢复与 spill guard 的核心测试，并把新增统计透出到 sortmerge 结果里，确保下一会话能够直接继续扩集成测试或调优默认值。

## 目标文件
- `core/src/test/java/com/jdragon/aggregation/core/sortmerge/AdaptiveMergeCoordinatorTest.java`
- `core/src/test/java/com/jdragon/aggregation/core/streaming/SpillGuardTest.java`
- `core/src/test/java/com/jdragon/aggregation/core/streaming/StreamExecutionOptionsTest.java`
- `core/src/test/java/com/jdragon/aggregation/core/streaming/PartitionedSpillStoreTest.java`
- `core/src/main/java/com/jdragon/aggregation/core/sortmerge/SortMergeStats.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/AdaptiveSortMergeConsistencyExecutor.java`
- `data-mock/src/test/java/com/jdragon/aggregation/datamock/sortmerge/*`

## 计划改动点
- 协调器级测试覆盖：
  - 本地缓冲吸收局部乱序
  - `RECOVER_LOCAL`
  - `BUCKET`
  - `FAIL`
  - `bucketUpperBound` 前缀污染路由
- spill guard 测试覆盖：
  - 总配额超限
  - 低磁盘余量失败
  - 共享 guard 累计字节
- 结果透出：
  - `localReorderedGroupCount`
  - `orderRecoveryCount`
  - `spillBytes`
  - `activeSpillBytes`
  - `spillGuardTriggered`
  - `spillGuardReason`

## 关键不变量
- 单测优先覆盖 coordinator 与 spill store 这两个语义最敏感的点
- data-mock 集成测试如果环境依赖较重，可先保持增量补充，不阻塞核心功能落地

## 风险与回退点
- 风险：只改统计不跑测试，容易把语义偏差带到下个会话
- 风险：data-mock MySQL 依赖环境不足时，测试不能作为当前会话的硬门槛
- 回退点：至少保证 core 层单测覆盖新逻辑，集成测试作为可继续扩展项

## 本批次完成标准
- core 层新增测试可跑通
- sortmerge 结果能看到新增关键统计
- 交接文档中明确说明 data-mock 是否已覆盖、是否受环境限制

## 执行记录
- 2026-03-30：开始实施。当前已确认：
  - `AdaptiveMergeCoordinatorTest` 是最直接的 sortmerge 语义基线
  - spill guard 需要新增独立测试类验证共享配额行为
  - data-mock 现有 MySQL benchmark/integration 受环境开关控制，可作为后续补充验证层
- 2026-03-30：验证过程中先后遇到两类环境噪音：
  - 默认 Java 版本会让项目历史代码和 `java.lang.Record` 产生编译歧义
  - Maven settings 存在与旧版 Maven 不完全兼容的告警标签
  这两类问题都不是本轮 sortmerge 改动引入的，最终已切换到可兼容的编译链完成验证。
- 2026-03-30：core 层针对性验证已完成。
  - 执行 `AdaptiveMergeCoordinatorTest` 与 `SpillGuardTest`
  - 结果：`AdaptiveMergeCoordinatorTest` 7 个测试通过，`SpillGuardTest` 3 个测试通过，总计 10 个测试全部通过。
- 2026-03-30：后续补充验证已覆盖 rebalance 倍率与 eager cleanup。
  - 执行：`AdaptiveMergeCoordinatorTest`、`SpillGuardTest`、`StreamExecutionOptionsTest`、`PartitionedSpillStoreTest`
  - 结果：7 + 5 + 4 + 3，共 19 个测试通过
  - 新覆盖点：
    - `rebalancePartitionMultiplier` 按当前父分区数放大 child 分区
    - 热点父桶在更大 child 分区下可重新分散
    - consumed partition 文件可在读取完成后删除
    - eager cleanup 会同步释放 `activeSpillBytes`
- 2026-03-30：`data-mock` 集成层本轮未扩跑。当前 core 层语义已覆盖：
  - source 侧局部乱序缓冲
  - `RECOVER_LOCAL`
  - `BUCKET` / `FAIL` 兼容分支
  - spill quota / free disk watermark
  下个会话若继续，可在 `data-mock` 中追加“基本有序 + 稀疏倒退”场景。
- 2026-03-30：继续实施时，`data-mock` 的下一步切入点已明确：
  - 通过可选自定义 query 顺序构造“基本有序 + 稀疏局部倒退”场景
  - 通过可选 `AdaptiveMergeConfig` 覆盖构造“小 spill 配额快速失败”场景
  - 保持默认 `mysql_small / medium / large` 场景行为不变
- 2026-03-30：本批次继续落地 `data-mock` 验证层，计划顺序如下：
  - 补齐 `AdaptiveSortMergeTestSupport` 中的 `ScenarioOptions`、结果统计字段与错误透出字段
  - 在 MySQL integration test 中新增“稀疏局部倒退但不应过早 bucket”场景
  - 在 MySQL integration test 中新增“小 spill 配额触发快速失败”场景
  - 最后按可运行范围补跑 core 与 data-mock 的针对性测试
- 2026-03-30：根据当前会话约束，本批次只完成代码与交接文档更新，不执行新的 MySQL 验证。
- 2026-03-30：当前机器已补跑 MySQL integration 验证。
  - 执行 `mvn -q -pl data-mock -am "-Dtest=AdaptiveSortMergeMysqlIntegrationTest" test -DfailIfNoTests=false`
  - 结果：3 个场景全部通过
  - 已验证：
    - 默认小样本场景
    - 稀疏局部倒退但不应过早进入 bucket
    - spill guard 小配额快速失败
- 2026-03-30：当前机器已补跑 benchmark 小 / 中 / 定向场景，并生成报告：
  - `validation-reports/part3/07-benchmark-and-validation-20260330.md`
  - 已完成：
    - `mysql_small`
    - `mysql_medium`
    - `mysql_sparse_local_disorder`
    - `mysql_small_spill_guard`
- 2026-03-30：`mysql_large` 在本轮重新扩跑时暴露出新的长尾风险。
  - full benchmark 进入 `mysql_large` 后，超过 55 分钟仍未自然结束
  - 当前已确认它会持续写出 `consistency-overflow` 分区文件
  - 本轮据此将其记录为新的性能问题，不在同一批次继续修改 sortmerge 核心算法
- 2026-03-30：随后又补跑了一次 `mysql_large` 单场景，用于缩小问题范围。
  - 命令：`mvn -q -pl data-mock -am -DrunSortMergeMysqlBenchmarks=true "-DsortMergeScenario=mysql_large" "-DsortMergeReportDir=target/part3-report/mysql_large" "-Dtest=AdaptiveSortMergeMysqlBenchmarkTest" test -DfailIfNoTests=false`
  - 现象：`aggregation-10_05_05.254.log` 在 `10:06:38` 后不再追加，但 `consistency-overflow` 在 `11:36:08 ~ 11:36:09` 仍持续写出 8 个约 `42.4 MB` 的分区文件
  - 同时 `consistency-spill/consistency-differences-*.spill` 增长到约 `4.0 MB`
  - 90 分钟窗口内仍未自然结束，且目录下没有出现 fusion 阶段产物
  - 结论：当前机器上的长尾问题更像是 consistency 阶段 overflow / spill 回放过慢，而不是 benchmark 主流程完全无动作
> 2026-03-31 注：本文档包含的 `RECOVER_LOCAL`、`local disorder` 测试计划属于旧方案。当前实现已改为无序 pending window 等待合并，本文仅保留为历史记录。
