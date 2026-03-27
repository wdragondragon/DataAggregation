# Fusion / Consistency 排序归并测试方法与结论

生成日期: 2026-03-27

## 1. 本文件用途

本文件用于说明本次会话中，针对 `fusion-sortmerge` 与 `consistency-sortmerge` 的测试方法、执行命令、测试数据生成方式以及最终结论。

下次会话如果需要重新验证，只需按本文命令执行即可，不需要重新设计测试方案。

## 2. 测试基础信息

### 2.1 测试模块

测试模块位于：

- `data-mock`

关键文件：

- `data-mock/src/test/java/com/jdragon/aggregation/datamock/sortmerge/AdaptiveSortMergeTestSupport.java`
- `data-mock/src/test/java/com/jdragon/aggregation/datamock/sortmerge/AdaptiveSortMergeMysqlIntegrationTest.java`
- `data-mock/src/test/java/com/jdragon/aggregation/datamock/sortmerge/AdaptiveSortMergeMysqlBenchmarkTest.java`

### 2.2 数据源

本次测试直接复用框架自己的 MySQL 数据源能力，连接配置来自：

- `core/src/main/resources/fusion-mysql-demo.json`

测试库：

- `agg_test`

### 2.3 测试表

测试会动态创建并删除以下表：

- `sm_<label>_a`
- `sm_<label>_b`
- `sm_<label>_c`

例如：

- `sm_mysql_small_a`
- `sm_mysql_medium_b`
- `sm_mysql_large_c`

测试结束后会自动删表。

## 3. 测试数据生成规则

测试数据由 `AdaptiveSortMergeTestSupport` 自动生成，规则为固定且可复现。

### 3.1 三个源的基础语义

- `sourceA`
  - 每个 `biz_id` 都有记录
- `sourceB`
  - 某些 key 缺失
  - 某些 key 存在字段差异
  - 某些 key 存在重复记录
- `sourceC`
  - 某些 key 缺失
  - 某些 key 存在字段差异
  - 某些 key 存在重复记录

### 3.2 缺失与差异规则

- `sourceB` 缺失：`biz_id % 50 == 0`
- `sourceC` 缺失：`biz_id % 70 == 0`
- `sourceB.age` 差异：`biz_id % 90 == 0`
- `sourceB.salary` 差异：`biz_id % 125 == 0`
- `sourceC.department` 差异：`biz_id % 120 == 0`
- `sourceC.status` 差异：`biz_id % 135 == 0`
- `sourceB` 重复：`biz_id % 333 == 0`
- `sourceC` 重复：`biz_id % 500 == 0`

### 3.3 融合优先级

融合测试中 source 优先级为：

- `sourceB = 30`
- `sourceC = 20`
- `sourceA = 10`

因此：

- 优先选 `sourceB`
- `sourceB` 缺失时回退 `sourceC`
- `sourceB` 与 `sourceC` 都缺失时回退 `sourceA`

## 4. 已执行的测试命令

### 4.1 小规模集成测试

用于快速验证百条数据是否功能正确：

```bash
mvn -q -pl data-mock -am test -Dtest=AdaptiveSortMergeMysqlIntegrationTest -DfailIfNoTests=false
```

### 4.2 三档基准测试

用于执行百条、万条、百万条全套测试：

```bash
mvn -q -pl data-mock -am test -Dtest=AdaptiveSortMergeMysqlBenchmarkTest -DrunSortMergeMysqlBenchmarks=true -DfailIfNoTests=false
```

### 4.3 单独执行某一个规模场景

用于只重跑某个场景，便于定位问题：

```bash
mvn -q -pl data-mock -am test -Dtest=AdaptiveSortMergeMysqlBenchmarkTest -DrunSortMergeMysqlBenchmarks=true -DsortMergeScenario=mysql_large -DfailIfNoTests=false
```

可选值：

- `mysql_small`
- `mysql_medium`
- `mysql_large`

### 4.4 data-mock 模块常规测试

用于确认测试模块自身没有回归：

```bash
mvn -q -pl data-mock -am test -DfailIfNoTests=false
```

## 5. 校验内容

### 5.1 Consistency 校验项

- `totalRecords`
- `inconsistentRecords`
- `duplicateIgnoredCount`
- `executionEngine`
- `mergeSpilledKeyCount`
- `fallbackReason`

### 5.2 Fusion 校验项

- 输出总行数
- 样例 key 的最终融合结果
- 内容 digest
- `executionEngine`
- `duplicateIgnoredCount`
- `mergeSpilledKeyCount`
- `fallbackReason`

说明：

- 小中场景下要求输出顺序保持有序，因此校验有序 digest
- 百万级 hybrid 场景下允许输出顺序变化，因此使用“内容 digest”校验结果集内容是否一致

## 6. 最终测试结论

### 6.1 测试用例结论

来自 `validation-reports/part2/sortmerge-mysql-benchmark-20260327.md` 的最终结果：

- `mysql_small`
  - 数据量：`100`
  - `consistency engine = sortmerge`
  - `fusion engine = sortmerge`
  - 内容校验：`PASS`
  - spilled keys：`0`

- `mysql_medium`
  - 数据量：`10000`
  - `consistency engine = sortmerge`
  - `fusion engine = sortmerge`
  - 内容校验：`PASS`
  - spilled keys：`0`

- `mysql_large`
  - 数据量：`1000000`
  - `consistency engine = hybrid`
  - `fusion engine = hybrid`
  - 内容校验：`PASS`
  - spilled keys：`879450`

### 6.2 性能结论

三档数据的性能结果如下：

- `mysql_small`
  - 建表造数：`2070 ms`
  - consistency：`187 ms`
  - fusion：`56 ms`
  - consistency heap：`30.48 MB`
  - fusion heap：`39.23 MB`

- `mysql_medium`
  - 建表造数：`1423 ms`
  - consistency：`613 ms`
  - fusion：`553 ms`
  - consistency heap：`298.31 MB`
  - fusion heap：`369.12 MB`

- `mysql_large`
  - 建表造数：`87480 ms`
  - consistency：`72723 ms`
  - fusion：`81538 ms`
  - consistency heap：`1686.54 MB`
  - fusion heap：`2234.12 MB`

### 6.3 关键业务结论

本次测试已经确认：

1. 实验 reader 与真实 MySQL 数据源链路是通的
2. 小中数据量下可稳定运行在纯 sortmerge 模式
3. 百万级数据下会自动进入 hybrid
4. 即使进入 hybrid，结果内容仍然正确
5. duplicate first-row 语义仍然保持一致

### 6.4 当前最重要的结论

当前必须记住的结论只有一条：

- 百万级场景下“内容正确”已经被验证
- 但 hybrid 场景下“最终输出顺序不再保证全局有序”

因此如果下次要继续推进，需要先明确需求到底是：

- 只要求内容正确
- 还是要求 spill 后也必须保持全局有序输出

## 7. 报告位置

本次测试报告已保存两份，内容相同：

- `validation-reports/part2/sortmerge-mysql-benchmark-20260327.md`
- `data-mock/validation-reports/sortmerge-mysql-benchmark-20260327.md`

## 8. 下次会话可直接引用的上下文

下次如果需要直接续上，可以把下面这段话原样发出：

> 我们已经用 `data-mock` 模块和 `fusion-mysql-demo.json` 的真实 MySQL 数据源，完成了 `fusion-sortmerge` / `consistency-sortmerge` 的百条、万条、百万条测试。结果是：100 和 10000 走纯 sortmerge，1000000 进入 hybrid，内容正确但输出顺序不再全局有序。请基于 `validation-reports/part2/implementation-progress.md` 和 `validation-reports/part2/testing-usage-and-conclusion.md` 继续。

