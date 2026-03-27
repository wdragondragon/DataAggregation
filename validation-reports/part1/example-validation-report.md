# ConsistencyExample / FusionExample 测试验证文档

## 1. 测试范围

- 测试日期：2026-03-27
- 工作目录：`C:\dev\ideaProject\DataAggregation`
- 运行基线：当前工作区已编译产物 + `package_all\aggregation` 插件目录
- 一致性验证入口：调用 `ConsistencyExample` 同源规则工厂 `createSampleRule()`，执行真实 `DataConsistencyService`
- 融合验证入口：使用 `core\src\main\resources\fusion-mysql-demo.json` 的原始配置，执行真实 `FusionReader + JobContainer`
- 判定方式：
  - 一致性：对比 `users_1` 运行前后数据、`comparison_result/differences/resolutions/report` 文件以及 `ComparisonResult` 返回统计
  - 融合：对比 `fusion_3` 运行前后目标表数据、`fusion_details_*.json/html` 文件，以及运行日志中的任务状态

## 2. 一致性比对验证

### 2.1 实际运行结果

- 运行时间：2026-03-27 13:04:56 至 2026-03-27 13:04:59
- 运行状态：`PARTIAL_SUCCESS`
- 统计结果：
  - `totalRecords = 7`
  - `consistentRecords = 0`
  - `inconsistentRecords = 7`
  - `resolvedRecords = 7`
- 数据源计数：
  - `source-1 = 6`
  - `source-2 = 6`
  - `source-3 = 7`
- 字段差异统计：
  - `salary = 7`
  - `age = 7`
  - `department = 3`
  - `email = 1`

### 2.2 生成结果文件

- 对比总结果：`C:\dev\ideaProject\DataAggregation\consistency-results\comparison_result_rule-001_20260327_130459.json`
- 差异明细：`C:\dev\ideaProject\DataAggregation\consistency-results\differences_20260327_130459.json`
- 解决结果：`C:\dev\ideaProject\DataAggregation\consistency-results\resolutions_20260327_130459.json`
- HTML 报告：`C:\dev\ideaProject\DataAggregation\consistency-results\consistency_report_rule-001_20260327_130459.html`
- 原始运行日志：`C:\dev\ideaProject\DataAggregation\validation-reports\logs\consistency-example.log`

这些文件均已成功生成，说明“比对识别差异”和“解决结果落文件”两段链路是通的。

### 2.3 运行前后目标数据对比

以 `source-1 / users_1` 作为自动修复目标表，执行前后快照如下：

- `users_1` 行数：`6 -> 6`
- 主键组合 `user_id + username` 重复数：`0 -> 0`
- 变更行数：`0`

这说明本次运行没有对 `users_1` 产生任何实际写入。

### 2.4 解决结果回放验证

我将 `resolutions_20260327_130459.json` 中的每一条 `resolvedValues` 回放到运行后的 `users_1` 快照上做逐条校验，结果如下：

- 共 7 条解决结果
- 其中 6 条在目标表中能够找到对应行，且字段值完全一致
- 其中 1 条无法在目标表中找到对应行

唯一不匹配的记录是：

```json
{
  "key": "user_id=7|username=wujiu",
  "resolvedValues": {
    "user_id": "7",
    "salary": null,
    "department": null,
    "age": null,
    "email": null,
    "username": "wujiu"
  },
  "afterRow": null,
  "problem": "resolved row exists in resolutions.json but was not inserted into users_1"
}
```

而其余 6 条记录都被标记为 `SKIP`，并且运行后的 `users_1` 与 `resolvedValues` 一致，说明这些键上的目标库本身已经是期望值，不需要再次更新。

### 2.5 一致性结论

结论分两层：

- 比对结果本身是可信的。
  - 7 条差异、7 条解决结果、差异字段统计、HTML 报告彼此一致。
  - `source-1/source-2/source-3` 的数据量与差异条数也能对上。
- 自动应用结果不完整。
  - 示例配置开启了 `autoApplyResolutions=true`，目标源是 `source-1`。
  - 但对于只存在于其他源、在 `source-1` 中缺失的 `user_id=7|username=wujiu`，系统没有补插这条记录。

因此，一致性模块当前状态应判定为：

**比对与解决结果生成正确，但“自动落库/补齐缺失记录”这段功能仍不完整，不能算完全正确。**

## 3. 融合验证

### 3.1 第一次运行结果

第一次按 `FusionExample` 原始配置执行时，日志显示任务失败：

- 日志文件：`C:\dev\ideaProject\DataAggregation\validation-reports\logs\fusion-example.log`
- 任务状态：`FAILED`
- 关键错误：`java.sql.SQLNonTransientConnectionException: No operations allowed after connection closed.`
- 日志定位：`RdbmsSourcePlugin.scanQuery(...)` 流式读取过程中抛出连接已关闭异常

这次失败运行仍然生成了详情文件，但内容为空：

- `fusion_details_20260327_130513.json`
- `fusion_details_20260327_130513.html`
- `summary.totalRecords = 0`
- `summary.successfulRecords = 0`

这说明第一次运行并没有真正完成融合输出。

### 3.2 第二次重跑结果

为了继续验证“融合结果值本身是否正确”，我在确认 `fusion_3` 为空表后进行了第二次重跑。

第二次重跑前后对比：

- 重跑前 `fusion_3`：`[]`
- 重跑后 `fusion_3`：7 行
- 最新详情文件：
  - `C:\dev\ideaProject\DataAggregation\fusion_details\fusion_details_20260327_130630.json`
  - `C:\dev\ideaProject\DataAggregation\fusion_details\fusion_details_20260327_130630.html`

### 3.3 源表与目标表核对

源表实际数据：

- `fusion_1`：7 行，字段为 `user_id / username / age`
- `fusion_2`：6 行，字段为 `user_id / salary / department / email`
- Join 类型：`LEFT`
- Join Key：`user_id`

重跑后 `fusion_3` 实际数据为：

```json
[
  {"user_id":1,"username":"zhangsan","age":31,"salary":15000.00,"dept":"Technology","email":"zhangsan@company.com","double_salary":30000.00,"ste":"zhangsan@company.com_test","condition_value":"31_old","constant":"用户","constant_int":123},
  {"user_id":2,"username":"lisi","age":28,"salary":12500.00,"dept":"Marketing","email":"lisi@company.com","double_salary":25000.00,"ste":"lisi@company.com_test","condition_value":"28_young","constant":"用户","constant_int":123},
  {"user_id":3,"username":"wangwu","age":35,"salary":18000.00,"dept":"Sales","email":"wangwu@company.com","double_salary":36000.00,"ste":"wangwu@company.com_test","condition_value":"35_old","constant":"用户","constant_int":123},
  {"user_id":4,"username":"zhaoliu","age":32,"salary":16000.00,"dept":"HR","email":"zhaoliu@company.com","double_salary":32000.00,"ste":"zhaoliu@company.com_test","condition_value":"32_old","constant":"用户","constant_int":123},
  {"user_id":5,"username":"sunqi","age":29,"salary":13000.00,"dept":"Finance","email":"sunqi@company.com","double_salary":26000.00,"ste":"sunqi@company.com_test","condition_value":"29_young","constant":"用户","constant_int":123},
  {"user_id":6,"username":"zhouba","age":31,"salary":14200.00,"dept":"Technology","email":"zhouba@company.com","double_salary":28400.00,"ste":"zhouba@company.com_test","condition_value":"31_old","constant":"用户","constant_int":123},
  {"user_id":7,"username":"huangsan","age":32,"double_salary":0.00,"ste":"no_email_test","condition_value":"32_old","constant":"用户","constant_int":123}
]
```

### 3.4 结果正确性核对

对照 `fusion-mysql-demo.json` 中的映射规则，重跑后的 7 行结果与预期一致：

- `user_id / username / age` 正确来自 `fusion_1`
- `salary / dept / email` 正确来自 `fusion_2`
- `double_salary = salary * 2`，例如 `15000 -> 30000`
- `ste = IFNULL(email, "no_email") + "_test"`
- `condition = age > 30 ? age + "_old" : age + "_young"`
- `constant = "用户"`
- `constant_int = 123`

重点核查两个代表样本：

1. `user_id=1`
- `fusion_1`: `zhangsan, 31`
- `fusion_2`: `15000, Technology, zhangsan@company.com`
- `fusion_3`: `30000 / zhangsan@company.com_test / 31_old`
- 结论：完全符合配置规则

2. `user_id=7`
- `fusion_1` 中存在
- `fusion_2` 中不存在
- 因为是 `LEFT JOIN + LENIENT`，所以：
  - `salary/dept/email = null`
  - `double_salary = 0`
  - `ste = no_email_test`
  - `condition = 32_old`
- 结论：完全符合 LEFT JOIN 缺失右表时的预期行为

第二次运行时控制台里出现的这些警告：

- `字段映射错误 [joinKey=7, targetField=dept]: 数据源不存在: fusion_2`
- `字段映射错误 [joinKey=7, targetField=salary]: 数据源不存在: fusion_2`
- `字段映射错误 [joinKey=7, targetField=email]: 数据源不存在: fusion_2`

在当前 `LEFT JOIN + LENIENT` 语义下属于可接受现象，不影响最终输出值正确性。

### 3.5 融合结论

融合模块也需要分两层结论：

- 数据结果层面：正确。
  - 在成功执行的那次重跑中，`fusion_3` 的 7 行数据与源表和配置规则完全一致。
- 执行稳定性层面：有缺陷。
  - 第一次真实运行明确失败，错误为 `No operations allowed after connection closed`。
  - 第二次重跑才成功，说明当前流式扫描路径存在不稳定问题，至少不是稳定可复现成功。

因此，融合模块当前状态应判定为：

**融合结果值本身是对的，但流式执行链路存在不稳定缺陷，不能认为已经完全可靠。**

## 4. 总结结论

综合两个 Example 的真实运行结果：

- 一致性：
  - 差异识别、解决结果和报告文件生成是正确的
  - 但自动应用到目标库时，缺失记录没有被补插，端到端结果不完整
- 融合：
  - 成功运行时，目标表结果值正确
  - 但第一次运行出现 `connection closed` 失败，说明流式扫描实现仍有稳定性问题

最终判定：

**当前重构版本还不能视为“完全验证通过”。**

更准确地说：

- 一致性比对：`比对正确，自动落库不完整`
- 数据融合：`结果正确，执行稳定性不足`

建议优先修复的两个问题：

1. 一致性更新链路中，对“目标源不存在但解决结果已给出”的记录增加补插逻辑
2. JDBC 流式扫描中 `Connection / Statement / ResultSet` 的生命周期管理，避免再次出现 `No operations allowed after connection closed`
