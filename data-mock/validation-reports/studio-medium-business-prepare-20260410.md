# Medium 模型同步与业务元数据准备结果

## 用例

- 模型同步: PASS (source=3000, target=3000)
- 业务元数据回填: PASS (updated=3000, distinctValues=12)
- 索引队列排空: PASS
- 业务统计验证: PASS (matchedModels=3000, matchedItems=3000)

## 结论

- 当前项目已完成 medium 规模模型同步，并已给源侧 3000 个模型写入 12 组稳定业务值。
- 统计字段为 business:org:table.major，可直接在页面做柱状图、扇图、TopN 等统计。
- 当前分布为: {"sales_domain":438,"finance_domain":438,"risk_domain":438,"customer_domain":438,"supply_domain":187,"product_domain":187,"operations_domain":187,"content_domain":187,"settlement_domain":125,"compliance_domain":125,"device_domain":125,"analytics_domain":125}
- 统计桶样例: [{"key":"customer_domain","label":"customer_domain","value":"customer_domain","lowerBound":null,"upperBound":null,"count":"438"},{"key":"finance_domain","label":"finance_domain","value":"finance_domain","lowerBound":null,"upperBound":null,"count":"438"},{"key":"risk_domain","label":"risk_domain","value":"risk_domain","lowerBound":null,"upperBound":null,"count":"438"},{"key":"sales_domain","label":"sales_domain","value":"sales_domain","lowerBound":null,"upperBound":null,"count":"438"},{"key":"content_domain","label":"content_domain","value":"content_domain","lowerBound":null,"upperBound":null,"count":"187"},{"key":"operations_domain","label":"operations_domain","value":"operations_domain","lowerBound":null,"upperBound":null,"count":"187"},{"key":"product_domain","label":"product_domain","value":"product_domain","lowerBound":null,"upperBound":null,"count":"187"},{"key":"supply_domain","label":"supply_domain","value":"supply_domain","lowerBound":null,"upperBound":null,"count":"187"},{"key":"analytics_domain","label":"analytics_domain","value":"analytics_domain","lowerBound":null,"upperBound":null,"count":"125"},{"key":"compliance_domain","label":"compliance_domain","value":"compliance_domain","lowerBound":null,"upperBound":null,"count":"125"},{"key":"device_domain","label":"device_domain","value":"device_domain","lowerBound":null,"upperBound":null,"count":"125"},{"key":"settlement_domain","label":"settlement_domain","value":"settlement_domain","lowerBound":null,"upperBound":null,"count":"125"}]
