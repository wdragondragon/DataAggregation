# Studio 万表压测报告

generatedAt: 2026-04-10 11:36:00

runId: 20260410110758

| scenario | projectCode | workerCount | syncMs | queryP95Ms | statsP95Ms | taskDrainMs | workflowDrainMs | taskSuccess | workflowSuccess |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| medium | pressure_mock_20260410110758_medium | 1 | 1255910 | 1776 | 39599 | 137373 | 80405 | 64 | 32 |

## 测试用例

### medium

- 模型同步: PASS (sourceModels=3000, targetModels=3000)
- 模型筛选: PASS (success=48, failed=0, p95=1776ms)
- 模型统计分析: PASS (success=48, failed=0, p95=39599ms)
- 采集任务并发: PASS (success=64, failed=0, timeout=0)
- 工作流并发: PASS (success=32, failed=0, timeout=0)

## 结论

- 总体结论: 通过
- medium: 通过；模型同步=通过，模型筛选=通过，模型统计=通过，采集任务并发=通过，工作流并发=通过

## Details

### medium

- project: `pressure_mock_20260410110758_medium`
- workers: 1
- synced source models: 3000
- synced target models: 3000
- query: total=48, success=48, failed=0, throughput=6.45/s, p95=1776ms, p99=1781ms
- statistics: total=48, success=48, failed=0, throughput=0.40/s, p95=39599ms, p99=54765ms
- collection tasks: total=64, success=64, failed=0, timeout=0, drainMs=137373
- collection tasks workerDistribution: {studio-online-worker-01=64}
- workflows: total=32, success=32, failed=0, timeout=0, drainMs=80405
- workflows workerDistribution: {studio-online-worker-01=32}

