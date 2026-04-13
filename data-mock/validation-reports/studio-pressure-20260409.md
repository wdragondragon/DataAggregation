# Studio 万表压测报告

generatedAt: 2026-04-09 23:53:33

runId: 20260409233706

| scenario | projectCode | workerCount | syncMs | queryP95Ms | statsP95Ms | taskDrainMs | workflowDrainMs | taskSuccess | workflowSuccess |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small | pressure_mock_20260409233706_small | 1 | 920724 | 180 | 1004 | 26753 | 15808 | 16 | 8 |

## 测试用例

### small

- 模型同步: PASS (sourceModels=500, targetModels=500)
- 模型筛选: PASS (success=24, failed=0, p95=180ms)
- 模型统计分析: PASS (success=24, failed=0, p95=1004ms)
- 采集任务并发: PASS (success=16, failed=0, timeout=0)
- 工作流并发: PASS (success=8, failed=0, timeout=0)

## 结论

- 总体结论: 通过
- small: 通过；模型同步=通过，模型筛选=通过，模型统计=通过，采集任务并发=通过，工作流并发=通过

## Details

### small

- project: `pressure_mock_20260409233706_small`
- workers: 1
- synced source models: 500
- synced target models: 500
- query: total=24, success=24, failed=0, throughput=25.92/s, p95=180ms, p99=182ms
- statistics: total=24, success=24, failed=0, throughput=5.16/s, p95=1004ms, p99=1016ms
- collection tasks: total=16, success=16, failed=0, timeout=0, drainMs=26753
- collection tasks workerDistribution: {studio-online-worker-01=16}
- workflows: total=8, success=8, failed=0, timeout=0, drainMs=15808
- workflows workerDistribution: {studio-online-worker-01=8}

