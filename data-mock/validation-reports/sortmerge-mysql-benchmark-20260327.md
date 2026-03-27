# SortMerge MySQL 测试报告

生成时间: 2026-03-27

数据源配置: 复用 `fusion-mysql-demo.json` 中的 `mysql8` 连接，测试库为 `agg_test`。

## 测试用例报告

| 场景 | 数据量 | consistency total | consistency inconsistent | duplicateIgnored | fusion output | fusion content match | consistency engine | fusion engine | spilled keys | fallback reason |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | ---: | --- |
| mysql_small | 100 | 100 | 4 | 0 | 100 | PASS | sortmerge | sortmerge | 0 |  |
| mysql_medium | 10000 | 10000 | 506 | 48 | 10000 | PASS | sortmerge | sortmerge | 0 |  |
| mysql_large | 1000000 | 1000000 | 50413 | 4658 | 1000000 | PASS | hybrid | hybrid | 879450 |  |

## 性能报告

| 场景 | 数据量 | 建表造数耗时(ms) | consistency耗时(ms) | consistency吞吐(keys/s) | consistency峰值堆MB | fusion耗时(ms) | fusion吞吐(rows/s) | fusion峰值堆MB | spilled keys | ordered output |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| mysql_small | 100 | 2070 | 187 | 534.76 | 30.48 | 56 | 1785.71 | 39.23 | 0 | YES |
| mysql_medium | 10000 | 1423 | 613 | 16313.21 | 298.31 | 553 | 18083.18 | 369.12 | 0 | YES |
| mysql_large | 1000000 | 87480 | 72723 | 13750.81 | 1686.54 | 81538 | 12264.22 | 2234.12 | 879450 | NO |

## 样例校验

以下样例来自融合结果，用于确认优先级回退和字段值选择正确：

### mysql_small

| biz_id | chosen_name | age | salary | department | status |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | B_name_0000001 | 21 | 10000.17 | finance | ACTIVE |
| 50 | C_name_0000050 | 33 | 10008.5 | marketing | HOLD |
| 70 | B_name_0000070 | 53 | 10011.9 | support | HOLD |
| 90 | B_name_0000090 | 37 | 10015.3 | engineering | HOLD |

### mysql_medium

| biz_id | chosen_name | age | salary | department | status |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | B_name_0000001 | 21 | 10000.17 | finance | ACTIVE |
| 50 | C_name_0000050 | 33 | 10008.5 | marketing | HOLD |
| 70 | B_name_0000070 | 53 | 10011.9 | support | HOLD |
| 90 | B_name_0000090 | 37 | 10015.3 | engineering | HOLD |
| 120 | B_name_0000120 | 29 | 10020.4 | engineering | NEW |
| 125 | B_name_0000125 | 34 | 10046.25 | ops | ACTIVE |
| 135 | B_name_0000135 | 44 | 10022.95 | sales | CLOSED |
| 333 | B_name_0000333 | 20 | 10056.61 | sales | ACTIVE |
| 350 | A_name_0000350 | 37 | 10059.5 | marketing | HOLD |
| 500 | C_name_0000500 | 39 | 10085.0 | marketing | NEW |
| 9999 | B_name_0009999 | 29 | 11699.83 | sales | CLOSED |
| 10000 | C_name_0010000 | 30 | 11700.0 | support | NEW |

### mysql_large

| biz_id | chosen_name | age | salary | department | status |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | B_name_0000001 | 21 | 10000.17 | finance | ACTIVE |
| 50 | C_name_0000050 | 33 | 10008.5 | marketing | HOLD |
| 70 | B_name_0000070 | 53 | 10011.9 | support | HOLD |
| 90 | B_name_0000090 | 37 | 10015.3 | engineering | HOLD |
| 120 | B_name_0000120 | 29 | 10020.4 | engineering | NEW |
| 125 | B_name_0000125 | 34 | 10046.25 | ops | ACTIVE |
| 135 | B_name_0000135 | 44 | 10022.95 | sales | CLOSED |
| 333 | B_name_0000333 | 20 | 10056.61 | sales | ACTIVE |
| 350 | A_name_0000350 | 37 | 10059.5 | marketing | HOLD |
| 500 | C_name_0000500 | 39 | 10085.0 | marketing | NEW |
| 9999 | B_name_0009999 | 29 | 11699.83 | sales | CLOSED |
| 10000 | C_name_0010000 | 30 | 11700.0 | support | NEW |
| 99999 | B_name_0099999 | 45 | 26999.83 | sales | CLOSED |
| 100000 | C_name_0100000 | 46 | 27000.0 | support | NEW |
| 999950 | A_name_0999950 | 45 | 179991.5 | marketing | HOLD |
| 999990 | B_name_0999990 | 49 | 179998.3 | engineering | HOLD |
| 1000000 | C_name_1000000 | 21 | 180000.0 | support | NEW |

