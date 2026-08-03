# Studio 插件 OSS 热更新全业务验收台账

## 使用约定

- 本台账只记录已完成验证的脱敏事实、用例编号、时间和结论。
- 不记录账号、Token、密钥、原始 endpoint、资源 ID、完整 Nacos 配置或任何可恢复会话的信息。
- 每个结论必须可由非秘密 Maven 输出、脱敏日志 marker、缓存状态或部署预检结果复核。
- 测试数据、不可变 OSS release、独立缓存和 Studio 测试资源按本轮约定保留；既有 OSS pointer 和 Nacos 配置必须恢复。

## 验收范围

| 用例组 | 业务入口 | 期望插件坐标 |
| --- | --- | --- |
| E2E-ASSET | 数据源连接、模型发现/水合、模型预览 | `source/mysql8` |
| E2E-DEV | SQL、Java、Python 数据开发脚本 | `source/mysql8` |
| E2E-SERVICE | 数据服务字段解析、调试、REST 调用 | `source/mysql8`、`transformer/trim_spaces_str` |
| E2E-INGESTION | 数据接入服务 REST 写入 | `writer/mysql8writer` |
| E2E-ETL | 单表采集与 ETL 工作流 | `reader/mysql8reader`、`writer/mysql8writer`、`transformer/trim_spaces_str` |
| E2E-FUSION | 融合工作流 | `reader/fusionreader`、`source/mysql8`、writer、transformer |
| E2E-CONSISTENCY | 一致性工作流 | `reader/consistencyreader`、`source/mysql8`、writer |
| E2E-QUALITY | 数据质量任务 | `source/mysql8` |
| E2E-FLINK | Flink SQL Gateway/TaskManager | `source/mysql8` |

协议转换的 transformer 为 Studio 内置安全逻辑，外部 reporter 无 Studio 配置入口；两者不作为真实业务热更新用例。

## 执行记录

| 时间 (UTC+8) | 用例/操作 | 结论 | 脱敏证据 |
| --- | --- | --- | --- |
| 2026-07-31 | 台账创建、范围确认 | 进行中 | 本文档 |
| 2026-07-31 | E2E-BUILD-01 插件加载层定向测试 | 阻断已定位 | 默认 Maven 使用 Java 8，无法编译 Java 17 目标；后续固定 Java 17 重新执行 |
| 2026-07-31 | E2E-BUILD-01 插件加载层定向测试（Java 17） | 通过 | `plugins-loader-center`：8/8 测试通过，包含 Manifest marker 读取与版本 session 隔离 |
| 2026-07-31 13:33 UTC+8 | E2E-BUILD-02 六个验收插件 v1 制品与仓库脚本 | 通过 | `source/mysql8`、三个 reader、一个 writer、一个 transformer 均含 `Plugin-E2E-Marker: v1`；`production` 本地仓库 manifest、摘要/大小校验及重复 release 拒绝均通过 |
| 2026-07-31 13:50 UTC+8 | E2E-BUILD-03 v2 marker 制品初次构建 | 失败，已恢复中 | Maven 增量 `package` 未因 Manifest 属性变化重建既有 JAR，验证发现仍为 v1；未上传 OSS，改用 clean package 和新不可变本地 release |
| 2026-07-31 14:21 UTC+8 | E2E-BUILD-03 v2 clean recovery | 通过 | `clean package` 后六个 JAR 与六个 `production` release 均含 `Plugin-E2E-Marker: v2`；pointer 的 schema、release、runtimeVersion、大小和 SHA 校验通过 |
| 2026-07-31 | E2E-FLINK-01 Flink runtime capability 定向测试 | 通过 | `studio-flink` 8 tests 与 `studio-worker` capability filter 4 tests 通过；制品按 identity 缓存并受 ZIP 路径/大小/文件数限制，未向 TaskManager 传播 OSS 凭证 |
| 2026-07-31 14:35 UTC+8 | E2E-GATE-DA-01 DataAggregation `plugins-loader-center,core` test | 通过 | Java 17；Reactor 构建成功，累计 18 个 core 测试通过、无失败/错误 |
| 2026-07-31 14:36 UTC+8 | E2E-GATE-DA-02 DataAggregation `plugins-loader-center,core` install | 通过 | Java 17；`BUILD SUCCESS`，插件加载层与 core 安装到本地 Maven 仓库 |
| 2026-07-31 14:38 UTC+8 | E2E-GATE-STUDIO-01 `studio-worker -am test` 首次执行 | 失败，已修复 | `studio-flink` 2 项失败：无本地 bundled runtime 时，在 HTTP 无效参数/always-false 短路前初始化插件；未涉及生产业务逻辑 |
| 2026-07-31 14:40 UTC+8 | E2E-GATE-STUDIO-02 Connector 回归修复定向测试 | 通过 | `AggregationConnectorRoutingTest` 72/72；校验和短路路径不再依赖完整本地 aggregation runtime |
| 2026-07-31 14:43 UTC+8 | E2E-GATE-STUDIO-03 `studio-worker -am test` 复测 | 通过 | Java 17；`studio-flink` 110/110、`studio-worker` 94 项（1 项跳过）无失败/错误，Reactor `BUILD SUCCESS` |
| 2026-07-31 14:44 UTC+8 | E2E-GATE-STUDIO-04 `studio-worker -am -DskipTests install` | 通过 | Java 17；Worker、Flink 与依赖模块安装成功 |
| 2026-07-31 14:44 UTC+8 | E2E-GATE-DEPLOY-01 部署预检 | 通过 | `test-check-studio-deployment.ps1` 12/12；覆盖 Eager、Lazy OSS、缓存根初始化与缺失配置拒绝 |
| 2026-07-31 15:23 UTC+8 | E2E-BUILD-04 Manifest marker 回归复测 | 通过 | Java 17；`PluginE2eMarkerTest` 2/2，常规无 marker 类不产生验收日志 |
| 2026-07-31 | E2E-ENV-01 真实环境连通性与会话检查 | 阻断 | 本机 Nacos 端口可达，但目标命名空间需要认证；当前无可用 Nacos 或 Studio 登录上下文，未读取或落盘任何凭据，未修改 Nacos/OSS pointer/缓存 |
| 2026-07-31 15:17 UTC+8 | E2E-ENV-02 独立 Lazy 缓存根准备 | 通过 | 已创建新的空运行时根目录；既有缓存未删除、未修改，尚未发布 Nacos 配置或 OSS pointer |
| 2026-07-31 15:26 UTC+8 | E2E-ENV-03 Worker 命令行启动与 marker 采集审计 | 通过 | 已确认 Java 17 fat JAR、空闲 Worker 端口和仅采集脱敏 `PLUGIN_E2E_MARKER` 的日志方案；未启动或停止任何进程 |
| 2026-07-31 15:31 UTC+8 | E2E-AUDIT-01 热更新并发与跨平面审计 | 失败，修复中 | 发现 session 线程池绑定、缓存清理竞态、受限下载、Flink 制品 identity 固定及数据服务 transformer 生命周期缺口；未进入真实环境、未修改任何远端状态 |
| 2026-07-31 15:36 UTC+8 | E2E-FLINK-PIN-01 capability identity 固定回归 | 通过 | capability 发放时固定 source plugin identity；controller v1/v2 制品隔离与 connector bootstrap 定向组 3/3，通过后未触碰真实环境 |
| 2026-07-31 16:08 UTC+8 | E2E-AUDIT-01 session 与缓存清理竞态修复回归 | 通过 | Java 17；`PluginRuntimeSessionTest` 7/7、`ObjectStoragePluginRuntimeResolverTest` 13/13。线程池复用不继承已关闭 session；resolve 与 loader lease 在缓存清理临界区内完成，旧 release 在 active/in-use 时不删除 |
| 2026-07-31 16:20 UTC+8 | E2E-AUDIT-02 数据服务 session/transformer 生命周期回归 | 通过 | Java 17；`DataServicePluginRuntimeSessionTest` 2/2。count、data、response transformer 共用一个 session，外部 transformer lease 覆盖构建与 evaluate；identity 回滚不复用已关闭 loader |
| 2026-07-31 16:08 UTC+8 | E2E-AUDIT-02 DataService session 与外部 transformer lease | 通过 | Java 17；`DataServicePluginRuntimeSessionTest` 2/2。count/data/response transformer 固定同一 operation session；独立外部 transformer 在 build 至 evaluate 期间持有 lease，结束后 session 与文件句柄均释放 |
| 2026-07-31 17:31 UTC+8 | E2E-AUDIT-03 Core pipeline session propagation | 通过 | Java 17；`PipelinePluginRuntimeSessionTest` 4/4。standard、legacy、async pipeline 与 ContinuousStream producer/consumer 均验证显式 session 绑定；无 session 的旧调用路径保持兼容 |
| 2026-07-31 17:40 UTC+8 | E2E-AUDIT-02 DataService session 与外部 transformer identity 回滚 | 通过 | Java 17；`DataServicePluginRuntimeSessionTest` 3/3。count/data/response transformer 固定同一 operation session；独立 transformer 从构建至 evaluate 持有 lease；v1 -> v2 -> v1 后不复用已关闭 loader |
| 2026-07-31 21:33 UTC+8 | E2E-AUDIT-04 缓存硬上限与发布保护 | 通过 | Java 17；`ObjectStoragePluginRuntimeResolverTest` 17/17。新 release 按实际解压量预检；容量不足时保留 active/in-use 并进入 `DEGRADED`；可淘汰旧版本时先释放空间，pending 版本在激活前不可清理 |
| 2026-07-31 21:38 UTC+8 | E2E-GATE-DA-03 DataAggregation 门禁重跑 | 失败，已修复 | `JobContainer.start()` 在前次任务中断时留下双层 `finally` 括号不完整；编译器稳定复现，未进入运行或远端阶段 |
| 2026-07-31 21:42 UTC+8 | E2E-GATE-DA-04 DataAggregation 门禁恢复 | 通过 | Java 17；`plugins-loader-center,core -am test` 构建成功，共 32 项测试无失败/错误；包含 JobContainer 正常、异常和并发任务 session/revision 释放验证 |
| 2026-07-31 21:43 UTC+8 | E2E-GATE-DA-05 DataAggregation 安装门禁 | 通过 | Java 17；`plugins-loader-center,core -am -DskipTests install` Reactor 全部成功，最新插件加载层与 core 已安装到本地 Maven 仓库 |
| 2026-07-31 21:45 UTC+8 | E2E-AUDIT-05 运行 payload 插件版本留痕 | 通过 | Java 17；Data Ingestion、AggregationNodeExecutor、ExecutionEvent 三组共 18 项测试通过；正常与失败结果均保留脱敏 `pluginRevisions`，不写入持久化任务定义 |
| 2026-07-31 21:47 UTC+8 | E2E-GATE-STUDIO-05 `studio-worker -am test` 全量复测 | 通过 | Java 17；Studio 八模块 Reactor 全部成功；`studio-worker` 100 项（1 项跳过）无失败/错误，最新 resolver 17/17 亦纳入全量门禁 |
| 2026-07-31 21:49 UTC+8 | E2E-GATE-STUDIO-06 `studio-worker -am install` | 通过 | Java 17；八模块 `-DskipTests install` 全部成功，最新 Worker 与 Flink fat JAR 已生成并安装 |
| 2026-07-31 21:54 UTC+8 | E2E-AUDIT-06 marker 定向测试首次命令 | 失败，已恢复 | PowerShell 将未加引号的 Surefire 点号参数拆分为 Maven lifecycle；未执行测试、不是代码失败，随后改用逐参数引用重跑 |
| 2026-07-31 21:55 UTC+8 | E2E-AUDIT-06 marker session 与 I/O 修复 | 通过 | Java 17；`PluginE2eMarkerTest` 3/3。Manifest 结果按插件 Class 缓存，常规无 marker 制品仍零日志；去重范围由 JVM 全局改为单次 runtime session，使每个真实业务实例都可观察自身 marker |
| 2026-07-31 22:14 UTC+8 | E2E-OSS-00 Java OSS 只读连通性 | 通过（基线差异按预期） | `PluginRepositoryObjectStorageTool` 经 Nacos 配置创建 `CloudObjectStorageService`，真实 production pointer 可读；本地 v1 与远端现行 release 不同，verify 安全拒绝且未写远端 |
| 2026-07-31 22:15 UTC+8 | E2E-OSS-01 production pointer 备份 | 通过 | Java 工具备份 6 个坐标的 pointer、存在性、大小和 SHA 摘要到本地临时状态；未记录凭证或完整配置，未修改 OSS |
| 2026-07-31 22:28 UTC+8 | E2E-AUDIT-07 Studio 脚本与资产操作级 session 固定 | 通过 | Java 17；Java/Python 脚本、Python bridge 工作线程、资产发现与水合共 6 项回归测试通过；同次业务操作固定 identity，新操作可选择新版本 |
| 2026-07-31 22:28 UTC+8 | E2E-AUDIT-08 异常 revision 留痕与接入中断释放 | 通过 | Java 17；Worker 与 Studio 定向共 34 项测试通过；JobContainer 抛错后的终态 payload 保留脱敏 revisions，接入中断限时等待 session 清理后恢复调用线程中断状态 |
| 2026-07-31 22:35 UTC+8 | E2E-GATE-STUDIO-07 跨模块定向回归首次合并执行 | 失败，修复中 | `studio-infra` 6 项与 Flink 4 项已通过；Worker 编译遇到 Java OSS 操作器并行完善中的缺失辅助方法，未访问任何远端状态，待操作器定向测试完成后整组复跑 |
| 2026-07-31 23:01 UTC+8 | E2E-OSS-TOOL-01 Java OSS 发布与恢复操作器 | 通过 | Java 17；8 项 Fake Object Storage 测试通过，覆盖 artifact-first/pointer-last、不可变 release 全量校验、备份完整性、第三方 pointer 冲突零写入恢复及原子移动回退；全程未使用界面或真实远端 |
| 2026-07-31 23:01 UTC+8 | E2E-NACOS-TOOL-01 Nacos 配置事务操作器 | 通过 | Java 17；6 项测试及 `studio-worker -am` 编译通过，覆盖 allow-list YAML 合并、CAS 应用/恢复冲突、状态防篡改和原文精确恢复；输出仅含动作与 MD5 前缀 |
| 2026-07-31 23:01 UTC+8 | E2E-GATE-DA-06 DataAggregation 门禁再次复测 | 通过 | Java 17；`plugins-loader-center,core -am test` Reactor 成功，core 23 项及 loader 定向测试均无失败/错误；`-DskipTests install` 同步成功 |
| 2026-07-31 23:12 UTC+8 | E2E-ENV-04 Studio Server 命令行启动基线 | 通过 | Java 17 后台命令启动；健康检查为 `UP`，Nacos 刷新订阅、数据库连接及 Redis ping 均成功；未读取登录信息、未创建业务资源、未修改 OSS/Nacos |
| 2026-07-31 23:12 UTC+8 | E2E-FLINK-PIN-02 远程 lookup capability 固定 | 通过 | Java 17；Flink 14 项定向测试通过。lookup copy 保留任务短期 capability，v1/v2 分别命中对应 pinned identity；runtime/audit payload 不包含 token，OSS 凭证不进入 DDL/日志 |
| 2026-07-31 23:38 UTC+8 | E2E-GATE-DA-07 DataAggregation 定向门禁复测 | 通过 | Java 17；`plugins-loader-center,core -am test` Reactor 成功，core 23 项及插件加载层测试无失败/错误；任务正常、异常和 session/revision 释放路径均通过 |
| 2026-07-31 23:39 UTC+8 | E2E-GATE-DA-08 DataAggregation 安装门禁复测 | 通过 | Java 17；`plugins-loader-center,core -am -DskipTests install` Reactor 成功，最新加载层与 core 已安装到本地 Maven 仓库 |
| 2026-08-01 00:01 UTC+8 | E2E-OSS-TOOL-02 Java 操作器合并定向门禁首次执行 | 失败，待复测 | 并行加固期间 `studio-flink` 增量测试编译命中被清理的主类输出，尚未运行目标操作器测试；未连接或修改 Nacos/OSS，待代码冻结后 clean compile 并复测 |
| 2026-08-01 00:27 UTC+8 | E2E-OSS-TOOL-03 Java OSS/Nacos 操作器定向门禁复测 | 通过 | Java 17；`PluginRepositoryObjectStorageToolTest` 14 项、Nacos transaction/tool 测试合计 17 项，31 项无失败/错误；覆盖条件指针写/删、v1→v2→v1、部分发布/恢复重试、state ACL/CAS、Aliyun OSS provenance；仍未访问真实远端 |
| 2026-08-01 00:33 UTC+8 | E2E-OSS-TOOL-04 Java-only 远程写入守门最终定向复测 | 通过 | Java 17；32 项无失败/错误。新增操作器仅接受 Nacos 同源 Aliyun OSS 配置、强制 object-storage/plugin-runtime bucket 一致、关闭第三方启动日志；后续真实 OSS 备份、上传、指针切换和恢复均不使用界面 |
| 2026-08-01 00:36 UTC+8 | E2E-GATE-STUDIO-08 `studio-worker -am test` 全量门禁 | 通过 | Java 17；八模块 Reactor 全部成功，Worker 133 项（1 项跳过）无失败/错误；Java-only OSS 条件更新、Nacos CAS、resolver、Flink capability 与业务 session 回归均纳入本次全量执行 |
| 2026-08-01 00:39 UTC+8 | E2E-GATE-STUDIO-09 `studio-worker -am -DskipTests install` | 通过 | Java 17；八模块 Reactor 安装成功，最新 Worker/Flink executable JAR 已生成；未启动业务任务或修改远端状态 |
| 2026-08-01 00:39 UTC+8 | E2E-GATE-DEPLOY-02 Worker/Studio 部署预检复测 | 通过 | `test-check-studio-deployment.ps1` 12/12；Eager 回归、Lazy 缓存根初始化、缺失 OSS 配置拒绝及新增运行参数校验均通过 |
| 2026-08-01 00:43 UTC+8 | E2E-GATE-STUDIO-SERVER-01 Server 新代码测试与安装 | 通过 | Java 17；`studio-server -am test` 全部成功（Server 62 项无失败/错误），随后 `-DskipTests install` 成功；为真实业务验证准备命令行重启 |
| 2026-08-01 00:45 UTC+8 | E2E-ENV-05 Studio Server 按新代码命令行重启 | 通过 | 停止旧 Server 后以原 `-Xms256m -Xmx512m -jar` 方式启动新 executable JAR；健康状态 `UP`，新 PID/日志路径仅作本地运行记录，未记录认证信息 |
| 2026-08-01 00:50 UTC+8 | E2E-NACOS-REAL-01 Nacos Lazy 配置事务实测 | 通过（首次验证遇到最终一致性，已复核） | Java Nacos tool 对固定 prod Data ID 执行 apply；首次立即校验收到旧 revision，随后独立 verify 返回 `matched`。state 文件 owner-only ACL，未输出配置正文或凭据 |
| 2026-08-01 00:50 UTC+8 | E2E-OSS-REAL-01 production pointer Java 备份 | 通过 | Java `CloudObjectStorageService` 从 Aliyun OSS 备份六个业务坐标的 `current.json`；仅记录坐标、大小和 SHA 前缀，未使用 OSS Browser/CLI，未修改远端对象 |
| 2026-08-01 01:02 UTC+8 | E2E-NACOS-REAL-02 Nacos 首轮事务恢复 | 通过（修复一次最终一致性误报） | restore 的 CAS 已成功，但同进程立即读取曾返回旧 revision；新增有界重试与 `verify-restored`，18 项定向测试通过，真实只读复核返回 original revision `matched`；Nacos 已恢复测试前内容 |
| 2026-08-01 04:55 UTC+8 | E2E-OSS-TOOL-05 Java 条件写入探针 Fake 回归 | 通过 | Java 17；`PluginRepositoryObjectStorageToolTest` 16/16。`probe-conditions` 无需本地 repository/state，覆盖 create、匹配替换、过期 ETag 替换拒绝、过期 ETag 删除拒绝、匹配删除及零残留清理；未访问远端 |
| 2026-08-01 05:01 UTC+8 | E2E-OSS-REAL-02 阿里云 OSS 条件替换探针 | 失败，已定位并恢复 | Java `CloudObjectStorageService` 成功读取 Nacos 同源配置并创建唯一探针对象，但 Aliyun OSS 对当前 `PutObject` + `If-Match` 条件替换返回条件冲突；未触碰 production pointer。随后 Nacos 临时配置已通过受保护 CAS restore，`verify-restored=matched` |
| 2026-08-01 05:24 UTC+8 | E2E-OSS-REAL-03 阿里云 OSS Java 守门适配复测 | 阻断已定位，环境已恢复 | 将 Aliyun 条件替换改为 Java 协作锁、锁内 ETag 复核和原子 Put 后，匹配替换与过期 ETag 拒绝已通过真实 OSS；匹配删除仍返回冲突，尚不能安全执行 originally-absent pointer 恢复。production pointer 未修改；临时 Nacos 配置已 restore 且 `verify-restored=matched` |
| 2026-08-01 05:34 UTC+8 | E2E-OSS-TOOL-06 current.json 直接原子写模型回归 | 通过 | 按单一 Java 发布进程约束移除 Aliyun 不支持的 ETag 条件替换和协作锁；release 仍 no-overwrite，pointer 采用允许列表预检、对象级原子 Put/Delete、回读摘要校验。`PluginRepositoryObjectStorageToolTest` 14/14 通过，production pointer 未修改 |
| 2026-08-01 06:02 UTC+8 | E2E-OSS-REAL-04 阿里云 OSS Java 原子上传探针 | 通过 | Nacos 同源 Aliyun 配置下，Java 完成唯一对象 no-overwrite 创建、直接覆盖和回读内容校验；不再依赖 `PutObject If-Match`。六个待测 production pointer 的测试前备份均为已存在对象，真实恢复路径只需原子上传备份，不进入缺失 pointer 删除分支 |
| 2026-08-01 | E2E-FLINK-ENV-01 WSL 运行基线补充 | 待执行 | Flink 位于 WSL `/opt/dataaggregation/flink-2.3.0-upload-all-source`；任何 connector 更新前必须先备份原 connector，验证结束恢复并记录摘要，不在台账记录远程资源标识或凭据 |
| 2026-08-01 06:06 UTC+8 | E2E-GATE-STUDIO-10 直接原子 pointer 模型全量门禁 | 通过 | Java 17；`studio-worker -am test` 全部成功，Worker 133 项（1 项跳过）无失败/错误；随后 `-DskipTests install` 成功，部署预检 12/12 通过 |
| 2026-08-01 06:08 UTC+8 | E2E-OSS-V1-01 六个业务插件 v1 Java 发布 | 通过 | Java `CloudObjectStorageService` 先上传并校验 6 个不可变 ZIP，最后直接原子覆盖各 `current.json` 并回读校验；全部 pointer 指向脱敏 v1 marker release，未使用 OSS UI/CLI，备份 state 已记录允许修订用于后续恢复 |
| 2026-08-01 06:10 UTC+8 | E2E-ENV-06 Worker 首次重启失败与配置修复 | 失败，已修复 | 首次新代码启动因重复 `NacosClientManager` bean 失败；操作器配置改为私有属性后重新启动成功，随后 Worker 健康检查为 `UP`，Lazy resolver 从空缓存根正常初始化 |
| 2026-08-01 06:21 UTC+8 | E2E-ENV-07 Studio 项目临时 Worker 绑定 | 通过 | 项目绑定 `studio-online-worker-01` 并启用，Worker 保持 ONLINE；绑定仅用于本轮业务矩阵，最终恢复阶段删除 |
| 2026-08-01 06:23 UTC+8 | E2E-ASSET-01 资产连接、模型发现/水合、模型预览 v1 | 通过（发现接口返回重复对象同步保护） | 真实 Studio REST 调用连接测试返回 `AVAILABLE`；`source/mysql8` v1 marker 在 Worker 日志出现；发现返回 RDBMS 对象并完成已有模型预览，预览返回真实样例行。全量同步因已有同名模型唯一约束被安全拒绝，未改写既有模型 |
| 2026-08-01 06:27 UTC+8 | E2E-DEV-01 SQL 数据开发 v1 | 通过 | Studio SQL 执行返回 1 行真实结果；Worker 日志包含 `PLUGIN_E2E_MARKER coordinate=source/mysql8 marker=v1 phase=connection` |
| 2026-08-01 06:36 UTC+8 | E2E-DEV-02 Java/Python 数据开发 v1 | 通过 | 通过 Studio 保存并执行 Java、Python 脚本；结果分别含脱敏 `PLUGIN_E2E_JAVA_SCRIPT`、`PLUGIN_E2E_PYTHON_SCRIPT`，两次脚本内部 SQL 均触发 source/mysql8 v1 marker |
| 2026-08-01 06:44 UTC+8 | E2E-SERVICE-01 数据服务字段解析、调试、发布 REST 与响应转换 v1 | 通过 | 独立数据服务完成字段解析、调试、发布及开放 REST 调用；SQL 原值两侧含空格，返回值为 `spaced-v1`。Worker 同次业务日志包含 source/mysql8 v1 connection 与 transformer/trim_spaces_str v1 evaluate marker，访问日志记录成功且请求/实际集群一致 |
| 2026-08-01 06:48 UTC+8 | E2E-INGESTION-01 数据接入 REST 写入 v1 | 通过 | 独立 JSON 接入服务经开放 REST 写入 1 行，响应 `received=1/success=1/failed=0` 且带 writer/mysql8writer revision；Worker 日志包含 writer v1 start/finish marker，随后 Studio SQL 回读目标表得到预期值，访问日志请求/实际集群一致 |
| 2026-08-01 06:52 UTC+8 | E2E-ETL-01 单表采集与 ETL_SINGLE v1 | 通过 | 独立 `SINGLE_TABLE` 采集任务执行成功，采集/读取/转换/写入均为 1；运行 payload 同时固定 reader/mysql8reader、writer/mysql8writer、transformer/trim_spaces_str v1 revisions，目标回读为去空格后的 `ui06` |
| 2026-08-01 16:22 UTC+8 | E2E-WORKFLOW-01 单表采集工作流 v1 | 通过 | 通过 Studio 工作流真实保存、发布并触发资源绑定节点；工作流 1/1 节点成功，运行结果为读取/转换/写入各 1 条，payload 固定 reader、writer、transformer 的 v1 revisions，Worker 日志同时出现三类 v1 lifecycle marker |
| 2026-08-01 16:30 UTC+8 | E2E-FUSION-01 双 MySQL 融合采集与工作流 v1 | 通过 | Studio 创建两个独立别名的 MySQL 输入并按 `id` 执行 INNER 融合，采集任务与其资源绑定工作流均成功；输出回读为 `ui06-fusion`，运行 payload 固定 fusionreader、writer、transformer 的 v1 revisions，Worker 日志出现 fusionreader init/start/finish、source connection、writer 与 transformer v1 marker |
| 2026-08-01 16:41 UTC+8 | E2E-QUALITY-01 数据质量任务 v1 | 通过 | 触发绑定测试 MySQL 模型的既有 ONLINE 表行数质量任务，真实查询返回单行统计结果且运行成功；Worker 同次执行出现 source/mysql8 v1 connection marker |
| 2026-08-01 16:43 UTC+8 | E2E-FLINK-ENV-02 WSL connector 原件备份 | 通过 | 更新前已在 Flink 安装根目录的独立 `backup` 子目录复制现有 `studio-flink-0.1.0-SNAPSHOT-connector-upload.jar`；原件与备份均为 817385869 字节，SHA-256 均为 `af8c7ae7fdcb...299b8b4`。首次放入 `lib/backup` 会被 Flink 递归加入 classpath，已在正式查询前停止进程并移出 `lib` 后重启；集群恢复为 1 个 TaskManager、10 个可用 slot |
| 2026-08-01 16:57 UTC+8 | E2E-FLINK-02 Studio Flink SQL v1 首次真实执行 | 失败，修复中 | 请求已解析测试模型并固定 `source/mysql8` v1 revision，但执行误走 Worker 进程内 embedded MiniCluster，未进入 WSL Gateway/TaskManager；本地提交在反序列化 `CodeGenOperatorFactory` 时失败。该失败证明当前 `runtimeClusterId` 只完成集群选择，尚未把执行端路由到 WSL；未修改 OSS pointer、Nacos 或业务数据。 |
| 2026-08-01 17:11 UTC+8 | E2E-FLINK-ENV-03 当前 connector 构建与受保护部署 | 通过 | Java 17 使用 `flink-connector-bundle` profile 构建成功；WSL 旧 connector 的既有独立备份摘要再次核验后，以 staging 文件替换运行副本，新副本 SHA-256 为 `558021ba00a...20f2306d`，备份未改动。 |
| 2026-08-01 17:21 UTC+8 | E2E-ENV-08 Worker Gateway 模式命令行重启 | 通过 | 重启前确认无运行中 Studio 任务；保留 Nacos Lazy OSS 配置和同一可写 cache/state，仅以命令行增加 Flink Gateway 执行参数。Worker 恢复 `UP/ONLINE`，Lazy resolver 从重启前 5 个已缓存 release 恢复。 |
| 2026-08-01 17:27 UTC+8 | E2E-FLINK-03 WSL Gateway/TaskManager v1 真实查询 | 通过 | Studio 只读 Flink SQL 返回真实 MySQL 单行结果，summary 明确为 `executionMode=gateway`；WSL 为 1 个 TaskManager、10 个 slot，TaskManager 日志输出 `PLUGIN_E2E_MARKER coordinate=source/mysql8 marker=v1 phase=connection`。查询未走 Worker embedded MiniCluster，DDL/日志未出现 OSS 凭证或 capability 明文。 |
| 2026-08-01 17:59 UTC+8 | E2E-CONSISTENCY-AUTO-00 定向测试首次命令 | 失败，已恢复 | PowerShell 将未加引号的 Surefire 属性拆成 Maven lifecycle；未执行测试、未连接 Studio/Nacos/OSS，随后改为逐参数引用。 |
| 2026-08-01 18:02 UTC+8 | E2E-CONSISTENCY-AUTO-01 Studio 一致性资源绑定与运行时水合 | 通过 | Java 17；11 项定向测试无失败/错误。新增 `leftBinding/rightBinding/outputBinding` 仅持久化资源 ID、字段和非敏感选项；保存时校验模型归属、访问权限、字段、独立输出及凭证零落盘；Worker 创建 `JobContainer` 前才水合并解密连接。旧 raw `reader/writer` 路径保持兼容。 |
| 2026-08-01 18:19 UTC+8 | E2E-CONSISTENCY-GATE-01 Server 一致性改造全量回归 | 通过 | Java 17；`studio-server -am test` 七模块 Reactor 全部成功，Server 62 项无失败/错误；一致性资源绑定保存逻辑及现有 Server 路由未发生回归。 |
| 2026-08-01 18:24 UTC+8 | E2E-ENV-09 Server/Worker 一致性代码命令行重启 | 通过 | 重启前未发现运行中任务；保持 `-Xms256m -Xmx512m`、原 Lazy aggregation home、Nacos 配置和 Gateway 路由。Server/Worker 健康均为 `UP`，Worker `ONLINE`，从同一 cache/state 恢复 5 个已缓存 release；登录链路使用新 fat JAR 后恢复正常。 |
| 2026-08-01 18:40 UTC+8 | E2E-CONSISTENCY-01 真实资源绑定工作流 v1 首次运行 | 失败，已定位 | 独立三表模型同步 3/3 成功，工作流持久化仅含 datasource/model ID、字段、别名和非敏感 writer 选项；运行被能力校验将虚拟 `consistency` reader 误作数据源类型而拒绝，尚未进入插件执行。 |
| 2026-08-01 18:48 UTC+8 | E2E-CONSISTENCY-AUTO-02 内部 reader 能力校验修复 | 通过 | `ConsistencyWorkflowAssemblerRegressionTest` 与 `AggregationNodeExecutorRegressionTest` 共 8 项通过；水合配置携带实际 datasource type，执行器逐源校验 `mysql8`，不再校验虚拟 `consistency` 类型。 |
| 2026-08-01 18:52 UTC+8 | E2E-CONSISTENCY-GATE-02 Worker 全量测试与安装 | 通过 | Java 17；`studio-worker -am test` Worker 133 项（1 项跳过）无失败/错误；随后 Worker/Server 及依赖模块安装成功并重新生成 executable JAR。 |
| 2026-08-01 18:56 UTC+8 | E2E-CONSISTENCY-02 一致性资源绑定工作流 v1 | 通过 | Studio 真实工作流执行成功并输出 2 条差异，类型为 `MISSING`、`BINARY_CONFLICT`；持久化配置仅含数据源/模型 ID、别名、字段、规则和非敏感 writer 选项。Worker 日志确认 consistency reader 的 init/start/finish、MySQL source connection、writer start/finish 均为 v1 marker，运行 payload 固定 reader 与 writer 的 v1 revision。 |
| 2026-08-01 19:20 UTC+8 | E2E-HOT-A-PRE-01 首次 120 秒任务窗口校验 | 通过（未切换） | 长查询实例完整运行期间 reader、writer、source 均为 v1，查询返回后 transformer 也输出 v1 marker，任务成功结束；因 Windows PowerShell stdin BOM 排查占用窗口，本次没有切换 pointer，随后改用更长的独立实例，不把该次结果冒充热更新结论。 |
| 2026-08-01 19:23 UTC+8 | E2E-HOT-A-01 长任务 A 固定 v1 | 通过 | 600 秒阻塞查询启动后，Worker 日志确认 MySQL reader 与 writer 的 v1 start marker，source connection 也为 v1；任务保持运行，用于跨越后续无重启 pointer 切换。 |
| 2026-08-01 19:25 UTC+8 | E2E-OSS-V2-01 六个业务插件 v2 Java 发布 | 通过 | Java `CloudObjectStorageService` 先 no-overwrite 上传并回读校验 6 个不可变 ZIP，最后直接原子覆盖并回读校验 6 个 `current.json`；未使用 OSS UI/CLI，Worker 未重启。 |
| 2026-08-01 19:25 UTC+8 | E2E-HOT-ACTIVE-01 Worker 无重启激活 v2 | 通过 | 后台刷新依次激活 source、writer、fusion reader、consistency reader、MySQL reader 和 transformer 的 v2 release；长任务 A 仍在运行，进程 boot 不变。 |
| 2026-08-01 19:36 UTC+8 | E2E-V2-MATRIX-PRE-01 v2 矩阵首次调用参数/超时 | 失败，已定位 | 资产测试与预览首次遗漏必填 runtime cluster 参数而被 400 拒绝；Java/Python 脚本执行在长任务 A 占用同步 dispatch 轮询期间超过 CLI 默认 60 秒。未据此声明业务失败，随后补齐参数并把排队型调用延后到 A 释放后复测。 |
| 2026-08-01 19:38 UTC+8 | E2E-ASSET-02 资产连接与模型预览 v2 | 通过 | 补齐目标 runtime cluster 后连接测试返回 `AVAILABLE`，模型预览返回真实 5 行；Worker 两次操作均输出 source/mysql8 v2 connection marker。 |
| 2026-08-01 19:40 UTC+8 | E2E-SERVICE-02 数据服务 REST 与响应转换 v2 | 通过 | 已发布 REST 返回真实单行，空格字段仍转换为 `spaced-v1`（业务内容不随 marker 修改）；同次 Worker 日志出现 source/mysql8 v2 connection 与 transformer/trim_spaces_str v2 evaluate marker。 |
| 2026-08-01 19:40 UTC+8 | E2E-INGESTION-02 数据接入 REST 写入 v2 | 通过 | 独立 JSON 接入成功写入 1 行，响应 `received=1/success=1/failed=0` 并固定 v2 writer revision；Worker 日志出现 writer v2 start/finish marker。长任务 A 此时仍在旧 session 内运行。 |
| 2026-08-01 20:12 UTC+8 | E2E-HOT-A-END-01 长任务 A 中断收尾与旧版本固定 | 通过（预期中断） | 测试 SQL 的 `SLEEP(600)` 被 MySQL 按多行重复求值，A 持续运行约 49 分钟；在 Worker 已激活 v2 且多个新操作输出 v2 marker 后，仅关闭 A 的 v1 reader classloader 所属连接池以覆盖中断路径。A 按预期失败收尾，writer 仍输出 v1 finish marker，期间未出现 A 使用 v2 的版本漂移。 |
| 2026-08-01 20:12 UTC+8 | E2E-HOT-B-01 下一任务实例自动使用 v2 | 通过 | A 释放后，排队的同表采集实例 B 无需重启 Worker 即开始执行并成功完成；source connection、MySQL reader start/finish、writer start/finish 与 trim transformer evaluate 均输出 v2 marker，业务统计为读取/转换/写入各 1 条。 |
| 2026-08-01 20:16 UTC+8 | E2E-DEV-03 SQL、Java、Python 数据开发 v2 | 通过 | Studio SQL 返回 1 行真实结果；已保存 Java/Python 脚本均执行成功并分别输出脱敏脚本 marker，脚本内部 SQL 也各触发 source/mysql8 v2 connection marker。此前因 A 占用同步调度而超时的请求已在 A 释放后自动执行，本次显式重跑结果一致。 |
| 2026-08-01 20:17 UTC+8 | E2E-ETL-02 单表采集与 ETL 工作流 v2 | 通过 | Studio 真实采集实例与资源绑定工作流均成功；读取、转换、写入各 1 条，Worker 同一任务生命周期出现 mysql8reader、mysql8writer、trim transformer 的 v2 marker，运行 payload 固定三者 v2 revision。 |
| 2026-08-01 20:18 UTC+8 | E2E-FUSION-02 双 MySQL 融合工作流 v2 | 通过 | Studio 真实 FUSION 工作流成功，输出业务结果保持不变；Worker 同一任务生命周期出现 fusionreader init/start/finish、两个 source connection、writer 与 trim transformer 的 v2 marker。 |
| 2026-08-01 20:19 UTC+8 | E2E-CONSISTENCY-03 一致性工作流 v2 首次运行 | 失败，已定位并修复代码 | reader、source、writer 均已固定 v2，但 spill 回读因 Fastjson 兼容层跨两个插件 JarLoader 复用同名枚举反序列化器而触发同名类型 ClassCastException；失败未写入错误业务结果，旧 v1 实例不受影响。 |
| 2026-08-01 20:38 UTC+8 | E2E-CONSISTENCY-AUTO-03 首次修复门禁命令 | 失败，环境已纠正 | 当前 PowerShell 默认 JAVA_HOME 指向 Java 8，无法运行此前由 Java 17 编译的测试类；未连接或修改 Studio、Nacos、OSS，随后显式切换项目基线 JDK 17 重跑。 |
| 2026-08-01 20:41 UTC+8 | E2E-CONSISTENCY-AUTO-04 spill 类加载隔离修复 | 通过 | spill list 改为每实例独立 Jackson ObjectMapper、TypeFactory 与类型缓存；DataAggregation core 23 项通过并安装，Worker 真实同名类双子加载器精确回归 1/1 通过，第二版本枚举保持所属插件 loader。 |
| 2026-08-01 20:49 UTC+8 | E2E-RESTART-01 应用代码修复后的 Worker 缓存恢复 | 通过 | 重启前 Studio 运行记录 runningCount=0；按相同堆参数、相同 aggregation home 与同一 cache/state 启动新 Worker JAR，健康与 ONLINE 恢复。启动日志直接恢复 6 个 active 缓存坐标，未出现插件下载，缓存目录未删除。 |
| 2026-08-01 20:54 UTC+8 | E2E-CONSISTENCY-04 一致性工作流 v2 修复复测 | 通过 | Studio 真实资源绑定工作流成功，输出 2 条差异并写入 2 条结果；consistencyreader init/start/finish、两个 source connection、writer start/finish 均为 v2，同一 run payload 固定 v2 identity，未再出现跨 JarLoader 类型转换错误。 |
| 2026-08-01 21:00 UTC+8 | E2E-BUILD-04 consistency spill 修复版不可变 release | 通过 | 重新构建仅含 `reader/consistencyreader` 的 v2 验收制品，生成新的不可变 release；本地 `current.json` 的坐标、runtimeVersion、ZIP 大小和 SHA-256 与制品一致，未复用或覆盖既有 release。 |
| 2026-08-01 21:18 UTC+8 | E2E-OSS-TOOL-07 Aliyun 已存在 release 幂等发布修复 | 通过 | 真实 Aliyun OSS 对不可变对象重复创建返回非标准 `InvalidResponse`；Java 发布器仅在异常后重新 HEAD 确认对象存在时接受幂等，随后仍回读校验完整大小和 SHA。`CloudObjectStorageServiceTest` 3/3、`PluginRepositoryObjectStorageToolTest` 14/14 通过。 |
| 2026-08-01 21:29 UTC+8 | E2E-OSS-V2-02 consistency 修复版 Java 发布与无重启激活 | 通过 | Java `CloudObjectStorageService` 发布新的 consistency release，先验证不可变 ZIP、最后直接原子写入并回读验证 `current.json`；Worker 未重启即由后台刷新激活新 identity，未使用 OSS UI/CLI。 |
| 2026-08-01 21:31 UTC+8 | E2E-CONSISTENCY-05 consistency 修复版真实工作流 | 通过 | Studio 真实一致性工作流读取并写出 2 条差异；consistencyreader、两个 MySQL source connection 和 writer 均输出 v2 marker，运行 payload 固定同一 consistency identity，无跨 loader 类型错误或版本漂移。 |
| 2026-08-01 21:40 UTC+8 | E2E-QUALITY-02 数据质量任务 v2 | 通过 | 触发绑定测试 MySQL 模型的 ONLINE 表行数质量任务，真实查询返回单行统计结果并执行成功；运行日志输出 source/mysql8 v2 connection marker，请求与实际运行集群一致。 |
| 2026-08-01 21:45 UTC+8 | E2E-FLINK-04 v2 首次执行参数漂移 | 失败，已纠正 | 最近一次 Worker 代码修复重启漏带 Gateway 执行参数，Studio 查询因此退回 embedded MiniCluster 并在本地类反序列化阶段失败；WSL 集群和 OSS pointer 未被修改，未将该结果计为插件失败。 |
| 2026-08-01 21:50 UTC+8 | E2E-ENV-10 Worker Gateway 参数命令行恢复 | 通过 | 确认 Studio 无运行中任务后，以相同堆参数、与 Nacos 相同的 aggregation home、`default-local` 集群代码和 Gateway 参数重启 Worker；健康恢复 `UP`，Lazy resolver 从同一 cache/state 恢复 6 个已缓存 release。 |
| 2026-08-01 21:52 UTC+8 | E2E-FLINK-05 WSL Gateway/TaskManager v2 真实查询 | 通过 | Studio 只读 Flink SQL 经 WSL Gateway/TaskManager 返回真实 MySQL 单行结果，summary 为 `executionMode=gateway`；TaskManager 输出 source/mysql8 v2 connection marker，集群保持 1 个 TaskManager、10 个可用 slot，查询完成后无运行中作业。 |

| 2026-08-01 22:25 UTC+8 | E2E-ENV-11 Studio Flink 命令行启动基线确认 | 通过 | 后续命令行启动 `StudioFlinkApplication` 必须逐项继承 IDEA 本地 Run Configuration 的环境变量、JDK 17、堆参数和 aggregation home；仅额外保留共享配置中的 18084 端口与 Redis 超时参数。敏感变量只从本机 IDEA 配置注入进程，不在命令、日志或台账中展开。WSL Gateway/TaskManager 仍使用其 Linux 侧配置，不把 Windows 路径或 Studio 服务密钥传入 Flink 作业。 |
| 2026-08-01 22:31 UTC+8 | E2E-OFFLINE-03 Nacos OSS 不可达与已缓存真实业务 | 通过 | 通过受保护 CAS 将对象存储临时切换到固定 loopback 不可达端点；Worker 使用原 cache/state 启动，6 个缓存插件的后台刷新均保留最后有效版本。真实数据质量任务仍为 `SUCCESS` 并输出 source/mysql8 v2 marker。运行日志对象存储共用该端点，故 actuator 为 `DOWN`，但 Worker 进程、心跳和已缓存插件执行未中断。 |
| 2026-08-01 22:36 UTC+8 | E2E-OFFLINE-AUTO-01 未缓存冷加载首次状态断言 | 失败，已定位 | 独立 JUnit 临时 aggregation home 为 0 个缓存；真实 Aliyun SDK 访问固定 loopback 不可达端点，冷加载按 2 秒约定失败。首次断言紧跟调用方超时，后台 SDK 重试尚未结束，故状态仍短暂为 `UP`；未触碰 Worker 原缓存、Nacos 或 OSS。 |
| 2026-08-01 22:37 UTC+8 | E2E-OFFLINE-AUTO-02 未缓存冷加载与降级状态 | 通过 | 增加有界等待后台单飞 Future 收敛后复测；独立空缓存的未缓存插件冷加载失败、无缓存目录发布，状态最终为 `DEGRADED`，1 项定向测试通过。测试只访问固定 loopback 故障端点，不使用 MinIO、不读取真实凭据。 |
| 2026-08-01 22:40 UTC+8 | E2E-OFFLINE-RESTORE-01 Nacos OSS endpoint 恢复 | 通过 | 使用离线事务 state 对固定 prod Data ID 执行受保护 CAS restore，随后独立 `verify-restored` 返回 `matched`；未输出配置正文或凭据。原 Worker 动态刷新配置后 health 恢复 `UP`，缓存 source/mysql8 操作仍输出 v2 marker。 |
| 2026-08-01 22:54 UTC+8 | E2E-RESTART-02 恢复后首次 Worker 命令行重启 | 失败，已立即停止 | PowerShell 变量名误用了只读自动变量 `$HOME`，使临时进程的 `-Daggregation.home` 指向用户目录；发现命令行不匹配后在其承接任务前立即停止，未修改原插件 cache/state、Nacos 或 OSS。 |
| 2026-08-01 22:55 UTC+8 | E2E-RESTART-03 恢复后 Worker 缓存重启 | 通过 | 确认无 `RUNNING` 任务后，按 Java 17、256/512 MiB、原 Lazy aggregation home、`default-local` 及原 Gateway 四项基线重新命令启动。health 为 `UP`，启动日志从同一 cache/state 恢复 6 个 active release，未重新下载插件。 |
| 2026-08-01 23:01 UTC+8 | E2E-ROLLBACK-PRE-01 六插件 v1 pointer 预回滚 | 通过 | Java 发布器复核既有不可变 v1 ZIP 后最后覆盖 6 个 `current.json`；Worker 无重启激活全部 v1 identity。为补足运行中 v2 回滚边界，随后按同一受保护 state 临时切回已验证 v2。 |
| 2026-08-01 23:03 UTC+8 | E2E-ROLLBACK-PRE-02 六插件 v2 受控恢复 | 通过 | 六个 v2 pointer 经允许列表和远端回读校验，Worker 无重启重新激活；consistency 使用 spill 修复版 v2，其余坐标使用原 v2。 |
| 2026-08-01 23:08 UTC+8 | E2E-ROLLBACK-A-01 运行中 v2 操作跨 pointer 回滚 | 通过 | Studio 真实 SQL 操作先输出 source/mysql8 v2 connection marker，再执行 45 秒阻塞查询；其运行期间 Java 发布器把全部 pointer 回滚到 v1。该操作最终成功返回一行，固定的 v2 identity 未漂移。 |
| 2026-08-01 23:10 UTC+8 | E2E-ROLLBACK-B-01 回滚后下一业务实例使用 v1 | 通过 | Worker 无重启激活 6 个 v1 release 后，下一次 Studio SQL 查询成功返回一行，并输出 source/mysql8 v1 connection marker；回滚切换单位符合任务/操作实例边界。 |
| 2026-08-01 23:11 UTC+8 | E2E-RESTORE-OSS-01 production pointers 恢复 | 通过 | Java 操作器仅接受备份 state 已记录的原始或测试 revision，逐个原子上传六个测试前 `current.json` 并回读大小/SHA；restore count=6。测试不可变 release 保留，未使用 OSS UI/CLI。 |
| 2026-08-01 23:13 UTC+8 | E2E-RESTORE-NACOS-01 Nacos 测试前配置恢复 | 通过 | 主事务 state 的 CAS restore 成功，随后独立 `verify-restored` 返回 `matched`；未覆盖并发第三方 revision，未输出配置正文或凭据。恢复配置回到原 aggregation home，未保留 Lazy 测试段。 |
| 2026-08-01 23:15 UTC+8 | E2E-RESTORE-WORKER-01 Worker IDEA Eager 基线恢复 | 通过 | 确认 Studio 无 `RUNNING` 任务后，以 IDEA Worker 基线的 Java 17、256/512 MiB、完整本地 aggregation home 和仅 `default-local` 集群变量命令启动；移除测试 Gateway 环境变量。Worker health 为 `UP`，SystemConstants 加载原完整目录。 |
| 2026-08-01 23:19 UTC+8 | E2E-RESTORE-FLINK-00 WSL Flink 首次重启 | 失败，已定位 | Connector 已从独立备份原子恢复并核验原 SHA-256/817385869 字节；首次 SQL Gateway 启动漏带历史 REST address/port 参数而退出，随后从测试前日志还原原参数。短命令 WSL 会话退出又向 Flink 子进程发送 SIGHUP，未影响 connector 文件。 |
| 2026-08-01 23:26 UTC+8 | E2E-RESTORE-FLINK-01 WSL 原 connector 与集群恢复 | 通过 | 使用持久隐藏 WSL launcher、原 Java 17 和原 SQL Gateway REST 参数启动。运行 connector 摘要与测试前备份完全一致；JobManager、1 个 TaskManager、SQL Gateway 均存活，REST 显示 10/10 slot 可用、0 个运行作业、Flink 2.3.0。 |
| 2026-08-01 23:40 UTC+8 | E2E-RESTORE-STUDIO-FLINK-01 Studio Flink IDEA 环境基线恢复 | 通过（纠正一次无效 VM 参数） | 命令行逐项继承 IDEA `StudioFlinkApplication` 的全部环境变量，并使用同一 Java 17 与 256/512 MiB 堆参数；共享内部密钥仅从已恢复配置注入当前进程内存。首次照搬 IDEA VM 参数中的 Worker 专用 `-Daggregation.home` 被启动守门拒绝，随后保留环境变量但剔除该无效 VM 参数，应用健康检查为 `UP`。未在命令、日志或台账展开任何秘密值。 |
| 2026-08-01 23:48 UTC+8 | E2E-RESTORE-STUDIO-RESOURCES-01 Studio 临时资源清理 | 通过 | 在无运行中任务后，通过 Studio REST API 删除本轮临时项目 Worker 绑定、3 个工作流、4 个采集任务、1 个数据服务、1 个数据接入服务、2 个脚本、5 个模型及 1 个模型同步批次；需下线的开放服务先下线再删除。独立复查未发现本轮名称或创建时间匹配的残留资源，测试前既有数据源、模型、质量任务及历史验收资源未修改。 |
| 2026-08-01 23:50 UTC+8 | E2E-GATE-DA-FINAL DataAggregation 最终测试与安装门禁 | 通过 | Java 17；`plugins-loader-center,core -am test` Reactor 全部成功，core 23 项及插件加载层测试无失败/错误；随后 `-DskipTests install` 成功，最终加载层与 core 已安装到本地 Maven 仓库。 |
| 2026-08-01 23:53 UTC+8 | E2E-GATE-STUDIO-FINAL Studio Worker 最终测试门禁 | 通过 | Java 17；`studio-worker -am test` 八模块 Reactor 成功，Worker 138 项（1 项跳过）无失败/错误；未记录测试日志中的密钥或连接信息。 |
| 2026-08-01 23:54 UTC+8 | E2E-GATE-STUDIO-INSTALL-FINAL Studio Worker/Flink 最终安装门禁 | 通过 | Java 17；`studio-worker -am -DskipTests install` 八模块安装成功，Worker 与 Flink executable JAR 已生成并安装。 |
| 2026-08-01 23:55 UTC+8 | E2E-GATE-FLINK-CONNECTOR-FINAL Flink connector 定向门禁 | 通过 | Java 17；4 个 connector/runtime bootstrap 测试类合计 86 项无失败/错误，覆盖路由、行转换、runtime registry 和插件 bootstrap。 |
| 2026-08-01 23:55 UTC+8 | E2E-GATE-DEPLOY-FINAL Studio 部署预检 | 通过 | `test-check-studio-deployment.ps1` 12/12 通过；Eager/Lazy 启动守门、秘密非默认校验、运行参数和缓存约束均通过。 |
| 2026-08-02 00:21 UTC+8 | E2E-GATE-PUBLISH-SCRIPT-FINAL Bash 发布脚本门禁 | 通过 | WSL 中执行 `package_all/tests/test_build_plugin_repository.sh` 成功；覆盖单插件 release、ZIP、摘要、大小、pointer 和 `production` 目录结构，临时输出由测试自身清理。 |
| 2026-08-02 00:22 UTC+8 | E2E-RESTORE-AUDIT-FINAL 环境恢复独立审计 | 通过 | 只读 Java 审计经 Nacos 同源 Aliyun OSS 配置逐字节比对 6 个 production pointer 与测试前备份，全部匹配；Nacos `verify-restored` 匹配原始 revision。Server、Worker、Studio Flink 健康均为 `UP`；Worker 日志确认完整本地 Eager aggregation home；测试 Lazy cache 仍存在且未删除。WSL 原 connector 与备份摘要/大小一致，JobManager、1 个 TaskManager、SQL Gateway 存活，10/10 slot 可用且 0 个运行作业。临时只读审计 helper 已清理。 |

## 最终结论

**结论：通过。** Studio Worker OSS 插件运行时已完成自动化门禁与真实阿里云 OSS 全业务验收。

- v1/v2 六类插件在资产、数据开发、数据服务、数据接入、单表采集、ETL、融合、一致性、质量及 WSL Flink SQL 真实业务中均产生对应 marker，业务结果保持一致。
- 运行中实例在 pointer 切换和回滚期间保持原 identity；后续实例使用新 active identity，Worker 无需重启且未发生版本漂移。
- OSS 不可达时，已有缓存的真实业务继续成功；未缓存插件按冷加载超时失败并进入 `DEGRADED`。恢复后回到可用状态，Worker 重启从 cache/state 恢复且未重新下载。
- DataAggregation、Studio Worker、Flink connector、Bash 发布脚本及部署预检最终门禁全部通过。
- 六个 production pointer、Nacos 原始配置、Worker Eager 基线、Studio 临时资源和 WSL 原 connector 均已恢复并独立复核；验收 release 与测试 cache 按约定保留。
- Studio Flink 命令行进程继承 IDEA Run Configuration 的全部环境变量；IDEA 中误配的 Worker 专用 `-Daggregation.home` 不属于环境变量，并按启动守门要求未传给 Studio Flink。
- 协议转换继续使用 Studio 内置安全转换逻辑，外部 reporter 仍无 Studio 配置入口；两项按既定覆盖边界未伪造业务验收。

## 验收后清理

| 完成时间 | 用例 | 结论 | 非秘密摘要 |
|---|---|---|---|
| 2026-08-02 01:02 UTC+8 | E2E-CLEANUP-01 验收观察代码清理 | 通过 | 已从生产源码、插件 Manifest 与构建制品移除临时 `PluginE2eMarker`、六类业务插件 marker 调用、临时 OSS 错误打印和任务 revision 观察日志；无语义差异的测试编辑文件已恢复。保留 classloader/session/cache/OSS/Flink capability/spill 隔离及插件生命周期等功能与 Bug 修复。Java 17 下 `plugins-loader-center,core -am test` 成功，core 23 项无失败/错误；7 个相关 JAR 均未发现验收类或 Manifest marker，两个仓库 `git diff --check` 通过。 |

## 验收后 Flink Connector 轻量化

| 完成时间 | 用例 | 结论 | 非秘密摘要 |
|---|---|---|---|
| 2026-08-02 01:21 UTC+8 | E2E-FLINK-THIN-01 轻量 remote connector 构建 | 通过 | 新增独立 `flink-connector-remote` profile；生成的 upload JAR 为 6,269,691 字节，不含 `dataaggregation-plugin-runtime` 或 MySQL 等具体插件实现。原 bundle profile 保留，继续支持离线全量部署。 |
| 2026-08-02 01:24 UTC+8 | E2E-FLINK-THIN-02 WSL 受保护部署 | 通过 | 更新前将 817,385,869 字节的原 connector 复制到 Flink 安装目录外的独立 backup 目录并核验摘要；运行副本替换为轻量 connector，摘要为 `406f88485131...131b8cb`。Flink 以原 Java 17、Gateway 参数和 10 slots 基线完整重启。 |
| 2026-08-02 01:37 UTC+8 | E2E-FLINK-THIN-03 首次真实请求环境漂移 | 失败，已定位 | 恢复后的 Worker 仍为 embedded 模式，未进入 WSL，并因 Worker 内 Flink 规划依赖缺失返回非 JSON 错误页；该失败未产生 Flink 作业、TaskManager 缓存、OSS/Nacos/pointer 变更。 |
| 2026-08-02 01:58 UTC+8 | E2E-FLINK-THIN-04 Studio 真实按需加载 | 通过 | Server 与 Worker 使用同一 Nacos 现有内部认证和加密配置，Worker 临时切换 Gateway 模式。Studio 只读查询真实模型返回 `id=1, test_param=ui06`，summary 为 `executionMode=gateway`。空 remote cache 新建唯一 identity 目录并下载 `source/mysql8` 的 plugin.json、主 JAR 和依赖；旧缓存隔离目录未被命中。 |
| 2026-08-02 01:59 UTC+8 | E2E-FLINK-THIN-05 制品与集群收尾门禁 | 通过 | 制品门禁复测通过；WSL 运行 connector 保持 6,269,691 字节且摘要一致，查询后 1 个 TaskManager、10/10 slots 可用、0 个运行作业、1 个成功作业；两个仓库 `git diff --check` 通过。未改 Nacos 配置、OSS pointer 或 Worker 本地插件目录。 |
| 2026-08-02 02:02 UTC+8 | E2E-FLINK-THIN-06 已缓存 identity 复用 | 通过 | 相同 Studio 查询再次返回同一业务行且仍为 Gateway 模式；remote identity 目录保持 1 个，插件主 JAR 时间戳未改变，证明第二次任务复用已下载制品而未重复发布缓存。查询后仍为 10/10 slots 可用、0 个运行作业。 |
| 2026-08-02 02:09 UTC+8 | E2E-FLINK-THIN-07 命令启动基线与重启恢复 | 通过 | Server/Worker 最终均以最新 executable JAR、Java 17、`-Xms256m -Xmx512m` 命令启动；环境分别继承 IDEA configuration，内部认证与加密配置只读自 Nacos 且未落盘。Worker 保留 Gateway 模式，重启后真实查询约 2 秒返回同一业务行，既有 TaskManager 插件制品时间戳未改变，10/10 slots 可用且无运行作业。 |
