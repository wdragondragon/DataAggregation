# Studio 插件 OSS 按需冷加载全业务 UI 验收

## 记录约定

- 验收日期：2026-08-02
- 测试方式：Studio Web UI 真实业务操作，本地缓存与 OSS 只用于版本证据核对。
- 数据策略：新建的项目、数据源、模型、任务、服务、物理表和测试数据保留，不删除。
- 安全约定：不记录账号密码、Token、OSS/Nacos/数据库密钥、原始 endpoint、完整配置或可恢复会话信息。
- 进度规则：只追加已完成用例和非秘密证据，不将本文档作为登录会话或资源 ID 恢复点。

## 验收矩阵

| 编号 | 模块 | UI 路径/复现操作 | 插件坐标 | 结论 | 问题/修复 | 复测 |
|---|---|---|---|---|---|---|
| BASE-01 | 登录与项目上下文 | Web 登录页进入总览 | 不适用 | 通过 | 使用授权账号成功进入 Studio；登录信息未落盘 | 不适用 |
| BASE-02 | Worker Lazy OSS 基线 | 运维中心 / Worker 状态 | 全部 | 通过 | Worker 在线；JVM `aggregation.home` 指向 `aggregation_lazy`；Nacos 有效配置为 Lazy/production | 冷启后再核对 |
| BASE-03 | 项目执行面授权 | 运行集群管理 / 项目授权 / 运行实例 | 全部 | 通过 | 新项目已授权 `DEFAULT-LOCAL` 且设为首选；冷启 Worker 实例在线 | 通过 |
| BASE-04 | Worker 组下发 | 系统管理 / Worker 下发；运维中心复核 | 全部 | 通过 | 初次发现新项目未下发 Worker 组；已通过 UI 下发并启用在线组 | 通过 |
| OSS-01 | OSS 发布仓库 | 按 production pointer 核对必需坐标 | 六个目标坐标 | 通过 | Java 只读工具基于实际 Nacos 配置核对 pointer 和 ZIP，6/6 完全匹配 | 不适用 |
| ASSET-01 | 数据源与资产模型 | 数据源测试、发现、水合、模型预览 | source/mysql8 | 通过 | 空缓存冷加载成功；7 张表发现并水合；样例数据正确 | 通过 |
| DEV-01 | 数据开发 | SQL、Java、Python 脚本查询 | source/mysql8 | 通过 | 三种脚本均由 UI 创建、保存和执行，结果一致 | 通过 |
| SERVICE-01 | 数据服务 | 字段解析、调试、发布、响应转换 | source/mysql8, transformer/trim_spaces_str | 通过 | 首次调试完成 transformer 冷加载，发布后 UI 再次调用成功 | 通过 |
| INGEST-01 | 数据接入 | REST 接入调试、发布、写入回读 | writer/mysql8writer | 通过 | 两条记录写入成功，UI 模型预览完成回读 | 通过 |
| COLLECT-01 | 单表采集 | reader + transformer + writer 全量运行 | reader/mysql8reader, writer/mysql8writer, transformer/trim_spaces_str | 通过 | `mysql8reader` 首次运行从 OSS 冷加载；5 行全量采集、转换和写入成功 | 通过 |
| FUSION-01 | 融合采集 | 两个 MySQL 输入 LEFT JOIN 并输出 | reader/fusionreader, source/mysql8, writer/mysql8writer | 通过 | `fusionreader` 首次从 OSS 冷加载；5 行 LEFT JOIN 输出正确 | 通过 |
| CONSISTENCY-01 | 一致性 | 检查 Web 工作流画布新建入口 | reader/consistencyreader | 阻断 | Web 画布当前不提供 CONSISTENCY 创建/配置入口；按本次约定不扩展 UI，不伪造业务验收 | 不适用 |
| QUALITY-01 | 数据质量 | 完整性规则与质量任务执行 | source/mysql8 | 通过 | UI 新建项目级字段空值规则和任务；预置空值数为 1 | 通过 |
| WORKFLOW-01 | 工作流 | 绑定采集、质量、数据脚本并逐节点执行 | 复用上述坐标 | 通过 | 三节点按 ON_SUCCESS 顺序完成；采集改为 replace 以支持保留数据后的重复验收 | 通过 |
| FLINK-01 | 智能问数 / Flink SQL | Studio UI 只读问数 | source/mysql8 | 通过 | 首次空远程缓存暴露 30 秒 Gateway 超时不足；已将查询/取数默认超时调整为 120 秒、connector 制品请求超时调整为 300 秒 | 空 remote cache 首次执行约 20.2 秒成功，返回 5 / 151.5 / 1 |
| ASSISTANT-01 | AI 助手自然语言问数 | Studio UI AI 助手 -> 提问 -> 确认只读执行 | source/mysql8 | 通过 | 当前助手将该问题路由为普通数据开发 SQL，不伪记为 Flink Gateway 链路 | 返回 5 / 151.50 / 1 |
| BOUNDARY-01 | 协议转换 | UI 内置安全转换逻辑 | 无外部 aggregation 插件 | 不适用 | 不将内置协议转换伪记为 transformer 插件加载 | 不适用 |
| BOUNDARY-02 | reporter | Lazy `core.json` 与 Studio UI 入口核对 | report/* | 不适用 | Lazy 骨架明确配置 `reporter: []`，Studio 无 reporter 配置入口 | 不适用 |

## 执行记录

### 2026-08-02 BASE-01

- 复现路径：打开 Studio Web 登录页，通过已授权账号登录。
- 实际结果：成功进入总览页，页面显示当前租户/项目上下文和全部业务菜单。
- 结论：通过。

### 2026-08-02 BASE-02

- UI 路径：系统管理 -> 运行集群管理 -> 默认集群 -> 运行实例。
- 实际结果：默认集群与 Worker 实例在线，运行版本为 `1.0_jdk17-SNAPSHOT`。
- 非秘密运行证据：JVM `aggregation.home` 指向 `DataAggregation/package_all/aggregation_lazy`；Nacos 有效配置为 `LAZY_OBJECT_STORAGE`、`production`、`aggregation-plugins`。
- 冷加载前现状：新目录中已有 `source/mysql8` 的单个已验证版本，将在 Worker 停止后非破坏性改名备份。
- 结论：通过，冷启后需复核。

### 2026-08-02 OSS-01

- 操作：使用复用 `CloudObjectStorageService` 的 Java 只读工具，从当前 Nacos 生效配置连接 production 发布仓库；未使用 OSS 图形界面，未执行上传或 pointer 变更。
- 核对结果：6 个坐标的 `current.json` 与不可变 ZIP 均存在，大小和 SHA-256 均与本地发布摘要一致。
- 版本摘要：`source/mysql8 c1f419ea421d`，`reader/mysql8reader 7a942fbb769b`，`writer/mysql8writer b64c95a96160`，`reader/fusionreader 18cf5fab7f7e`，`reader/consistencyreader dd33f2e70f59`，`transformer/trim_spaces_str 9aa14c9edf8f`。
- 全部 release 为 `20260730-v1`，runtimeVersion 为 `1.0_jdk17-SNAPSHOT`。
- 结论：通过，无缺失制品，不需要发布或备份 production pointer。

### 2026-08-02 BASE-03

- UI 路径：切换到独立测试项目 -> 系统管理 -> 运行集群管理 -> 项目授权。
- 操作与结果：通过 UI 授权 `DEFAULT-LOCAL` 并设为首选集群；运行集群页显示默认集群在线，运行版本为 `1.0_jdk17-SNAPSHOT`。
- 实例证据：运行实例弹窗显示 `studio-online-worker-01` 在线，实例地址为本机 Worker 端口；新 Worker 缓存基线仍为空。
- 结论：通过，后续业务任务可由新项目调度到本次冷启 Worker。

### 2026-08-02 BASE-04

- UI 复现路径：统一运维中心 -> Worker 组状态。
- 初次实际结果：`studio-online-worker-01` 在租户内在线，但新项目显示“未下发”，页面明确提示任务不会被 Worker 领取。
- 处理：系统管理 -> Worker 下发 -> 下发到当前项目，保持启用并保存。
- 复测：Worker 下发表显示“已下发 / 启用 / 在线”，1 个在线实例；该配置前置问题已解决。

### 2026-08-02 ASSET-01 / source/mysql8 冷加载阶段

- 测试数据：创建并保留独立 MySQL schema；预置 5 条订单、3 条维表记录、采集/融合/接入目标表及一致性边界数据。连接信息和凭证未写入本文。
- UI 复现路径：数据资产 -> 数据源中心 -> 新建数据源 -> 选择 `mysql8` -> 填写连接 -> 测试连接。
- 冷加载前：Worker `cache/.state/.staging` 文件数均为 0。
- UI 业务结果：默认集群返回“可用 / Connection success”，耗时约 5.3 秒；数据源随后经 UI 保存并纳入管理。
- 插件证据：`source/mysql8` 从 OSS 激活 release `20260730-v1`；本地目录 identity 为该 release 加完整 SHA，SHA 前缀 `c1f419ea421d`，缓存 35 个文件、约 21.3 MiB；`.state` 的 release、SHA、runtimeVersion 与首次触发前 pointer 一致。
- 心跳证据：插件 fingerprint 从空基线更新为新的稳定 SHA-256；capability 显示 `LAZY_OBJECT_STORAGE`、active=1、cached=1、state=`UP`。
- 结论：首次从零按需下载、校验、原子激活和真实连接测试通过；ASSET-01 的发现、水合、目录和预览仍待继续。

### 2026-08-02 ASSET-01 / 发现、水合与预览完成

- UI 路径：模型中心 -> 选择 `mysql8` 和测试数据源 -> 同步 -> 选择全部物理表。
- 发现结果：UI 通过已缓存的 `source/mysql8` 发现 7 张真实表，名称与预置 schema 完全一致。
- 水合结果：7 张表全部同步为当前项目模型；`source_orders` 水合出 7 个字段，主键、可空性、DECIMAL 精度和 TIMESTAMP 类型均正确。
- 预览结果：模型详情显示 5 条订单样例，主键 1-5、金额合计预期 151.50，`nullable_note` 恰有 1 个空值，状态和值与预置数据一致。
- 目录结果：工作台 -> 能力矩阵可见 mysql8 为已纳入、可读、可写，reader/writer 支持均为 mysql8。
- 插件复用：期间 Worker 未新增其他插件缓存，`source/mysql8` identity 保持 `20260730-v1` 对应 SHA，未发生版本漂移。
- 结论：ASSET-01 通过。

### 2026-08-02 DEV-01 / SQL、Java、Python 脚本

- UI 路径：数据开发 -> 新建脚本；分别选择 SQL、Java、Python，绑定默认运行集群并通过 UI 保存、执行。
- SQL 结果：`row_count=5`、`total_amount=151.5`、`null_count=1`，成功返回 1 行 3 列，约 642 ms。
- Java 结果：脚本通过平台白名单 `context.getServices().executeSql(...)` 查询同一测试数据源，返回相同的 5 行、金额合计 151.5 和 1 个空值，约 2.4 s。
- Python 结果：脚本通过平台白名单 `context.services.execute_sql(...)` 查询同一测试数据源，返回相同结果，约 2.9 s。
- 插件证据：三条真实执行链均复用已激活的 `source/mysql8`；Java 运行日志可见插件创建连接并完成查询，缓存 identity 未漂移且未回退完整 aggregation 目录。
- 保留资源：三个脚本均保留在测试项目根目录，后续工作流可直接绑定。
- 结论：通过；无代码修改，无待修复问题。

### 2026-08-02 SERVICE-01 / 数据服务与响应转换

- UI 路径：数据服务 -> 新建服务 -> 模型发布；选择测试数据源和 `source_orders`，通过“解析字段”生成 7 个返回字段。
- 转换配置：在 `customer_name` 返回字段上配置“规整 / 去除字符串前后空格”，随后通过 UI 保存、发送调试、发布并再次发送已发布服务调试请求。
- 冷加载前后：首次调试前 Worker 仅有 `source/mysql8`；调试期间从 OSS 激活 `transformer/trim_spaces_str` release `20260730-v1`，SHA 前缀 `9aa14c9edf8f`，本地缓存 26 个文件、约 17.8 MiB，并写入独立 `.state`。
- 业务结果：调试与发布后调用均返回 `success=true`、5 条订单；数据库原值含前后空格的第一条 `customer_name` 在 UI 返回 JSON 中为精确的 `Alice`。
- 运行证据：Worker 日志显示外部 transformer 首次下载激活并初始化成功；第二次已发布调用直接复用缓存 identity，没有重复下载或完整本地 aggregation 兜底。
- 保留资源：服务 `OSS冷加载订单服务20260802` 保留为“已发布”。
- 结论：通过；无代码修改，无待修复问题。

### 2026-08-02 INGEST-01 / 数据接入服务

- UI 路径：数据接入服务 -> 新建接入服务；选择 JSON 数组来源、MySQL 8、测试数据源和 `target_ingest`，解析 4 个目标字段并保留 Writer 默认 insert 配置。
- 冷加载证据：选择数据源渲染 Writer 表单时首次从 OSS 激活 `writer/mysql8writer` release `20260730-v1`；SHA 前缀 `b64c95a96160`，缓存 42 个文件、约 21.9 MiB，独立 `.state` 与 pointer 匹配。
- 调试结果：通过 UI 发送包含两条记录的 JSON，请求汇总为 received=2、success=2、failed=0、status=`SUCCESS`；UI 的 `pluginRevisions` 明确记录 `writer/mysql8writer=sha256:b64c95a...`。
- 发布结果：接入服务 `OSS冷加载订单接入20260802` 保留为“已发布”。
- 数据复核：模型中心 -> `target_ingest` -> 样例数据，UI 显示主键 9001/9002、金额 12.34/56.78、备注 first/second、名称 API One/API Two，共 2 行，与请求完全一致。
- 结论：通过；无代码修改，无待修复问题。

### 2026-08-02 COLLECT-01 / 单表采集

- UI 路径：数据采集 -> 采集任务管理 -> 新建采集任务；选择 `source_orders` 到 `target_collect` 的全量单表任务，逐字段映射，并为 `customer_name` 配置“规整 / 去除字符串前后空格”；保存草稿、上线、触发并从采集任务日志下钻核对。
- 冷加载证据：运行前没有 `reader/mysql8reader` 缓存；首次触发从 OSS 激活 release `20260730-v1`，SHA 前缀 `7a942fbb769b`，写入独立 `.state`，缓存 42 个文件、约 21.9 MiB。Writer 与 transformer 复用此前已激活版本，没有回退完整本地 aggregation 目录。
- 运行结果：UI 运行记录状态“成功”，采集数 5、成功数 5、失败数 0、转换数 5、转换成功 5、过滤数 0；实际 Worker 和运行集群均为本次 Lazy OSS 基线实例。
- 数据复核：模型中心 -> `target_collect` -> 样例数据显示 5 行，主键 1-5、金额合计 151.50、恰有 1 个空值；源值带前后空格的名称在目标表为精确 `Alice`，其余字段与源表一致。
- 保留资源：任务 `OSS冷加载单表采集20260802` 保留为“已上线”，目标表数据和新缓存均保留。
- 结论：通过；无代码修改，无待修复问题。

### 2026-08-02 FUSION-01 / 融合采集

- UI 路径：数据采集 -> 采集任务管理 -> 新建采集任务；新增第二个源使任务切换为“融合任务”，配置 `source_orders` 为 `src1`、`source_customer_dim` 为 `src2`，按 `customer_id` 执行 LEFT JOIN，输出到 `target_fusion`，并对名称字段复用 trim 转换器。
- 冷加载证据：首次运行前没有 `reader/fusionreader` 缓存；触发后从 OSS 激活 release `20260730-v1`，SHA 前缀 `18cf5fab7f7e`，写入独立 `.state`，缓存 3 个文件、约 145 KiB。两个 MySQL 源、Writer 与 transformer 均复用已验证版本。
- 运行结果：UI 运行记录状态“成功”，采集数 5、成功数 5、失败数 0、过滤数 0，消息为 `fusion -> mysql8` 节点完成。
- 数据复核：模型中心 -> `target_fusion` -> 样例数据共 5 行；101/102/104 三个客户匹配 GOLD/NORTH、SILVER/SOUTH、BRONZE/EAST，103/105 两行维表字段为空并仍被 LEFT JOIN 保留；名称已去除首尾空格，金额合计仍为 151.50。
- 保留资源：任务 `OSS冷加载融合采集20260802` 保留为“已上线”，目标表数据和新缓存均保留。
- 结论：通过；无代码修改，无待修复问题。

### 2026-08-02 QUALITY-01 / 数据质量

- UI 路径：数据质量 -> 质量规则 -> 新建项目级“完整性 / 字段级”规则；SQL 模板统计字段空值数。随后在质量任务中绑定测试数据源、`source_orders.nullable_note` 和该规则，完成 SQL 预览、语义校验、保存、发布、手动执行及运行日志下钻。
- 规则结果：参数解析正确生成表与字段两个运行时参数，最终 SQL 指向预置订单表的可空字段；UI 语义校验状态“通过”，结果列为 `null_count`，样例值精确为 1。
- 运行结果：质量任务运行记录状态“成功”，在本次 Lazy OSS Worker 与默认集群执行并返回 1 行；质量指标页显示已纳管资产 1、执行健康分 100，资产最后执行时间与本次运行一致。
- 插件证据：真实执行复用已激活 `source/mysql8` identity，期间没有新插件下载、版本漂移或完整本地 aggregation 兜底。
- 保留资源：规则 `OSS冷加载空值统计规则20260802` 与任务 `OSS冷加载空值质量任务20260802` 均保留，任务状态为“已发布”。
- 结论：通过；无代码修改，无待修复问题。

### 2026-08-02 WORKFLOW-01 / 全业务工作流

- UI 路径：数据开发 -> 工作流编排；创建并发布 `OSS冷加载全业务工作流20260802`，绑定单表采集任务、空值质量任务和 SQL 数据脚本，以两条 `ON_SUCCESS` 边组成顺序 DAG；随后从列表“更多 -> 触发”选择默认运行集群，并在工作流日志中逐节点下钻。
- 幂等处理：由于测试数据要求永久保留，采集任务 Writer 通过 UI 从 `insert` 调整为 `replace`，避免工作流复测时目标表主键重复；字段映射和 trim transformer 均保持不变，任务继续为“已上线”。
- 运行结果：工作流状态“成功”，运行于 `DEFAULT-LOCAL`，总耗时 17.0 秒；3/3 节点成功、失败 0、运行中 0、未运行 0。
- 节点证据：采集节点约 471 ms 完成，日志显示读取 5、写入 5、转换成功 5、错误 0；质量节点返回 1 行，继续验证预置空值数 1；SQL 节点约 717 ms 返回 1 行，其绑定脚本的业务结果为 `row_count=5`、`total_amount=151.5`、`null_count=1`。
- 插件证据：采集节点复用固定的 reader、writer 和 trim transformer identity；质量与 SQL 节点复用固定的 `source/mysql8` identity，工作流运行期间没有新下载、版本漂移或完整本地 aggregation 兜底。
- 保留资源：工作流、三个绑定节点、ON_SUCCESS 边、运行记录和目标表数据全部保留。
- 结论：通过；无生产代码修改，无待修复问题。

### 2026-08-02 FLINK-01 / 首次执行失败留档

- UI 路径：数据开发 -> 新建脚本 -> 选择“模型 Flink SQL” -> 通过模型选择器添加 `source_orders` -> 保存 `OSS_COLD_UI_FLINK_20260802.sql` -> 执行 SQL。
- 冷加载前证据：TaskManager 的 `dataaggregation-flink-plugin-runtime/remote` 为空；原目录已按时间戳备份并保留。WSL `lib` 中 connector SHA-256 前缀为 `406f88485131`、大小约 6.0 MiB，本轮没有替换 connector。
- 执行 SQL：只读聚合查询 `row_count`、`total_amount` 和 `null_count`，目标虚拟表为 UI 所选模型生成的 `m_{modelId}`；脚本和模型绑定均已保留。
- 首轮实际结果：UI 在约 40 秒后显示“失败”，错误为 `Flink SQL Gateway operation timed out`，没有返回业务结果。
- 当前结论：失败，正在核对远程插件缓存、Worker capability、Gateway/TaskManager 日志和超时配置；尚未修改代码，不将该结果记为通过。

### 2026-08-02 FLINK-01 / 缺陷修复与空缓存复测

- 原因：TaskManager 首次远程获取约 21.3 MiB 的 `source/mysql8` 制品时，Studio 的 Gateway 查询/结果获取默认 30 秒超时先于冷加载完成；插件随后下载成功，但 UI 会话已经取消。该问题只影响首次冷加载，第二次命中缓存约 6.3 秒即可成功。
- 最小修复：Flink 查询超时和 Gateway 结果获取超时默认值由 30 秒调整为 120 秒；TaskManager connector 的 Worker 制品请求超时由 60 秒调整为 300 秒。Worker 配置文件同步开放相同参数入口，并新增默认值单元测试。
- 自动化预检：使用 JDK 17 执行 `FlinkExecutionModeTest`，7/7 通过；Worker 聚合构建和 remote connector 制品构建均通过。系统默认 JDK 8 的一次编译失败属于工具链不匹配，切换项目要求的 JDK 17 后通过。
- Connector 保护：替换前已在 WSL `lib` 原地备份旧 connector，旧制品 SHA-256 前缀 `406f88485131`、大小 6,269,691 字节；新制品 SHA-256 前缀 `44fcfa770643`、大小 6,265,684 字节。备份保留且未覆盖。
- 复测基线：停止 Flink 后再次将 TaskManager remote cache 非破坏性改名备份，创建空 remote 目录；以原 JDK 17、原 Flink 安装目录、原 Cluster/TaskManager/SQL Gateway 参数重新启动。首次点击“执行 SQL”前 remote 文件数为 0。
- UI 复现路径：数据开发 -> 选择已保存的 `OSS_COLD_UI_FLINK_20260802.sql` -> 点击“执行 SQL”。全程从 Studio Web UI 发起，没有绕过 UI 调用执行接口。
- 复测结果：空 remote cache 的第一次执行即成功，耗时 20,248 ms，返回 1 行 3 列：`row_count=5`、`total_amount=151.5`、`null_count=1`；执行摘要为 `executionMode=gateway`。
- TaskManager 证据：remote cache 新增唯一 `source/mysql8` identity，前缀 `a5b2968a40e2`；共 35 个文件、22,316,093 字节，包含主 JAR、`plugin.json` 和依赖目录。该目录在查询期间新建，未命中已隔离的旧缓存。
- Worker 证据：Worker 继续使用 `aggregation_lazy`，启动时从 `.state` 恢复 5 个 active release；`source/mysql8` 仍为首次 OSS pointer 对应的 `20260730-v1` 与 SHA 前缀 `c1f419ea421d`，fingerprint 未发生非预期漂移。TaskManager 通过 Worker runtime capability 获取制品，没有读取完整本地 aggregation 目录。
- 安全审计：没有在 DDL、UI 结果或报告中传播 OSS 凭证、内部 capability token 或数据库连接密钥；TaskManager 原始连接日志未复制到报告。
- 结论：修复后复测通过，FLINK-01 由失败关闭为通过。测试脚本、模型绑定、新 remote cache、旧缓存备份和 connector 备份全部保留。

### 2026-08-02 ASSISTANT-01 / AI 助手自然语言补测

- UI 路径：数据开发 -> AI 助手 -> 输入针对 `source_orders` 的总行数、金额合计与空值数问题 -> 在助手确认卡片点击“确认执行”。
- 实际结果：助手完成模型定位、详情核对和 SQL 生成，最终返回总行数 5、`amount` 总和 151.50、`nullable_note` 空值行数 1，执行耗时约 166 ms。
- 链路边界：助手本次明确路由为 `Data development: executeSql / sql`，因此复用 Worker 的 `source/mysql8`，不是 `studio-flink` Gateway 查询；真正的 Gateway/TaskManager 冷加载证据仍以 FLINK-01 的“模型 Flink SQL”UI 执行为准，不混淆两条链路。
- 状态复核：统一运维中心显示排队任务 0、在线 Worker 1；Worker 下发页显示目标组已下发、启用、在线，实例心跳持续更新。
- 结论：AI 助手自然语言只读问数通过；无新增插件下载、无版本漂移。

### 2026-08-02 GATE-DATAAGGREGATION / 定向门禁

- 工具链：JDK 17.0.12。
- `mvn -pl plugins-loader-center,core -am test`：通过；loader 4 个测试、core 25 个测试，失败 0、错误 0、跳过 0。
- `mvn -pl plugins-loader-center,core -am -DskipTests install`：通过，6 个 Reactor 模块全部成功安装。
- 备注：构建仍输出仓库既有的 Maven plugin version/重复声明警告，不影响本次门禁结论；异常路径测试中的预期异常日志不属于测试失败。

### 2026-08-02 GATE-STUDIO / Worker、Flink 与部署预检

- `mvn -pl studio-worker -am test`：通过；8 个 Reactor 模块成功，Worker 测试 138 项，失败 0，错误 0，跳过 1。
- `mvn -pl studio-worker -am -DskipTests install`：通过；Worker executable 制品重新安装成功。
- `mvn -pl studio-flink -am test`：通过；7 个 Reactor 模块成功，studio-infra 377 项、studio-flink 115 项，失败 0，错误 0。
- `mvn -pl studio-flink -am -DskipTests install`：通过；studio-flink executable 制品重新安装成功。
- `scripts/tests/test-flink-remote-connector-artifact.ps1`：通过；remote connector 6,265,684 字节，未包含聚合插件运行时、具体 source 插件类或打包插件目录，并包含 Worker artifact 下载入口。
- `scripts/tests/test-check-studio-deployment.ps1`：通过，12/12；覆盖 Lazy OSS 参数、秘密脱敏、旧变量拒绝、EAGER/Lazy 守门与 connector 部署约束。

### 2026-08-02 FINAL / 环境、制品与报告审计

- 服务状态：Worker PID 37464 使用 JDK 17、`-Xms256m -Xmx512m`，18081 健康；Studio Web 登录入口、Flink JobManager 8081 和 SQL Gateway 8083 均可访问。统一运维中心最后复核为 1 个 Worker 在线、0 个排队任务。
- 轻量运行状态：Worker 仍指向 `package_all/aggregation_lazy`；当前 `.state` 5 个、cache 148 个文件、5 个已验证版本目录、`.staging` 无文件。没有切换到完整 `package_all/aggregation`。
- 非破坏性备份：本地 `.state.before-cold-*`、`.staging.before-cold-*`、`cache.before-cold-*` 均保留；WSL TaskManager remote cache 的冷加载前备份和超时修复前备份均保留；原 connector `.before-timeout-fix-*.bak` 保留。新 remote cache 和新 connector 作为验收制品保留。
- OSS/Nacos 恢复：本次验收只读核对 production pointers，没有修改 `current.json`；没有修改 Nacos 生效配置，因此不存在需要覆盖他人变更的恢复动作。六个目标坐标仍为首次触发时记录的不可变 release。
- 测试数据保留：独立测试项目、MySQL 数据源、物理表、模型、脚本、数据服务、数据接入服务、采集任务、融合任务、质量规则/任务、工作流、运行记录和写入的目标表数据均未删除。
- 代码与日志审计：生产源码及当前构建制品中没有 `PluginE2eMarker`、`PLUGIN_E2E_MARKER` 或仅用于本轮观察的临时打印；报告仅保留脱敏坐标、release/SHA 前缀、数量级和业务结果，不包含密码、Token、密钥、原始 endpoint 或可恢复会话信息。两个仓库 `git diff --check` 通过。
- 最终结论：UI 可达 aggregation 插件均完成 OSS 从零按需下载、校验、缓存和真实业务加载；Flink 空 remote cache 首次执行复测通过；唯一不可达的 consistency UI 入口按计划明确记录为阻断，没有伪造通过；环境和测试资产按约定保留。
