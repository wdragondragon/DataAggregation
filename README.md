# DataAggregation

`DataAggregation` 是一个基于插件化架构的数据集成框架，面向多源异构数据采集、转换、写入和任务编排场景。整体设计参考 DataX 的 Reader / Transformer / Writer 模型，并在此基础上扩展数据源插件、作业插件、任务运行容器和 Studio 管理端能力。

## 项目定位

- 统一抽象 RDBMS、消息队列、文件系统、HTTP/WebService 等数据源。
- 通过插件机制加载数据源处理器、Reader、Transformer、Writer。
- 通过 JobContainer 执行结构化同步任务，支持单表采集、HTTP/SOAP 采集、转换处理和目标写入。
- 为上层 Studio 提供可视化配置、模型管理、任务编排、数据服务发布和运行监控能力。

## 主要模块

```text
DataAggregation/
├── commons/                         # 公共常量、配置、工具与基础模型
├── core/                            # JobContainer、任务上下文、运行流程和插件 SPI
├── plugins-loader-center/           # 插件扫描、加载与隔离
├── data-source-handler-abstract/    # 数据源处理器抽象
├── data-source-plugins/             # 数据源插件集合
├── job-plugins/                     # reader / transformer / writer 作业插件
├── starter/                         # Spring Boot 集成能力
├── package_all/                     # 本地打包与聚合发布脚本
└── data-aggregation-studio/          # 可视化管理端，独立 Git 仓库
```

## 核心能力

### 数据源插件

- RDBMS：MySQL、Hive 等关系型数据源。
- 文件系统：HDFS、FTP/SFTP、S3/MinIO 等文件类数据源。
- 消息队列：Kafka、RocketMQ、RabbitMQ 等消息类数据源。
- HTTP：REST JSON、REST XML、SOAP 1.1/1.2 等 HTTP/WebService 数据源。

### 作业插件

- Reader：从数据库、消息队列、HTTP/WebService 等来源读取数据。
- Transformer：复用 DataX 风格转换器，并支持扩展自定义转换器。
- Writer：写入数据库、消息队列、HTTP/WebService 等目标端。

### 运行模型

- 使用 JSON 配置描述任务。
- 通过 `JobContainer` 加载配置并执行。
- 通过插件目录和 `aggregation.home` 定位运行期插件资源。

## 快速开始

### 1. 构建本地 Maven 依赖

```powershell
cd C:\dev\ideaProject\DataAggregation
mvn -DskipTests install
```

### 2. 打包插件运行目录

```powershell
cd C:\dev\ideaProject\DataAggregation
mvn -DskipTests package
```

如需使用 `package_all` 下的聚合脚本，可按当前环境执行对应脚本生成 `aggregation` 运行目录。

### 3. 配置运行目录

运行 JobContainer 前，需要设置 `aggregation.home`，指向插件和运行资源所在目录：

```text
-Daggregation.home=C:\dev\ideaProject\DataAggregation\package_all\aggregation
```

### 4. 启动作业

```java
public static void main(String[] args) {
    Configuration configuration = Configuration.from(new File("job.json"));
    JobContainer container = new JobContainer();
    container.start(configuration);
}
```

## HTTP 与 WebService 采集

HTTP 采集能力由现有 `httpreader` / `httpwriter` 承载，不单独拆分 WebService 插件。

- REST JSON：适用于普通 JSON API 读取与写入。
- REST XML：适用于 XML API 读取与写入。
- SOAP：适用于 SOAP 1.1 / SOAP 1.2 WebService 调用。
- SOAP Writer 支持单对象和数组批量模板，数组模板使用 `{{#records}}...{{/records}}` repeat 块。

## Studio

`data-aggregation-studio` 是放在本工作区下的独立管理端项目，提供 Web 控制台和在线 Worker 运行能力。它不是外层 DataAggregation Maven Reactor 的子模块，也不共享外层父 POM。

Studio 主要覆盖：

- 数据源管理
- 模型中心
- 数据采集任务
- 工作流编排
- 数据开发
- 数据服务发布
- 数据接入服务
- 协议转换服务
- 数据质量
- 运维中心

Studio MySQL 环境的数据初始化采用 SQL 脚本方式，脚本位于 `data-aggregation-studio/backend/studio-server/src/main/resources/`。进入该目录后连接目标 MySQL 库，按 `schema-mysql.sql`、`data-mysql-base.sql`、`data-mysql-builtin.sql`、`data-mysql-runtime-options.sql` 顺序执行即可。

更多说明见 [data-aggregation-studio/README.md](./data-aggregation-studio/README.md)。

## 开发约定

- 插件能力优先通过 SPI 扩展，不在核心流程中硬编码具体数据源。
- 作业配置使用结构化 JSON，避免在运行期依赖页面状态。
- DataAggregation 核心与 Studio 管理端保持边界清晰：核心负责执行，Studio 负责配置、调度、监控和开放服务。
