# DataAggregation 数据采集到 Kafka 操作手册

| 项 | 值 |
|---|---|
| 版本 | v1.0 |
| 适用分支 | `jdk17`（默认分支） |
| 适用版本 | `1.0_jdk17-SNAPSHOT` |
| Java | 17 |
| Spring Boot | 3.5.13 |
| 文档维护 | Mavis |
| 最后更新 | 2026-08-21 |

---

## 目录

1. [概述](#1-概述)
2. [核心概念](#2-核心概念)
3. [Kafka Writer 参数清单](#3-kafka-writer-参数清单)
4. [文件来源 → Kafka](#4-文件来源--kafka)
   - 4.1 [FTP](#41-ftp)
   - 4.2 [SFTP](#42-sftp)
   - 4.3 [HDFS](#43-hdfs)
   - 4.4 [MinIO / S3 / OSS](#44-minio--s3--oss)
   - 4.5 [HTTP / WebService](#45-http--webservice)
5. [数据库来源 → Kafka](#5-数据库来源--kafka)
   - 5.1 [MySQL](#51-mysql)
   - 5.2 [Oracle](#52-oracle)
   - 5.3 [PostgreSQL](#53-postgresql)
   - 5.4 [DM（达梦）](#54-dm达梦)
   - 5.5 [Hive2](#55-hive2)
6. [字段映射与 Transformer](#6-字段映射与-transformer)
7. [安全认证](#7-安全认证plaintext--sasl--kerberos)
8. [Studio 配置流程](#8-studio-配置流程含路径)
9. [本地 Java 验证](#9-本地-java-验证)
10. [性能调优](#10-性能调优)
11. [常见错误与排障](#11-常见错误与排障)
12. [附录：10 份完整 JSON 模板](#12-附录10-份完整-json-模板)

---

## 1. 概述

DataAggregation 是 **DataX 风格**的数据集成框架。本手册覆盖 **Reader → Kafka Writer** 的完整链路，覆盖以下数据源：

- **文件类**：FTP、SFTP、HDFS、MinIO / S3 / OSS、HTTP / WebService
- **数据库类**：MySQL、Oracle、PostgreSQL、DM（达梦）、Hive2

> 所有 Reader 都不需要改造，直接组合 `kafkawriter` 即可完成端到端采集。

**典型使用场景**：

- 业务系统日终批量落地的 CSV / TXT 文件入 Kafka
- 关系数据库的全量 / 增量入 Kafka（CDC 镜像）
- 数据库变更捕获 + Hive 离线层 + Kafka 实时层并行
- 跨数据源（FTP 文件 → Kafka → 实时计算 → 数据库）
- 数据中台贴源层（ODS）建设

---

## 2. 核心概念

DataAggregation 的运行单元是 **Job**，一个 Job 由若干 **Content**（reader → [transformer] → writer 配对）组成。

```
┌──────────────────────────────────────────────────────────┐
│                        Job (JSON)                        │
│  ┌──────────────────────────────────────────────────┐    │
│  │  Content[0]                                      │    │
│  │  ┌────────┐    ┌────────────┐    ┌───────────┐   │    │
│  │  │ Reader │ →  │Transformer │ →  │  Writer   │   │    │
│  │  └────────┘    └────────────┘    └───────────┘   │    │
│  └──────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────┘
```

- **Reader**：从源读取数据，每行产生一条 `Record`（含多列）
- **Transformer**：可选，对 Reader 产出的 Record 做改字段、改类型、脱敏、过滤
- **Writer**：把 Record 写成 Kafka 消息

**作业结构骨架**：

```json
{
  "job": {
    "setting": {
      "speed": { "channel": 1, "byte": 10485760 }
    },
    "content": [
      {
        "reader": { "name": "ftpreader", "config": { /* ... */ } },
        "writer": { "name": "kafkawriter", "config": { /* ... */ } }
      }
    ]
  }
}
```

---

## 3. Kafka Writer 参数清单

> 源码：`job-plugins/writer/kafkawriter/.../KafkaWriter.java`、`Key.java`

### 3.1 必填参数

| 参数 | 类型 | 说明 |
|---|---|---|
| `bootstrapServers` | string | Kafka broker 地址，`host:port[,host:port]` |
| `topic` | string | 目标 topic |
| `columns[]` | array | 与 Reader 输出顺序对应的字段列表（`{index, name}`） |

### 3.2 可选参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `writeType` | `split` | `json` / `split` / `rawdata` |
| `fieldDelimiter` | `\t` | 仅 `writeType=split` 生效 |
| `autoCreateTopic` | `true` | 写前判断并自动建 topic |
| `createTopicNumPartition` | `1` | 自动建 topic 的分区数 |
| `createTopicReplicationFactor` | `1` | 自动建 topic 的副本数 |
| `ack` | `0` | producer acks：`0` / `1` / `all` |
| `retries` | `0` | producer 重试次数 |
| `batchSize` | `16384` | producer `batch.size` |
| `otherProperties` | `{}` | 透传 Kafka 原生 producer 参数（如 `linger.ms`、`compression.type`） |

### 3.3 认证参数

| 参数 | 场景 | 说明 |
|---|---|---|
| `kerberos` | Kerberos 开关 | 设为 `1` 开启 |
| `principal` | Kerberos | Kerberos principal，如 `etl@HADOOP.COM` |
| `kerberosKeytabFilePath` | Kerberos | 运行机上的 keytab 文件路径 |
| `krb5Conf` | Kerberos | 运行机上的 `krb5.conf` 路径 |
| `kerberosDomain` | Kerberos | Kerberos realm，如 `HADOOP.COM` |
| `username` / `password` | SASL_PLAINTEXT（PLAIN） | Kafka 用户名密码 |

### 3.4 三种 `writeType` 落地形态

| writeType | Kafka message value 示例 | 适用 |
|---|---|---|
| `json` | `{"id":"1001","name":"alice"}` | 最常用，下游易消费 |
| `split` | `1001\talice` | 与上游分隔符保持一致 |
| `rawdata` | reader 原始字符串 | 整行 / 整文件原始写入 |

---

## 4. 文件来源 → Kafka

### 4.1 FTP

**适用**：业务系统日终批量落地的 CSV / TXT 文件。

| 字段 | 必填 | 说明 |
|---|---|---|
| `connect.host` | ✅ | FTP 服务器 IP / 域名 |
| `connect.port` | ✅ | 21（默认） |
| `connect.username` / `connect.password` | ✅ | 凭据 |
| `connect.ftpTLS` | ❌ | `none` / `SSL` / `TLS` |
| `connect.connectMode` | ❌ | `PASV`（推荐）/ `PORT` |
| `connect.timeout` | ❌ | 毫秒，默认 60000 |
| `rootPath` | ✅ | FTP 上目录路径 |
| `partitionType` | ✅ | `glob`（通配）/ `regex` |
| `partition` | ✅ | 文件匹配模式，如 `*.csv` |
| `fileType` | ✅ | `csv` / `txt` / `orc` / `parquet` |
| `encoding` | ✅ | `UTF-8` / `GBK` |
| `hasHeader` | ✅ | `true` / `false` |
| `delimiter` | ✅ | 字段分隔符：`,` / `\t` |
| `columns[]` | ✅ | `{name, index, type}` |

完整模板见 [附录 1](#附录-1---ftp--kafka-kerberos)。

### 4.2 SFTP

**适用**：SSH 文件传输，安全场景。

与 FTP 几乎一致，差异：

- `name: "sftpreader"`
- 默认端口 `22`
- 移除 `ftpTLS` / `connectMode`
- `timeout` 建议设大（如 60000）

完整模板见 [附录 2](#附录-2---sftp--kafka)。

### 4.3 HDFS

**适用**：Hadoop 生态贴源采集。

Reader 插件：`tbds-hdfs2reader`（同 Hive2 类似但走 HDFS 文件 API）。

支持 `fileType`、`delimiter`、`columns[]` 等文件类参数。

### 4.4 MinIO / S3 / OSS

**适用**：对象存储上的数据文件。

`storageProvider` 取值：

- `oss`：阿里云 OSS
- `s3`：AWS S3
- `minio`：自建 MinIO

完整模板见 [附录 4](#附录-4---minio--kafka)。

### 4.5 HTTP / WebService

**适用**：调 REST / SOAP 接口拉数据。

核心参数（来源 `httpreader/Key.java`）：

| 字段 | 说明 |
|---|---|
| `url` | 接口地址 |
| `method` | `GET` / `POST` |
| `headers` | 自定义 Header |
| `body` | 请求体（POST 用） |
| `responseFormat` | `json` / `xml` |
| `columns` | 字段抽取规则（JSONPath / XPath） |

---

## 5. 数据库来源 → Kafka

所有数据库 Reader 共用 `job-plugins/utils/plugin-rdbms-util` 的通用逻辑，参数高度相似。

### 5.1 MySQL

Reader 插件：`mysql5reader` / `mysql8reader`

| 字段 | 必填 | 说明 |
|---|---|---|
| `connect.host` | ✅ | 数据库 IP |
| `connect.port` | ✅ | 3306（默认） |
| `connect.database` | ✅ | 数据库名 |
| `connect.userName` / `connect.password` | ✅ | 凭据 |
| `connect.other` | ❌ | JDBC URL 拼接参数，如 `?useSSL=false&serverTimezone=Asia/Shanghai` |
| `connect.userPool` | ❌ | `true` 开启连接池 |
| `connect.extraParams` | ❌ | 预留扩展 |
| `table` | 与 `selectSql` 二选一 | 表名 |
| `columns[]` | ✅ | 列名数组（`["id","name"]`），支持 `["*"]` 自动反射 |
| `selectSql` | 与 `table` 二选一 | 自定义 SQL，跳过 table/columns |
| `incrColumn` | ❌ | 增量字段名（如主键） |
| `incrModel` | ❌ | `>` / `>=` / `<` / `<=` / `=` |
| `pkValue` | ❌ | 增量初值，运行后会持久化 |
| `mandatoryEncoding` | ❌ | 默认 `utf-8` |

**全量采集**：不填 `incrColumn`，`columns: ["*"]`。

**增量采集**（关键三参数）：

```json
"incrColumn": "id",
"incrModel": ">",
"pkValue": 0
```

运行时 SQL 会变成 `SELECT * FROM orders WHERE id > 0`，结束位点由 `JobPointReporter` 持久化（重启自动续传）。

**自定义 SQL**：

```json
"selectSql": "SELECT id, name, amount FROM orders WHERE status = 1 AND updated_at > '2026-01-01'"
```

填了 `selectSql` 后，`table` / `columns` / `incrColumn` 都会被忽略。

完整模板见 [附录 6](#附录-6---mysql-增量--kafka)。

### 5.2 Oracle

Reader 插件：`oraclereader`

**注意**：Oracle Reader **必须使用 `selectSql`**，无 `table` 模式。

```json
{
  "reader": {
    "name": "oraclereader",
    "config": {
      "connect": {
        "host": "oracle-host",
        "port": "1521",
        "database": "ORCL",
        "userName": "etl",
        "password": "***"
      },
      "selectSql": "SELECT id, name, amount, status FROM orders WHERE status = 1"
    }
  }
}
```

完整模板见 [附录 7](#附录-7---oracle--kafka)。

### 5.3 PostgreSQL

Reader 插件：`postgresqlreader`

参数与 MySQL 一致（`connect` / `table` / `columns` / `incrColumn` / `incrModel` / `selectSql`）。

完整模板见 [附录 8](#附录-8---postgresql--kafka)。

### 5.4 DM（达梦）

Reader 插件：`dmreader`

参数与 MySQL 一致，端口默认 `5236`。

完整模板见 [附录 9](#附录-9---dm达梦--kafka)。

### 5.5 Hive2

Reader 插件：`tbds-hive2reader`

**特点**：依赖 Hadoop 生态，通常配合 Kerberos。

```json
{
  "reader": {
    "name": "tbds-hive2reader",
    "config": {
      "connect": {
        "host": "hive-host",
        "port": "10000",
        "database": "ods",
        "userName": "hive",
        "password": ""
      },
      "table": "dwd_orders",
      "columns": ["id", "user_id", "amount", "dt"],
      "incrColumn": "id",
      "incrModel": ">"
    }
  }
}
```

完整模板见 [附录 10](#附录-10---hive2--kafka)。

---

## 6. 字段映射与 Transformer

### 6.1 字段映射规则

Kafka Writer 的 `columns[]` 决定了 Kafka 消息中的字段顺序与名称：

```json
"columns": [
  { "index": 0, "name": "id" },
  { "index": 1, "name": "user_name" }
]
```

- `index`：对应 Reader 输出的第几列（从 0 开始）
- `name`：Kafka 消息中的字段名（`writeType=json` 时生效）

### 6.2 Transformer

在 reader 与 writer 之间插入一个或多个 Transformer，可做：

- 字段重命名
- 类型转换
- 字符串替换 / 脱敏
- 过滤脏数据

```json
{
  "content": [{
    "reader": { ... },
    "transformer": [
      { "name": "filter",   "param": { "column": ["status"], "value": "1" } },
      { "name": "replace",  "param": { "column": ["name"], "replaceRegex": "\\d+", "replaceValue": "*" } }
    ],
    "writer": { ... }
  }]
}
```

常用 Transformer（继承自 DataX 风格）：`filter` / `replace` / `substr` / `dateformat` / `map` / `pad`。

---

## 7. 安全认证（PLAINTEXT / SASL / Kerberos）

### 7.1 认证矩阵

| Kafka 协议 | Reader 侧 | Writer 侧配置 |
|---|---|---|
| PLAINTEXT | 不限 | `bootstrapServers:host:9092` |
| SASL_PLAINTEXT (PLAIN) | 不限 | `username` / `password` |
| SASL_PLAINTEXT (GSSAPI / Kerberos) | Hive/HDFS 配 `kerberos:1` | `kerberos:1` + `principal` + `keytab` + `krb5Conf` + `kerberosDomain` |
| SASL_SSL | — | `otherProperties.security.protocol=SASL_SSL` + `sasl.jaas.config` + `ssl.truststore.*` |
| SSL | — | `otherProperties.security.protocol=SSL` + `ssl.truststore.*` |

### 7.2 Kerberos 配置示例

```json
{
  "kerberos": 1,
  "principal": "etl@HADOOP.COM",
  "kerberosKeytabFilePath": "/etc/security/keytabs/etl.keytab",
  "krb5Conf": "/etc/krb5.conf",
  "kerberosDomain": "hadoop.com"
}
```

### 7.3 密钥管理约定

**禁止**：

- 把 keytab / `krb5.conf` / `jaas.conf` commit 到 Git 仓库
- 在 commit message 里出现密码、token、内部 IP

**推荐**：

- 通过 **Nacos Config** 分发敏感配置（见 `AGENTS.md §3.7`）
- 通过 **环境变量** 注入 keytab 路径
- 定期轮换 JWT signing key、keytab

### 7.4 部署机预校验

```bash
# 测试 Kerberos 票据获取
kinit -kt /etc/security/keytabs/etl.keytab etl@HADOOP.COM
klist

# 测试 Kafka 连通性
kafka-console-producer --broker-list broker:9092 --topic test \
  --producer-property security.protocol=SASL_PLAINTEXT \
  --producer-property sasl.kerberos.service.name=kafka
```

---

## 8. Studio 配置流程（含路径）

### 8.1 注册文件 / 数据库数据源

**路径**：`数据源管理 → 数据源列表 → 新建数据源`

| 数据源类型 | 选填位置 | 必填字段 |
|---|---|---|
| FTP | 协议 `ftp` | host / port / username / password / ftpTLS / connectMode |
| SFTP | 协议 `sftp` | host / port / username / password |
| MySQL | 协议 `mysql8` / `mysql5` | host / port / database / userName / password |
| Oracle | 协议 `oracle` | host / port / database / userName / password |
| PostgreSQL | 协议 `postgresql` | host / port / database / userName / password |
| DM | 协议 `dm` | host / port / database / userName / password |
| Hive2 | 协议 `tbds-hive2` | host / port / database / userName |
| MinIO | 协议 `minio` | endpoint / accessKey / secretKey / bucket / storageProvider |
| Kafka | **不需注册** | 凭据直接写在作业中 |

### 8.2 创建采集作业

**路径**：`数据采集任务 → 任务列表 → 新建任务`

填写字段：

- **任务名称**：自定义
- **Reader 类型**：从下拉选 `ftpreader` / `sftpreader` / `mysql8reader` / ...
- **Reader 配置**：来自附录 1~10 的 `reader` 段
- **Writer 类型**：选 `kafkawriter`
- **Writer 配置**：来自附录 1~10 的 `writer` 段

### 8.3 字段映射

**路径**：`任务详情 → 字段映射 Tab`

把 Reader 列拖拽到 Writer 列，或直接编辑 `index` / `name` 的对应关系。

### 8.4 调度（可选）

**路径**：`工作流编排 → 工作流列表 → 新建工作流`

- 触发器类型选 `Cron`
- Cron 表达式举例：
  - 每 5 分钟：`0 */5 * * * ?`
  - 每天凌晨 1 点：`0 0 1 * * ?`
  - 工作日 8 点：`0 0 8 * * MON-FRI`
- 失败重试：`3` 次，间隔 `30s`

### 8.5 监控运维

**路径**：`运维中心 → 任务运行历史`

可看：

- channel 数、字节流量
- 错误堆栈
- 增量水位（`pkValue`）
- 运行时长

---

## 9. 本地 Java 验证

开发期可以绕过 Studio，直接用 `JobContainer` 跑：

```java
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.JobContainer;

import java.io.File;

public class KafkaJobMain {
    public static void main(String[] args) {
        // 指向 package_all/aggregation 目录
        System.setProperty("aggregation.home",
            "/path/to/DataAggregation/package_all/aggregation");

        Configuration conf = Configuration.from(new File("job/mysql_to_kafka.json"));
        new JobContainer().start(conf);
    }
}
```

**编译运行**：

```bash
cd /home/admin/AiCoding/DataAggregation
mvn -DskipTests install        # 编译顶层 7 模块
mvn -DskipTests package        # 聚合插件到 package_all/aggregation

# 运行
java -cp target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout) \
     -Daggregation.home=/path/to/package_all/aggregation \
     KafkaJobMain
```

---

## 10. 性能调优

| 项 | 调整建议 |
|---|---|
| `speed.channel` | 并发数，建议 `1` ~ `CPU 核心数 / 2` |
| `speed.byte` | 单 channel 字节限流，默认 `10MB` |
| Kafka `batchSize` | `16384` → 调大到 `65536` 提升吞吐 |
| Kafka `linger.ms` | 在 writer `otherProperties` 里覆盖，默认 `1ms` |
| 文件 Reader 大文件 | `channel=1` + `byte=10MB`，分多次调度 |
| 数据库 Reader 大表 | 配 `incrColumn` 分批；`channel=1~4` |
| Kafka `compression.type` | `otherProperties.compression.type=snappy` 或 `lz4` 减网络 |
| MySQL `userPool` | 配 `true` 复用连接，提升吞吐 |

---

## 11. 常见错误与排障

| 现象 | 排查 |
|---|---|
| `KafkaWriterErrorCode.REQUIRED_VALUE` | `bootstrapServers` 或 `topic` 没填 |
| `CREATE_TOPIC` 失败 | 检查 Kafka 集群 broker 可用性、`autoCreateTopic` 权限 |
| `Kerberos login failed` | keytab 路径 / principal / krb5.conf / realm 校验；运行机 `kinit` 测试 |
| 增量不推进 | 检查 `JobPointReporter` 持久化（看运行日志 `pkValue`） |
| 文件读不到 | `partition` glob 没匹配 / `rootPath` 权限 |
| 数据库连接超时 | `connect.other` 加 `?connectTimeout=5000&socketTimeout=60000` |
| Writer 写入乱码 | `writeType=split` 时 `fieldDelimiter` 与下游约定一致；`json` 一般无问题 |
| Kafka topic 被自动建错分区数 | `autoCreateTopic=false` 关闭，预先建好 topic |
| FTP 主动模式被防火墙挡 | 改 `connectMode=PORT` 或 `PASV` |
| MySQL 时区错乱 | `connect.other` 加 `?serverTimezone=Asia/Shanghai` |
| Oracle 表空间 / 字符集问题 | `selectSql` 用 `to_char()` / `convert()` 显式处理 |

---

## 12. 附录：10 份完整 JSON 模板

### 附录 1 - FTP → Kafka（Kerberos）

```json
{
  "job": {
    "setting": { "speed": { "channel": 1, "byte": 10485760 } },
    "content": [
      {
        "reader": {
          "name": "ftpreader",
          "config": {
            "connect": {
              "host": "192.168.1.10",
              "port": 21,
              "username": "ftpuser",
              "password": "***",
              "ftpTLS": "none",
              "connectMode": "PASV",
              "timeout": 60000
            },
            "rootPath": "/data/orders",
            "partitionType": "glob",
            "partition": "*.csv",
            "fileType": "csv",
            "encoding": "UTF-8",
            "hasHeader": true,
            "delimiter": ",",
            "columns": [
              { "name": "id",     "index": 0, "type": "LONG" },
              { "name": "name",   "index": 1, "type": "STRING" },
              { "name": "amount", "index": 2, "type": "DOUBLE" }
            ]
          }
        },
        "writer": {
          "name": "kafkawriter",
          "config": {
            "bootstrapServers": "broker1:9092,broker2:9092",
            "topic": "orders_topic",
            "writeType": "json",
            "autoCreateTopic": true,
            "createTopicNumPartition": 3,
            "createTopicReplicationFactor": 2,
            "ack": "all",
            "retries": 3,
            "batchSize": 16384,
            "kerberos": 1,
            "principal": "etl@HADOOP.COM",
            "kerberosKeytabFilePath": "/etc/security/keytabs/etl.keytab",
            "krb5Conf": "/etc/krb5.conf",
            "kerberosDomain": "hadoop.com",
            "columns": [
              { "index": 0, "name": "id" },
              { "index": 1, "name": "name" },
              { "index": 2, "name": "amount" }
            ]
          }
        }
      }
    ]
  }
}
```

### 附录 2 - SFTP → Kafka

```json
{
  "job": {
    "setting": { "speed": { "channel": 1, "byte": 10485760 } },
    "content": [
      {
        "reader": {
          "name": "sftpreader",
          "config": {
            "connect": {
              "host": "192.168.1.20",
              "port": 22,
              "username": "sftpuser",
              "password": "***",
              "timeout": 60000
            },
            "rootPath": "/data/orders",
            "partitionType": "glob",
            "partition": "*.csv",
            "fileType": "csv",
            "encoding": "UTF-8",
            "hasHeader": true,
            "delimiter": ",",
            "columns": [
              { "name": "id",     "index": 0, "type": "LONG" },
              { "name": "name",   "index": 1, "type": "STRING" },
              { "name": "amount", "index": 2, "type": "DOUBLE" }
            ]
          }
        },
        "writer": {
          "name": "kafkawriter",
          "config": {
            "bootstrapServers": "broker1:9092,broker2:9092",
            "topic": "orders_topic",
            "writeType": "json",
            "autoCreateTopic": true,
            "ack": "all",
            "retries": 3,
            "kerberos": 1,
            "principal": "etl@HADOOP.COM",
            "kerberosKeytabFilePath": "/etc/security/keytabs/etl.keytab",
            "krb5Conf": "/etc/krb5.conf",
            "kerberosDomain": "hadoop.com",
            "columns": [
              { "index": 0, "name": "id" },
              { "index": 1, "name": "name" },
              { "index": 2, "name": "amount" }
            ]
          }
        }
      }
    ]
  }
}
```

### 附录 3 - HDFS → Kafka

```json
{
  "job": {
    "setting": { "speed": { "channel": 2, "byte": 20971520 } },
    "content": [
      {
        "reader": {
          "name": "tbds-hdfs2reader",
          "config": {
            "connect": {
              "defaultFS": "hdfs://nameservice1",
              "hadoopUser": "hdfs"
            },
            "rootPath": "/data/warehouse/orders",
            "partitionType": "glob",
            "partition": "*.csv",
            "fileType": "csv",
            "encoding": "UTF-8",
            "hasHeader": true,
            "delimiter": ",",
            "columns": [
              { "name": "id",     "index": 0, "type": "LONG" },
              { "name": "name",   "index": 1, "type": "STRING" },
              { "name": "amount", "index": 2, "type": "DOUBLE" }
            ]
          }
        },
        "writer": {
          "name": "kafkawriter",
          "config": {
            "bootstrapServers": "broker1:9092,broker2:9092",
            "topic": "hdfs_orders_topic",
            "writeType": "json",
            "autoCreateTopic": true,
            "ack": "all",
            "retries": 3,
            "columns": [
              { "index": 0, "name": "id" },
              { "index": 1, "name": "name" },
              { "index": 2, "name": "amount" }
            ]
          }
        }
      }
    ]
  }
}
```

### 附录 4 - MinIO → Kafka

```json
{
  "job": {
    "setting": { "speed": { "channel": 1, "byte": 10485760 } },
    "content": [
      {
        "reader": {
          "name": "minioreader",
          "config": {
            "connect": {
              "storageProvider": "minio",
              "endpoint": "http://minio:9000",
              "accessKey": "minioadmin",
              "secretKey": "***",
              "bucket": "orders-bucket"
            },
            "rootPath": "data/orders",
            "partitionType": "glob",
            "partition": "*.csv",
            "fileType": "csv",
            "encoding": "UTF-8",
            "hasHeader": true,
            "delimiter": ",",
            "columns": [
              { "name": "id",     "index": 0, "type": "STRING" },
              { "name": "name",   "index": 1, "type": "STRING" },
              { "name": "amount", "index": 2, "type": "STRING" }
            ]
          }
        },
        "writer": {
          "name": "kafkawriter",
          "config": {
            "bootstrapServers": "broker1:9092",
            "topic": "minio_orders_topic",
            "writeType": "json",
            "autoCreateTopic": true,
            "ack": "all",
            "columns": [
              { "index": 0, "name": "id" },
              { "index": 1, "name": "name" },
              { "index": 2, "name": "amount" }
            ]
          }
        }
      }
    ]
  }
}
```

### 附录 5 - HTTP REST → Kafka

```json
{
  "job": {
    "setting": { "speed": { "channel": 1, "byte": 5242880 } },
    "content": [
      {
        "reader": {
          "name": "httpreader",
          "config": {
            "url": "https://api.example.com/orders",
            "method": "GET",
            "headers": {
              "Authorization": "Bearer ***"
            },
            "responseFormat": "json",
            "columns": [
              { "name": "id",     "jsonPath": "$.data[*].id",     "type": "LONG" },
              { "name": "name",   "jsonPath": "$.data[*].name",   "type": "STRING" },
              { "name": "amount", "jsonPath": "$.data[*].amount", "type": "DOUBLE" }
            ]
          }
        },
        "writer": {
          "name": "kafkawriter",
          "config": {
            "bootstrapServers": "broker1:9092",
            "topic": "api_orders_topic",
            "writeType": "json",
            "autoCreateTopic": true,
            "ack": "all",
            "columns": [
              { "index": 0, "name": "id" },
              { "index": 1, "name": "name" },
              { "index": 2, "name": "amount" }
            ]
          }
        }
      }
    ]
  }
}
```

### 附录 6 - MySQL 增量 → Kafka

```json
{
  "job": {
    "setting": { "speed": { "channel": 2, "byte": 10485760 } },
    "content": [
      {
        "reader": {
          "name": "mysql8reader",
          "config": {
            "connect": {
              "host": "rmHost",
              "port": "3306",
              "database": "shop",
              "userName": "etl",
              "password": "***",
              "other": "?useSSL=false&serverTimezone=Asia/Shanghai",
              "userPool": true,
              "extraParams": {}
            },
            "table": "orders",
            "columns": ["id", "user_id", "amount", "created_at"],
            "incrColumn": "id",
            "incrModel": ">",
            "pkValue": 0
          }
        },
        "writer": {
          "name": "kafkawriter",
          "config": {
            "bootstrapServers": "broker1:9092,broker2:9092",
            "topic": "mysql_orders_topic",
            "writeType": "json",
            "autoCreateTopic": true,
            "ack": "all",
            "retries": 3,
            "kerberos": 1,
            "principal": "etl@HADOOP.COM",
            "kerberosKeytabFilePath": "/etc/security/keytabs/etl.keytab",
            "krb5Conf": "/etc/krb5.conf",
            "kerberosDomain": "hadoop.com",
            "columns": [
              { "index": 0, "name": "id" },
              { "index": 1, "name": "user_id" },
              { "index": 2, "name": "amount" },
              { "index": 3, "name": "created_at" }
            ]
          }
        }
      }
    ]
  }
}
```

### 附录 7 - Oracle → Kafka

```json
{
  "job": {
    "setting": { "speed": { "channel": 1, "byte": 10485760 } },
    "content": [
      {
        "reader": {
          "name": "oraclereader",
          "config": {
            "connect": {
              "host": "oracle-host",
              "port": "1521",
              "database": "ORCL",
              "userName": "etl",
              "password": "***"
            },
            "selectSql": "SELECT id, name, amount, status FROM orders WHERE status = 1"
          }
        },
        "writer": {
          "name": "kafkawriter",
          "config": {
            "bootstrapServers": "broker1:9092",
            "topic": "oracle_orders_topic",
            "writeType": "json",
            "autoCreateTopic": true,
            "ack": "all",
            "columns": [
              { "index": 0, "name": "id" },
              { "index": 1, "name": "name" },
              { "index": 2, "name": "amount" },
              { "index": 3, "name": "status" }
            ]
          }
        }
      }
    ]
  }
}
```

### 附录 8 - PostgreSQL → Kafka

```json
{
  "job": {
    "setting": { "speed": { "channel": 1, "byte": 10485760 } },
    "content": [
      {
        "reader": {
          "name": "postgresqlreader",
          "config": {
            "connect": {
              "host": "pg-host",
              "port": "5432",
              "database": "shop",
              "userName": "etl",
              "password": "***"
            },
            "table": "orders",
            "columns": ["id", "user_id", "amount"],
            "incrColumn": "id",
            "incrModel": ">"
          }
        },
        "writer": {
          "name": "kafkawriter",
          "config": {
            "bootstrapServers": "broker1:9092",
            "topic": "pg_orders_topic",
            "writeType": "json",
            "autoCreateTopic": true,
            "ack": "all",
            "columns": [
              { "index": 0, "name": "id" },
              { "index": 1, "name": "user_id" },
              { "index": 2, "name": "amount" }
            ]
          }
        }
      }
    ]
  }
}
```

### 附录 9 - DM（达梦）→ Kafka

```json
{
  "job": {
    "setting": { "speed": { "channel": 1, "byte": 10485760 } },
    "content": [
      {
        "reader": {
          "name": "dmreader",
          "config": {
            "connect": {
              "host": "dm-host",
              "port": "5236",
              "database": "SHOP",
              "userName": "etl",
              "password": "***"
            },
            "table": "ORDERS",
            "columns": ["ID", "AMOUNT"],
            "incrColumn": "ID",
            "incrModel": ">"
          }
        },
        "writer": {
          "name": "kafkawriter",
          "config": {
            "bootstrapServers": "broker1:9092",
            "topic": "dm_orders_topic",
            "writeType": "json",
            "autoCreateTopic": true,
            "ack": "all",
            "columns": [
              { "index": 0, "name": "id" },
              { "index": 1, "name": "amount" }
            ]
          }
        }
      }
    ]
  }
}
```

### 附录 10 - Hive2 → Kafka

```json
{
  "job": {
    "setting": { "speed": { "channel": 2, "byte": 20971520 } },
    "content": [
      {
        "reader": {
          "name": "tbds-hive2reader",
          "config": {
            "connect": {
              "host": "hive-host",
              "port": "10000",
              "database": "ods",
              "userName": "hive",
              "password": ""
            },
            "table": "dwd_orders",
            "columns": ["id", "user_id", "amount", "dt"],
            "incrColumn": "id",
            "incrModel": ">"
          }
        },
        "writer": {
          "name": "kafkawriter",
          "config": {
            "bootstrapServers": "broker1:9092",
            "topic": "hive_orders_topic",
            "writeType": "json",
            "autoCreateTopic": true,
            "ack": "all",
            "retries": 3,
            "kerberos": 1,
            "principal": "hive@HADOOP.COM",
            "kerberosKeytabFilePath": "/etc/security/keytabs/hive.keytab",
            "krb5Conf": "/etc/krb5.conf",
            "kerberosDomain": "hadoop.com",
            "columns": [
              { "index": 0, "name": "id" },
              { "index": 1, "name": "user_id" },
              { "index": 2, "name": "amount" },
              { "index": 3, "name": "dt" }
            ]
          }
        }
      }
    ]
  }
}
```

---

## 附录 B - 引用源码索引

| 模块 | 路径 | 关键类 |
|---|---|---|
| Kafka Writer | `job-plugins/writer/kafkawriter/src/main/java/com/jdragon/aggregation/writer/kafka/` | `KafkaWriter.java` / `Key.java` / `ColumnEntry.java` / `WriteType.java` |
| Kerberos 工具 | `data-source-plugins/data-source-handler-queue-kafka/src/main/java/com/jdragon/aggregation/datasource/queue/kafka/` | `KafkaAuthUtil.java` / `KafkaAuthKey.java` |
| RDBMS Reader 共用 | `job-plugins/utils/plugin-rdbms-util/src/main/java/com/jdragon/aggregation/rdbms/reader/` | `CommonRdbmsReader.java` |
| RDBMS Key | `job-plugins/utils/plugin-rdbms-util/src/main/java/com/jdragon/aggregation/rdbms/util/` | `Key.java` |
| FTP Reader | `job-plugins/reader/ftpreader/src/main/java/com/jdragon/aggregation/reader/ftp/` | `FtpReader.java` |
| SFTP Reader | `job-plugins/reader/sftpreader/src/main/java/com/jdragon/aggregation/reader/sftp/` | `SftpReader.java` |
| HTTP Reader Key | `job-plugins/reader/httpreader/src/main/java/com/jdragon/aggregation/httpreader/` | `Key.java` |

---

*文档用途: DataAggregation 文件 + 数据库 → Kafka 采集操作手册*
*维护者: Mavis*
*最后更新: 2026-08-21*